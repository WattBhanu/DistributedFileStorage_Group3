package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

// ReplicationManager handles data replication across nodes
// Strategy: Primary-Backup replication
// - Master (primary) accepts writes and replicates to slaves
// - Slaves (backups) receive replicated data
// - Periodic consistency checks via checksums
// - Version-based conflict resolution
type ReplicationManager struct {
	nodeID   string
	peers    map[string]string // nodeID -> address mapping
	isMaster bool

	// Replication state
	replicationStatus map[string]bool   // node -> replicated status
	versions          map[string]int64  // filename -> version
	checksums         map[string]string // filename -> checksum

	mu         sync.RWMutex
	stopCh     chan struct{}
	httpClient *http.Client
}

// NewReplicationManager creates a new replication manager for master-slave replication
func NewReplicationManager(nodeID string, peers map[string]string, isMaster bool) *ReplicationManager {
	return &ReplicationManager{
		nodeID:            nodeID,
		peers:             peers,
		isMaster:          isMaster,
		replicationStatus: make(map[string]bool),
		versions:          make(map[string]int64),
		checksums:         make(map[string]string),
		stopCh:            make(chan struct{}),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// Replicate replicates data to all slave nodes (master only)
// Returns true if successfully replicated to at least one slave
func (rm *ReplicationManager) Replicate(entry *types.LogEntry) (bool, error) {
	rm.mu.RLock()
	if !rm.isMaster {
		rm.mu.RUnlock()
		return false, fmt.Errorf("not a master")
	}

	peers := make([]string, 0, len(rm.peers))
	for peerID := range rm.peers {
		if peerID != rm.nodeID {
			peers = append(peers, peerID)
		}
	}
	rm.mu.RUnlock()

	if len(peers) == 0 {
		// No peers to replicate to - standalone or single node
		return true, nil
	}

	// Send replication requests to all slaves concurrently
	replyCh := make(chan bool, len(peers))
	wg := sync.WaitGroup{}

	for _, peer := range peers {
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()
			success := rm.replicateToNode(peerID, entry)
			replyCh <- success
		}(peer)
	}

	// Wait for all peers to respond
	go func() {
		wg.Wait()
		close(replyCh)
	}()

	// Count successful replications
	successCount := 0
	for success := range replyCh {
		if success {
			successCount++
		}
	}

	// Success if at least one slave received it
	// In production, you may want stricter requirements
	return successCount > 0 || len(peers) == 0, nil
}

// replicateToNode sends replication request to a specific slave node
func (rm *ReplicationManager) replicateToNode(peerID string, entry *types.LogEntry) bool {
	rm.mu.RLock()
	addr, exists := rm.peers[peerID]
	// Get current version for this file
	currentVersion := rm.versions[entry.Filename]
	rm.mu.RUnlock()

	if !exists {
		return false
	}

	req := &types.ReplicateRequest{
		Filename:  entry.Filename,
		Data:      entry.Data,
		Timestamp: entry.Timestamp,
		Checksum:  entry.Checksum,
		Version:   currentVersion + 1,
		NodeID:    rm.nodeID,
		Operation: entry.Op,
		Op:        entry.Op,
	}

	reqBody, _ := json.Marshal(req)
	url := fmt.Sprintf("http://%s/internal/replicate", addr)

	resp, err := rm.httpClient.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		log.Printf("[%s] Replication to %s failed: %v\n", rm.nodeID, peerID, err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[%s] Replication to %s returned status %d\n", rm.nodeID, peerID, resp.StatusCode)
		return false
	}

	var replicaResp types.ReplicateResponse
	if err := json.NewDecoder(resp.Body).Decode(&replicaResp); err != nil {
		log.Printf("[%s] Failed to decode replication response from %s: %v\n", rm.nodeID, peerID, err)
		return false
	}

	if replicaResp.Success {
		rm.mu.Lock()
		rm.replicationStatus[peerID] = true
		// Also update version tracking
		rm.versions[entry.Filename] = req.Version
		rm.checksums[entry.Filename] = req.Checksum
		rm.mu.Unlock()
		log.Printf("[%s] Successfully replicated %s to %s\n", rm.nodeID, req.Filename, peerID)
		return true
	}

	log.Printf("[%s] Slave %s rejected replication: %s\n", rm.nodeID, peerID, replicaResp.Error)
	return false
}

// HandleReplicateRequest handles incoming replication request from master
func (rm *ReplicationManager) HandleReplicateRequest(req *types.ReplicateRequest) *types.ReplicateResponse {
	resp := &types.ReplicateResponse{
		Filename: req.Filename,
		NodeID:   rm.nodeID,
		Success:  false,
	}

	if rm.isMaster {
		resp.Error = "master cannot receive replication"
		return resp
	}

	// For DELETE operations
	if req.Operation == "DELETE" {
		// Just mark as applied in our tracking
		rm.mu.Lock()
		delete(rm.versions, req.Filename)
		delete(rm.checksums, req.Filename)
		rm.mu.Unlock()

		resp.Success = true
		log.Printf("[%s] Applied DELETE for %s from master\n", rm.nodeID, req.Filename)
		return resp
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Check version and apply conflict resolution
	currentVersion := rm.versions[req.Filename]

	if currentVersion > req.Version {
		// Stale write - conflict detected
		resp.Error = fmt.Sprintf("stale write: current version %d, request version %d", currentVersion, req.Version)
		log.Printf("[%s] Conflict detected for %s: %s\n", rm.nodeID, req.Filename, resp.Error)
		return resp
	}

	// Apply the replicated data
	rm.versions[req.Filename] = req.Version
	rm.checksums[req.Filename] = req.Checksum

	resp.Success = true
	resp.Checksum = req.Checksum

	log.Printf("[%s] Applied replication for %s from master (version %d)\n", rm.nodeID, req.Filename, req.Version)

	return resp
}

// Start starts the replication manager
func (rm *ReplicationManager) Start() {
	go rm.runConsistencyChecker()
}

// runConsistencyChecker periodically checks consistency with other nodes
func (rm *ReplicationManager) runConsistencyChecker() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if rm.isMaster {
				rm.checkConsistencyWithSlaves()
			}
		case <-rm.stopCh:
			return
		}
	}
}

// checkConsistencyWithSlaves verifies checksums with all slave nodes
func (rm *ReplicationManager) checkConsistencyWithSlaves() {
	rm.mu.RLock()
	checksums := make(map[string]string)
	for filename, checksum := range rm.checksums {
		checksums[filename] = checksum
	}
	peers := make([]string, 0, len(rm.peers))
	for peerID := range rm.peers {
		if peerID != rm.nodeID {
			peers = append(peers, peerID)
		}
	}
	rm.mu.RUnlock()

	for filename, expectedChecksum := range checksums {
		for _, peerID := range peers {
			rm.verifyFileFonsistency(peerID, filename, expectedChecksum)
		}
	}
}

// verifyFileFonsistency verifies file checksum with a peer
func (rm *ReplicationManager) verifyFileFonsistency(peerID string, filename string, expectedChecksum string) {
	rm.mu.RLock()
	addr, exists := rm.peers[peerID]
	rm.mu.RUnlock()

	if !exists {
		return
	}

	syncReq := &types.SyncRequest{
		NodeID:   rm.nodeID,
		Filename: filename,
	}

	reqBody, _ := json.Marshal(syncReq)
	url := fmt.Sprintf("http://%s/internal/sync-request", addr)

	resp, err := rm.httpClient.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		log.Printf("[%s] Consistency check with %s failed: %v\n", rm.nodeID, peerID, err)
		return
	}
	defer resp.Body.Close()

	var syncResp types.SyncResponse
	if err := json.NewDecoder(resp.Body).Decode(&syncResp); err != nil {
		log.Printf("[%s] Failed to decode sync response from %s: %v\n", rm.nodeID, peerID, err)
		return
	}

	if syncResp.Found && syncResp.Checksum != expectedChecksum {
		log.Printf("[%s] INCONSISTENCY DETECTED: %s has different checksum on %s (expected: %s, got: %s)\n",
			rm.nodeID, filename, peerID, expectedChecksum, syncResp.Checksum)
		// In production, would trigger recovery
	} else if syncResp.Found {
		log.Printf("[%s] Consistency check passed for %s on %s\n", rm.nodeID, filename, peerID)
	}
}

// HandleSyncRequest handles file sync/consistency check request
func (rm *ReplicationManager) HandleSyncRequest(req *types.SyncRequest) *types.SyncResponse {
	rm.mu.RLock()
	checksum, exists := rm.checksums[req.Filename]
	version, hasVersion := rm.versions[req.Filename]
	rm.mu.RUnlock()

	resp := &types.SyncResponse{
		Filename: req.Filename,
		Found:    exists,
	}

	if exists {
		resp.Checksum = checksum
		if hasVersion {
			resp.Version = version
		}
	}

	return resp
}

// VerifyChecksum verifies checksum with a specific node
func (rm *ReplicationManager) VerifyChecksum(peerID string, peerAddr string, filename string, expectedChecksum string) {
	syncReq := &types.SyncRequest{
		NodeID:   rm.nodeID,
		Filename: filename,
	}

	reqBody, _ := json.Marshal(syncReq)
	url := fmt.Sprintf("http://%s/internal/sync-request", peerAddr)

	resp, err := rm.httpClient.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		log.Printf("[%s] Checksum verification with %s failed: %v\n", rm.nodeID, peerID, err)
		return
	}
	defer resp.Body.Close()

	var syncResp types.SyncResponse
	if err := json.NewDecoder(resp.Body).Decode(&syncResp); err != nil {
		return
	}

	if syncResp.Found && syncResp.Checksum != expectedChecksum {
		log.Printf("[%s] INCONSISTENCY: %s has different checksum (expected: %s, got: %s)\n",
			rm.nodeID, peerID, expectedChecksum, syncResp.Checksum)
	}
}

// Stop stops the replication manager
func (rm *ReplicationManager) Stop() {
	close(rm.stopCh)
}

// GetReplicationStatus returns replication status for all peers
func (rm *ReplicationManager) GetReplicationStatus() map[string]bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	status := make(map[string]bool)
	for peer, replicated := range rm.replicationStatus {
		status[peer] = replicated
	}
	return status
}

// CheckMasterHealth checks if the master node is responding
// Used by slave nodes to detect master failures
func (rm *ReplicationManager) CheckMasterHealth(masterURL string) error {
	healthURL := fmt.Sprintf("%s/health", masterURL)

	resp, err := rm.httpClient.Get(healthURL)
	if err != nil {
		return fmt.Errorf("master health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("master returned status %d", resp.StatusCode)
	}

	return nil
}
