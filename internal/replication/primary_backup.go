package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

// ============================================================================
// CORE REPLICATION ALGORITHM
// ============================================================================

// Replicate pushes data to all slave nodes in the cluster using concurrent replication
//
// Algorithm: Master-Directed Concurrent Replication
// Preconditions:
//   - Must be called on master node only (isMaster == true)
//   - Entry must have valid Filename, Data, Checksum, and Op fields
//
// Steps:
//  1. Verify caller is master (non-master returns error)
//  2. Collect list of peer nodes (excluding self)
//  3. If no peers exist (standalone), return success immediately
//  4. Spawn concurrent goroutines - one per peer
//  5. Each goroutine calls replicateToNode() to send HTTP POST request
//  6. Aggregate results via buffered channel
//  7. Return success if ≥1 slave acknowledged successful replication
//
// Concurrency: Each peer replication runs in parallel (sync.WaitGroup)
// Failures: Individual peer failures do not block other replications
// Timeout: HTTP client timeout is 5 seconds per request
//
// Parameters:
//   - entry: LogEntry containing file data, checksum, version info, and operation type
//
// Returns:
//   - success: true if at least one slave replicated successfully
//   - error: non-nil only if master check fails
//
// Example:
//
//	entry := &types.LogEntry{
//	    Filename: "data.txt",
//	    Data: []byte("content"),
//	    Checksum: "hash123",
//	    Op: "WRITE",
//	}
//	ok, err := rm.Replicate(entry)
//	if !ok { log.Fatal("replication failed") }
func (rm *ReplicationManager) Replicate(entry *types.LogEntry) (bool, error) {
	rm.mu.RLock()
	if !rm.isMaster {
		rm.mu.RUnlock()
		return false, fmt.Errorf("not a master")
	}

	// Snapshot current peer list (to avoid holding lock during network I/O)
	peers := make([]string, 0, len(rm.peers))
	for peerID := range rm.peers {
		if peerID != rm.nodeID {
			peers = append(peers, peerID)
		}
	}
	rm.mu.RUnlock()

	// Standalone node has nothing to replicate to
	if len(peers) == 0 {
		return true, nil
	}

	// Concurrent replication to all peers
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

	// Wait for all goroutines and close channel
	go func() {
		wg.Wait()
		close(replyCh)
	}()

	// Tally successful replications
	successCount := 0
	for success := range replyCh {
		if success {
			successCount++
		}
	}

	// Success threshold: at least one slave confirmed
	return successCount > 0, nil
}

// replicateToNode sends replication request to a single peer node via HTTP POST
//
// Algorithm: HTTP RPC Replication with Acknowledgment
// Protocol:
//  1. Acquire read lock to lookup peer address and current version
//  2. Increment version for this file (optimistic version management)
//  3. Build ReplicateRequest with version, checksum, data, operation
//  4. Serialize to JSON
//  5. POST to peer's /internal/replicate endpoint
//  6. Parse ReplicateResponse
//  7. If successful, update local replicationStatus and version tracking (with write lock)
//  8. Log all outcomes
//
// Failure Handling:
//   - Peer not found in peers map: log and return false
//   - HTTP connection error: log error, return false (peer may be down)
//   - Non-200 status code: log status, return false (peer rejected)
//   - JSON decode error: log error, return false (peer response malformed)
//   - Replication rejected (Success==false): log peer error message, return false
//
// Version Management:
//   - Each file version starts at 0
//   - First replication increments to version 1
//   - Subsequent replications increment sequentially
//   - Prevents stale writes at slave nodes via conflict detection
//
// Parameters:
//   - peerID: Unique identifier of target slave node
//   - entry: LogEntry to replicate
//
// Returns:
//   - true: Slave successfully applied replication
//   - false: Any step in the algorithm failed
//
// Side Effects on Success:
//   - Updates rm.replicationStatus[peerID] = true
//   - Updates rm.versions[filename] = newVersion
//   - Updates rm.checksums[filename] = checksum
func (rm *ReplicationManager) replicateToNode(peerID string, entry *types.LogEntry) bool {
	rm.mu.RLock()
	addr, exists := rm.peers[peerID]
	currentVersion := rm.versions[entry.Filename]
	rm.mu.RUnlock()

	if !exists {
		log.Printf("[%s] Peer %s not found in peer list\n", rm.nodeID, peerID)
		return false
	}

	// Build replication request with incremented version
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

	// Send HTTP POST request
	resp, err := rm.httpClient.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		log.Printf("[%s] HTTP POST to %s failed: %v\n", rm.nodeID, peerID, err)
		return false
	}
	defer resp.Body.Close()

	// Check HTTP status code
	if resp.StatusCode != 200 {
		log.Printf("[%s] Peer %s returned HTTP %d\n", rm.nodeID, peerID, resp.StatusCode)
		return false
	}

	// Decode peer's response
	var replicaResp types.ReplicateResponse
	if err := json.NewDecoder(resp.Body).Decode(&replicaResp); err != nil {
		log.Printf("[%s] Failed to decode response from %s: %v\n", rm.nodeID, peerID, err)
		return false
	}

	// Check if replication was successful at peer
	if !replicaResp.Success {
		log.Printf("[%s] Peer %s rejected replication: %s\n", rm.nodeID, peerID, replicaResp.Error)
		return false
	}

	// Update local state on successful replication
	rm.mu.Lock()
	rm.replicationStatus[peerID] = true
	rm.versions[entry.Filename] = req.Version
	rm.checksums[entry.Filename] = req.Checksum
	rm.mu.Unlock()

	log.Printf("[%s] ✓ Replicated %s (v%d) to %s\n", rm.nodeID, entry.Filename, req.Version, peerID)
	return true
}

// HandleReplicateRequest processes incoming replication from the master
//
// Algorithm: Slave-Side Replication Application with Conflict Resolution
//
// Entry Points:
//   - Master sending data: Operation is WRITE or DELETE
//   - Slave is receiver: isMaster == false
//
// Validations:
//  1. Reject if this node is master (masters don't accept replication)
//  2. For DELETE: Simply remove file from tracking (no version check needed)
//  3. For WRITE: Check version-based conflict
//
// Conflict Resolution (Version-Based):
// Algorithm: Last-Write-Wins with Stale-Write Rejection
//  1. Extract current version for file from local state
//  2. If incoming version ≤ current version: REJECT (stale write)
//  3. Otherwise: ACCEPT and update to new version
//  4. Rationale: Prevents overwriting newer data with older data
//  5. Assumes master always sends monotonically increasing versions
//
// State Updates on Success:
//   - versions[filename] = new version
//   - checksums[filename] = new checksum
//
// Parameters:
//   - req: ReplicateRequest from master
//
// Returns:
//   - ReplicateResponse with Success/Error fields
//
// Example Response Cases:
//   - Success=true, Checksum=hash: Data accepted and applied
//   - Success=false, Error="master cannot receive": Node is master
//   - Success=false, Error="stale write: ...": Version conflict detected
func (rm *ReplicationManager) HandleReplicateRequest(req *types.ReplicateRequest) *types.ReplicateResponse {
	resp := &types.ReplicateResponse{
		Filename: req.Filename,
		NodeID:   rm.nodeID,
		Success:  false,
	}

	// Only slaves accept replication
	if rm.isMaster {
		resp.Error = "master cannot receive replication"
		return resp
	}

	// Handle DELETE operations (no versioning needed)
	if req.Operation == "DELETE" {
		rm.mu.Lock()
		delete(rm.versions, req.Filename)
		delete(rm.checksums, req.Filename)
		rm.mu.Unlock()

		resp.Success = true
		log.Printf("[%s] ✓ Applied DELETE for %s\n", rm.nodeID, req.Filename)
		return resp
	}

	// Handle WRITE operations with version checking
	rm.mu.Lock()
	defer rm.mu.Unlock()

	currentVersion := rm.versions[req.Filename]

	// Conflict detection: reject stale writes
	if currentVersion > req.Version {
		resp.Error = fmt.Sprintf("stale write: local v%d > incoming v%d", currentVersion, req.Version)
		log.Printf("[%s] ✗ Stale write rejected for %s: %s\n", rm.nodeID, req.Filename, resp.Error)
		return resp
	}

	// Apply the replicated data
	rm.versions[req.Filename] = req.Version
	rm.checksums[req.Filename] = req.Checksum
	resp.Success = true
	resp.Checksum = req.Checksum

	log.Printf("[%s] ✓ Applied replication for %s (v%d)\n", rm.nodeID, req.Filename, req.Version)
	return resp
}
