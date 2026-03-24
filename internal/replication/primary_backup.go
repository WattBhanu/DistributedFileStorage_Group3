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
// Replicate sends a log entry to all follower nodes
//
// Algorithm: Leader-Initiated Concurrent Replication
// Protocol:
//  1. Check if this node is Raft leader (only leaders can replicate)
//  2. Snapshot current peer list (to avoid holding lock during network I/O)
//  3. For each peer, send replication request concurrently via HTTP
//  4. Wait for all responses
//  5. Return success if at least one follower confirmed
//
// Parameters:
//   - entry: LogEntry containing file data, checksum, version info, and operation type
//   - isLeader: true if this node is currently the Raft leader
//
// Returns:
//   - success: true if at least one follower replicated successfully
//   - error: non-nil only if not leader
//
// Example:
//
//	if !raftNode.IsLeader() {
//	    return fmt.Errorf("not leader")
//	}
//	entry := &types.LogEntry{
//	    Filename: "data.txt",
//	    Data: []byte("content"),
//	    Checksum: "hash123",
//	    Op: "WRITE",
//	}
//	ok, err := rm.Replicate(entry, true)
//	if !ok { log.Fatal("replication failed") }
func (rm *ReplicationManager) Replicate(entry *types.LogEntry, isLeader bool) (bool, error) {
	rm.mu.RLock()
	if !isLeader {
		rm.mu.RUnlock()
		return false, fmt.Errorf("not a leader")
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
//  1. Acquire read lock to lookup peer address
//  2. Build ReplicateRequest with checksum, data, operation
//  3. Serialize to JSON
//  4. POST to peer's /internal/replicate endpoint
//  5. Parse ReplicateResponse
//  6. If successful, update local replicationStatus
//  7. Log all outcomes
//
// Failure Handling:
//   - Peer not found in peers map: log and return false
//   - HTTP connection error: log error, return false (peer may be down)
//   - Non-200 status code: log status, return false (peer rejected)
//   - JSON decode error: log error, return false (peer response malformed)
//   - Replication rejected (Success==false): log peer error message, return false
//
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
//   - Updates rm.checksums[filename] = checksum
func (rm *ReplicationManager) replicateToNode(peerID string, entry *types.LogEntry) bool {
	rm.mu.RLock()
	addr, exists := rm.peers[peerID]
	rm.mu.RUnlock()

	if !exists {
		log.Printf("[%s] [REPLICATION] Peer %s not found in peer list\n", rm.nodeID, peerID)
		return false
	}

	// Build replication request
	req := &types.ReplicateRequest{
		Filename:  entry.Filename,
		Data:      entry.Data,
		Timestamp: entry.Timestamp,
		Checksum:  entry.Checksum,
		Version:   0,
		NodeID:    rm.nodeID,
		Operation: entry.Op,
		Op:        entry.Op,
	}

	reqBody, _ := json.Marshal(req)
	url := fmt.Sprintf("http://%s/internal/replicate", addr)

	// Send HTTP POST request
	log.Printf("[%s] [REPLICATION] Sending %s operation for %s to %s at %s", 
		rm.nodeID, req.Operation, entry.Filename, peerID, url)
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
	rm.checksums[entry.Filename] = req.Checksum
	rm.mu.Unlock()

	log.Printf("[%s] ✓ Replicated %s to %s\n", rm.nodeID, entry.Filename, peerID)
	return true
}

// HandleReplicateRequest processes incoming replication from the Raft leader
//
// Algorithm: Follower-Side Replication Application with Conflict Resolution
//
// Entry Points:
//   - Raft Leader sending data: Operation is WRITE or DELETE
//   - Follower is receiver: accepts replication from leader
//
// Validations:
//  1. For DELETE: Simply remove file from tracking (no version check needed)
//  2. For WRITE: Accepts replication without version conflict detection
//
// State Updates on Success:
//   - checksums[filename] = new checksum
//
// Parameters:
//   - req: ReplicateRequest from leader
//
// Returns:
//   - ReplicateResponse with Success/Error fields
//
// Example Response Cases:
//   - Success=true, Checksum=hash: Data accepted and applied
//   - Success=false, Error="stale write: ...": Version conflict detected
func (rm *ReplicationManager) HandleReplicateRequest(req *types.ReplicateRequest) *types.ReplicateResponse {
	resp := &types.ReplicateResponse{
		Filename: req.Filename,
		NodeID:   rm.nodeID,
		Success:  false,
	}

	// Handle DELETE operations
	if req.Operation == "DELETE" {
		rm.mu.Lock()
		delete(rm.checksums, req.Filename)
		rm.mu.Unlock()

		resp.Success = true
		log.Printf("[%s] ✓ Applied DELETE for %s\n", rm.nodeID, req.Filename)
		return resp
	}

	// Handle WRITE operations
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Direct apply on write since versions has been removed
	// Note: ReplicationManager doesn't have direct storage access,
	// so we rely on the API layer to handle storage after this returns success
	// The API handler will call h.Storage.Write() after receiving successful response
	
	// Apply the replicated data (update metadata)
	rm.checksums[req.Filename] = req.Checksum
	resp.Success = true
	resp.Checksum = req.Checksum

	log.Printf("[%s] ✓ Applied replication for %s\n", rm.nodeID, req.Filename)
	return resp
}
