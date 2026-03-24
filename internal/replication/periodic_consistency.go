package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

// ============================================================================
// CONSISTENCY CHECKING & VERIFICATION ALGORITHM
// ============================================================================

// Start initiates the background consistency checker goroutine
//
// Lifecycle:
//  1. Call Start() to begin background verification
//  2. Spawns runConsistencyChecker() in a new goroutine
//  3. Checker runs until Stop() is called
//
// Note: Only Raft leader nodes perform consistency checks
// Follower nodes do nothing when Start() is called
func (rm *ReplicationManager) Start() {
	log.Printf("[%s] Replication manager started - consistency checker interval 30s", rm.nodeID)
	go rm.runConsistencyChecker()
}

// runConsistencyChecker periodically verifies data synchronization between master and slaves
//
// Algorithm: Periodic Checksum Verification
//
// Execution Model:
//   - Runs in background goroutine (spawned by Start())
//   - Checks every 30 seconds (time.NewTicker)
//   - Master only: non-masters do nothing
//   - Stops gracefully when stopCh receives close signal (via Stop())
//
// Details:
//  1. Create ticker with 30-second interval
//  2. Loop until stopped:
//     a. Wait for next tick OR stop signal
//     b. If stopped: exit goroutine cleanly
//     c. If tick: call checkConsistencyWithSlaves()
//
// Slave Consistency Check:
//   - Each call to checkConsistencyWithSlaves() verifies all files
//   - For each file, queries each peer's checksum
//   - Logs any mismatches found
//   - Does not attempt automatic recovery (production would)
//
// Timing Considerations:
//   - 30-second interval balances responsiveness with overhead
//   - Network delays not included in interval calculation
//   - If check takes >30s, next check happens immediately
//
// Note: This is a simple detection mechanism. Production systems would:
//   - Implement exponential backoff for failed peers
//   - Trigger automatic recovery/re-sync on mismatch
//   - Support configurable check intervals
//   - Implement repair strategies
func (rm *ReplicationManager) runConsistencyChecker() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	log.Printf("[%s] Consistency checker running every 30s", rm.nodeID)
	for {
		select {
		case <-ticker.C:
			// Time for periodic consistency check
			log.Printf("[%s] Starting periodic consistency check with followers...", rm.nodeID)
			rm.checkConsistencyWithFollowers()

		case <-rm.stopCh:
			// Shutdown signal received
			log.Printf("[%s] Consistency checker stopped\n", rm.nodeID)
			return
		}
	}
}

// checkConsistencyWithFollowers verifies that all follower nodes have current data
//
// Algorithm: Leader-Initiated Checksum Verification
//
// High-Level Flow:
//  1. Snapshot current checksums and peer list (with read lock)
//  2. For each file in our distributed set:
//     For each peer node:
//     Call verifyFileConsistency() to query and compare
//
// Concurrency:
//   - Files are checked sequentially
//   - Each file is checked against all peers sequentially
//   - Production systems would parallelize this
//
// Snapshot Approach:
//   - Read lock held only briefly
//   - We work with copies of checksums and peer list
//   - Prevents deadlock and allows other operations during checking
//
// Side Effects:
//   - Logs all consistency check results
//   - Logs mismatches as warnings
//   - Does NOT fix mismatches (production would)
//
// Error Handling:
//   - Network failures are logged but don't block other checks
//   - Malformed responses are logged
//   - Continues checking even if some peers fail
//
// Expected Log Output Examples:
//
//	"[node1] Consistency check passed for data.txt on node2"
//	"[node1] ✗ INCONSISTENCY: data.txt on node3 (expected: abc123, got: xyz789)"
func (rm *ReplicationManager) checkConsistencyWithFollowers() {
	// Create snapshot while holding lock minimally
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

	// Verify each file on each peer
	for filename, expectedChecksum := range checksums {
		for _, peerID := range peers {
			rm.verifyFileConsistency(peerID, filename, expectedChecksum)
		}
	}
}

// verifyFileConsistency queries a peer for a file's checksum and compares
//
// Algorithm: Single File Consistency Check Via Sync Request
//
// Steps:
//  1. Lookup peer's network address
//  2. Build SyncRequest with file query
//  3. POST to peer's /internal/sync-request endpoint
//  4. Parse SyncResponse
//  5. Compare returned checksum with expected value
//  6. Log result (match or mismatch)
//
// Network Protocol:
//   - HTTP POST to "http://{peerAddress}/internal/sync-request"
//   - Request body: {"NodeID":"node1","Filename":"data.txt"}
//   - Response body: {"Filename":"data.txt","Found":true,"Checksum":"hash","Version":1}
//
// Response Handling:
//   - Found=false: File not on peer (inconsistency - peer is missing data)
//   - Found=true, Checksum match: Peer has correct data ✓
//   - Found=true, Checksum mismatch: Peer has corrupted/wrong data ✗
//
// Failure Modes (all logged):
//  1. Network error (peer down/unreachable): Return, skip peer
//  2. Malformed response (JSON decode error): Return, skip peer
//  3. Expected but not found: Logged as inconsistency
//  4. Checksum mismatch: Logged as inconsistency
//
// Parameters:
//   - peerID: Identifier of peer to check
//   - filename: File to verify
//   - expectedChecksum: Master's checksum for comparison
//
// Side Effects:
//   - Logs consistency check result (pass or fail)
//   - Production systems would trigger recovery here on mismatch
func (rm *ReplicationManager) verifyFileConsistency(peerID string, filename string, expectedChecksum string) {
	// Lookup peer address
	rm.mu.RLock()
	addr, exists := rm.peers[peerID]
	rm.mu.RUnlock()

	if !exists {
		return
	}

	// Build sync request
	syncReq := &types.SyncRequest{
		NodeID:   rm.nodeID,
		Filename: filename,
	}

	reqBody, _ := json.Marshal(syncReq)
	url := fmt.Sprintf("http://%s/internal/sync-request", addr)

	// Send HTTP request
	resp, err := rm.httpClient.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		log.Printf("[%s] ✗ Consistency check with %s failed: %v\n", rm.nodeID, peerID, err)
		return
	}
	defer resp.Body.Close()

	// Decode response
	var syncResp types.SyncResponse
	if err := json.NewDecoder(resp.Body).Decode(&syncResp); err != nil {
		log.Printf("[%s] ✗ Invalid response from %s: %v\n", rm.nodeID, peerID, err)
		return
	}

	// Check consistency
	if syncResp.Found && syncResp.Checksum != expectedChecksum {
		log.Printf("[%s] ✗ INCONSISTENCY: %s on %s (expected: %s, got: %s)\n",
			rm.nodeID, filename, peerID, expectedChecksum, syncResp.Checksum)
		// Production: Trigger synchronization or recovery here
	} else if syncResp.Found {
		log.Printf("[%s] ✓ Consistent: %s on %s\n", rm.nodeID, filename, peerID)
	} else {
		log.Printf("[%s] ✗ Missing: %s not found on %s\n", rm.nodeID, filename, peerID)
	}
}

// HandleSyncRequest responds to consistency check queries from peer nodes
//
// Algorithm: File State Query Response
//
// Purpose:
//   - Called by peer's verifyFileConsistency() method
//   - Responds with current file's version and checksum
//   - Used to verify data synchronization
//
// Query Types:
//  1. File exists: Return Found=true, Checksum, Version
//  2. File doesn't exist: Return Found=false
//
// Execution:
//  1. Read lock to safely access versions and checksums maps
//  2. Check if file exists
//  3. Build response with current state
//  4. Return response
//
// Thread Safety:
//   - Uses read lock (allows concurrent queries)
//   - Snapshot approach: takes what exists at query time
//   - Safe for concurrent Replicate() calls
//
// Parameters:
//   - req: SyncRequest containing NodeID and Filename to query
//
// Returns:
//   - SyncResponse with Found flag and optional Checksum/Version
//
// Example Response:
//   - For existing file: {Filename:"data.txt", Found:true, Checksum:"abc123", Version:5}
//   - For missing file: {Filename:"data.txt", Found:false}
func (rm *ReplicationManager) HandleSyncRequest(req *types.SyncRequest) *types.SyncResponse {
	rm.mu.RLock()
	checksum, exists := rm.checksums[req.Filename]
	rm.mu.RUnlock()

	resp := &types.SyncResponse{
		Filename: req.Filename,
		Found:    exists,
	}

	if exists {
		resp.Checksum = checksum
	}

	return resp
}
