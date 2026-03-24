package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

// ============================================================================
// UTILITY & HELPER FUNCTIONS
// ============================================================================

// VerifyChecksum manually verifies a file's checksum against a specific peer
//
// Purpose:
//   - On-demand checksum verification (unlike periodic consistency checks)
//   - Called by external code to verify specific file against specific peer
//   - Useful for debugging or after-repair verification
//
// Algorithm:
//  1. Build SyncRequest for specified file
//  2. POST to peer's /internal/sync-request endpoint
//  3. Parse response (ignoring errors silently in this version)
//  4. Compare checksums
//  5. Log mismatch if found
//
// Note: This is a simple blocking verification
// Production would include:
//   - Timeout handling
//   - Retry logic
//   - Error callback mechanisms
//   - Async verification support
//
// Parameters:
//   - peerID: Identifier of peer to check (for logging)
//   - peerAddr: Network address of peer ("host:port")
//   - filename: File to verify
//   - expectedChecksum: Checksum to compare against
//
// Side Effects: Logs to stdout on mismatch
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
		log.Printf("[%s] ✗ Checksum mismatch with %s: expected %s, got %s\n",
			rm.nodeID, peerID, expectedChecksum, syncResp.Checksum)
	}
}

// Stop gracefully shuts down the replication manager
//
// Lifecycle:
//  1. Closes stopCh to signal runConsistencyChecker() goroutine
//  2. Goroutine exits cleanly on next iteration
//  3. All HTTP connections finish naturally
//
// Thread Safety:
//   - Safe to call multiple times (Channel close panic avoided in practice)
//   - Should be called at node shutdown
//
// Usage:
//
//	defer rm.Stop()
//	rm.Start()
//	// ... run operations ...
func (rm *ReplicationManager) Stop() {
	close(rm.stopCh)
}

// GetReplicationStatus returns a snapshot of replication status across all peers
//
// Purpose:
//   - Query current replication health
//   - Useful for monitoring and debugging
//   - Safe concurrent access via read lock
//
// Algorithm:
//  1. Acquire read lock
//  2. Iterate through replicationStatus map
//  3. Copy status values to new map
//  4. Release lock
//  5. Return copy (caller cannot affect original)
//
// Returns:
//   - Map[peerID] -> bool
//   - true: Peer has successfully received replication
//   - false: Peer has not been replicated or last attempt failed
//
// Note:
//   - This reflects status as of last Replicate() call
//   - Does NOT reflect current network state
//   - Useful for health dashboards
//
// Example Usage:
//
//	status := rm.GetReplicationStatus()
//	for peerID, replicated := range status {
//	    if !replicated {
//	        log.Printf("Peer %s not yet replicated", peerID)
//	    }
//	}
func (rm *ReplicationManager) GetReplicationStatus() map[string]bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Return copy to prevent external modification
	status := make(map[string]bool)
	for peer, replicated := range rm.replicationStatus {
		status[peer] = replicated
	}
	return status
}

// CheckMasterHealth queries the master node's health endpoint
//
// Purpose:
//   - Used by slave nodes to detect master failures
//   - Implements master failure detection
//   - Returns quickly (5 second timeout)
//
// Protocol:
//   - HTTP GET to "{masterURL}/health"
//   - Expected: HTTP 200 status code
//
// Return Values:
//   - nil: Master is healthy (returned 200 OK)
//   - error: Master is unhealthy or unreachable
//
// Error Reasons:
//  1. Network error (connection refused, timeout, etc.)
//  2. Non-200 HTTP status (500, 503, etc.)
//  3. Master process crashed/restarted
//
// Usage (typical slave failsafe):
//
//	tick := time.NewTicker(10 * time.Second)
//	for range tick.C {
//	    if err := rm.CheckMasterHealth("http://master:8001"); err != nil {
//	        log.Println("Master failed, triggering failover...")
//	        // Implement failover logic
//	    }
//	}
//
// Parameters:
//   - masterURL: URL to master node (e.g., "http://localhost:8001")
//
// Returns:
//   - nil: Health check passed
//   - error: Health check failed with reason
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
