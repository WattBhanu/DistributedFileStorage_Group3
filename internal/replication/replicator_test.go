package replication

import (
	"testing"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

// ============================================================================
// INITIALIZATION & SETUP TESTS
// ============================================================================

// TestNewReplicationManager tests the creation of a new ReplicationManager
func TestNewReplicationManager(t *testing.T) {
	peers := map[string]string{
		"node1": "localhost:8001",
		"node2": "localhost:8002",
	}

	rm := NewReplicationManager("node1", peers, true)

	if rm.nodeID != "node1" {
		t.Errorf("Expected nodeID 'node1', got '%s'", rm.nodeID)
	}

	if !rm.isMaster {
		t.Errorf("Expected isMaster to be true")
	}

	if len(rm.peers) != 2 {
		t.Errorf("Expected 2 peers, got %d", len(rm.peers))
	}

	if rm.versions == nil || rm.checksums == nil || rm.replicationStatus == nil {
		t.Errorf("Maps should be initialized")
	}
}

// ============================================================================
// CORE REPLICATION ALGORITHM TESTS
// ============================================================================

// TestReplicateNonMaster tests that non-master nodes cannot replicate
func TestReplicateNonMaster(t *testing.T) {
	peers := map[string]string{
		"node1": "localhost:8001",
		"node2": "localhost:8002",
	}

	rm := NewReplicationManager("node2", peers, false) // Not a master

	entry := &types.LogEntry{
		Filename:  "test.txt",
		Data:      []byte("data"),
		Checksum:  "abc123",
		Timestamp: 0,
		Op:        "WRITE",
	}

	success, err := rm.Replicate(entry)

	if success {
		t.Errorf("Non-master should not replicate successfully")
	}

	if err == nil {
		t.Errorf("Expected error for non-master replication")
	}
}

// TestReplicateStandalone tests that master with no peers can replicate
func TestReplicateStandalone(t *testing.T) {
	rm := NewReplicationManager("node1", make(map[string]string), true)

	entry := &types.LogEntry{
		Filename:  "test.txt",
		Data:      []byte("data"),
		Checksum:  "abc123",
		Timestamp: 0,
		Op:        "WRITE",
	}

	success, err := rm.Replicate(entry)

	if !success {
		t.Errorf("Standalone master should replicate successfully")
	}

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestHandleReplicateRequestMaster tests that master cannot receive replication
func TestHandleReplicateRequestMaster(t *testing.T) {
	rm := NewReplicationManager("node1", make(map[string]string), true)

	req := &types.ReplicateRequest{
		Filename:  "test.txt",
		Data:      []byte("data"),
		Checksum:  "abc123",
		Version:   1,
		Operation: "WRITE",
	}

	resp := rm.HandleReplicateRequest(req)

	if resp.Success {
		t.Errorf("Master should not accept replication")
	}

	if resp.Error == "" {
		t.Errorf("Expected error message for master receiving replication")
	}
}

// TestHandleReplicateRequestSlave tests that slave accepts replication
func TestHandleReplicateRequestSlave(t *testing.T) {
	rm := NewReplicationManager("node2", make(map[string]string), false)

	req := &types.ReplicateRequest{
		Filename:  "test.txt",
		Data:      []byte("data"),
		Checksum:  "abc123",
		Version:   1,
		Operation: "WRITE",
	}

	resp := rm.HandleReplicateRequest(req)

	if !resp.Success {
		t.Errorf("Slave should accept replication: %s", resp.Error)
	}

	if resp.Checksum != "abc123" {
		t.Errorf("Expected checksum 'abc123', got '%s'", resp.Checksum)
	}

	// Verify version tracking
	rm.mu.RLock()
	version, exists := rm.versions["test.txt"]
	rm.mu.RUnlock()

	if !exists || version != 1 {
		t.Errorf("Version not tracked correctly")
	}
}

// TestHandleReplicateRequestDelete tests DELETE operation replication
func TestHandleReplicateRequestDelete(t *testing.T) {
	rm := NewReplicationManager("node2", make(map[string]string), false)

	// First add a file
	addReq := &types.ReplicateRequest{
		Filename:  "test.txt",
		Version:   1,
		Checksum:  "abc123",
		Operation: "WRITE",
	}
	addResp := rm.HandleReplicateRequest(addReq)
	if !addResp.Success {
		t.Errorf("Failed to add file")
	}

	// Now delete it
	deleteReq := &types.ReplicateRequest{
		Filename:  "test.txt",
		Operation: "DELETE",
	}
	deleteResp := rm.HandleReplicateRequest(deleteReq)

	if !deleteResp.Success {
		t.Errorf("Delete operation should succeed")
	}

	// Verify file is deleted from tracking
	rm.mu.RLock()
	_, exists := rm.versions["test.txt"]
	rm.mu.RUnlock()

	if exists {
		t.Errorf("File should be removed after DELETE operation")
	}
}

// TestConflictResolution tests version-based conflict resolution
func TestConflictResolution(t *testing.T) {
	rm := NewReplicationManager("node2", make(map[string]string), false)

	// Add initial file with version 1
	req1 := &types.ReplicateRequest{
		Filename:  "test.txt",
		Version:   1,
		Checksum:  "abc123",
		Operation: "WRITE",
	}
	resp1 := rm.HandleReplicateRequest(req1)
	if !resp1.Success {
		t.Errorf("Failed to add initial version")
	}

	// Try to add stale version (version 0)
	req2 := &types.ReplicateRequest{
		Filename:  "test.txt",
		Version:   0,
		Checksum:  "old123",
		Operation: "WRITE",
	}
	resp2 := rm.HandleReplicateRequest(req2)

	if resp2.Success {
		t.Errorf("Stale write should be rejected")
	}

	if resp2.Error == "" {
		t.Errorf("Expected error message for conflict")
	}

	// Verify current version is unchanged
	rm.mu.RLock()
	version := rm.versions["test.txt"]
	checksum := rm.checksums["test.txt"]
	rm.mu.RUnlock()

	if version != 1 || checksum != "abc123" {
		t.Errorf("Current version should remain unchanged")
	}
}

// ============================================================================
// CONSISTENCY CHECKING ALGORITHM TESTS
// ============================================================================

// TestHandleSyncRequest tests the sync request handling
func TestHandleSyncRequest(t *testing.T) {
	rm := NewReplicationManager("node1", make(map[string]string), true)

	// Add a file
	rm.mu.Lock()
	rm.versions["test.txt"] = 1
	rm.checksums["test.txt"] = "abc123"
	rm.mu.Unlock()

	// Query the file
	syncReq := &types.SyncRequest{
		Filename: "test.txt",
		NodeID:   "node2",
	}
	resp := rm.HandleSyncRequest(syncReq)

	if !resp.Found {
		t.Errorf("File should be found")
	}

	if resp.Checksum != "abc123" {
		t.Errorf("Expected checksum 'abc123', got '%s'", resp.Checksum)
	}

	if resp.Version != 1 {
		t.Errorf("Expected version 1, got %d", resp.Version)
	}
}

// TestHandleSyncRequestNotFound tests sync request for non-existent file
func TestHandleSyncRequestNotFound(t *testing.T) {
	rm := NewReplicationManager("node1", make(map[string]string), true)

	syncReq := &types.SyncRequest{
		Filename: "nonexistent.txt",
		NodeID:   "node2",
	}
	resp := rm.HandleSyncRequest(syncReq)

	if resp.Found {
		t.Errorf("File should not be found")
	}
}

// ============================================================================
// UTILITY FUNCTIONS TESTS
// ============================================================================

// TestGetReplicationStatus tests replication status retrieval
func TestGetReplicationStatus(t *testing.T) {
	rm := NewReplicationManager("node1", make(map[string]string), true)

	// Set some replication status
	rm.mu.Lock()
	rm.replicationStatus["node2"] = true
	rm.replicationStatus["node3"] = false
	rm.mu.Unlock()

	status := rm.GetReplicationStatus()

	if len(status) != 2 {
		t.Errorf("Expected 2 status entries, got %d", len(status))
	}

	if status["node2"] != true || status["node3"] != false {
		t.Errorf("Status values incorrect")
	}
}

// TestStartAndStop tests lifecycle management
func TestStartAndStop(t *testing.T) {
	rm := NewReplicationManager("node1", make(map[string]string), true)

	rm.Start()
	rm.Stop()

	// If no panic, test passes
	if rm.stopCh == nil {
		t.Errorf("Stop channel should not be nil")
	}
}
