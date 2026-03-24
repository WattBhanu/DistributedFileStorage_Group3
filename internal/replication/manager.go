package replication

import (
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/storage"
)

// ============================================================================
// REPLICATION MANAGER CORE STRUCTURE & INITIALIZATION
// ============================================================================

// ReplicationManager orchestrates data replication across distributed Raft cluster
//
// Architecture:
// - Raft Leader: Accepts all write operations and replicates to followers
// - Raft Followers: Receive replicated data from leader and maintain synchronized copies
// - Consensus: Periodic consistency checks using checksums
// - Conflict Resolution: Version-based approach with stale-write detection
//
// Data Flow:
// Leader Side:
//  1. NewReplicationManager() - Initialize manager
//  2. Start() - Begin consistency checker goroutine
//  3. Replicate() - Send data to all followers concurrently
//  4. runConsistencyChecker() - Periodic verification (30s interval)
//  5. Stop() - Graceful shutdown
//
// Follower Side:
//  1. NewReplicationManager() - Initialize as follower
//  2. HandleReplicateRequest() - Accept replicated data from leader
//  3. Perform version checking and conflict resolution
//  4. HandleSyncRequest() - Respond to consistency checks
//
// State Management:
// - versions: Track current version for each file (prevents stale writes)
// - checksums: Maintain data integrity verification
// - replicationStatus: Track which peers have successfully replicated
type ReplicationManager struct {
	nodeID string
	peers  map[string]string // nodeID -> network address mapping

	// Replication and version state
	replicationStatus map[string]bool   // peer nodeID -> replication success flag
	checksums         map[string]string // filename -> data checksum for verification

	// Concurrency and lifecycle control
	mu         sync.RWMutex  // Guards all maps above
	stopCh     chan struct{} // Shutdown signal for goroutines
	httpClient *http.Client  // Client for HTTP RPC to peers
}

// NewReplicationManager creates and initializes a new ReplicationManager instance
//
// Parameters:
//   - nodeID: Unique identifier for this node in the cluster
//   - peers: Map of peer node IDs to their network addresses (format: "host:port")
//
// State Initialization:
//   - Empty replication status (no peers marked as replicated yet)
//   - Empty versions map (first write will create version 1)
//   - Empty checksums map (populated as data arrives)
//
// Returns: Fully initialized ReplicationManager ready to accept Start()
//
// Example:
//
//	peers := map[string]string{
//	    "node2": "localhost:8002",
//	    "node3": "localhost:8003",
//	}
//	rm := NewReplicationManager("node1", peers)
func NewReplicationManager(nodeID string, peers map[string]string) *ReplicationManager {
	return &ReplicationManager{
		nodeID:            nodeID,
		peers:             peers,
		replicationStatus: make(map[string]bool),
		checksums:         make(map[string]string),
		stopCh:            make(chan struct{}),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// InitializeFromStorage loads existing files from storage into replication metadata
// This ensures that pre-existing files are tracked for consistency checks
//
// Parameters:
//   - storageLayer: Storage interface to scan for existing files
//
// Behavior:
//   - Lists all files in storage
//   - Computes checksum for each file
//   - Initializes version to 1 for each file
//   - Logs summary of loaded files
//
// Example:
//
//	rm.InitializeFromStorage(storageLayer)
func (rm *ReplicationManager) InitializeFromStorage(storageLayer storage.Storage) {
	files, err := storageLayer.List()
	if err != nil {
		log.Printf("[%s] [REPLICATION] Failed to list existing files: %v\n", rm.nodeID, err)
		return
	}

	if len(files) == 0 {
		log.Printf("[%s] [REPLICATION] No existing files to load from storage\n", rm.nodeID)
		return
	}

	loadedCount := 0
	for _, file := range files {
		// Get checksum for existing file
		checksum, err := storageLayer.GetChecksum(file.Filename)
		if err != nil {
			log.Printf("[%s] [REPLICATION] Failed to get checksum for %s: %v\n", rm.nodeID, file.Filename, err)
			continue
		}

		// Initialize metadata
		rm.mu.Lock()
		rm.checksums[file.Filename] = checksum
		rm.replicationStatus[file.Filename] = true
		rm.mu.Unlock()

		loadedCount++
		log.Printf("[%s] [REPLICATION] ✓ Loaded existing file: %s (size: %d bytes, checksum: %s)\n", 
			rm.nodeID, file.Filename, file.Size, checksum)
	}

	log.Printf("[%s] [REPLICATION] Initialized %d existing file(s) from storage\n", rm.nodeID, loadedCount)
}

// GetPeers returns the map of peer nodes (nodeID -> address)
func (rm *ReplicationManager) GetPeers() map[string]string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	
	// Return a copy to prevent external modification
	peersCopy := make(map[string]string)
	for k, v := range rm.peers {
		peersCopy[k] = v
	}
	return peersCopy
}

// GetHTTPClient returns the HTTP client for making requests to peers
func (rm *ReplicationManager) GetHTTPClient() *http.Client {
	return rm.httpClient
}
