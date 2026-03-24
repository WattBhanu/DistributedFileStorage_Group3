package replication

import (
	"net/http"
	"sync"
	"time"
)

// ============================================================================
// REPLICATION MANAGER CORE STRUCTURE & INITIALIZATION
// ============================================================================

// ReplicationManager orchestrates Primary-Backup data replication across distributed nodes
//
// Architecture:
// - Master (primary): Accepts all write operations and replicates to slaves
// - Slaves (backups): Receive replicated data and maintain synchronized copies
// - Consensus: Periodic consistency checks using checksums
// - Conflict Resolution: Version-based approach with stale-write detection
//
// Data Flow:
// Master Side:
//  1. NewReplicationManager() - Initialize manager
//  2. Start() - Begin consistency checker goroutine
//  3. Replicate() - Send data to all slaves concurrently
//  4. runConsistencyChecker() - Periodic verification (30s interval)
//  5. Stop() - Graceful shutdown
//
// Slave Side:
//  1. NewReplicationManager() - Initialize as slave
//  2. HandleReplicateRequest() - Accept replicated data from master
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

	isMaster bool // Role: true = master, false = slave

	// Replication and version state
	replicationStatus map[string]bool   // peer nodeID -> replication success flag
	versions          map[string]int64  // filename -> current version number
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
//   - isMaster: Role designation - true for master nodes, false for slave nodes
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
//	rm := NewReplicationManager("node1", peers, true)
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
