package node

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/api"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/network"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/replication"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/storage"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

// Node represents a single node in the distributed file storage system
// Simplified to focus on data replication and consistency
// Uses primary-backup replication model: first node is master, others are slaves
type Node struct {
	ID    string
	Addr  string
	Peers map[string]string // nodeID -> address
	Port  string

	// Core components
	storage    storage.Storage
	replicator *replication.ReplicationManager
	sender     *network.RequestSender
	handler    *api.Handler

	// Node role
	isMaster  bool
	masterID  string
	masterURL string

	// Coordination
	mu        sync.RWMutex
	stopCh    chan struct{}
	isRunning bool
}

// NewNode creates a new node with the given configuration
// Uses primary-backup replication model
func NewNode(
	nodeID string,
	addr string,
	port string,
	peers map[string]string,
	dataDir string,
) *Node {
	// Determine if this node is master (first node in peers or standalone)
	isMaster := len(peers) == 0 || nodeID == "node1" || nodeID == "node-master"
	var masterID, masterURL string

	if isMaster {
		masterID = nodeID
		masterURL = fmt.Sprintf("http://%s:%s", addr, port)
	} else {
		// For slaves, find master address from peers
		masterID = "node1" // Assume first node is master
		if masterAddr, exists := peers[masterID]; exists {
			masterURL = fmt.Sprintf("http://%s", masterAddr)
		}
	}

	// Initialize components
	storageLayer := storage.New(dataDir)
	replicator := replication.NewReplicationManager(nodeID, peers, isMaster)
	sender := network.NewRequestSender(addr)

	node := &Node{
		ID:         nodeID,
		Addr:       addr,
		Port:       port,
		Peers:      peers,
		storage:    storageLayer,
		replicator: replicator,
		sender:     sender,
		isMaster:   isMaster,
		masterID:   masterID,
		masterURL:  masterURL,
		stopCh:     make(chan struct{}),
	}

	// Create handler
	node.handler = api.NewHandler(nodeID, node.isMaster, replicator, storageLayer)

	return node
}

// Start starts the node and all its components
func (n *Node) Start() error {
	n.mu.Lock()
	if n.isRunning {
		n.mu.Unlock()
		return fmt.Errorf("node already running")
	}
	n.isRunning = true
	n.mu.Unlock()

	log.Printf("[%s] Starting node...\n", n.ID)

	// Start replication manager
	n.replicator.Start()
	log.Printf("[%s] Replication manager started\n", n.ID)

	// Start consistency checker
	go n.runConsistencyChecker()

	// Start master health monitoring (for slaves to detect master failures)
	if !n.isMaster {
		go n.runMasterHealthMonitor()
	}

	// Start API server
	go func() {
		err := api.Start(n.Port, n.handler)
		if err != nil {
			log.Printf("[%s] API server error: %v\n", n.ID, err)
		}
	}()

	roleStr := "MASTER"
	if !n.isMaster {
		roleStr = "SLAVE"
	}
	log.Printf("[%s] API server listening on port %s (Role: %s)\n", n.ID, n.Port, roleStr)
	log.Printf("[%s] Node started successfully\n", n.ID)
	return nil
}

// Stop stops the node and all its components
func (n *Node) Stop() error {
	n.mu.Lock()
	if !n.isRunning {
		n.mu.Unlock()
		return fmt.Errorf("node not running")
	}
	n.isRunning = false
	n.mu.Unlock()

	log.Printf("[%s] Stopping node...\n", n.ID)

	// Stop all components
	n.replicator.Stop()
	n.sender.Close()

	close(n.stopCh)

	log.Printf("[%s] Node stopped\n", n.ID)
	return nil
}

// runConsistencyChecker periodically checks data consistency across nodes
// Verifies that all replicas have the same data via checksums
func (n *Node) runConsistencyChecker() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if n.isMaster {
				n.checkConsistency()
			}
		case <-n.stopCh:
			return
		}
	}
}

// checkConsistency verifies data consistency across all nodes
func (n *Node) checkConsistency() {
	files, err := n.storage.List()
	if err != nil {
		log.Printf("[%s] Failed to list files for consistency check: %v\n", n.ID, err)
		return
	}

	for _, file := range files {
		checksum, err := n.storage.GetChecksum(file.Filename)
		if err != nil {
			log.Printf("[%s] Failed to get checksum for %s: %v\n", n.ID, file.Filename, err)
			continue
		}

		// Verify with replicas
		for peerID, peerAddr := range n.Peers {
			if peerID == n.ID {
				continue
			}

			n.replicator.VerifyChecksum(peerID, peerAddr, file.Filename, checksum)
		}
	}

	log.Printf("[%s] Consistency check completed for %d files\n", n.ID, len(files))
}

// UploadFile uploads a file to the distributed system
// Only works if this node is the master
func (n *Node) UploadFile(filename string, data []byte) error {
	if !n.isMaster {
		return fmt.Errorf("node is not master, cannot upload")
	}

	// Write to local storage first
	if err := n.storage.Write(filename, data); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	// Get checksum for consistency verification
	checksum, _ := n.storage.GetChecksum(filename)

	// Replicate to all slave nodes
	entry := &types.LogEntry{
		Op:       "WRITE",
		Filename: filename,
		Data:     data,
		Checksum: checksum,
	}

	success, _ := n.replicator.Replicate(entry)
	if !success {
		log.Printf("[%s] Warning: Replication failed for %s, but locally written\n", n.ID, filename)
	}

	return nil
}

// DownloadFile downloads a file from storage
func (n *Node) DownloadFile(filename string) ([]byte, error) {
	return n.storage.Read(filename)
}

// DeleteFile deletes a file from the distributed system
// Only works if this node is the master
func (n *Node) DeleteFile(filename string) error {
	if !n.isMaster {
		return fmt.Errorf("node is not master, cannot delete")
	}

	// Delete from local storage
	if err := n.storage.Delete(filename); err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}

	// Replicate deletion to all slave nodes
	entry := &types.LogEntry{
		Op:       "DELETE",
		Filename: filename,
	}

	success, _ := n.replicator.Replicate(entry)
	if !success {
		log.Printf("[%s] Warning: Replication of delete failed for %s\n", n.ID, filename)
	}

	return nil
}

// ListFiles lists all files in the system
func (n *Node) ListFiles() ([]types.FileMetadata, error) {
	return n.storage.List()
}

// GetStatus returns the current status of the node
func (n *Node) GetStatus() map[string]interface{} {
	files, _ := n.storage.List()

	role := "MASTER"
	if !n.isMaster {
		role = "SLAVE"
	}

	return map[string]interface{}{
		"node_id":     n.ID,
		"role":        role,
		"address":     n.Addr,
		"port":        n.Port,
		"files_count": len(files),
		"peers":       n.Peers,
		"master_id":   n.masterID,
	}
}

// GetNodeID returns the node ID
func (n *Node) GetNodeID() string {
	return n.ID
}

// IsMaster returns whether this node is a master
func (n *Node) IsMaster() bool {
	return n.isMaster
}

// runMasterHealthMonitor monitors master node health for slave nodes
// Periodically checks if master is responding and detects potential failures
func (n *Node) runMasterHealthMonitor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var consecutiveFailures int

	for {
		select {
		case <-ticker.C:
			// Check if master is responding
			err := n.replicator.CheckMasterHealth(n.masterURL)
			if err != nil {
				consecutiveFailures++
				log.Printf("[%s] Master health check failed (attempt %d): %v\n", n.ID, consecutiveFailures, err)

				// Alert after 3 consecutive failures
				if consecutiveFailures >= 3 {
					log.Printf("[%s] WARNING: Master %s may be unavailable after %d failed checks\n",
						n.ID, n.masterID, consecutiveFailures)
				}
			} else {
				// Reset on successful health check
				if consecutiveFailures > 0 {
					log.Printf("[%s] Master %s is healthy again\n", n.ID, n.masterID)
					consecutiveFailures = 0
				}
			}

		case <-n.stopCh:
			return
		}
	}
}
