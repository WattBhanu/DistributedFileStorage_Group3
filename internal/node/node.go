
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
	"bytes"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/api"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/consensus"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/fault"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/replication"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/storage"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/timesync"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

// HTTPHealthChecker implements HealthChecker using HTTP requests
type HTTPHealthChecker struct {
	timeout time.Duration
}

func (hc *HTTPHealthChecker) Ping(addr string) error {
	client := &http.Client{
		Timeout: hc.timeout,
	}
	url := fmt.Sprintf("http://%s/api/status", addr)
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed with status: %d", resp.StatusCode)
	}
	return nil
}

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
	detector   *fault.Detector
	consensus  *consensus.Raft
	timeSync   *timesync.BerkeleyNode
	handler    *api.Handler

	// Node role (determined by Raft consensus)
	// isLeader is determined by Raft consensus algorithm
	masterID  string
	masterURL string

	// Coordination
	mu        sync.RWMutex
	stopCh    chan struct{}
	isRunning bool
}

// NewNode creates a new node with the given configuration
// Uses Raft consensus for leader election and data replication
func NewNode(
	nodeID string,
	addr string,
	port string,
	peers map[string]string,
	dataDir string,
) *Node {
	// Determine master URL for initial setup (will be overridden by Raft)
	var masterID, masterURL string

	// For initial setup, assume node1 is the first node
	if nodeID == "node1" {
		masterID = nodeID
		masterURL = fmt.Sprintf("http://%s:%s", addr, port)
	} else {
		// For other nodes, find master address from peers
		masterID = "node1" // Assume first node is initial master
		if masterAddr, exists := peers[masterID]; exists {
			masterURL = fmt.Sprintf("http://%s", masterAddr)
		}
	}

	// Initialize components
	storageLayer := storage.New(dataDir)
	replicator := replication.NewReplicationManager(nodeID, peers)
	
	// Initialize fault detector with automatic restart capability
	healthChecker := &HTTPHealthChecker{timeout: 2 * time.Second}
	
	// Create process supervisor for automatic node restart
	supervisor := fault.NewProcessSupervisor(30*time.Second, 3) // 30s delay, max 3 restarts
	
	// Create detector with supervisor
	detector := fault.NewDetectorWithSupervisor(fault.DefaultConfig(), healthChecker, nil, supervisor)
	
	// Add all known peers to fault detector
	for peerID, peerAddr := range peers {
		detector.AddNode(peerID, peerAddr)
	}
	// Add self
	detector.AddNode(nodeID, fmt.Sprintf("%s:%s", addr, port))
	
	// Initialize Raft consensus for distributed leader election
	// Map node ID to integer for Raft (node1=1, node2=2, etc.)
	raftID := 1 // Default for node1/master
	if nodeID == "node2" {
		raftID = 2
	} else if nodeID == "node3" {
		raftID = 3
	}
	
	// Create peer map excluding self
	peerMap := make(map[string]string)
	for pid, paddr := range peers {
		if pid != nodeID {
			peerMap[pid] = paddr
		}
	}
	
	consensusNode := consensus.NewRaft(raftID, peerMap)
	log.Printf("[RAFT] [NODE %d] Created with %d peers", raftID, len(peerMap))
	
	// Initialize Berkeley time synchronization
	// Note: Berkeley algorithm has its own leader concept, but we'll keep it simple
	// In production, you might want to integrate this with Raft leader
	clock := timesync.NewMonotonicClock()
	berkeleyNode := timesync.NewBerkeleyNode(
		nodeID,
		clock,
		false, // All nodes start as follower in Berkeley (will coordinate via Raft)
		100*time.Millisecond,
	)

	// Add Cristian Client
	cristianSync := timesync.NewCristianClient(clock, "leader", 10*time.Second)
	
	// Add Event Clock
	eventClock := timesync.NewEventClock(nodeID, clock.Now().UnixNano())

	node := &Node{
		ID:         nodeID,
		Addr:       addr,
		Port:       port,
		Peers:      peers,
		storage:    storageLayer,
		replicator: replicator,
		detector:   detector,
		consensus:  consensusNode,
		timeSync:   berkeleyNode,
		masterID:   masterID,
		masterURL:  masterURL,
		stopCh:     make(chan struct{}),
	}

	// Create handler with detector and consensus
	node.handler = api.NewHandlerWithDetector(nodeID, false, replicator, storageLayer, detector)
	node.handler.Consensus = consensusNode
	node.handler.TimeSync = berkeleyNode
	node.handler.CristianSync = cristianSync
	node.handler.EventClock = eventClock
	node.handler.Clock = clock

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

	// Start Raft consensus for leader election
	n.consensus.Run()
	log.Printf("[%s] Raft consensus started\n", n.ID)
	
	// Initialize replication manager with existing files from storage
	// This loads pre-existing files into the replication metadata
	n.replicator.InitializeFromStorage(n.storage)
	
	// Start fault detector
	ctx := context.Background()
	go n.detector.Run(ctx)
	log.Printf("[%s] Fault detector started\n", n.ID)

	// Start replication manager
	n.replicator.Start()
	log.Printf("[%s] Replication manager started\n", n.ID)
	
	// Start Berkeley time synchronization
	// In production, only Raft leader would coordinate time sync
	go n.runTimeSynchronization()

	// Start Cristian time synchronization
	go n.runCristianSynchronization()

	// Start consistency checker (only Raft leader performs checks)
	go n.runConsistencyChecker()

	// Start master health monitoring (for followers to detect leader failures)
	// Followers need to monitor if the Raft leader is healthy
	if !n.consensus.IsLeader() {
		go n.runMasterHealthMonitor()
	}

	// Start API server
	go func() {
		err := api.Start(n.Port, n.handler)
		if err != nil {
			log.Printf("[%s] API server error: %v\n", n.ID, err)
		}
	}()

	log.Printf("[%s] API server listening on port %s\n", n.ID, n.Port)
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

	close(n.stopCh)

	log.Printf("[%s] Node stopped\n", n.ID)
	return nil
}

// runConsistencyChecker periodically checks data consistency across nodes
// Verifies that all replicas have the same data via checksums
// Only Raft leader performs consistency checks
func (n *Node) runConsistencyChecker() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	log.Printf("[%s] [CONSISTENCY] Checker started with 30s interval", n.ID)
	checkCount := 0
	for {
		select {
		case <-ticker.C:
			// Only Raft leader performs consistency checks
			if n.consensus.IsLeader() {
				checkCount++
				log.Printf("[%s] [CONSISTENCY] Starting periodic check #%d...", n.ID, checkCount)
				n.checkConsistency()
				log.Printf("[%s] [CONSISTENCY] Check #%d completed", n.ID, checkCount)
			} else {
				log.Printf("[%s] [CONSISTENCY] Skipping check - not Raft leader", n.ID)
			}
		case <-n.stopCh:
			log.Printf("[%s] [CONSISTENCY] Checker stopped after %d checks", n.ID, checkCount)
			return
		}
	}
}

// runTimeSynchronization runs Berkeley algorithm for time sync
func (n *Node) runTimeSynchronization() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	log.Printf("[%s] [TIMESYNC] Berkeley time synchronization started with 10s interval", n.ID)
	roundCount := 0
	
	// Wait for Raft to elect a leader first
	log.Printf("[%s] [TIMESYNC] Waiting for Raft leader election...", n.ID)
	time.Sleep(2 * time.Second) // Give Raft time to elect leader
	
	for {
		select {
		case <-ticker.C:
			roundCount++
			// Perform Berkeley time synchronization round
			log.Printf("[%s] [TIMESYNC] Round %d: Starting time synchronization...", n.ID, roundCount)
			
			// Only Raft leader performs actual coordination
			if n.consensus.IsLeader() {
				log.Printf("[%s] [TIMESYNC] Acting as Berkeley coordinator (Raft leader)", n.ID)
				
				// Collect time from all peers
				samples := n.collectTimeSamples()
				peerSamples := samples[1:] // Exclude self (always first)
				if len(peerSamples) > 0 {
					// Calculate average delta across all nodes including self
					averageDelta := n.calculateAverageDelta(samples)
					log.Printf("[%s] [TIMESYNC] Round %d: Collected %d peer samples, averageDelta=%v", n.ID, roundCount, len(peerSamples), averageDelta)
					
					// Apply and broadcast per-node adjustments
					for _, sample := range samples {
						// adj = what offset this node needs to reach the average
						adj := averageDelta - sample.Delta
						
						if sample.NodeID == n.ID {
							// Coordinator applies its own adjustment
							n.timeSync.ApplyAdjustment(adj)
							log.Printf("[%s] [TIMESYNC] ✓ Coordinator adjustment: %v", n.ID, adj)
						} else {
							peerAddr := n.Peers[sample.NodeID]
							go n.sendTimeAdjustment(sample.NodeID, peerAddr, adj)
							log.Printf("[%s] [TIMESYNC] → Sending adjustment %v to %s", n.ID, adj, sample.NodeID)
						}
					}
				} else {
					log.Printf("[%s] [TIMESYNC] Skipping sync - no peers reachable (run multi-node cluster)", n.ID)
				}
			} else {
				log.Printf("[%s] [TIMESYNC] Skipping coordination - not Raft leader", n.ID)
			}
			
			log.Printf("[%s] [TIMESYNC] Round %d: ✓ Sync completed", n.ID, roundCount)
		case <-n.stopCh:
			log.Printf("[%s] [TIMESYNC] Stopped after %d rounds", n.ID, roundCount)
			return
		}
	}
}

// runCristianSynchronization runs Cristian's algorithm for time sync
func (n *Node) runCristianSynchronization() {
	n.handler.CristianSync.Start(func() (time.Time, error) {
		if n.consensus.IsLeader() {
			return n.handler.Clock.Now(), nil
		}

		leaderID := n.consensus.GetKnownLeader()
		if leaderID == "" || leaderID == "unknown" {
			return time.Time{}, fmt.Errorf("no known leader")
		}

		peerAddr, exists := n.Peers[leaderID]
		if !exists {
			return time.Time{}, fmt.Errorf("leader address not found")
		}

		url := fmt.Sprintf("http://%s/internal/cristian-time", peerAddr)
		resp, err := n.replicator.GetHTTPClient().Get(url)
		if err != nil {
			return time.Time{}, err
		}
		defer resp.Body.Close()

		var data map[string]float64
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			return time.Time{}, err
		}

		return time.Unix(0, int64(data["time"])), nil
	})
}

// collectTimeSamples gathers time readings from all peer nodes
func (n *Node) collectTimeSamples() []timesync.TimeSample {
	samples := make([]timesync.TimeSample, 0)
	
	// Add self
	selfTime := n.handler.Clock.Now()
	samples = append(samples, timesync.TimeSample{
		NodeID:    n.ID,
		LocalTime: selfTime,
		Delta:     0,
	})
	
	// Query each peer
	for peerID, peerAddr := range n.Peers {
		if peerID == n.ID {
			continue
		}
		
		// Send HTTP request to get peer's time
		url := fmt.Sprintf("http://%s/internal/time-sync", peerAddr)
		resp, err := n.replicator.GetHTTPClient().Get(url)
		if err != nil {
			log.Printf("[%s] [TIMESYNC] Failed to get time from %s: %v", n.ID, peerID, err)
			continue
		}
		
		var respData map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()
		
		peerTimeNano := int64(respData["time"].(float64))
		peerTime := time.Unix(0, peerTimeNano)
		delta := peerTime.Sub(selfTime)
		
		samples = append(samples, timesync.TimeSample{
			NodeID:    peerID,
			LocalTime: peerTime,
			Delta:     delta,
		})
		
		log.Printf("[%s] [TIMESYNC] Collected time from %s: delta=%v", n.ID, peerID, delta)
	}
	
	return samples
}

// calculateAverageDelta computes the average time delta from all samples
func (n *Node) calculateAverageDelta(samples []timesync.TimeSample) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	
	var totalDelta time.Duration
	for _, s := range samples {
		totalDelta += s.Delta
	}
	
	return totalDelta / time.Duration(len(samples))
}

// sendTimeAdjustment sends clock offset correction to a follower
func (n *Node) sendTimeAdjustment(peerID string, peerAddr string, adjustment time.Duration) {
	url := fmt.Sprintf("http://%s/internal/time-adjust", peerAddr)
	
	reqBody, _ := json.Marshal(map[string]interface{}{
		"adjustment_ns": adjustment.Nanoseconds(),
	})
	
	resp, err := n.replicator.GetHTTPClient().Post(url, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Printf("[%s] [TIMESYNC] Failed to send adjustment to %s: %v", n.ID, peerID, err)
		return
	}
	defer resp.Body.Close()
	log.Printf("[%s] [TIMESYNC] Sent adjustment %v to %s", n.ID, adjustment, peerID)
}

// checkConsistency verifies data consistency across all nodes
func (n *Node) checkConsistency() {
	files, err := n.storage.List()
	if err != nil {
		log.Printf("[%s] [CONSISTENCY] Failed to list files: %v\n", n.ID, err)
		return
	}

	if len(files) == 0 {
		log.Printf("[%s] [CONSISTENCY] No files to check - storage is empty", n.ID)
		return
	}

	log.Printf("[%s] [CONSISTENCY] Checking %d files for consistency...", n.ID, len(files))
	for _, file := range files {
		checksum, err := n.storage.GetChecksum(file.Filename)
		if err != nil {
			log.Printf("[%s] [CONSISTENCY] Failed to get checksum for %s: %v\n", n.ID, file.Filename, err)
			continue
		}

		log.Printf("[%s] [CONSISTENCY] File: %s | Size: %d bytes | Checksum: %s", 
			n.ID, file.Filename, file.Size, checksum)
		
		// Verify with replicas
		for peerID, peerAddr := range n.Peers {
			if peerID == n.ID {
				continue
			}

			log.Printf("[%s] [CONSISTENCY] → Verifying with peer %s at %s", n.ID, peerID, peerAddr)
			n.replicator.VerifyChecksum(peerID, peerAddr, file.Filename, checksum)
		}
	}

	log.Printf("[%s] [CONSISTENCY] ✓ Consistency check completed for %d files", n.ID, len(files))
}

// UploadFile uploads a file to the distributed system
// Only works if this node is the Raft leader
func (n *Node) UploadFile(filename string, data []byte) error {
	if !n.consensus.IsLeader() {
		return fmt.Errorf("node is not Raft leader, cannot upload")
	}

	// Write to local storage first
	if err := n.storage.Write(filename, data); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	// Get checksum for consistency verification
	checksum, _ := n.storage.GetChecksum(filename)

	// Replicate to all follower nodes (only if this node is Raft leader)
	entry := &types.LogEntry{
		Op:       "WRITE",
		Filename: filename,
		Data:     data,
		Checksum: checksum,
	}

	success, _ := n.replicator.Replicate(entry, n.consensus.IsLeader())
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
// Only works if this node is the Raft leader
func (n *Node) DeleteFile(filename string) error {
	if !n.consensus.IsLeader() {
		return fmt.Errorf("node is not Raft leader, cannot delete")
	}

	// Delete from local storage
	if err := n.storage.Delete(filename); err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}

	// Replicate deletion to all follower nodes (only if this node is Raft leader)
	entry := &types.LogEntry{
		Op:       "DELETE",
		Filename: filename,
	}

	success, _ := n.replicator.Replicate(entry, n.consensus.IsLeader())
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

	// Determine role from Raft consensus state
	role := "FOLLOWER"
	if n.consensus.IsLeader() {
		role = "LEADER"
	}

	return map[string]interface{}{
		"node_id":     n.ID,
		"role":        role,
		"raft_state":  n.consensus.GetState(),
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

// runMasterHealthMonitor monitors master node health for slave nodes
// Periodically checks if master is responding and detects potential failures
func (n *Node) runMasterHealthMonitor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var consecutiveFailures int
	log.Printf("[%s] Master health monitor started - checking %s every 5s", n.ID, n.masterID)
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
				} else {
					log.Printf("[%s] Master %s health check OK", n.ID, n.masterID)
				}
			}

		case <-n.stopCh:
			log.Printf("[%s] Master health monitor stopped", n.ID)
			return
		}
	}
}
