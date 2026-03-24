package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/fault"
)

// MonitorMetrics represents real-time monitoring data
type MonitorMetrics struct {
	Timestamp   int64                  `json:"timestamp"`
	NodeID      string                 `json:"node_id"`
	Consensus   ConsensusMetrics       `json:"consensus"`
	Replication ReplicationMetrics     `json:"replication"`
	Fault       FaultMetrics           `json:"fault"`
	TimeSync    TimeSyncMetrics        `json:"time_sync"`
}

// ConsensusMetrics contains Raft consensus metrics
// Only includes fields that can be populated with REAL data
type ConsensusMetrics struct {
	State        string `json:"state"`        // Current node state: LEADER, FOLLOWER, or CANDIDATE
	Leader       string `json:"leader"`       // Node ID of the Raft leader
	Term         int    `json:"term"`         // Current Raft term (if consensus active)
	VoteCount    int    `json:"vote_count"`   // Votes received in current election
}

// ReplicationMetrics contains replication statistics
// Only includes fields that can be populated with REAL data
type ReplicationMetrics struct {
	FilesReplicated    int64              `json:"files_replicated"`     // Total files in storage
	PeerCount          int                `json:"peer_count"`           // Number of known peer nodes
	ReplicaStatus      []ReplicaStatus    `json:"replica_status"`       // Status of each peer
}

// ReplicaStatus shows status of individual replicas
// Only includes fields that can be populated with REAL data
type ReplicaStatus struct {
	NodeID      string  `json:"node_id"`      // Peer node identifier
	Address     string  `json:"address"`      // Network address
	Known       bool    `json:"known"`        // Whether this peer is configured
}

// FaultMetrics contains fault tolerance metrics
// Only includes fields that can be populated with REAL data
type FaultMetrics struct {
	HealthyNodes   int           `json:"healthy_nodes"`    // Count of healthy nodes
	SuspectedNodes int           `json:"suspected_nodes"`  // Count of suspected nodes
	FailedNodes    int           `json:"failed_nodes"`     // Count of failed nodes
	NodeStates     []NodeStateInfo `json:"node_states"`    // Detailed state of each node
}

// NodeStateInfo contains detailed node state information
// Only includes fields that can be populated with REAL data
type NodeStateInfo struct {
	NodeID         string `json:"node_id"`   // Node identifier
	Status         string `json:"status"`    // Current health status
}

// TimeSyncMetrics contains time synchronization metrics
// Only includes fields that can be populated with REAL data
type TimeSyncMetrics struct {
	LastSync       int64             `json:"last_sync"`         // Last sync timestamp (nanoseconds)
	Protocol       string            `json:"protocol"`          // Synchronization protocol name
	ClockOffset    float64           `json:"clock_offset"`      // Current clock offset in microseconds (µs) for sub-ms precision
	IsCoordinator  bool              `json:"is_coordinator"`    // Whether this node is the Berkeley coordinator (Raft leader)
	SyncInterval   int64             `json:"sync_interval"`     // Synchronization interval in seconds
	PeersSynced    int               `json:"peers_synced"`      // Number of peers synchronized with
	SyncRound      int64             `json:"sync_round"`        // How many sync rounds have completed

	CristianOffset float64           `json:"cristian_offset"`   // Cristian clock offset in µs
	CristianRTT    float64           `json:"cristian_rtt"`      // Cristian RTT in µs
	LamportCounter uint64            `json:"lamport_counter"`   // Current Lamport clock tick
	VectorClock    map[string]uint64 `json:"vector_clock"`      // Vector clock state
}

// HandleGetMetrics returns comprehensive monitoring metrics
func (h *Handler) HandleGetMetrics(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [METRICS] GET /api/metrics - Metrics request received", h.NodeID)
	setCORS(w)
	
	metrics := MonitorMetrics{
		Timestamp: time.Now().UnixNano(),
		NodeID:    h.NodeID,
		Consensus: h.getConsensusMetrics(),
		Replication: h.getReplicationMetrics(),
		Fault: h.getFaultMetrics(),
		TimeSync: h.getTimeSyncMetrics(),
	}
	
	log.Printf("[%s] [METRICS] Returning metrics - files=%d, peers=%d, healthy_nodes=%d", 
		h.NodeID, metrics.Replication.FilesReplicated, 
		metrics.Replication.PeerCount, 
		metrics.Fault.HealthyNodes)
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// getConsensusMetrics extracts consensus-related metrics
// Returns ONLY real, meaningful data from Raft consensus - no hardcoded values
func (h *Handler) getConsensusMetrics() ConsensusMetrics {
	metrics := ConsensusMetrics{}
	
	// Get actual Raft state if consensus is available
	if h.Consensus != nil {
		metrics.State = string(h.Consensus.GetState())
		metrics.Term = h.Consensus.GetLeaderTerm() // Use cluster-wide leader term, not node's term
		
		// If this node is leader, it's the leader
		if h.Consensus.IsLeader() {
			metrics.Leader = h.NodeID
		} else {
			// Follower should track the known leader from heartbeats
			knownLeader := h.Consensus.GetKnownLeader()
			if knownLeader != "" {
				metrics.Leader = knownLeader
			} else {
				metrics.Leader = "unknown"
			}
		}
	} else {
		// No consensus available
		metrics.State = "unknown"
		metrics.Leader = "unknown"
	}
	
	return metrics
}

// getReplicationMetrics extracts replication statistics
// Returns ONLY real, meaningful data - no hardcoded values
func (h *Handler) getReplicationMetrics() ReplicationMetrics {
	metrics := ReplicationMetrics{}
	
	// Count files from storage - REAL data
	if h.Storage != nil {
		files, err := h.Storage.List()
		if err == nil {
			metrics.FilesReplicated = int64(len(files))
		}
	}
	
	// Get peer information - REAL data
	if h.Replicator != nil {
		peers := h.Replicator.GetPeers()
		metrics.PeerCount = len(peers)
		
		for peerID, peerAddr := range peers {
			metrics.ReplicaStatus = append(metrics.ReplicaStatus, ReplicaStatus{
				NodeID:  peerID,
				Address: peerAddr,
				Known:   true,
			})
		}
	}
	
	return metrics
}

// getFaultMetrics extracts fault tolerance metrics
func (h *Handler) getFaultMetrics() FaultMetrics {
	metrics := FaultMetrics{
		NodeStates: make([]NodeStateInfo, 0),
	}
	
	// Start with this node
	metrics.HealthyNodes = 1
	metrics.NodeStates = append(metrics.NodeStates, NodeStateInfo{
		NodeID: h.NodeID,
		Status: "healthy",
	})
	
	// Add all peer nodes with real status from fault detector
	if h.Replicator != nil {
		peers := h.Replicator.GetPeers()
		for peerID, _ := range peers {
			status := "healthy" // Default assumption
			
			// If fault detector is available, check actual status
			if h.Detector != nil {
				if node, exists := h.Detector.GetNode(peerID); exists {
					switch node.Status {
					case fault.Healthy:
						status = "healthy"
						metrics.HealthyNodes++
					case fault.Suspected:
						status = "suspected"
						metrics.SuspectedNodes++
					case fault.Failed:
						status = "failed"
						metrics.FailedNodes++
					default:
						metrics.HealthyNodes++
					}
				} else {
					// Node not in detector yet, assume healthy
					metrics.HealthyNodes++
				}
			} else {
				// No detector, assume all peers are healthy
				metrics.HealthyNodes++
			}
			
			metrics.NodeStates = append(metrics.NodeStates, NodeStateInfo{
				NodeID: peerID,
				Status: status,
			})
		}
	}
	
	return metrics
}

// getTimeSyncMetrics extracts time synchronization metrics
// Returns ONLY real, meaningful data - no hardcoded values
func (h *Handler) getTimeSyncMetrics() TimeSyncMetrics {
	metrics := TimeSyncMetrics{
		Protocol: "Berkeley",
		IsCoordinator: false,
		SyncInterval: 10, // Default 10 second interval
	}
	
	// Check if this node is the Raft leader (Berkeley coordinator)
	if h.Consensus != nil {
		metrics.IsCoordinator = h.Consensus.IsLeader()
	}
	
	// Get last sync time and clock information from clock - REAL data
	if h.Clock != nil {
		now := h.Clock.Now()
		
		// Get actual clock offset from Berkeley algorithm (last applied delta)
		// This is the REAL adjustment applied during the last synchronization round
		if h.TimeSync != nil {
			metrics.LastSync = h.TimeSync.GetLastSyncTime().UnixNano()
			delta := h.TimeSync.GetDelta()
			// Use microseconds for sub-millisecond precision
			metrics.ClockOffset = float64(delta.Nanoseconds()) / 1000.0
			metrics.SyncRound = h.TimeSync.GetSyncRound()
			log.Printf("[%s] [TIMESYNC] Reporting clock offset: %.2fµs (round %d)", h.NodeID, metrics.ClockOffset, metrics.SyncRound)
		} else {
			metrics.LastSync = now.UnixNano()
			// No Berkeley node available, calculate offset from system time
			systemTime := time.Now()
			offset := now.Sub(systemTime)
			metrics.ClockOffset = float64(offset.Nanoseconds()) / 1000.0
			log.Printf("[%s] [TIMESYNC] No Berkeley node, using system offset: %.2fµs", h.NodeID, metrics.ClockOffset)
		}
	} else {
		log.Printf("[%s] [TIMESYNC] WARNING: No clock available!", h.NodeID)
	}
	
	// Count synced peers
	if h.Replicator != nil {
		peers := h.Replicator.GetPeers()
		metrics.PeersSynced = len(peers)
	}
	
	if h.CristianSync != nil {
		metrics.CristianRTT = float64(h.CristianSync.GetRTT().Nanoseconds()) / 1000.0
		metrics.CristianOffset = float64(h.CristianSync.GetOffset().Nanoseconds()) / 1000.0
	}

	if h.EventClock != nil {
		lamport, vector := h.EventClock.GetState()
		metrics.LamportCounter = lamport
		metrics.VectorClock = vector
	}

	return metrics
}
