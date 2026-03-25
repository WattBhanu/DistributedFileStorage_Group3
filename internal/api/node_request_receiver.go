package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/consensus"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/fault"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/replication"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/storage"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/timesync"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

type Handler struct {
	NodeID       string
	Replicator   *replication.ReplicationManager
	Storage      storage.Storage
	Consensus    *consensus.Raft
	Detector     *fault.Detector
	TimeSync     *timesync.BerkeleyNode
	CristianSync *timesync.CristianClient
	EventClock   *timesync.EventClock
	Clock        *timesync.MonotonicClock
}

func NewHandler(nodeID string, replicator *replication.ReplicationManager, storageLayer storage.Storage) *Handler {
	return &Handler{
		NodeID:     nodeID,
		Replicator: replicator,
		Storage:    storageLayer,
		Clock:      timesync.NewMonotonicClock(),
	}
}

// NewHandlerWithDetector creates a new API handler with fault detector
func NewHandlerWithDetector(nodeID string, isLeader bool, replicator *replication.ReplicationManager, storageLayer storage.Storage, detector *fault.Detector) *Handler {
	return &Handler{
		NodeID:     nodeID,
		Replicator: replicator,
		Storage:    storageLayer,
		Detector:   detector,
		Clock:      timesync.NewMonotonicClock(),
	}
}

func (h *Handler) HandleVoteRequest(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [RAFT] POST /internal/request-vote - Vote request received", h.NodeID)
	var req types.VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[%s] [RAFT] Vote request decode failed: %v", h.NodeID, err)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// Forward vote request to Raft consensus
	voteGranted := false
	if h.Consensus != nil {
		// Convert CandidateID to int if needed
		cid := 0
		switch v := req.CandidateID.(type) {
		case float64:
			cid = int(v)
		case string:
			fmt.Sscanf(v, "%d", &cid)
		}
		voteGranted = h.Consensus.HandleVoteRequest(fmt.Sprintf("%d", cid), int(req.Term))
	}

	resp := types.VoteResponse{
		Term:        req.Term,
		VoteGranted: voteGranted,
	}
	log.Printf("[%s] [RAFT] Vote request from term %d - vote granted: %v", h.NodeID, req.Term, resp.VoteGranted)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) HandleAppendEntries(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [RAFT] POST /internal/append-entries - Append entries request received", h.NodeID)
	var req types.AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[%s] [RAFT] Append entries decode failed: %v", h.NodeID, err)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// Convert LeaderID to string if needed
	lid := ""
	switch v := req.LeaderID.(type) {
	case float64:
		lid = fmt.Sprintf("%d", int(v))
	case string:
		lid = v
	}

	// Forward heartbeat to Raft consensus
	success := false
	if h.Consensus != nil {
		success = h.Consensus.HandleHeartbeat(lid, int(req.Term))
	}

	resp := types.AppendEntriesResponse{
		Term:    req.Term,
		Success: success,
	}
	log.Printf("[%s] [RAFT] Append entries from leader %s (term %d) - success: %v", h.NodeID, lid, req.Term, resp.Success)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) HandleReplicate(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [REPLICATION] POST /internal/replicate - Replication request received", h.NodeID)
	var req types.ReplicateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[%s] [REPLICATION] Decode failed: %v", h.NodeID, err)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	log.Printf("[%s] [REPLICATION] Processing %s operation for %s (version %d)", h.NodeID, req.Operation, req.Filename, req.Version)

	// Process replication request through replicator
	resp := h.Replicator.HandleReplicateRequest(&req)

	// If replication was successful, persist changes to disk
	if resp.Success {
		switch req.Operation {
		case "WRITE":
			if len(req.Data) > 0 {
				if err := h.Storage.Write(req.Filename, req.Data); err != nil {
					log.Printf("[%s] [REPLICATION] Failed to write replicated file %s to storage: %v\n", h.NodeID, req.Filename, err)
					resp.Success = false
					resp.Error = fmt.Sprintf("failed to write to storage: %v", err)
				} else {
					log.Printf("[%s] [REPLICATION] ✓ Persisted replicated file %s to disk (%d bytes)\n", h.NodeID, req.Filename, len(req.Data))
				}
			}
		case "DELETE":
			if err := h.Storage.Delete(req.Filename); err != nil {
				log.Printf("[%s] [REPLICATION] Failed to delete replicated file %s from storage: %v\n", h.NodeID, req.Filename, err)
				// Don't fail the response for delete errors - metadata is already updated
			} else {
				log.Printf("[%s] [REPLICATION] ✓ Deleted replicated file %s from disk\n", h.NodeID, req.Filename)
			}
		}

		if h.EventClock != nil {
			h.EventClock.NewEvent("REPLICATE")
		}
	}

	log.Printf("[%s] [REPLICATION] Replication response: success=%v", h.NodeID, resp.Success)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) HandleSyncRequest(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [CONSISTENCY] POST /internal/sync-request - Sync request received", h.NodeID)
	var req types.SyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[%s] [CONSISTENCY] Sync request decode failed: %v", h.NodeID, err)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	log.Printf("[%s] [CONSISTENCY] Querying file: %s", h.NodeID, req.Filename)
	// Get missed entries from storage for recovering node
	entries, err := h.Storage.GetLogFrom(0)
	if err != nil {
		log.Printf("[%s] [CONSISTENCY] Failed to get log entries: %v", h.NodeID, err)
		http.Error(w, "failed to get log entries", http.StatusInternalServerError)
		return
	}
	log.Printf("[%s] [CONSISTENCY] Returning %d log entries", h.NodeID, len(entries))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [HEALTH] GET /health - Health check ping", h.NodeID)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HandleTimeSync(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [BERKELEY] POST /internal/time-sync - Time sync request", h.NodeID)
	// Handle time synchronization request
	currentTime := h.Clock.Now()
	response := map[string]interface{}{
		"node_id": h.NodeID,
		"time":    currentTime.UnixNano(),
	}
	log.Printf("[%s] [BERKELEY] Current time: %d ns", h.NodeID, currentTime.UnixNano())
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *Handler) HandleNodeRecovered(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [FAULT] POST /internal/node-recovered - Node recovery notification received", h.NodeID)
	
	var req struct {
		RecoveredNodeID   string `json:"RecoveredNodeID"`
		RecoveredNodeAddr string `json:"RecoveredNodeAddr"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[%s] [FAULT] Recovery notification decode failed: %v", h.NodeID, err)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	
	// Re-add the recovered node to fault detector
	if h.Detector != nil {
		// Check if node already exists
		if _, exists := h.Detector.GetNode(req.RecoveredNodeID); !exists {
			h.Detector.AddNode(req.RecoveredNodeID, req.RecoveredNodeAddr)
			log.Printf("[%s] [FAULT] ✓ Re-added recovered node %s at %s", h.NodeID, req.RecoveredNodeID, req.RecoveredNodeAddr)
		} else {
			// Node exists, reset its status using RecordHeartbeat
			h.Detector.RecordHeartbeat(req.RecoveredNodeID, time.Now())
			log.Printf("[%s] [FAULT] ✓ Reset recovered node %s to healthy", h.NodeID, req.RecoveredNodeID)
		}
	} else {
		log.Printf("[%s] [FAULT] WARNING: No fault detector available to handle recovery", h.NodeID)
	}
	
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HandleTimeAdjust(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [BERKELEY] POST /internal/time-adjust - Time adjust received", h.NodeID)

	var req map[string]float64
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	if adjNs, ok := req["adjustment_ns"]; ok {
		adj := time.Duration(adjNs) * time.Nanosecond
		if h.TimeSync != nil {
			h.TimeSync.ApplyAdjustment(adj)
			log.Printf("[%s] [BERKELEY] Applied time adjustment from leader: %v", h.NodeID, adj)
		}
	}

	w.WriteHeader(http.StatusOK)
}

// HandleGetTimeSyncMetrics returns detailed Berkeley time synchronization metrics
func (h *Handler) HandleGetTimeSyncMetrics(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [TIMESYNC] GET /api/timesync - Detailed time sync metrics requested", h.NodeID)
	setCORS(w)

	// Build time sync metrics object
	timeSyncMetrics := map[string]interface{}{
		"protocol":        "Berkeley",
		"is_coordinator":  false,
		"sync_interval":   10, // seconds
		"clock_offset_ms": int64(0),
		"last_sync":       time.Now().UnixNano(),
		"peers_synced":    0,
		"peer_details":    []map[string]interface{}{},
		"cristian_offset": int64(0),
		"cristian_rtt":    int64(0),
		"lamport_counter": uint64(0),
		"vector_clock":    map[string]uint64{},
	}

	// Add Raft leader status
	if h.Consensus != nil {
		timeSyncMetrics["is_coordinator"] = h.Consensus.IsLeader()
	}

	// Get clock offset if available
	if h.TimeSync != nil {
		timeSyncMetrics["last_sync"] = h.TimeSync.GetLastSyncTime().UnixNano()
		delta := h.TimeSync.GetDelta()
		timeSyncMetrics["clock_offset_ms"] = delta.Milliseconds()
	}

	// Add EventClock data
	if h.EventClock != nil {
		lamport, vector := h.EventClock.GetState()
		timeSyncMetrics["lamport_counter"] = lamport
		timeSyncMetrics["vector_clock"] = vector
	}

	// Get peer synchronization details
	if h.Replicator != nil {
		peers := h.Replicator.GetPeers()
		timeSyncMetrics["peers_synced"] = len(peers)

		peerDetails := make([]map[string]interface{}, 0, len(peers))
		for peerID, peerAddr := range peers {
			peerInfo := map[string]interface{}{
				"node_id":         peerID,
				"address":         peerAddr,
				"clock_offset_ms": int64(0),
				"status":          "unknown",
			}

			// Try to get peer's time sync status
			url := fmt.Sprintf("http://%s/api/metrics", peerAddr)
			resp, err := h.Replicator.GetHTTPClient().Get(url)
			if err == nil && resp != nil {
				var peerMetrics map[string]interface{}
				if err := json.NewDecoder(resp.Body).Decode(&peerMetrics); err == nil {
					if timeSync, ok := peerMetrics["time_sync"].(map[string]interface{}); ok {
						if offset, ok := timeSync["clock_offset_ms"].(float64); ok {
							peerInfo["clock_offset_ms"] = int64(offset)
						}
						if lastSync, ok := timeSync["last_sync"].(float64); ok {
							peerInfo["last_sync"] = int64(lastSync)
						}

						// Determine sync status based on offset
						if offsetVal, ok := peerInfo["clock_offset_ms"].(int64); ok {
							if offsetVal == 0 {
								peerInfo["status"] = "synchronized"
							} else if offsetVal < -10 {
								peerInfo["status"] = "behind"
							} else if offsetVal > 10 {
								peerInfo["status"] = "ahead"
							} else {
								peerInfo["status"] = "synchronized"
							}
						}
					}
				}
				resp.Body.Close()
			}

			peerDetails = append(peerDetails, peerInfo)
		}

		timeSyncMetrics["peer_details"] = peerDetails
	}

	// Create the full metrics response
	metrics := map[string]interface{}{
		"time_sync": timeSyncMetrics,
	}

	log.Printf("[%s] [TIMESYNC] Returning time sync metrics - coordinator=%v, peers=%d, lamport=%d",
		h.NodeID, timeSyncMetrics["is_coordinator"], timeSyncMetrics["peers_synced"], timeSyncMetrics["lamport_counter"])

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// HandleCristianTime returns the precise server time for Cristian synchronization
func (h *Handler) HandleCristianTime(w http.ResponseWriter, r *http.Request) {
	currentTime := h.Clock.Now()
	response := map[string]interface{}{
		"time": currentTime.UnixNano(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
