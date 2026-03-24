package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/fault"
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

func (h *Handler) HandleUpload(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [HTTP] POST /api/upload - Request received from %s", h.NodeID, r.RemoteAddr)
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Only Raft leader can handle uploads
	if h.Consensus != nil && !h.Consensus.IsLeader() {
		log.Printf("[%s] [HTTP] Upload rejected - node is not Raft leader", h.NodeID)
		http.Error(w, "only Raft leader accepts uploads", http.StatusForbidden)
		return
	}

	r.ParseMultipartForm(32 << 20)
	file, header, err := r.FormFile("file")
	if err != nil {
		log.Printf("[%s] [HTTP] Upload failed - cannot read file: %v", h.NodeID, err)
		http.Error(w, "failed to read file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		log.Printf("[%s] [HTTP] Upload failed - cannot read file data: %v", h.NodeID, err)
		http.Error(w, "failed to read file", http.StatusInternalServerError)
		return
	}

	log.Printf("[%s] [HTTP] Uploading file: %s (%d bytes)", h.NodeID, header.Filename, len(data))

	// Write to local storage
	if err := h.Storage.Write(header.Filename, data); err != nil {
		log.Printf("[%s] [HTTP] Upload failed - storage write error: %v", h.NodeID, err)
		http.Error(w, "failed to write file", http.StatusInternalServerError)
		return
	}
	log.Printf("[%s] [HTTP] File %s written to local storage", h.NodeID, header.Filename)

	if h.EventClock != nil {
		h.EventClock.NewEvent("UPLOAD")
	}

	// Get checksum for consistency verification
	checksum, _ := h.Storage.GetChecksum(header.Filename)

	// Replicate to all follower nodes (only if this node is Raft leader)
	entry := &types.LogEntry{
		Op:       "WRITE",
		Filename: header.Filename,
		Data:     data,
		Checksum: checksum,
	}

	// Check if this node is Raft leader
	isLeader := false
	if h.Consensus != nil {
		isLeader = h.Consensus.IsLeader()
	}

	success, err := h.Replicator.Replicate(entry, isLeader)
	if !success {
		log.Printf("[%s] [HTTP] Warning: Replication failed for %s: %v\n", h.NodeID, header.Filename, err)
	} else {
		log.Printf("[%s] [HTTP] ✓ Replication successful for %s", h.NodeID, header.Filename)
	}

	log.Printf("[%s] [HTTP] ✓ Upload complete: %s (replication=%v)", h.NodeID, header.Filename, success)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":  success,
		"filename": header.Filename,
		"size":     len(data),
	})
}

func (h *Handler) HandleListFiles(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [HTTP] GET /api/files - Listing files", h.NodeID)
	setCORS(w)
	files, err := h.Storage.List()
	if err != nil {
		log.Printf("[%s] [HTTP] Failed to list files: %v", h.NodeID, err)
		http.Error(w, "failed to list files", http.StatusInternalServerError)
		return
	}
	log.Printf("[%s] [HTTP] Listed %d files", h.NodeID, len(files))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(files)
}

func (h *Handler) HandleDownload(w http.ResponseWriter, r *http.Request) {
	filename := r.URL.Path[len("/api/files/"):]
	log.Printf("[%s] [HTTP] GET /api/files/%s - Download request", h.NodeID, filename)
	if filename == "" {
		log.Printf("[%s] [HTTP] Download failed - filename required", h.NodeID)
		http.Error(w, "filename required", http.StatusBadRequest)
		return
	}

	data, err := h.Storage.Read(filename)
	if err != nil {
		log.Printf("[%s] [HTTP] Download failed - file not found: %s", h.NodeID, filename)
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	log.Printf("[%s] [HTTP] ✓ Downloading %s (%d bytes)", h.NodeID, filename, len(data))

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (h *Handler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [HTTP] GET /api/status - Status request", h.NodeID)
	setCORS(w)
	files, _ := h.Storage.List()

	status := map[string]interface{}{
		"node_id":     h.NodeID,
		"role":        "Follower",
		"raft_state":  "Unknown",
		"raft_leader": "unknown",
		"files_count": len(files),
		"timestamp":   h.Clock.Now().UnixNano(),
	}

	// Get actual Raft state if available
	if h.Consensus != nil {
		state := string(h.Consensus.GetState()) // Already CAPITAL from State enum
		term := h.Consensus.GetTerm()
		leader := h.Consensus.GetKnownLeader()

		status["raft_state"] = state
		status["raft_term"] = term
		status["raft_leader"] = leader

		if h.Consensus.IsLeader() {
			status["role"] = "Leader"
		} else {
			status["role"] = "Follower"
		}
	}

	log.Printf("[%s] [HTTP] Status: role=%s, files=%d", h.NodeID, status["role"], len(files))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (h *Handler) HandleKillNode(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [HTTP] DELETE /api/admin/kill/ - Kill node request", h.NodeID)
	// Extract node ID from URL path
	path := r.URL.Path
	nodeID := path[len("/api/admin/kill/"):]
	if nodeID == "" {
		log.Printf("[%s] [HTTP] Kill failed - node ID required", h.NodeID)
		http.Error(w, "node ID required", http.StatusBadRequest)
		return
	}

	log.Printf("[%s] [HTTP] Simulating failure for node %s", h.NodeID, nodeID)
	// Mark node as failed in fault detector
	if h.Detector != nil {
		h.Detector.AddNode(nodeID, "")
		h.Detector.CheckNode(nodeID)
		log.Printf("[%s] [HTTP] ✓ Node %s marked as failed", h.NodeID, nodeID)
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HandleHealNode(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [HTTP] POST /api/admin/heal/ - Heal node request", h.NodeID)
	// Extract node ID from URL path
	path := r.URL.Path
	nodeID := path[len("/api/admin/heal/"):]
	if nodeID == "" {
		log.Printf("[%s] [HTTP] Heal failed - node ID required", h.NodeID)
		http.Error(w, "node ID required", http.StatusBadRequest)
		return
	}

	log.Printf("[%s] [HTTP] Recovering node %s", h.NodeID, nodeID)
	// Mark node as healthy in fault detector
	if h.Detector != nil {
		if node, exists := h.Detector.GetNode(nodeID); exists {
			node.Status = fault.Healthy
			node.MissedHeartbeats = 0
			log.Printf("[%s] [HTTP] ✓ Node %s marked as healthy", h.NodeID, nodeID)
		} else {
			log.Printf("[%s] [HTTP] Heal failed - node %s not found in detector", h.NodeID, nodeID)
		}
	}

	w.WriteHeader(http.StatusOK)
}

// HandleDelete deletes a file from the distributed system
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] [HTTP] DELETE /api/delete/ - Delete request received from %s", h.NodeID, r.RemoteAddr)
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Only Raft leader can handle deletes
	if h.Consensus != nil && !h.Consensus.IsLeader() {
		log.Printf("[%s] [HTTP] Delete rejected - node is not Raft leader", h.NodeID)
		http.Error(w, "only Raft leader accepts deletes", http.StatusForbidden)
		return
	}

	// Extract filename from URL path
	filename := r.URL.Path[len("/api/delete/"):]
	if filename == "" {
		log.Printf("[%s] [HTTP] Delete failed - filename required", h.NodeID)
		http.Error(w, "filename required", http.StatusBadRequest)
		return
	}

	log.Printf("[%s] [HTTP] Deleting file: %s", h.NodeID, filename)

	// Delete from local storage
	if err := h.Storage.Delete(filename); err != nil {
		log.Printf("[%s] [HTTP] Delete failed - storage error: %v", h.NodeID, err)
		http.Error(w, fmt.Sprintf("failed to delete: %v", err), http.StatusInternalServerError)
		return
	}
	log.Printf("[%s] [HTTP] File %s deleted from local storage", h.NodeID, filename)

	if h.EventClock != nil {
		h.EventClock.NewEvent("DELETE")
	}

	// Replicate delete to all follower nodes (only if this node is Raft leader)
	entry := &types.LogEntry{
		Op:       "DELETE",
		Filename: filename,
	}

	// Check if this node is Raft leader
	isLeader := false
	if h.Consensus != nil {
		isLeader = h.Consensus.IsLeader()
	}

	success, err := h.Replicator.Replicate(entry, isLeader)
	if !success {
		log.Printf("[%s] [HTTP] Warning: Replication of delete failed for %s: %v\n", h.NodeID, filename, err)
	} else {
		log.Printf("[%s] [HTTP] ✓ Delete replicated successfully for %s", h.NodeID, filename)
	}

	log.Printf("[%s] [HTTP] ✓ Delete complete: %s (replication=%v)", h.NodeID, filename, success)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":  success,
		"filename": filename,
	})
}

func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}
