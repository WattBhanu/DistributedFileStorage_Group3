package api

import (
	"encoding/json"
	"net/http"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

type Handler struct {
	// TODO (Week 2): add consensus, replication, fault, timesync, storage fields during integration
}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) HandleVoteRequest(w http.ResponseWriter, r *http.Request) {
	var req types.VoteRequest
	json.NewDecoder(r.Body).Decode(&req)
	// TODO (Week 2): add consensus logic to process incoming vote request
	resp := types.VoteResponse{VoteGranted: false}
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) HandleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req types.AppendEntriesRequest
	json.NewDecoder(r.Body).Decode(&req)
	// TODO (Week 2): add consensus logic to process incoming log entries from leader
	resp := types.AppendEntriesResponse{Success: false}
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) HandleReplicate(w http.ResponseWriter, r *http.Request) {
	var entry types.LogEntry
	json.NewDecoder(r.Body).Decode(&entry)
	// TODO (Week 2): add replication logic to store incoming file chunk
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HandleSyncRequest(w http.ResponseWriter, r *http.Request) {
	// TODO (Week 2): add fault logic to send missed entries to recovering node
	json.NewEncoder(w).Encode([]types.LogEntry{})
}

func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HandleTimeSync(w http.ResponseWriter, r *http.Request) {
	// TODO (Week 2): add timesync logic to handle incoming time sync request
	w.WriteHeader(http.StatusOK)
}
