package api

import (
	"net/http"
)

func NewRouter(handler *Handler) *http.ServeMux {
	mux := http.NewServeMux()

	// browser facing
	mux.HandleFunc("/api/upload", handler.HandleUpload)
	mux.HandleFunc("/api/files", handler.HandleListFiles)
	mux.HandleFunc("/api/files/", handler.HandleDownload)
	mux.HandleFunc("/api/status", handler.HandleStatus)
	mux.HandleFunc("/api/metrics", handler.HandleGetMetrics)
	mux.HandleFunc("/api/timesync", handler.HandleGetTimeSyncMetrics)
	mux.HandleFunc("/api/delete/", handler.HandleDelete)
	mux.HandleFunc("/api/admin/kill/", handler.HandleKillNode)
	mux.HandleFunc("/api/admin/heal/", handler.HandleHealNode)

	// node to node
	mux.HandleFunc("/internal/request-vote", handler.HandleVoteRequest)
	mux.HandleFunc("/internal/append-entries", handler.HandleAppendEntries)
	mux.HandleFunc("/internal/replicate", handler.HandleReplicate)
	mux.HandleFunc("/internal/sync-request", handler.HandleSyncRequest)
	mux.HandleFunc("/health", handler.HandleHealth)
	mux.HandleFunc("/internal/time-sync", handler.HandleTimeSync)
	mux.HandleFunc("/internal/time-adjust", handler.HandleTimeAdjust)
	mux.HandleFunc("/internal/cristian-time", handler.HandleCristianTime)
	mux.HandleFunc("/internal/node-recovered", handler.HandleNodeRecovered)

	return mux
}

func Start(port string, handler *Handler) error {
	mux := NewRouter(handler)
	return http.ListenAndServe(":"+port, mux)
}
