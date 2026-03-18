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
	mux.HandleFunc("/api/admin/kill/", handler.HandleKillNode)
	mux.HandleFunc("/api/admin/heal/", handler.HandleHealNode)

	// node to node
	mux.HandleFunc("/internal/request-vote", handler.HandleVoteRequest)
	mux.HandleFunc("/internal/append-entries", handler.HandleAppendEntries)
	mux.HandleFunc("/internal/replicate", handler.HandleReplicate)
	mux.HandleFunc("/internal/sync-request", handler.HandleSyncRequest)
	mux.HandleFunc("/health", handler.HandleHealth)
	mux.HandleFunc("/internal/time-sync", handler.HandleTimeSync)

	return mux
}

func Start(port string, handler *Handler) error {
	mux := NewRouter(handler)
	return http.ListenAndServe(":"+port, mux)
}
