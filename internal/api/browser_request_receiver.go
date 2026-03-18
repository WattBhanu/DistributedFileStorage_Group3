package api

import (
	"encoding/json"
	"io"
	"net/http"
)

func (h *Handler) HandleUpload(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		return
	}
	r.ParseMultipartForm(32 << 20)
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "failed to read file", http.StatusBadRequest)
		return
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "failed to read file", http.StatusInternalServerError)
		return
	}
	// TODO (Week 2): add replication logic to distribute file to nodes
	// TODO (Week 2): add consensus logic to commit the write
	_ = data
	_ = header
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HandleListFiles(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	// TODO (Week 2): add storage logic to fetch and return file list
	json.NewEncoder(w).Encode([]string{})
}

func (h *Handler) HandleDownload(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	// TODO (Week 2): add storage logic to fetch and serve requested file
	filename := r.URL.Path[len("/api/files/"):]
	_ = filename
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func (h *Handler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	// TODO (Week 2): add consensus logic to return current node state
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) HandleKillNode(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	// TODO (Week 2): add fault logic to simulate node failure
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HandleHealNode(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	// TODO (Week 2): add fault logic to simulate node recovery
	w.WriteHeader(http.StatusOK)
}

func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}
