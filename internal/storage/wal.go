package storage

import (
	"encoding/json"
	"os"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

type WAL struct {
	path string
}

func NewWAL(path string) *WAL {
	return &WAL{path: path}
}

func (w *WAL) Append(entry types.LogEntry) error {
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(entry)
}

func (w *WAL) ReadFrom(index int64) ([]types.LogEntry, error) {
	f, err := os.Open(w.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []types.LogEntry
	decoder := json.NewDecoder(f)
	for {
		var entry types.LogEntry
		if err := decoder.Decode(&entry); err != nil {
			break
		}
		if entry.Index >= index {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}
