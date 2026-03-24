package storage

import (
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

type Storage interface {
	Write(filename string, data []byte) error
	Read(filename string) ([]byte, error)
	Delete(filename string) error
	List() ([]types.FileMetadata, error)
	GetLogFrom(index int64) ([]types.LogEntry, error)
	GetChecksum(filename string) (string, error)
}

type FileStorage struct {
	dataDir string
}

func New(dataDir string) *FileStorage {
	return &FileStorage{dataDir: dataDir}
}

// Write writes data to a file
func (fs *FileStorage) Write(filename string, data []byte) error {
	path := filepath.Join(fs.dataDir, filename)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		log.Printf("[STORAGE] Failed to create directory for %s: %v", filename, err)
		return err
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		log.Printf("[STORAGE] Failed to write %s (%d bytes): %v", filename, len(data), err)
		return err
	}
	log.Printf("[STORAGE] ✓ Wrote %s (%d bytes) to %s", filename, len(data), path)
	return nil
}

// Read reads data from a file
func (fs *FileStorage) Read(filename string) ([]byte, error) {
	path := filepath.Join(fs.dataDir, filename)
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[STORAGE] Failed to read %s: %v", filename, err)
		return nil, err
	}
	log.Printf("[STORAGE] ✓ Read %s (%d bytes) from %s", filename, len(data), path)
	return data, nil
}

// Delete deletes a file
func (fs *FileStorage) Delete(filename string) error {
	path := filepath.Join(fs.dataDir, filename)
	if err := os.Remove(path); err != nil {
		log.Printf("[STORAGE] Failed to delete %s: %v", filename, err)
		return err
	}
	log.Printf("[STORAGE] ✓ Deleted %s from %s", filename, path)
	return nil
}

// List lists all files in storage
func (fs *FileStorage) List() ([]types.FileMetadata, error) {
	var files []types.FileMetadata
	err := filepath.Walk(fs.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relPath, _ := filepath.Rel(fs.dataDir, path)
			files = append(files, types.FileMetadata{
				Filename:  relPath,
				Size:      info.Size(),
				CreatedAt: info.ModTime().UnixNano(),
			})
		}
		return nil
	})
	if err != nil {
		log.Printf("[STORAGE] Failed to list files in %s: %v", fs.dataDir, err)
	} else {
		log.Printf("[STORAGE] Listed %d files in %s", len(files), fs.dataDir)
	}
	return files, err
}

// GetLogFrom returns log entries from index (placeholder)
func (fs *FileStorage) GetLogFrom(index int64) ([]types.LogEntry, error) {
	// TODO: Implement WAL-based log retrieval
	return []types.LogEntry{}, nil
}

// GetChecksum calculates SHA256 checksum of a file
func (fs *FileStorage) GetChecksum(filename string) (string, error) {
	data, err := fs.Read(filename)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash), nil
}
