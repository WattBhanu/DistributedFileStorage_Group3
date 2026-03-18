package storage

import (
	"github.com/WattBhanu/DistributedFileStorage_Group3/internal/types"
)

type Storage interface {
	Write(filename string, data []byte) error
	Read(filename string) ([]byte, error)
	Delete(filename string) error
	List() ([]types.FileMetadata, error)
	GetLogFrom(index int64) ([]types.LogEntry, error)
}

type FileStorage struct {
	dataDir string
}

func New(dataDir string) *FileStorage {
	return &FileStorage{dataDir: dataDir}
}
