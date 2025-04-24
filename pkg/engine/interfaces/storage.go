package interfaces

import (
	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/wal"
)

// Storage defines the core storage operations interface
// This abstracts the actual storage implementation from the engine
type Storage interface {
	// Core operations
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	IsDeleted(key []byte) (bool, error)
	
	// Iterator access
	GetIterator() (iterator.Iterator, error)
	GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error)
	
	// Batch operations
	ApplyBatch(entries []*wal.Entry) error
	
	// Flushing operations
	FlushMemTables() error
	
	// Lifecycle management
	Close() error
}

// StorageManager extends Storage with management operations
type StorageManager interface {
	Storage
	
	// Memtable management
	GetMemTableSize() uint64
	IsFlushNeeded() bool
	
	// SSTable management
	GetSSTables() []string
	ReloadSSTables() error
	
	// WAL management
	RotateWAL() error
	
	// Statistics
	GetStorageStats() map[string]interface{}
}