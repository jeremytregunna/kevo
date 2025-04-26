package interfaces

import (
	"errors"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/stats"
	"github.com/KevoDB/kevo/pkg/wal"
)

// Engine defines the core interface for the storage engine
// This is the primary interface clients will interact with
type Engine interface {
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

	// Transaction management
	BeginTransaction(readOnly bool) (Transaction, error)

	// Maintenance operations
	FlushImMemTables() error
	TriggerCompaction() error
	CompactRange(startKey, endKey []byte) error

	// Statistics
	GetStats() map[string]interface{}
	GetCompactionStats() (map[string]interface{}, error)

	// Lifecycle management
	Close() error
}

// Components is a struct containing all the components needed by the engine
// This allows for dependency injection and easier testing
type Components struct {
	Storage        StorageManager
	TransactionMgr TransactionManager
	CompactionMgr  CompactionManager
	StatsCollector stats.Collector
}

// Engine related errors
var (
	// ErrEngineClosed is returned when operations are performed on a closed engine
	ErrEngineClosed = errors.New("engine is closed")

	// ErrKeyNotFound is returned when a key is not found
	ErrKeyNotFound = errors.New("key not found")
)
