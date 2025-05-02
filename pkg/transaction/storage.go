package transaction

import (
	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/wal"
)

// StorageBackend defines the minimal interface that a storage backend must implement
// to be used with transactions
type StorageBackend interface {
	// Get retrieves a value for the given key
	Get(key []byte) ([]byte, error)

	// ApplyBatch applies a batch of operations atomically
	ApplyBatch(entries []*wal.Entry) error

	// GetIterator returns an iterator over all keys
	GetIterator() (iterator.Iterator, error)

	// GetRangeIterator returns an iterator limited to a specific key range
	GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error)
}
