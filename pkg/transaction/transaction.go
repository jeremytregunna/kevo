package transaction

import (
	"github.com/KevoDB/kevo/pkg/common/iterator"
)

// TransactionMode defines the transaction access mode (ReadOnly or ReadWrite)
type TransactionMode int

const (
	// ReadOnly transactions only read from the database
	ReadOnly TransactionMode = iota

	// ReadWrite transactions can both read and write to the database
	ReadWrite
)

// Transaction represents a database transaction that provides ACID guarantees
// It follows an concurrency model using reader-writer locks
type Transaction interface {
	// Get retrieves a value for the given key
	Get(key []byte) ([]byte, error)

	// Put adds or updates a key-value pair (only for ReadWrite transactions)
	Put(key, value []byte) error

	// Delete removes a key (only for ReadWrite transactions)
	Delete(key []byte) error

	// NewIterator returns an iterator for all keys in the transaction
	NewIterator() iterator.Iterator

	// NewRangeIterator returns an iterator limited to the given key range
	NewRangeIterator(startKey, endKey []byte) iterator.Iterator

	// Commit makes all changes permanent
	// For ReadOnly transactions, this just releases resources
	Commit() error

	// Rollback discards all transaction changes
	Rollback() error

	// IsReadOnly returns true if this is a read-only transaction
	IsReadOnly() bool
}
