package transaction

import (
	"context"
	"sync"

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
// This matches the interfaces.Transaction interface from pkg/engine/interfaces/transaction.go
type Transaction interface {
	// Core operations
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error

	// Iterator access
	NewIterator() iterator.Iterator
	NewRangeIterator(startKey, endKey []byte) iterator.Iterator

	// Transaction management
	Commit() error
	Rollback() error
	IsReadOnly() bool
}

// TransactionManager handles transaction lifecycle
// This matches the interfaces.TransactionManager interface from pkg/engine/interfaces/transaction.go
type TransactionManager interface {
	// Create a new transaction
	BeginTransaction(readOnly bool) (Transaction, error)

	// Get the lock used for transaction isolation
	GetRWLock() *sync.RWMutex

	// Transaction statistics
	IncrementTxCompleted()
	IncrementTxAborted()
	GetTransactionStats() map[string]interface{}
}

// Registry manages transaction lifecycle and connections
// This matches the interfaces.TxRegistry interface from pkg/engine/interfaces/transaction.go
type Registry interface {
	// Begin starts a new transaction
	Begin(ctx context.Context, eng interface{}, readOnly bool) (string, error)

	// Get retrieves a transaction by ID
	Get(txID string) (Transaction, bool)

	// Remove removes a transaction from the registry
	Remove(txID string)

	// CleanupConnection cleans up all transactions for a given connection
	CleanupConnection(connectionID string)

	// GracefulShutdown performs cleanup on shutdown
	GracefulShutdown(ctx context.Context) error
}
