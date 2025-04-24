package interfaces

import (
	"sync"

	"github.com/KevoDB/kevo/pkg/common/iterator"
)

// Transaction defines the interface for a database transaction
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