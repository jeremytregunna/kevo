package transaction

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/common/iterator/bounded"
	"github.com/KevoDB/kevo/pkg/common/iterator/composite"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	engineIterator "github.com/KevoDB/kevo/pkg/engine/iterator"
	"github.com/KevoDB/kevo/pkg/wal"
)

// Common errors for transaction operations
var (
	ErrReadOnlyTransaction = errors.New("cannot write to a read-only transaction")
	ErrTransactionClosed   = errors.New("transaction already committed or rolled back")
	ErrKeyNotFound         = errors.New("key not found")
)

// Transaction implements the interfaces.Transaction interface
type Transaction struct {
	// Reference to the transaction manager
	manager interfaces.TransactionManager

	// Reference to the storage
	storage interfaces.StorageManager

	// Read-only flag
	readOnly bool

	// Buffer for transaction operations
	buffer *Buffer

	// Transaction state
	active atomic.Bool

	// For read-only transactions, tracks if we have a read lock
	hasReadLock atomic.Bool

	// For read-write transactions, tracks if we have the write lock
	hasWriteLock atomic.Bool

	// Iterator factory
	iterFactory *engineIterator.Factory

	// Start time for tracking latency
	startTime time.Time
}

// NewTransaction creates a new transaction
func NewTransaction(manager interfaces.TransactionManager, storage interfaces.StorageManager, readOnly bool) *Transaction {
	tx := &Transaction{
		manager:     manager,
		storage:     storage,
		readOnly:    readOnly,
		buffer:      NewBuffer(),
		iterFactory: engineIterator.NewFactory(),
		startTime:   time.Now(),
	}

	// Set active flag
	tx.active.Store(true)

	// Acquire appropriate lock
	lock := manager.GetRWLock()
	if readOnly {
		lock.RLock()
		tx.hasReadLock.Store(true)
	} else {
		lock.Lock()
		tx.hasWriteLock.Store(true)
	}

	return tx
}

// Get retrieves a value for the given key
func (tx *Transaction) Get(key []byte) ([]byte, error) {
	// Check if transaction is still active
	if !tx.active.Load() {
		return nil, ErrTransactionClosed
	}

	// First check the transaction buffer for any pending changes
	if val, found := tx.buffer.Get(key); found {
		if val == nil {
			// This is a deletion marker
			return nil, ErrKeyNotFound
		}
		return val, nil
	}

	// Not in the buffer, get from the underlying storage
	val, err := tx.storage.Get(key)

	return val, err
}

// Put adds or updates a key-value pair
func (tx *Transaction) Put(key, value []byte) error {
	// Check if transaction is still active
	if !tx.active.Load() {
		return ErrTransactionClosed
	}

	// Check if transaction is read-only
	if tx.readOnly {
		return ErrReadOnlyTransaction
	}

	// Buffer the change - it will be applied on commit
	tx.buffer.Put(key, value)
	return nil
}

// Delete removes a key
func (tx *Transaction) Delete(key []byte) error {
	// Check if transaction is still active
	if !tx.active.Load() {
		return ErrTransactionClosed
	}

	// Check if transaction is read-only
	if tx.readOnly {
		return ErrReadOnlyTransaction
	}

	// Buffer the deletion - it will be applied on commit
	tx.buffer.Delete(key)
	return nil
}

// NewIterator returns an iterator over the entire keyspace
func (tx *Transaction) NewIterator() iterator.Iterator {
	// Check if transaction is still active
	if !tx.active.Load() {
		// Return an empty iterator from the engine iterator package
		return engineIterator.NewFactory().CreateIterator(nil, nil)
	}

	// Get the storage iterator
	storageIter, err := tx.storage.GetIterator()
	if err != nil {
		// If we can't get a storage iterator, return a buffer-only iterator
		return tx.buffer.NewIterator()
	}

	// If there are no changes in the buffer, just use the storage's iterator
	if tx.buffer.Size() == 0 {
		return storageIter
	}

	// Merge buffer and storage iterators
	bufferIter := tx.buffer.NewIterator()

	// Using composite.NewHierarchicalIterator from common/iterator/composite
	// with the transaction buffer having higher priority
	return composite.NewHierarchicalIterator([]iterator.Iterator{bufferIter, storageIter})
}

// NewRangeIterator returns an iterator limited to a specific key range
func (tx *Transaction) NewRangeIterator(startKey, endKey []byte) iterator.Iterator {
	// Check if transaction is still active
	if !tx.active.Load() {
		// Return an empty iterator from the engine iterator package
		return engineIterator.NewFactory().CreateIterator(nil, nil)
	}

	// Get the storage iterator for the range
	storageIter, err := tx.storage.GetRangeIterator(startKey, endKey)
	if err != nil {
		// If we can't get a storage iterator, use a bounded buffer iterator
		bufferIter := tx.buffer.NewIterator()
		return bounded.NewBoundedIterator(bufferIter, startKey, endKey)
	}

	// If there are no changes in the buffer, just use the storage's range iterator
	if tx.buffer.Size() == 0 {
		return storageIter
	}

	// Create a bounded buffer iterator
	bufferIter := tx.buffer.NewIterator()
	boundedBufferIter := bounded.NewBoundedIterator(bufferIter, startKey, endKey)

	// Merge the bounded buffer iterator with the storage range iterator
	return composite.NewHierarchicalIterator([]iterator.Iterator{boundedBufferIter, storageIter})
}

// Commit makes all changes permanent
func (tx *Transaction) Commit() error {
	// Only proceed if the transaction is still active
	if !tx.active.CompareAndSwap(true, false) {
		return ErrTransactionClosed
	}

	var err error

	// For read-only transactions, just release the read lock
	if tx.readOnly {
		tx.releaseReadLock()

		// Track transaction completion
		tx.manager.IncrementTxCompleted()

		return nil
	}

	// For read-write transactions, apply the changes
	if tx.buffer.Size() > 0 {
		// Get operations from the buffer
		ops := tx.buffer.Operations()

		// Create a batch for all operations
		walBatch := make([]*wal.Entry, 0, len(ops))

		// Build WAL entries for each operation
		for _, op := range ops {
			if op.IsDelete {
				// Create delete entry
				walBatch = append(walBatch, &wal.Entry{
					Type: wal.OpTypeDelete,
					Key:  op.Key,
				})
			} else {
				// Create put entry
				walBatch = append(walBatch, &wal.Entry{
					Type:  wal.OpTypePut,
					Key:   op.Key,
					Value: op.Value,
				})
			}
		}

		// Apply the batch atomically
		err = tx.storage.ApplyBatch(walBatch)
	}

	// Release the write lock
	tx.releaseWriteLock()

	// Track transaction completion
	tx.manager.IncrementTxCompleted()

	return err
}

// Rollback discards all transaction changes
func (tx *Transaction) Rollback() error {
	// Only proceed if the transaction is still active
	if !tx.active.CompareAndSwap(true, false) {
		return ErrTransactionClosed
	}

	// Clear the buffer
	tx.buffer.Clear()

	// Release locks based on transaction mode
	if tx.readOnly {
		tx.releaseReadLock()
	} else {
		tx.releaseWriteLock()
	}

	// Track transaction abort
	tx.manager.IncrementTxAborted()

	return nil
}

// IsReadOnly returns true if this is a read-only transaction
func (tx *Transaction) IsReadOnly() bool {
	return tx.readOnly
}

// releaseReadLock safely releases the read lock for read-only transactions
func (tx *Transaction) releaseReadLock() {
	if tx.hasReadLock.CompareAndSwap(true, false) {
		tx.manager.GetRWLock().RUnlock()
	}
}

// releaseWriteLock safely releases the write lock for read-write transactions
func (tx *Transaction) releaseWriteLock() {
	if tx.hasWriteLock.CompareAndSwap(true, false) {
		tx.manager.GetRWLock().Unlock()
	}
}
