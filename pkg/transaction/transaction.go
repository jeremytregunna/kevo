package transaction

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/common/iterator/bounded"
	"github.com/KevoDB/kevo/pkg/common/iterator/composite"
	"github.com/KevoDB/kevo/pkg/wal"
)

// TransactionImpl implements the Transaction interface
type TransactionImpl struct {
	// Reference to the storage backend
	storage StorageBackend

	// Transaction mode (ReadOnly or ReadWrite)
	mode TransactionMode

	// Buffer for transaction operations
	buffer *Buffer

	// Tracks if the transaction is still active
	active atomic.Bool

	// For read-only transactions, tracks if we have a read lock
	hasReadLock atomic.Bool

	// For read-write transactions, tracks if we have the write lock
	hasWriteLock atomic.Bool

	// Lock for transaction-level synchronization
	mu sync.Mutex

	// RWLock for transaction isolation
	rwLock *sync.RWMutex

	// Stats collector
	stats StatsCollector

	// TTL tracking
	creationTime   time.Time
	lastActiveTime time.Time
	ttl            time.Duration
}

// StatsCollector defines the interface for collecting transaction statistics
type StatsCollector interface {
	IncrementTxCompleted()
	IncrementTxAborted()
}

// Get retrieves a value for the given key
func (tx *TransactionImpl) Get(key []byte) ([]byte, error) {
	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if transaction is still active
	if !tx.active.Load() {
		return nil, ErrTransactionClosed
	}

	// Update last active time
	tx.lastActiveTime = time.Now()

	// First check the transaction buffer for any pending changes
	if val, found := tx.buffer.Get(key); found {
		if val == nil {
			// This is a deletion marker
			return nil, ErrKeyNotFound
		}
		return val, nil
	}

	// Not in the buffer, get from the underlying storage
	return tx.storage.Get(key)
}

// Put adds or updates a key-value pair
func (tx *TransactionImpl) Put(key, value []byte) error {
	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if transaction is still active
	if !tx.active.Load() {
		return ErrTransactionClosed
	}

	// Update last active time
	tx.lastActiveTime = time.Now()

	// Check if transaction is read-only
	if tx.mode == ReadOnly {
		return ErrReadOnlyTransaction
	}

	// Buffer the change - it will be applied on commit
	tx.buffer.Put(key, value)
	return nil
}

// Delete removes a key
func (tx *TransactionImpl) Delete(key []byte) error {
	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if transaction is still active
	if !tx.active.Load() {
		return ErrTransactionClosed
	}

	// Update last active time
	tx.lastActiveTime = time.Now()

	// Check if transaction is read-only
	if tx.mode == ReadOnly {
		return ErrReadOnlyTransaction
	}

	// Buffer the deletion - it will be applied on commit
	tx.buffer.Delete(key)
	return nil
}

// NewIterator returns an iterator over the entire keyspace
func (tx *TransactionImpl) NewIterator() iterator.Iterator {
	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if transaction is still active
	if !tx.active.Load() {
		// Return an empty iterator
		return &emptyIterator{}
	}

	// Update last active time
	tx.lastActiveTime = time.Now()

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

	// Use composite hierarchical iterator
	return composite.NewHierarchicalIterator([]iterator.Iterator{bufferIter, storageIter})
}

// NewRangeIterator returns an iterator limited to a specific key range
func (tx *TransactionImpl) NewRangeIterator(startKey, endKey []byte) iterator.Iterator {
	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if transaction is still active
	if !tx.active.Load() {
		// Return an empty iterator
		return &emptyIterator{}
	}

	// Update last active time
	tx.lastActiveTime = time.Now()

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

// emptyIterator is a simple iterator implementation that returns no results
type emptyIterator struct{}

func (it *emptyIterator) SeekToFirst()      {}
func (it *emptyIterator) SeekToLast()       {}
func (it *emptyIterator) Seek([]byte) bool  { return false }
func (it *emptyIterator) Next() bool        { return false }
func (it *emptyIterator) Key() []byte       { return nil }
func (it *emptyIterator) Value() []byte     { return nil }
func (it *emptyIterator) Valid() bool       { return false }
func (it *emptyIterator) IsTombstone() bool { return false }

// Commit makes all changes permanent
func (tx *TransactionImpl) Commit() error {
	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Only proceed if the transaction is still active
	if !tx.active.CompareAndSwap(true, false) {
		return ErrTransactionClosed
	}

	var err error

	// For read-only transactions, just release the read lock
	if tx.mode == ReadOnly {
		tx.releaseReadLock()

		// Track transaction completion
		if tx.stats != nil {
			tx.stats.IncrementTxCompleted()
		}

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
	if tx.stats != nil {
		tx.stats.IncrementTxCompleted()
	}

	return err
}

// Rollback discards all transaction changes
func (tx *TransactionImpl) Rollback() error {
	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Only proceed if the transaction is still active
	if !tx.active.CompareAndSwap(true, false) {
		return ErrTransactionClosed
	}

	// Clear the buffer
	tx.buffer.Clear()

	// Release locks based on transaction mode
	if tx.mode == ReadOnly {
		tx.releaseReadLock()
	} else {
		tx.releaseWriteLock()
	}

	// Track transaction abort
	if tx.stats != nil {
		tx.stats.IncrementTxAborted()
	}

	return nil
}

// IsReadOnly returns true if this is a read-only transaction
func (tx *TransactionImpl) IsReadOnly() bool {
	return tx.mode == ReadOnly
}

// releaseReadLock safely releases the read lock for read-only transactions
func (tx *TransactionImpl) releaseReadLock() {
	if tx.hasReadLock.CompareAndSwap(true, false) {
		tx.rwLock.RUnlock()
	}
}

// releaseWriteLock safely releases the write lock for read-write transactions
func (tx *TransactionImpl) releaseWriteLock() {
	if tx.hasWriteLock.CompareAndSwap(true, false) {
		tx.rwLock.Unlock()
	}
}
