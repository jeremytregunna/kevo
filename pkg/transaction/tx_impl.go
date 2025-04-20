package transaction

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/jer/kevo/pkg/common/iterator"
	"github.com/jer/kevo/pkg/engine"
	"github.com/jer/kevo/pkg/transaction/txbuffer"
	"github.com/jer/kevo/pkg/wal"
)

// Common errors for transaction operations
var (
	ErrReadOnlyTransaction = errors.New("cannot write to a read-only transaction")
	ErrTransactionClosed   = errors.New("transaction already committed or rolled back")
	ErrInvalidEngine       = errors.New("invalid engine type")
)

// EngineTransaction uses reader-writer locks for transaction isolation
type EngineTransaction struct {
	// Reference to the main engine
	engine *engine.Engine

	// Transaction mode (ReadOnly or ReadWrite)
	mode TransactionMode

	// Buffer for transaction operations
	buffer *txbuffer.TxBuffer

	// For read-write transactions, tracks if we have the write lock
	writeLock *sync.RWMutex

	// Tracks if the transaction is still active
	active int32

	// For read-only transactions, ensures we release the read lock exactly once
	readUnlocked int32
}

// NewTransaction creates a new transaction
func NewTransaction(eng *engine.Engine, mode TransactionMode) (*EngineTransaction, error) {
	tx := &EngineTransaction{
		engine: eng,
		mode:   mode,
		buffer: txbuffer.NewTxBuffer(),
		active: 1,
	}

	// For read-write transactions, we need a write lock
	if mode == ReadWrite {
		// Get the engine's lock - we'll use the same one for all transactions
		lock := eng.GetRWLock()

		// Acquire the write lock
		lock.Lock()
		tx.writeLock = lock
	} else {
		// For read-only transactions, just acquire a read lock
		lock := eng.GetRWLock()
		lock.RLock()
		tx.writeLock = lock
	}

	return tx, nil
}

// Get retrieves a value for the given key
func (tx *EngineTransaction) Get(key []byte) ([]byte, error) {
	if atomic.LoadInt32(&tx.active) == 0 {
		return nil, ErrTransactionClosed
	}

	// First check the transaction buffer for any pending changes
	if val, found := tx.buffer.Get(key); found {
		if val == nil {
			// This is a deletion marker
			return nil, engine.ErrKeyNotFound
		}
		return val, nil
	}

	// Not in the buffer, get from the underlying engine
	return tx.engine.Get(key)
}

// Put adds or updates a key-value pair
func (tx *EngineTransaction) Put(key, value []byte) error {
	if atomic.LoadInt32(&tx.active) == 0 {
		return ErrTransactionClosed
	}

	if tx.mode == ReadOnly {
		return ErrReadOnlyTransaction
	}

	// Buffer the change - it will be applied on commit
	tx.buffer.Put(key, value)
	return nil
}

// Delete removes a key
func (tx *EngineTransaction) Delete(key []byte) error {
	if atomic.LoadInt32(&tx.active) == 0 {
		return ErrTransactionClosed
	}

	if tx.mode == ReadOnly {
		return ErrReadOnlyTransaction
	}

	// Buffer the deletion - it will be applied on commit
	tx.buffer.Delete(key)
	return nil
}

// NewIterator returns an iterator that first reads from the transaction buffer
// and then from the underlying engine
func (tx *EngineTransaction) NewIterator() iterator.Iterator {
	if atomic.LoadInt32(&tx.active) == 0 {
		// Return an empty iterator if transaction is closed
		return &emptyIterator{}
	}

	// Get the engine iterator for the entire keyspace
	engineIter, err := tx.engine.GetIterator()
	if err != nil {
		// If we can't get an engine iterator, return a buffer-only iterator
		return tx.buffer.NewIterator()
	}

	// If there are no changes in the buffer, just use the engine's iterator
	if tx.buffer.Size() == 0 {
		return engineIter
	}

	// Create a transaction iterator that merges buffer changes with engine state
	return newTransactionIterator(tx.buffer, engineIter)
}

// NewRangeIterator returns an iterator limited to a specific key range
func (tx *EngineTransaction) NewRangeIterator(startKey, endKey []byte) iterator.Iterator {
	if atomic.LoadInt32(&tx.active) == 0 {
		// Return an empty iterator if transaction is closed
		return &emptyIterator{}
	}

	// Get the engine iterator for the range
	engineIter, err := tx.engine.GetRangeIterator(startKey, endKey)
	if err != nil {
		// If we can't get an engine iterator, use a buffer-only iterator
		// and apply range bounds to it
		bufferIter := tx.buffer.NewIterator()
		return newRangeIterator(bufferIter, startKey, endKey)
	}

	// If there are no changes in the buffer, just use the engine's range iterator
	if tx.buffer.Size() == 0 {
		return engineIter
	}

	// Create a transaction iterator that merges buffer changes with engine state
	mergedIter := newTransactionIterator(tx.buffer, engineIter)

	// Apply range constraints
	return newRangeIterator(mergedIter, startKey, endKey)
}

// transactionIterator merges a transaction buffer with the engine state
type transactionIterator struct {
	bufferIter *txbuffer.Iterator
	engineIter iterator.Iterator
	currentKey []byte
	isValid    bool
	isBuffer   bool // true if current position is from buffer
}

// newTransactionIterator creates a new iterator that merges buffer and engine state
func newTransactionIterator(buffer *txbuffer.TxBuffer, engineIter iterator.Iterator) *transactionIterator {
	return &transactionIterator{
		bufferIter: buffer.NewIterator(),
		engineIter: engineIter,
		isValid:    false,
	}
}

// SeekToFirst positions at the first key in either the buffer or engine
func (it *transactionIterator) SeekToFirst() {
	it.bufferIter.SeekToFirst()
	it.engineIter.SeekToFirst()
	it.selectNext()
}

// SeekToLast positions at the last key in either the buffer or engine
func (it *transactionIterator) SeekToLast() {
	it.bufferIter.SeekToLast()
	it.engineIter.SeekToLast()
	it.selectPrev()
}

// Seek positions at the first key >= target
func (it *transactionIterator) Seek(target []byte) bool {
	it.bufferIter.Seek(target)
	it.engineIter.Seek(target)
	it.selectNext()
	return it.isValid
}

// Next advances to the next key
func (it *transactionIterator) Next() bool {
	// If we're currently at a buffer key, advance it
	if it.isValid && it.isBuffer {
		it.bufferIter.Next()
	} else if it.isValid {
		// If we're at an engine key, advance it
		it.engineIter.Next()
	}

	it.selectNext()
	return it.isValid
}

// Key returns the current key
func (it *transactionIterator) Key() []byte {
	if !it.isValid {
		return nil
	}

	return it.currentKey
}

// Value returns the current value
func (it *transactionIterator) Value() []byte {
	if !it.isValid {
		return nil
	}

	if it.isBuffer {
		return it.bufferIter.Value()
	}

	return it.engineIter.Value()
}

// Valid returns true if the iterator is valid
func (it *transactionIterator) Valid() bool {
	return it.isValid
}

// IsTombstone returns true if the current entry is a deletion marker
func (it *transactionIterator) IsTombstone() bool {
	if !it.isValid {
		return false
	}

	if it.isBuffer {
		return it.bufferIter.IsTombstone()
	}

	return it.engineIter.IsTombstone()
}

// selectNext finds the next valid position in the merged view
func (it *transactionIterator) selectNext() {
	// First check if either iterator is valid
	bufferValid := it.bufferIter.Valid()
	engineValid := it.engineIter.Valid()

	if !bufferValid && !engineValid {
		// Neither is valid, so we're done
		it.isValid = false
		it.currentKey = nil
		it.isBuffer = false
		return
	}

	if !bufferValid {
		// Only engine is valid, so use it
		it.isValid = true
		it.currentKey = it.engineIter.Key()
		it.isBuffer = false
		return
	}

	if !engineValid {
		// Only buffer is valid, so use it
		// Check if this is a deletion marker
		if it.bufferIter.IsTombstone() {
			// Skip the tombstone and move to the next valid position
			it.bufferIter.Next()
			it.selectNext() // Recursively find the next valid position
			return
		}

		it.isValid = true
		it.currentKey = it.bufferIter.Key()
		it.isBuffer = true
		return
	}

	// Both are valid, so compare keys
	bufferKey := it.bufferIter.Key()
	engineKey := it.engineIter.Key()

	cmp := bytes.Compare(bufferKey, engineKey)

	if cmp < 0 {
		// Buffer key is smaller, use it
		// Check if this is a deletion marker
		if it.bufferIter.IsTombstone() {
			// Skip the tombstone
			it.bufferIter.Next()
			it.selectNext() // Recursively find the next valid position
			return
		}

		it.isValid = true
		it.currentKey = bufferKey
		it.isBuffer = true
	} else if cmp > 0 {
		// Engine key is smaller, use it
		it.isValid = true
		it.currentKey = engineKey
		it.isBuffer = false
	} else {
		// Keys are the same, buffer takes precedence
		// If buffer has a tombstone, we need to skip both
		if it.bufferIter.IsTombstone() {
			// Skip both iterators for this key
			it.bufferIter.Next()
			it.engineIter.Next()
			it.selectNext() // Recursively find the next valid position
			return
		}

		it.isValid = true
		it.currentKey = bufferKey
		it.isBuffer = true

		// Need to advance engine iterator to avoid duplication
		it.engineIter.Next()
	}
}

// selectPrev finds the previous valid position in the merged view
// This is a fairly inefficient implementation for now
func (it *transactionIterator) selectPrev() {
	// This implementation is not efficient but works for now
	// We actually just rebuild the full ordering and scan to the end
	it.SeekToFirst()

	// If already invalid, just return
	if !it.isValid {
		return
	}

	// Scan to the last key
	var lastKey []byte
	var isBuffer bool

	for it.isValid {
		lastKey = it.currentKey
		isBuffer = it.isBuffer
		it.Next()
	}

	// Reposition at the last key we found
	if lastKey != nil {
		it.isValid = true
		it.currentKey = lastKey
		it.isBuffer = isBuffer
	}
}

// rangeIterator applies range bounds to an existing iterator
type rangeIterator struct {
	iterator.Iterator
	startKey []byte
	endKey   []byte
}

// newRangeIterator creates a new range-limited iterator
func newRangeIterator(iter iterator.Iterator, startKey, endKey []byte) *rangeIterator {
	ri := &rangeIterator{
		Iterator: iter,
	}

	// Make copies of bounds
	if startKey != nil {
		ri.startKey = make([]byte, len(startKey))
		copy(ri.startKey, startKey)
	}

	if endKey != nil {
		ri.endKey = make([]byte, len(endKey))
		copy(ri.endKey, endKey)
	}

	return ri
}

// SeekToFirst seeks to the range start or the first key
func (ri *rangeIterator) SeekToFirst() {
	if ri.startKey != nil {
		ri.Iterator.Seek(ri.startKey)
	} else {
		ri.Iterator.SeekToFirst()
	}
	ri.checkBounds()
}

// Seek seeks to the target or range start
func (ri *rangeIterator) Seek(target []byte) bool {
	// If target is before range start, use range start
	if ri.startKey != nil && bytes.Compare(target, ri.startKey) < 0 {
		target = ri.startKey
	}

	// If target is at or after range end, fail
	if ri.endKey != nil && bytes.Compare(target, ri.endKey) >= 0 {
		return false
	}

	if ri.Iterator.Seek(target) {
		return ri.checkBounds()
	}
	return false
}

// Next advances to the next key within bounds
func (ri *rangeIterator) Next() bool {
	if !ri.checkBounds() {
		return false
	}

	if !ri.Iterator.Next() {
		return false
	}

	return ri.checkBounds()
}

// Valid checks if the iterator is valid and within bounds
func (ri *rangeIterator) Valid() bool {
	return ri.Iterator.Valid() && ri.checkBounds()
}

// checkBounds ensures the current position is within range bounds
func (ri *rangeIterator) checkBounds() bool {
	if !ri.Iterator.Valid() {
		return false
	}

	// Check start bound
	if ri.startKey != nil && bytes.Compare(ri.Iterator.Key(), ri.startKey) < 0 {
		return false
	}

	// Check end bound
	if ri.endKey != nil && bytes.Compare(ri.Iterator.Key(), ri.endKey) >= 0 {
		return false
	}

	return true
}

// Commit makes all changes permanent
func (tx *EngineTransaction) Commit() error {
	// Only proceed if the transaction is still active
	if !atomic.CompareAndSwapInt32(&tx.active, 1, 0) {
		return ErrTransactionClosed
	}

	var err error

	// For read-only transactions, just release the read lock
	if tx.mode == ReadOnly {
		tx.releaseReadLock()

		// Track transaction completion
		tx.engine.IncrementTxCompleted()
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
		err = tx.engine.ApplyBatch(walBatch)
	}

	// Release the write lock
	if tx.writeLock != nil {
		tx.writeLock.Unlock()
		tx.writeLock = nil
	}

	// Track transaction completion
	tx.engine.IncrementTxCompleted()

	return err
}

// Rollback discards all transaction changes
func (tx *EngineTransaction) Rollback() error {
	// Only proceed if the transaction is still active
	if !atomic.CompareAndSwapInt32(&tx.active, 1, 0) {
		return ErrTransactionClosed
	}

	// Clear the buffer
	tx.buffer.Clear()

	// Release locks based on transaction mode
	if tx.mode == ReadOnly {
		tx.releaseReadLock()
	} else {
		// Release write lock
		if tx.writeLock != nil {
			tx.writeLock.Unlock()
			tx.writeLock = nil
		}
	}

	// Track transaction abort in engine stats
	tx.engine.IncrementTxAborted()

	return nil
}

// IsReadOnly returns true if this is a read-only transaction
func (tx *EngineTransaction) IsReadOnly() bool {
	return tx.mode == ReadOnly
}

// releaseReadLock safely releases the read lock for read-only transactions
func (tx *EngineTransaction) releaseReadLock() {
	// Only release once to avoid panics from multiple unlocks
	if atomic.CompareAndSwapInt32(&tx.readUnlocked, 0, 1) {
		if tx.writeLock != nil {
			tx.writeLock.RUnlock()
			tx.writeLock = nil
		}
	}
}

// Simple empty iterator implementation for closed transactions
type emptyIterator struct{}

func (e *emptyIterator) SeekToFirst()      {}
func (e *emptyIterator) SeekToLast()       {}
func (e *emptyIterator) Seek([]byte) bool  { return false }
func (e *emptyIterator) Next() bool        { return false }
func (e *emptyIterator) Key() []byte       { return nil }
func (e *emptyIterator) Value() []byte     { return nil }
func (e *emptyIterator) Valid() bool       { return false }
func (e *emptyIterator) IsTombstone() bool { return false }
