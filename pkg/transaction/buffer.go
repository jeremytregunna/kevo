package transaction

import (
	"bytes"
	"sort"
	"sync"
)

// Operation represents a single transaction operation (put or delete)
type Operation struct {
	// Key is the key being operated on
	Key []byte

	// Value is the value to set (nil for delete operations)
	Value []byte

	// IsDelete is true for deletion operations
	IsDelete bool
}

// Buffer maintains a buffer of transaction operations before they are committed
type Buffer struct {
	// Maps string(key) -> Operation for fast lookups
	operations map[string]*Operation

	// Mutex for concurrent access
	mu sync.RWMutex
}

// NewBuffer creates a new transaction buffer
func NewBuffer() *Buffer {
	return &Buffer{
		operations: make(map[string]*Operation),
	}
}

// Put adds a key-value pair to the transaction buffer
func (b *Buffer) Put(key, value []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create safe copies of key and value
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	// Store in the operations map
	b.operations[string(keyCopy)] = &Operation{
		Key:      keyCopy,
		Value:    valueCopy,
		IsDelete: false,
	}
}

// Delete marks a key as deleted in the transaction buffer
func (b *Buffer) Delete(key []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create a safe copy of the key
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	// Store in the operations map
	b.operations[string(keyCopy)] = &Operation{
		Key:      keyCopy,
		Value:    nil,
		IsDelete: true,
	}
}

// Get retrieves a value from the transaction buffer
// Returns (value, true) if found, (nil, false) if not found
func (b *Buffer) Get(key []byte) ([]byte, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	op, found := b.operations[string(key)]
	if !found {
		return nil, false
	}

	if op.IsDelete {
		return nil, true // Key exists but is marked for deletion
	}

	// Return a copy of the value to prevent modification
	valueCopy := make([]byte, len(op.Value))
	copy(valueCopy, op.Value)
	return valueCopy, true
}

// Operations returns a sorted list of all operations in the transaction
func (b *Buffer) Operations() []*Operation {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Create a slice of operations
	ops := make([]*Operation, 0, len(b.operations))
	for _, op := range b.operations {
		// Make a copy of the operation
		opCopy := &Operation{
			Key:      make([]byte, len(op.Key)),
			IsDelete: op.IsDelete,
		}
		copy(opCopy.Key, op.Key)

		if op.Value != nil {
			opCopy.Value = make([]byte, len(op.Value))
			copy(opCopy.Value, op.Value)
		}

		ops = append(ops, opCopy)
	}

	// Sort by key for consistent application order
	sort.Slice(ops, func(i, j int) bool {
		return bytes.Compare(ops[i].Key, ops[j].Key) < 0
	})

	return ops
}

// Clear empties the transaction buffer
func (b *Buffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.operations = make(map[string]*Operation)
}

// Size returns the number of operations in the buffer
func (b *Buffer) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.operations)
}

// NewIterator returns an iterator over the transaction buffer
func (b *Buffer) NewIterator() *BufferIterator {
	ops := b.Operations() // This returns a sorted copy of operations

	return &BufferIterator{
		operations: ops,
		position:   -1,
	}
}

// BufferIterator is an iterator over the transaction buffer
type BufferIterator struct {
	operations []*Operation
	position   int
}

// SeekToFirst positions the iterator at the first key
func (it *BufferIterator) SeekToFirst() {
	if len(it.operations) > 0 {
		it.position = 0
	} else {
		it.position = -1
	}
}

// SeekToLast positions the iterator at the last key
func (it *BufferIterator) SeekToLast() {
	if len(it.operations) > 0 {
		it.position = len(it.operations) - 1
	} else {
		it.position = -1
	}
}

// Seek positions the iterator at the first key >= target
func (it *BufferIterator) Seek(target []byte) bool {
	if len(it.operations) == 0 {
		return false
	}

	// Binary search to find the first key >= target
	i := sort.Search(len(it.operations), func(i int) bool {
		return bytes.Compare(it.operations[i].Key, target) >= 0
	})

	if i >= len(it.operations) {
		it.position = -1
		return false
	}

	it.position = i
	return true
}

// Next advances to the next key
func (it *BufferIterator) Next() bool {
	if it.position < 0 {
		it.SeekToFirst()
		return it.Valid()
	}
	
	if it.position >= len(it.operations)-1 {
		it.position = -1
		return false
	}

	it.position++
	return true
}

// Key returns the current key
func (it *BufferIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.operations[it.position].Key
}

// Value returns the current value
func (it *BufferIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.operations[it.position].Value
}

// Valid returns true if the iterator is valid
func (it *BufferIterator) Valid() bool {
	return it.position >= 0 && it.position < len(it.operations)
}

// IsTombstone returns true if the current entry is a deletion marker
func (it *BufferIterator) IsTombstone() bool {
	if !it.Valid() {
		return false
	}
	return it.operations[it.position].IsDelete
}