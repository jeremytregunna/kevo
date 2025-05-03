package transaction

import (
	"bytes"
	"encoding/base64"
	"sort"
	"sync"
)

// Operation represents a single operation in the transaction buffer
type Operation struct {
	Key      []byte
	Value    []byte
	IsDelete bool
}

// Buffer stores pending changes for a transaction
type Buffer struct {
	operations map[string]*Operation // Key string -> Operation (using base64 encoding for binary safety)
	mu         sync.RWMutex
}

// NewBuffer creates a new transaction buffer
func NewBuffer() *Buffer {
	return &Buffer{
		operations: make(map[string]*Operation),
	}
}

// Put adds or updates a key-value pair in the buffer
func (b *Buffer) Put(key, value []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Copy the key and value to avoid external modification
	keyCopy := make([]byte, len(key))
	valueCopy := make([]byte, len(value))
	copy(keyCopy, key)
	copy(valueCopy, value)

	// Create or update the operation - use base64 encoding for map key to avoid binary encoding issues
	b.operations[base64.StdEncoding.EncodeToString(key)] = &Operation{
		Key:      keyCopy,
		Value:    valueCopy,
		IsDelete: false,
	}
}

// Delete marks a key for deletion in the buffer
func (b *Buffer) Delete(key []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Copy the key to avoid external modification
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	// Create or update the operation - use base64 encoding for map key to avoid binary encoding issues
	b.operations[base64.StdEncoding.EncodeToString(key)] = &Operation{
		Key:      keyCopy,
		Value:    nil,
		IsDelete: true,
	}
}

// Get retrieves a value for the given key from the buffer
// Returns the value and a boolean indicating if the key was found
func (b *Buffer) Get(key []byte) ([]byte, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	encodedKey := base64.StdEncoding.EncodeToString(key)
	op, ok := b.operations[encodedKey]

	if !ok {
		return nil, false
	}

	// If this is a deletion marker, return nil
	if op.IsDelete {
		return nil, true
	}

	// Return a copy of the value to prevent modification
	valueCopy := make([]byte, len(op.Value))
	copy(valueCopy, op.Value)
	return valueCopy, true
}

// Clear removes all operations from the buffer
func (b *Buffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create a new operations map
	b.operations = make(map[string]*Operation)
}

// Size returns the number of operations in the buffer
func (b *Buffer) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.operations)
}

// Operations returns a sorted list of operations
// This is used for applying the changes in order
func (b *Buffer) Operations() []*Operation {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Create a list of operations
	ops := make([]*Operation, 0, len(b.operations))
	for _, op := range b.operations {
		ops = append(ops, op)
	}

	// Sort by key for consistent application order
	sort.Slice(ops, func(i, j int) bool {
		return bytes.Compare(ops[i].Key, ops[j].Key) < 0
	})

	return ops
}

// Iterator returns a new iterator over the buffer
func (b *Buffer) NewIterator() *BufferIterator {
	// Get all operations
	ops := b.Operations()

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
	if it.position < 0 || it.position >= len(it.operations)-1 {
		it.position = -1
		return false
	}

	it.position++
	return true
}

// Key returns the current key
func (it *BufferIterator) Key() []byte {
	if it.position < 0 || it.position >= len(it.operations) {
		return nil
	}

	return it.operations[it.position].Key
}

// Value returns the current value
func (it *BufferIterator) Value() []byte {
	if it.position < 0 || it.position >= len(it.operations) {
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
	if it.position < 0 || it.position >= len(it.operations) {
		return false
	}

	return it.operations[it.position].IsDelete
}
