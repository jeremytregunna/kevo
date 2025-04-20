package txbuffer

import (
	"bytes"
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

// TxBuffer maintains a buffer of transaction operations before they are committed
type TxBuffer struct {
	// Buffers all operations for the transaction
	operations []Operation

	// Cache of key -> value for fast lookups without scanning the operation list
	// Maps to nil for deletion markers
	cache map[string][]byte

	// Protects against concurrent access
	mu sync.RWMutex
}

// NewTxBuffer creates a new transaction buffer
func NewTxBuffer() *TxBuffer {
	return &TxBuffer{
		operations: make([]Operation, 0, 16),
		cache:      make(map[string][]byte),
	}
}

// Put adds a key-value pair to the transaction buffer
func (b *TxBuffer) Put(key, value []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create a safe copy of key and value to prevent later modifications
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	// Add to operations list
	b.operations = append(b.operations, Operation{
		Key:      keyCopy,
		Value:    valueCopy,
		IsDelete: false,
	})

	// Update cache
	b.cache[string(keyCopy)] = valueCopy
}

// Delete marks a key as deleted in the transaction buffer
func (b *TxBuffer) Delete(key []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create a safe copy of the key
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	// Add to operations list
	b.operations = append(b.operations, Operation{
		Key:      keyCopy,
		Value:    nil,
		IsDelete: true,
	})

	// Update cache to mark key as deleted (nil value)
	b.cache[string(keyCopy)] = nil
}

// Get retrieves a value from the transaction buffer
// Returns (value, true) if found, (nil, false) if not found
func (b *TxBuffer) Get(key []byte) ([]byte, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	value, found := b.cache[string(key)]
	return value, found
}

// Has returns true if the key exists in the buffer, even if it's marked for deletion
func (b *TxBuffer) Has(key []byte) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	_, found := b.cache[string(key)]
	return found
}

// IsDeleted returns true if the key is marked for deletion in the buffer
func (b *TxBuffer) IsDeleted(key []byte) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	value, found := b.cache[string(key)]
	return found && value == nil
}

// Operations returns the list of all operations in the transaction
// This is used when committing the transaction
func (b *TxBuffer) Operations() []Operation {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Return a copy to prevent modification
	result := make([]Operation, len(b.operations))
	copy(result, b.operations)
	return result
}

// Clear empties the transaction buffer
// Used when rolling back a transaction
func (b *TxBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.operations = b.operations[:0]
	b.cache = make(map[string][]byte)
}

// Size returns the number of operations in the buffer
func (b *TxBuffer) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.operations)
}

// Iterator returns an iterator over the transaction buffer
type Iterator struct {
	// The buffer this iterator is iterating over
	buffer *TxBuffer

	// The current position in the keys slice
	pos int

	// Sorted list of keys
	keys []string
}

// NewIterator creates a new iterator over the transaction buffer
func (b *TxBuffer) NewIterator() *Iterator {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Get all keys and sort them
	keys := make([]string, 0, len(b.cache))
	for k := range b.cache {
		keys = append(keys, k)
	}

	// Sort the keys
	keys = sortStrings(keys)

	return &Iterator{
		buffer: b,
		pos:    -1, // Start before the first position
		keys:   keys,
	}
}

// SeekToFirst positions the iterator at the first key
func (it *Iterator) SeekToFirst() {
	it.pos = 0
}

// SeekToLast positions the iterator at the last key
func (it *Iterator) SeekToLast() {
	if len(it.keys) > 0 {
		it.pos = len(it.keys) - 1
	} else {
		it.pos = 0
	}
}

// Seek positions the iterator at the first key >= target
func (it *Iterator) Seek(target []byte) bool {
	targetStr := string(target)

	// Binary search would be more efficient for large sets
	for i, key := range it.keys {
		if key >= targetStr {
			it.pos = i
			return true
		}
	}

	// Not found - position past the end
	it.pos = len(it.keys)
	return false
}

// Next advances the iterator to the next key
func (it *Iterator) Next() bool {
	if it.pos < 0 {
		it.pos = 0
		return it.pos < len(it.keys)
	}

	it.pos++
	return it.pos < len(it.keys)
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}

	return []byte(it.keys[it.pos])
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}

	// Get the value from the buffer
	it.buffer.mu.RLock()
	defer it.buffer.mu.RUnlock()

	value := it.buffer.cache[it.keys[it.pos]]
	return value // Returns nil for deletion markers
}

// Valid returns true if the iterator is positioned at a valid entry
func (it *Iterator) Valid() bool {
	return it.pos >= 0 && it.pos < len(it.keys)
}

// IsTombstone returns true if the current entry is a deletion marker
func (it *Iterator) IsTombstone() bool {
	if !it.Valid() {
		return false
	}

	it.buffer.mu.RLock()
	defer it.buffer.mu.RUnlock()

	// The value is nil for tombstones in our cache implementation
	value := it.buffer.cache[it.keys[it.pos]]
	return value == nil
}

// Simple implementation of string sorting for the iterator
func sortStrings(strings []string) []string {
	// In-place sort
	for i := 0; i < len(strings); i++ {
		for j := i + 1; j < len(strings); j++ {
			if bytes.Compare([]byte(strings[i]), []byte(strings[j])) > 0 {
				strings[i], strings[j] = strings[j], strings[i]
			}
		}
	}
	return strings
}
