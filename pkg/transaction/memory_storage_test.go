package transaction

import (
	"bytes"
	"sort"
	"sync"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/wal"
)

// MemoryStorage is a simple in-memory storage implementation for tests
type MemoryStorage struct {
	data map[string][]byte
	mu   sync.RWMutex
}

// NewMemoryStorage creates a new memory storage instance
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string][]byte),
	}
}

// Get retrieves a value for the given key
func (s *MemoryStorage) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Return a copy to avoid modification
	result := make([]byte, len(val))
	copy(result, val)
	return result, nil
}

// ApplyBatch applies a batch of operations atomically
func (s *MemoryStorage) ApplyBatch(entries []*wal.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Apply all operations
	for _, entry := range entries {
		key := string(entry.Key)
		switch entry.Type {
		case wal.OpTypePut:
			valCopy := make([]byte, len(entry.Value))
			copy(valCopy, entry.Value)
			s.data[key] = valCopy
		case wal.OpTypeDelete:
			delete(s.data, key)
		}
	}

	return nil
}

// GetIterator returns an iterator over all keys
func (s *MemoryStorage) GetIterator() (iterator.Iterator, error) {
	return s.newIterator(nil, nil), nil
}

// GetRangeIterator returns an iterator limited to a specific key range
func (s *MemoryStorage) GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error) {
	return s.newIterator(startKey, endKey), nil
}

// MemoryIterator implements the iterator.Iterator interface for MemoryStorage
type MemoryIterator struct {
	keys     [][]byte
	values   [][]byte
	position int
}

// newIterator creates a new iterator over the storage
func (s *MemoryStorage) newIterator(startKey, endKey []byte) *MemoryIterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get all keys and sort them
	keys := make([][]byte, 0, len(s.data))
	for k := range s.data {
		keyBytes := []byte(k)
		
		// Apply range filtering if specified
		if startKey != nil && bytes.Compare(keyBytes, startKey) < 0 {
			continue
		}
		if endKey != nil && bytes.Compare(keyBytes, endKey) >= 0 {
			continue
		}
		
		keys = append(keys, keyBytes)
	}

	// Sort the keys
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// Collect values in the same order
	values := make([][]byte, len(keys))
	for i, k := range keys {
		val := s.data[string(k)]
		valCopy := make([]byte, len(val))
		copy(valCopy, val)
		values[i] = valCopy
	}

	return &MemoryIterator{
		keys:     keys,
		values:   values,
		position: -1,
	}
}

// SeekToFirst positions the iterator at the first key
func (it *MemoryIterator) SeekToFirst() {
	if len(it.keys) > 0 {
		it.position = 0
	} else {
		it.position = -1
	}
}

// SeekToLast positions the iterator at the last key
func (it *MemoryIterator) SeekToLast() {
	if len(it.keys) > 0 {
		it.position = len(it.keys) - 1
	} else {
		it.position = -1
	}
}

// Seek positions the iterator at the first key >= target
func (it *MemoryIterator) Seek(target []byte) bool {
	if len(it.keys) == 0 {
		return false
	}

	// Binary search to find the first key >= target
	i := sort.Search(len(it.keys), func(i int) bool {
		return bytes.Compare(it.keys[i], target) >= 0
	})

	if i >= len(it.keys) {
		it.position = -1
		return false
	}

	it.position = i
	return true
}

// Next advances to the next key
func (it *MemoryIterator) Next() bool {
	if it.position < 0 {
		it.SeekToFirst()
		return it.Valid()
	}
	
	if it.position >= len(it.keys)-1 {
		it.position = -1
		return false
	}

	it.position++
	return true
}

// Key returns the current key
func (it *MemoryIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.keys[it.position]
}

// Value returns the current value
func (it *MemoryIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.values[it.position]
}

// Valid returns true if the iterator is valid
func (it *MemoryIterator) Valid() bool {
	return it.position >= 0 && it.position < len(it.keys)
}

// IsTombstone returns true if the current entry is a deletion marker
func (it *MemoryIterator) IsTombstone() bool {
	return false // Memory storage doesn't use tombstones
}

// Put directly sets a key-value pair (helper method for tests)
func (s *MemoryStorage) Put(key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Make a copy of the key and value
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	
	s.data[string(keyCopy)] = valueCopy
}

// Delete directly removes a key (helper method for tests)
func (s *MemoryStorage) Delete(key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	delete(s.data, string(key))
}

// Size returns the number of key-value pairs in the storage
func (s *MemoryStorage) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	return len(s.data)
}