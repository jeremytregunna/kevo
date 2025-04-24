package memtable

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/wal"
)

// MemTable is an in-memory table that stores key-value pairs
// It is implemented using a skip list for efficient inserts and lookups
type MemTable struct {
	skipList     *SkipList
	nextSeqNum   uint64
	creationTime time.Time
	immutable    atomic.Bool
	size         int64
	mu           sync.RWMutex
}

// NewMemTable creates a new memory table
func NewMemTable() *MemTable {
	return &MemTable{
		skipList:     NewSkipList(),
		creationTime: time.Now(),
	}
}

// Put adds a key-value pair to the MemTable
func (m *MemTable) Put(key, value []byte, seqNum uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.IsImmutable() {
		// Don't modify immutable memtables
		return
	}

	e := newEntry(key, value, TypeValue, seqNum)
	m.skipList.Insert(e)

	// Update maximum sequence number
	if seqNum > m.nextSeqNum {
		m.nextSeqNum = seqNum + 1
	}
}

// Delete marks a key as deleted in the MemTable
func (m *MemTable) Delete(key []byte, seqNum uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.IsImmutable() {
		// Don't modify immutable memtables
		return
	}

	e := newEntry(key, nil, TypeDeletion, seqNum)
	m.skipList.Insert(e)

	// Update maximum sequence number
	if seqNum > m.nextSeqNum {
		m.nextSeqNum = seqNum + 1
	}
}

// Get retrieves the value associated with the given key
// Returns (nil, true) if the key exists but has been deleted
// Returns (nil, false) if the key does not exist
// Returns (value, true) if the key exists and has a value
func (m *MemTable) Get(key []byte) ([]byte, bool) {
	// Use atomic check for immutability first
	if m.IsImmutable() {
		// For immutable memtables, we can bypass the write lock completely
		e := m.skipList.Find(key)
		if e == nil {
			return nil, false
		}

		// Check if this is a deletion marker
		if e.valueType == TypeDeletion {
			return nil, true // Key exists but was deleted
		}

		return e.value, true
	} else {
		// For mutable memtables, we still need read lock protection
		// as the structure could be modified during reads
		m.mu.RLock()
		defer m.mu.RUnlock()

		e := m.skipList.Find(key)
		if e == nil {
			return nil, false
		}

		// Check if this is a deletion marker
		if e.valueType == TypeDeletion {
			return nil, true // Key exists but was deleted
		}

		return e.value, true
	}
}

// Contains checks if the key exists in the MemTable
func (m *MemTable) Contains(key []byte) bool {
	// For immutable memtables, we can bypass the RWLock completely
	if m.IsImmutable() {
		return m.skipList.Find(key) != nil
	} else {
		// For mutable memtables, we still need read lock protection
		m.mu.RLock()
		defer m.mu.RUnlock()
		
		return m.skipList.Find(key) != nil
	}
}

// ApproximateSize returns the approximate size of the MemTable in bytes
func (m *MemTable) ApproximateSize() int64 {
	return m.skipList.ApproximateSize()
}

// SetImmutable marks the MemTable as immutable
// After this is called, no more modifications are allowed
func (m *MemTable) SetImmutable() {
	m.immutable.Store(true)
}

// IsImmutable returns whether the MemTable is immutable
func (m *MemTable) IsImmutable() bool {
	return m.immutable.Load()
}

// Age returns the age of the MemTable in seconds
func (m *MemTable) Age() float64 {
	return time.Since(m.creationTime).Seconds()
}

// NewIterator returns an iterator for the MemTable
func (m *MemTable) NewIterator() *Iterator {
	// For immutable memtables, we can bypass the lock
	if m.IsImmutable() {
		return m.skipList.NewIterator()
	} else {
		// For mutable memtables, we need read lock to ensure stability during iteration
		m.mu.RLock()
		defer m.mu.RUnlock()
		
		return m.skipList.NewIterator()
	}
}

// GetNextSequenceNumber returns the next sequence number to use
func (m *MemTable) GetNextSequenceNumber() uint64 {
	// For immutable memtables, nextSeqNum won't change
	if m.IsImmutable() {
		return m.nextSeqNum
	} else {
		// For mutable memtables, we need read lock
		m.mu.RLock()
		defer m.mu.RUnlock()
		return m.nextSeqNum
	}
}

// ProcessWALEntry processes a WAL entry and applies it to the MemTable
func (m *MemTable) ProcessWALEntry(entry *wal.Entry) error {
	switch entry.Type {
	case wal.OpTypePut:
		m.Put(entry.Key, entry.Value, entry.SequenceNumber)
	case wal.OpTypeDelete:
		m.Delete(entry.Key, entry.SequenceNumber)
	case wal.OpTypeBatch:
		// Process batch operations
		batch, err := wal.DecodeBatch(entry)
		if err != nil {
			return err
		}

		for i, op := range batch.Operations {
			seqNum := batch.Seq + uint64(i)
			switch op.Type {
			case wal.OpTypePut:
				m.Put(op.Key, op.Value, seqNum)
			case wal.OpTypeDelete:
				m.Delete(op.Key, seqNum)
			}
		}
	}
	return nil
}
