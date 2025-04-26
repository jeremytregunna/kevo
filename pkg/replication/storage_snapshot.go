package replication

import (
	"io"
)

// StorageSnapshot provides an interface for taking a snapshot of the storage
// for replication bootstrap purposes.
type StorageSnapshot interface {
	// CreateSnapshotIterator creates an iterator for a storage snapshot
	CreateSnapshotIterator() (SnapshotIterator, error)

	// KeyCount returns the approximate number of keys in storage
	KeyCount() int64
}

// SnapshotIterator provides iteration over key-value pairs in storage
type SnapshotIterator interface {
	// Next returns the next key-value pair
	// Returns io.EOF when there are no more items
	Next() (key []byte, value []byte, err error)

	// Close closes the iterator
	Close() error
}

// StorageSnapshotProvider is implemented by storage engines that support snapshots
type StorageSnapshotProvider interface {
	// CreateSnapshot creates a snapshot of the current storage state
	CreateSnapshot() (StorageSnapshot, error)
}

// MemoryStorageSnapshot is a simple in-memory implementation of StorageSnapshot
// Useful for testing or small datasets
type MemoryStorageSnapshot struct {
	Pairs    []KeyValuePair
	position int
}

// KeyValuePair represents a key-value pair in storage
type KeyValuePair struct {
	Key   []byte
	Value []byte
}

// CreateSnapshotIterator creates an iterator for a memory storage snapshot
func (m *MemoryStorageSnapshot) CreateSnapshotIterator() (SnapshotIterator, error) {
	return &MemorySnapshotIterator{
		snapshot: m,
		position: 0,
	}, nil
}

// KeyCount returns the number of keys in the snapshot
func (m *MemoryStorageSnapshot) KeyCount() int64 {
	return int64(len(m.Pairs))
}

// MemorySnapshotIterator is an iterator for MemoryStorageSnapshot
type MemorySnapshotIterator struct {
	snapshot *MemoryStorageSnapshot
	position int
}

// Next returns the next key-value pair
func (it *MemorySnapshotIterator) Next() ([]byte, []byte, error) {
	if it.position >= len(it.snapshot.Pairs) {
		return nil, nil, io.EOF
	}

	pair := it.snapshot.Pairs[it.position]
	it.position++

	return pair.Key, pair.Value, nil
}

// Close closes the iterator
func (it *MemorySnapshotIterator) Close() error {
	return nil
}

// NewMemoryStorageSnapshot creates a new in-memory storage snapshot
func NewMemoryStorageSnapshot(pairs []KeyValuePair) *MemoryStorageSnapshot {
	return &MemoryStorageSnapshot{
		Pairs: pairs,
	}
}
