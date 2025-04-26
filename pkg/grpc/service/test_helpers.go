package service

import (
	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/wal"
)

// For testing purposes, we need our mocks to be convertible to WALReplicator
// The issue is that the WALReplicator has unexported fields, so we can't just embed it
// Let's create a clean test implementation of the replication.WALReplicator interface

// CreateTestReplicator creates a replication.WALReplicator for tests
func CreateTestReplicator(highTS uint64) *replication.WALReplicator {
	return &replication.WALReplicator{}
}

// Cast mock storage snapshot
func castToStorageSnapshot(s interface {
	CreateSnapshotIterator() (replication.SnapshotIterator, error)
	KeyCount() int64
}) replication.StorageSnapshot {
	return s.(replication.StorageSnapshot)
}

// MockGetEntriesAfter implements mocking for WAL replicator GetEntriesAfter
func MockGetEntriesAfter(position replication.ReplicationPosition) ([]*wal.Entry, error) {
	return nil, nil
}
