package service

import (
	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/wal"
)

// EntryReplicator defines the interface for replicating WAL entries
type EntryReplicator interface {
	// GetHighestTimestamp returns the highest Lamport timestamp seen
	GetHighestTimestamp() uint64

	// AddProcessor registers a processor to handle replicated entries
	AddProcessor(processor replication.EntryProcessor)

	// RemoveProcessor unregisters a processor
	RemoveProcessor(processor replication.EntryProcessor)

	// GetEntriesAfter retrieves entries after a given position
	GetEntriesAfter(pos replication.ReplicationPosition) ([]*wal.Entry, error)
}

// SnapshotProvider defines the interface for database snapshot operations
type SnapshotProvider interface {
	// CreateSnapshotIterator creates an iterator for snapshot data
	CreateSnapshotIterator() (replication.SnapshotIterator, error)

	// KeyCount returns the approximate number of keys in the snapshot
	KeyCount() int64
}

// EntryApplier defines the interface for applying WAL entries
type EntryApplier interface {
	// ResetHighestApplied sets the highest applied LSN
	ResetHighestApplied(lsn uint64)
}
