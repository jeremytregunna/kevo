package replication

import (
	"github.com/KevoDB/kevo/pkg/wal"
)

// EntryReplicator defines the interface for replicating WAL entries
type EntryReplicator interface {
	// GetHighestTimestamp returns the highest Lamport timestamp seen
	GetHighestTimestamp() uint64

	// AddProcessor registers a processor to handle replicated entries
	AddProcessor(processor EntryProcessor)

	// RemoveProcessor unregisters a processor
	RemoveProcessor(processor EntryProcessor)

	// GetEntriesAfter retrieves entries after a given position
	GetEntriesAfter(pos ReplicationPosition) ([]*wal.Entry, error)
}

// EntryApplier defines the interface for applying WAL entries
type EntryApplier interface {
	// ResetHighestApplied sets the highest applied LSN
	ResetHighestApplied(lsn uint64)
}

// Ensure our concrete types implement these interfaces
var _ EntryReplicator = (*WALReplicator)(nil)
var _ EntryApplier = (*WALApplier)(nil)
