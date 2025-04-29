// Package replication implements primary-replica replication for Kevo database.
package replication

import (
	"context"

	"github.com/KevoDB/kevo/pkg/wal"
	proto "github.com/KevoDB/kevo/proto/kevo/replication"
)

// WALProvider abstracts access to the Write-Ahead Log
type WALProvider interface {
	// GetEntriesFrom retrieves WAL entries starting from the given sequence number
	GetEntriesFrom(sequenceNumber uint64) ([]*wal.Entry, error)

	// GetNextSequence returns the next sequence number that will be assigned
	GetNextSequence() uint64

	// RegisterObserver registers a WAL observer for notifications
	RegisterObserver(id string, observer WALObserver)

	// UnregisterObserver removes a previously registered observer
	UnregisterObserver(id string)
}

// WALObserver defines how components observe WAL operations
type WALObserver interface {
	// OnWALEntryWritten is called when a single WAL entry is written
	OnWALEntryWritten(entry *wal.Entry)

	// OnWALBatchWritten is called when a batch of WAL entries is written
	OnWALBatchWritten(startSeq uint64, entries []*wal.Entry)

	// OnWALSync is called when the WAL is synced to disk
	OnWALSync(upToSeq uint64)
}

// WALEntryApplier defines how components apply WAL entries
type WALEntryApplier interface {
	// Apply applies a single WAL entry
	Apply(entry *wal.Entry) error

	// Sync ensures all applied entries are persisted
	Sync() error
}

// PrimaryNode defines the behavior of a primary node
type PrimaryNode interface {
	// StreamWAL handles streaming WAL entries to replicas
	StreamWAL(req *proto.WALStreamRequest, stream proto.WALReplicationService_StreamWALServer) error

	// Acknowledge handles acknowledgments from replicas
	Acknowledge(ctx context.Context, req *proto.Ack) (*proto.AckResponse, error)

	// NegativeAcknowledge handles negative acknowledgments (retransmission requests)
	NegativeAcknowledge(ctx context.Context, req *proto.Nack) (*proto.NackResponse, error)

	// Close shuts down the primary node
	Close() error
}

// ReplicaNode defines the behavior of a replica node
type ReplicaNode interface {
	// Start begins the replication process
	Start() error

	// Stop halts the replication process
	Stop() error

	// GetLastAppliedSequence returns the last successfully applied sequence
	GetLastAppliedSequence() uint64

	// GetCurrentState returns the current state of the replica
	GetCurrentState() ReplicaState

	// GetStateString returns a string representation of the current state
	GetStateString() string
}

// ReplicaState is defined in state.go

// Batcher manages batching of WAL entries for transmission
type Batcher interface {
	// Add adds a WAL entry to the current batch
	Add(entry *proto.WALEntry) bool

	// CreateResponse creates a WALStreamResponse from the current batch
	CreateResponse() *proto.WALStreamResponse

	// Count returns the number of entries in the current batch
	Count() int

	// Size returns the size of the current batch in bytes
	Size() int

	// Clear resets the batcher
	Clear()
}

// Compressor manages compression of WAL entries
type Compressor interface {
	// Compress compresses data
	Compress(data []byte, codec proto.CompressionCodec) ([]byte, error)

	// Decompress decompresses data
	Decompress(data []byte, codec proto.CompressionCodec) ([]byte, error)

	// Close releases resources
	Close() error
}

// SessionManager manages replica sessions
type SessionManager interface {
	// RegisterSession registers a new replica session
	RegisterSession(sessionID string, conn proto.WALReplicationService_StreamWALServer)

	// UnregisterSession removes a replica session
	UnregisterSession(sessionID string)

	// GetSession returns a replica session by ID
	GetSession(sessionID string) (proto.WALReplicationService_StreamWALServer, bool)

	// BroadcastBatch sends a batch to all active sessions
	BroadcastBatch(batch *proto.WALStreamResponse) int

	// CountSessions returns the number of active sessions
	CountSessions() int
}
