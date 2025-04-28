package replication

import (
	"fmt"
	"sync"

	replication_proto "github.com/KevoDB/kevo/pkg/replication/proto"
	"github.com/KevoDB/kevo/pkg/wal"
)

// DefaultMaxBatchSizeKB is the default maximum batch size in kilobytes
const DefaultMaxBatchSizeKB = 256

// WALBatcher manages batching of WAL entries for efficient replication
type WALBatcher struct {
	// Maximum batch size in kilobytes
	maxBatchSizeKB int

	// Current batch of entries
	buffer *WALEntriesBuffer

	// Compression codec to use
	codec replication_proto.CompressionCodec

	// Whether to respect transaction boundaries
	respectTxBoundaries bool

	// Map to track transactions by sequence numbers
	txSequences map[uint64]uint64

	// Mutex to protect txSequences
	mu sync.Mutex
}

// NewWALBatcher creates a new WAL batcher with specified maximum batch size
func NewWALBatcher(maxSizeKB int, codec replication_proto.CompressionCodec, respectTxBoundaries bool) *WALBatcher {
	if maxSizeKB <= 0 {
		maxSizeKB = DefaultMaxBatchSizeKB
	}

	return &WALBatcher{
		maxBatchSizeKB:      maxSizeKB,
		buffer:              NewWALEntriesBuffer(maxSizeKB, codec),
		codec:               codec,
		respectTxBoundaries: respectTxBoundaries,
		txSequences:         make(map[uint64]uint64),
	}
}

// AddEntry adds a WAL entry to the current batch
// Returns true if a batch is ready to be sent
func (b *WALBatcher) AddEntry(entry *wal.Entry) (bool, error) {
	// Create a proto entry
	protoEntry, err := WALEntryToProto(entry, replication_proto.FragmentType_FULL)
	if err != nil {
		return false, fmt.Errorf("failed to convert WAL entry to proto: %w", err)
	}

	// Track transaction boundaries if enabled
	if b.respectTxBoundaries {
		b.trackTransaction(entry)
	}

	// Add the entry to the buffer
	added := b.buffer.Add(protoEntry)
	if !added {
		// Buffer is full
		return true, nil
	}

	// Check if we've reached a transaction boundary
	if b.respectTxBoundaries && b.isTransactionBoundary(entry) {
		return true, nil
	}

	// Return true if the buffer has reached its size limit
	return b.buffer.Size() >= b.maxBatchSizeKB*1024, nil
}

// GetBatch retrieves the current batch and clears the buffer
func (b *WALBatcher) GetBatch() *replication_proto.WALStreamResponse {
	response := b.buffer.CreateResponse()
	b.buffer.Clear()
	return response
}

// GetBatchCount returns the number of entries in the current batch
func (b *WALBatcher) GetBatchCount() int {
	return b.buffer.Count()
}

// GetBatchSize returns the size of the current batch in bytes
func (b *WALBatcher) GetBatchSize() int {
	return b.buffer.Size()
}

// trackTransaction tracks a transaction by its sequence numbers
func (b *WALBatcher) trackTransaction(entry *wal.Entry) {
	if entry.Type == wal.OpTypeBatch {
		b.mu.Lock()
		defer b.mu.Unlock()

		// Track the start of a batch as a transaction
		// The value is the expected end sequence number
		// For simplicity in this implementation, we just store the sequence number itself
		// In a real implementation, we would parse the batch to determine the actual end sequence
		b.txSequences[entry.SequenceNumber] = entry.SequenceNumber
	}
}

// isTransactionBoundary determines if an entry is a transaction boundary
func (b *WALBatcher) isTransactionBoundary(entry *wal.Entry) bool {
	if !b.respectTxBoundaries {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if this sequence is an end of a tracked transaction
	for _, endSeq := range b.txSequences {
		if entry.SequenceNumber == endSeq {
			// Clean up the transaction tracking
			delete(b.txSequences, entry.SequenceNumber)
			return true
		}
	}

	return false
}

// Reset clears the batcher state
func (b *WALBatcher) Reset() {
	b.buffer.Clear()

	b.mu.Lock()
	defer b.mu.Unlock()
	b.txSequences = make(map[uint64]uint64)
}

// WALBatchApplier manages the application of batches of WAL entries on the replica side
type WALBatchApplier struct {
	// Maximum sequence number applied
	maxAppliedSeq uint64

	// Last acknowledged sequence number
	lastAckSeq uint64

	// Sequence number gap detection
	expectedNextSeq uint64

	// Lock to protect sequence numbers
	mu sync.Mutex
}

// NewWALBatchApplier creates a new WAL batch applier
func NewWALBatchApplier(startSeq uint64) *WALBatchApplier {
	return &WALBatchApplier{
		maxAppliedSeq:   startSeq,
		lastAckSeq:      startSeq,
		expectedNextSeq: startSeq + 1,
	}
}

// ApplyEntries applies a batch of WAL entries with proper ordering and gap detection
// Returns the highest applied sequence, a flag indicating if a gap was detected, and any error
func (a *WALBatchApplier) ApplyEntries(entries []*replication_proto.WALEntry, applyFn func(*wal.Entry) error) (uint64, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(entries) == 0 {
		return a.maxAppliedSeq, false, nil
	}

	// Check for sequence gaps
	hasGap := false
	firstSeq := entries[0].SequenceNumber

	if firstSeq != a.expectedNextSeq {
		// We have a gap
		hasGap = true
		return a.maxAppliedSeq, hasGap, fmt.Errorf("sequence gap detected: expected %d, got %d",
			a.expectedNextSeq, firstSeq)
	}

	// Process entries in order
	var lastAppliedSeq uint64
	for i, protoEntry := range entries {
		// Verify entries are in sequence
		if i > 0 && protoEntry.SequenceNumber != entries[i-1].SequenceNumber+1 {
			// Gap within the batch
			hasGap = true
			return a.maxAppliedSeq, hasGap, fmt.Errorf("sequence gap within batch: %d -> %d",
				entries[i-1].SequenceNumber, protoEntry.SequenceNumber)
		}

		// Deserialize and apply the entry
		entry, err := DeserializeWALEntry(protoEntry.Payload)
		if err != nil {
			return a.maxAppliedSeq, false, fmt.Errorf("failed to deserialize entry %d: %w",
				protoEntry.SequenceNumber, err)
		}

		// Apply the entry
		if err := applyFn(entry); err != nil {
			return a.maxAppliedSeq, false, fmt.Errorf("failed to apply entry %d: %w",
				protoEntry.SequenceNumber, err)
		}

		lastAppliedSeq = protoEntry.SequenceNumber
	}

	// Update tracking
	a.maxAppliedSeq = lastAppliedSeq
	a.expectedNextSeq = lastAppliedSeq + 1

	return a.maxAppliedSeq, false, nil
}

// AcknowledgeUpTo marks sequences as acknowledged
func (a *WALBatchApplier) AcknowledgeUpTo(seq uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if seq > a.lastAckSeq {
		a.lastAckSeq = seq
	}
}

// GetLastAcknowledged returns the last acknowledged sequence
func (a *WALBatchApplier) GetLastAcknowledged() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.lastAckSeq
}

// GetMaxApplied returns the maximum applied sequence
func (a *WALBatchApplier) GetMaxApplied() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.maxAppliedSeq
}

// GetExpectedNext returns the next expected sequence number
func (a *WALBatchApplier) GetExpectedNext() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.expectedNextSeq
}

// Reset resets the applier state to the given sequence
func (a *WALBatchApplier) Reset(seq uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.maxAppliedSeq = seq
	a.lastAckSeq = seq
	a.expectedNextSeq = seq + 1
}
