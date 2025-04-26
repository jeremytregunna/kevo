package replication

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/KevoDB/kevo/pkg/wal"
)

var (
	// ErrReplicatorClosed indicates the replicator has been closed and no longer accepts entries
	ErrReplicatorClosed = errors.New("replicator is closed")

	// ErrReplicatorFull indicates the replicator's entry buffer is full
	ErrReplicatorFull = errors.New("replicator entry buffer is full")

	// ErrInvalidPosition indicates an invalid replication position was provided
	ErrInvalidPosition = errors.New("invalid replication position")
)

// EntryProcessor is an interface for components that process WAL entries for replication
type EntryProcessor interface {
	// ProcessEntry processes a single WAL entry
	ProcessEntry(entry *wal.Entry) error

	// ProcessBatch processes a batch of WAL entries
	ProcessBatch(entries []*wal.Entry) error
}

// ReplicationPosition represents a position in the replication stream
type ReplicationPosition struct {
	// Timestamp is the Lamport timestamp of the position
	Timestamp uint64
}

// WALReplicator captures WAL entries and makes them available for replication
type WALReplicator struct {
	// Entries is a map of timestamp -> entry for all captured entries
	entries map[uint64]*wal.Entry

	// Batches is a map of batch start timestamp -> batch entries
	batches map[uint64][]*wal.Entry

	// EntryChannel is a channel of captured entries for subscribers
	entryChannel chan *wal.Entry

	// BatchChannel is a channel of captured batches for subscribers
	batchChannel chan []*wal.Entry

	// Highest timestamp seen so far
	highestTimestamp uint64

	// MaxBufferedEntries is the maximum number of entries to buffer
	maxBufferedEntries int

	// Concurrency control
	mu sync.RWMutex

	// Closed indicates if the replicator is closed
	closed int32

	// EntryProcessors are components that process entries as they're captured
	processors []EntryProcessor
}

// NewWALReplicator creates a new WAL replicator
func NewWALReplicator(maxBufferedEntries int) *WALReplicator {
	if maxBufferedEntries <= 0 {
		maxBufferedEntries = 10000 // Default to 10,000 entries
	}

	return &WALReplicator{
		entries:            make(map[uint64]*wal.Entry),
		batches:            make(map[uint64][]*wal.Entry),
		entryChannel:       make(chan *wal.Entry, 1000),
		batchChannel:       make(chan []*wal.Entry, 100),
		maxBufferedEntries: maxBufferedEntries,
		processors:         make([]EntryProcessor, 0),
	}
}

// OnEntryWritten implements the wal.ReplicationHook interface
func (r *WALReplicator) OnEntryWritten(entry *wal.Entry) {
	if atomic.LoadInt32(&r.closed) == 1 {
		return
	}

	r.mu.Lock()

	// Update highest timestamp
	if entry.SequenceNumber > r.highestTimestamp {
		r.highestTimestamp = entry.SequenceNumber
	}

	// Store the entry (make a copy to avoid potential mutation)
	entryCopy := &wal.Entry{
		SequenceNumber: entry.SequenceNumber,
		Type:           entry.Type,
		Key:            append([]byte{}, entry.Key...),
	}
	if entry.Value != nil {
		entryCopy.Value = append([]byte{}, entry.Value...)
	}

	r.entries[entryCopy.SequenceNumber] = entryCopy

	// Cleanup old entries if we exceed the buffer size
	if len(r.entries) > r.maxBufferedEntries {
		r.cleanupOldestEntries(r.maxBufferedEntries / 10) // Remove ~10% of entries
	}

	r.mu.Unlock()

	// Send to channel (non-blocking)
	select {
	case r.entryChannel <- entryCopy:
		// Successfully sent
	default:
		// Channel full, skip sending but entry is still stored
	}

	// Process the entry
	r.processEntry(entryCopy)
}

// OnBatchWritten implements the wal.ReplicationHook interface
func (r *WALReplicator) OnBatchWritten(entries []*wal.Entry) {
	if atomic.LoadInt32(&r.closed) == 1 || len(entries) == 0 {
		return
	}

	r.mu.Lock()

	// Make copies to avoid potential mutation
	entriesCopy := make([]*wal.Entry, len(entries))
	batchTimestamp := entries[0].SequenceNumber

	for i, entry := range entries {
		entriesCopy[i] = &wal.Entry{
			SequenceNumber: entry.SequenceNumber,
			Type:           entry.Type,
			Key:            append([]byte{}, entry.Key...),
		}
		if entry.Value != nil {
			entriesCopy[i].Value = append([]byte{}, entry.Value...)
		}

		// Store individual entry
		r.entries[entriesCopy[i].SequenceNumber] = entriesCopy[i]

		// Update highest timestamp
		if entry.SequenceNumber > r.highestTimestamp {
			r.highestTimestamp = entry.SequenceNumber
		}
	}

	// Store the batch
	r.batches[batchTimestamp] = entriesCopy

	// Cleanup old entries if we exceed the buffer size
	if len(r.entries) > r.maxBufferedEntries {
		r.cleanupOldestEntries(r.maxBufferedEntries / 10)
	}

	// Cleanup old batches if we have too many
	if len(r.batches) > r.maxBufferedEntries/10 {
		r.cleanupOldestBatches(r.maxBufferedEntries / 100)
	}

	r.mu.Unlock()

	// Send to batch channel (non-blocking)
	select {
	case r.batchChannel <- entriesCopy:
		// Successfully sent
	default:
		// Channel full, skip sending but entries are still stored
	}

	// Process the batch
	r.processBatch(entriesCopy)
}

// GetHighestTimestamp returns the highest timestamp seen so far
func (r *WALReplicator) GetHighestTimestamp() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.highestTimestamp
}

// GetEntriesAfter returns all entries with timestamps greater than the given position
func (r *WALReplicator) GetEntriesAfter(position ReplicationPosition) ([]*wal.Entry, error) {
	if atomic.LoadInt32(&r.closed) == 1 {
		return nil, ErrReplicatorClosed
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Create a result slice with appropriate capacity
	result := make([]*wal.Entry, 0, min(100, len(r.entries)))

	// Find all entries with timestamps greater than the position
	for timestamp, entry := range r.entries {
		if timestamp > position.Timestamp {
			result = append(result, entry)
		}
	}

	// Sort the entries by timestamp
	sortEntriesByTimestamp(result)

	return result, nil
}

// GetEntryCount returns the number of entries currently stored
func (r *WALReplicator) GetEntryCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// GetBatchCount returns the number of batches currently stored
func (r *WALReplicator) GetBatchCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.batches)
}

// SubscribeToEntries returns a channel that receives entries as they're captured
func (r *WALReplicator) SubscribeToEntries() <-chan *wal.Entry {
	return r.entryChannel
}

// SubscribeToBatches returns a channel that receives batches as they're captured
func (r *WALReplicator) SubscribeToBatches() <-chan []*wal.Entry {
	return r.batchChannel
}

// AddProcessor adds an EntryProcessor to receive entries as they're captured
func (r *WALReplicator) AddProcessor(processor EntryProcessor) {
	if atomic.LoadInt32(&r.closed) == 1 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.processors = append(r.processors, processor)
}

// Close closes the replicator and its channels
func (r *WALReplicator) Close() error {
	// Set closed flag
	if !atomic.CompareAndSwapInt32(&r.closed, 0, 1) {
		return nil // Already closed
	}

	// Close channels
	close(r.entryChannel)
	close(r.batchChannel)

	// Clear entries and batches
	r.mu.Lock()
	defer r.mu.Unlock()

	r.entries = make(map[uint64]*wal.Entry)
	r.batches = make(map[uint64][]*wal.Entry)
	r.processors = nil

	return nil
}

// cleanupOldestEntries removes the oldest entries from the buffer
func (r *WALReplicator) cleanupOldestEntries(count int) {
	// Find the oldest timestamps
	oldestTimestamps := findOldestTimestamps(r.entries, count)

	// Remove the oldest entries
	for _, ts := range oldestTimestamps {
		delete(r.entries, ts)
	}
}

// cleanupOldestBatches removes the oldest batches from the buffer
func (r *WALReplicator) cleanupOldestBatches(count int) {
	// Find the oldest timestamps
	oldestTimestamps := findOldestTimestamps(r.batches, count)

	// Remove the oldest batches
	for _, ts := range oldestTimestamps {
		delete(r.batches, ts)
	}
}

// processEntry sends the entry to all registered processors
func (r *WALReplicator) processEntry(entry *wal.Entry) {
	r.mu.RLock()
	processors := r.processors
	r.mu.RUnlock()

	for _, processor := range processors {
		_ = processor.ProcessEntry(entry) // Ignore errors for now
	}
}

// processBatch sends the batch to all registered processors
func (r *WALReplicator) processBatch(entries []*wal.Entry) {
	r.mu.RLock()
	processors := r.processors
	r.mu.RUnlock()

	for _, processor := range processors {
		_ = processor.ProcessBatch(entries) // Ignore errors for now
	}
}

// findOldestTimestamps finds the n oldest timestamps in a map
func findOldestTimestamps[T any](m map[uint64]T, n int) []uint64 {
	if len(m) <= n {
		// If we don't have enough entries, return all timestamps
		result := make([]uint64, 0, len(m))
		for ts := range m {
			result = append(result, ts)
		}
		return result
	}

	// Find the n smallest timestamps
	result := make([]uint64, 0, n)
	for ts := range m {
		if len(result) < n {
			// Add to result if we don't have enough yet
			result = append(result, ts)
		} else {
			// Find the largest timestamp in our result
			largestIdx := 0
			for i, t := range result {
				if t > result[largestIdx] {
					largestIdx = i
				}
			}

			// Replace the largest with this one if it's smaller
			if ts < result[largestIdx] {
				result[largestIdx] = ts
			}
		}
	}

	return result
}

// sortEntriesByTimestamp sorts a slice of entries by their timestamps
func sortEntriesByTimestamp(entries []*wal.Entry) {
	// Simple insertion sort for small slices
	for i := 1; i < len(entries); i++ {
		j := i
		for j > 0 && entries[j-1].SequenceNumber > entries[j].SequenceNumber {
			entries[j], entries[j-1] = entries[j-1], entries[j]
			j--
		}
	}
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
