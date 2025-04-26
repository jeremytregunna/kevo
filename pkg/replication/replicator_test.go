package replication

import (
	"sync"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/wal"
)

// MockEntryProcessor implements the EntryProcessor interface for testing
type MockEntryProcessor struct {
	mu               sync.Mutex
	processedEntries []*wal.Entry
	processedBatches [][]*wal.Entry
	entriesProcessed int
	batchesProcessed int
	failProcessEntry bool
	failProcessBatch bool
}

func (m *MockEntryProcessor) ProcessEntry(entry *wal.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.processedEntries = append(m.processedEntries, entry)
	m.entriesProcessed++

	if m.failProcessEntry {
		return ErrReplicatorClosed // Just use an existing error
	}
	return nil
}

func (m *MockEntryProcessor) ProcessBatch(entries []*wal.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.processedBatches = append(m.processedBatches, entries)
	m.batchesProcessed++

	if m.failProcessBatch {
		return ErrReplicatorClosed
	}
	return nil
}

func (m *MockEntryProcessor) GetStats() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entriesProcessed, m.batchesProcessed
}

func TestWALReplicatorBasic(t *testing.T) {
	replicator := NewWALReplicator(1000)
	defer replicator.Close()

	// Create some test entries
	entry1 := &wal.Entry{
		SequenceNumber: 1,
		Type:           wal.OpTypePut,
		Key:            []byte("key1"),
		Value:          []byte("value1"),
	}

	entry2 := &wal.Entry{
		SequenceNumber: 2,
		Type:           wal.OpTypePut,
		Key:            []byte("key2"),
		Value:          []byte("value2"),
	}

	// Process some entries
	replicator.OnEntryWritten(entry1)
	replicator.OnEntryWritten(entry2)

	// Check entry count
	if count := replicator.GetEntryCount(); count != 2 {
		t.Errorf("Expected 2 entries, got %d", count)
	}

	// Check highest timestamp
	if ts := replicator.GetHighestTimestamp(); ts != 2 {
		t.Errorf("Expected highest timestamp 2, got %d", ts)
	}

	// Get entries after timestamp 0
	entries, err := replicator.GetEntriesAfter(ReplicationPosition{Timestamp: 0})
	if err != nil {
		t.Fatalf("Error getting entries: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries after timestamp 0, got %d", len(entries))
	}

	// Check entries are sorted by timestamp
	if entries[0].SequenceNumber != 1 || entries[1].SequenceNumber != 2 {
		t.Errorf("Entries not sorted by timestamp")
	}

	// Get entries after timestamp 1
	entries, err = replicator.GetEntriesAfter(ReplicationPosition{Timestamp: 1})
	if err != nil {
		t.Fatalf("Error getting entries: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry after timestamp 1, got %d", len(entries))
	}

	if entries[0].SequenceNumber != 2 {
		t.Errorf("Expected entry with timestamp 2, got %d", entries[0].SequenceNumber)
	}
}

func TestWALReplicatorBatches(t *testing.T) {
	replicator := NewWALReplicator(1000)
	defer replicator.Close()

	// Create a batch of entries
	entries := []*wal.Entry{
		{
			SequenceNumber: 10,
			Type:           wal.OpTypePut,
			Key:            []byte("key1"),
			Value:          []byte("value1"),
		},
		{
			SequenceNumber: 11,
			Type:           wal.OpTypePut,
			Key:            []byte("key2"),
			Value:          []byte("value2"),
		},
	}

	// Process the batch
	replicator.OnBatchWritten(entries)

	// Check entry count
	if count := replicator.GetEntryCount(); count != 2 {
		t.Errorf("Expected 2 entries, got %d", count)
	}

	// Check batch count
	if count := replicator.GetBatchCount(); count != 1 {
		t.Errorf("Expected 1 batch, got %d", count)
	}

	// Check highest timestamp
	if ts := replicator.GetHighestTimestamp(); ts != 11 {
		t.Errorf("Expected highest timestamp 11, got %d", ts)
	}

	// Get entries after timestamp 9
	result, err := replicator.GetEntriesAfter(ReplicationPosition{Timestamp: 9})
	if err != nil {
		t.Fatalf("Error getting entries: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("Expected 2 entries after timestamp 9, got %d", len(result))
	}
}

func TestWALReplicatorProcessors(t *testing.T) {
	replicator := NewWALReplicator(1000)
	defer replicator.Close()

	// Create a processor
	processor := &MockEntryProcessor{}

	// Add the processor
	replicator.AddProcessor(processor)

	// Create an entry and a batch
	entry := &wal.Entry{
		SequenceNumber: 1,
		Type:           wal.OpTypePut,
		Key:            []byte("key1"),
		Value:          []byte("value1"),
	}

	batch := []*wal.Entry{
		{
			SequenceNumber: 10,
			Type:           wal.OpTypePut,
			Key:            []byte("key10"),
			Value:          []byte("value10"),
		},
		{
			SequenceNumber: 11,
			Type:           wal.OpTypePut,
			Key:            []byte("key11"),
			Value:          []byte("value11"),
		},
	}

	// Process the entry and batch
	replicator.OnEntryWritten(entry)
	replicator.OnBatchWritten(batch)

	// Check processor stats
	entriesProcessed, batchesProcessed := processor.GetStats()
	if entriesProcessed != 1 {
		t.Errorf("Expected 1 entry processed, got %d", entriesProcessed)
	}
	if batchesProcessed != 1 {
		t.Errorf("Expected 1 batch processed, got %d", batchesProcessed)
	}
}

func TestWALReplicatorSubscribe(t *testing.T) {
	replicator := NewWALReplicator(1000)
	defer replicator.Close()

	// Subscribe to entries and batches
	entryChannel := replicator.SubscribeToEntries()
	batchChannel := replicator.SubscribeToBatches()

	// Create an entry and a batch
	entry := &wal.Entry{
		SequenceNumber: 1,
		Type:           wal.OpTypePut,
		Key:            []byte("key1"),
		Value:          []byte("value1"),
	}

	batch := []*wal.Entry{
		{
			SequenceNumber: 10,
			Type:           wal.OpTypePut,
			Key:            []byte("key10"),
			Value:          []byte("value10"),
		},
		{
			SequenceNumber: 11,
			Type:           wal.OpTypePut,
			Key:            []byte("key11"),
			Value:          []byte("value11"),
		},
	}

	// Create channels to receive the results
	entryReceived := make(chan *wal.Entry, 1)
	batchReceived := make(chan []*wal.Entry, 1)

	// Start goroutines to receive from the channels
	go func() {
		select {
		case e := <-entryChannel:
			entryReceived <- e
		case <-time.After(time.Second):
			close(entryReceived)
		}
	}()

	go func() {
		select {
		case b := <-batchChannel:
			batchReceived <- b
		case <-time.After(time.Second):
			close(batchReceived)
		}
	}()

	// Process the entry and batch
	replicator.OnEntryWritten(entry)
	replicator.OnBatchWritten(batch)

	// Check that we received the entry
	select {
	case receivedEntry := <-entryReceived:
		if receivedEntry.SequenceNumber != 1 {
			t.Errorf("Expected entry with timestamp 1, got %d", receivedEntry.SequenceNumber)
		}
	case <-time.After(time.Second):
		t.Errorf("Timeout waiting for entry")
	}

	// Check that we received the batch
	select {
	case receivedBatch := <-batchReceived:
		if len(receivedBatch) != 2 {
			t.Errorf("Expected batch with 2 entries, got %d", len(receivedBatch))
		}
	case <-time.After(time.Second):
		t.Errorf("Timeout waiting for batch")
	}
}

func TestWALReplicatorCleanup(t *testing.T) {
	// Create a replicator with a small buffer
	replicator := NewWALReplicator(10)
	defer replicator.Close()

	// Add more entries than the buffer can hold
	for i := 0; i < 20; i++ {
		entry := &wal.Entry{
			SequenceNumber: uint64(i),
			Type:           wal.OpTypePut,
			Key:            []byte("key"),
			Value:          []byte("value"),
		}
		replicator.OnEntryWritten(entry)
	}

	// Check that some entries were cleaned up
	count := replicator.GetEntryCount()
	if count > 20 {
		t.Errorf("Expected fewer than 20 entries after cleanup, got %d", count)
	}

	// The most recent entries should still be there
	entries, err := replicator.GetEntriesAfter(ReplicationPosition{Timestamp: 15})
	if err != nil {
		t.Fatalf("Error getting entries: %v", err)
	}

	if len(entries) == 0 {
		t.Errorf("Expected some entries after timestamp 15")
	}
}

func TestWALReplicatorClose(t *testing.T) {
	replicator := NewWALReplicator(1000)

	// Add some entries
	entry := &wal.Entry{
		SequenceNumber: 1,
		Type:           wal.OpTypePut,
		Key:            []byte("key"),
		Value:          []byte("value"),
	}
	replicator.OnEntryWritten(entry)

	// Close the replicator
	if err := replicator.Close(); err != nil {
		t.Fatalf("Error closing replicator: %v", err)
	}

	// Check that we can't add more entries
	replicator.OnEntryWritten(entry)

	// Entry count should still be 0 after closure and cleanup
	if count := replicator.GetEntryCount(); count != 0 {
		t.Errorf("Expected 0 entries after close, got %d", count)
	}

	// Try to get entries (should return an error)
	_, err := replicator.GetEntriesAfter(ReplicationPosition{Timestamp: 0})
	if err != ErrReplicatorClosed {
		t.Errorf("Expected ErrReplicatorClosed, got %v", err)
	}
}

func TestFindOldestTimestamps(t *testing.T) {
	// Create a map with some timestamps
	m := map[uint64]string{
		1: "one",
		2: "two",
		3: "three",
		4: "four",
		5: "five",
	}

	// Find the 2 oldest timestamps
	result := findOldestTimestamps(m, 2)

	// Check the result length
	if len(result) != 2 {
		t.Fatalf("Expected 2 timestamps, got %d", len(result))
	}

	// Check that the result contains the 2 smallest timestamps
	for _, ts := range result {
		if ts != 1 && ts != 2 {
			t.Errorf("Expected timestamp 1 or 2, got %d", ts)
		}
	}
}

func TestSortEntriesByTimestamp(t *testing.T) {
	// Create some entries with unsorted timestamps
	entries := []*wal.Entry{
		{SequenceNumber: 3},
		{SequenceNumber: 1},
		{SequenceNumber: 2},
	}

	// Sort the entries
	sortEntriesByTimestamp(entries)

	// Check that the entries are sorted
	for i := 0; i < len(entries)-1; i++ {
		if entries[i].SequenceNumber > entries[i+1].SequenceNumber {
			t.Errorf("Entries not sorted at index %d: %d > %d",
				i, entries[i].SequenceNumber, entries[i+1].SequenceNumber)
		}
	}
}
