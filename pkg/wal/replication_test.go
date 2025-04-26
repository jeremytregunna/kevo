package wal

import (
	"os"
	"sync"
	"testing"

	"github.com/KevoDB/kevo/pkg/config"
)

// MockLamportClock implements the LamportClock interface for testing
type MockLamportClock struct {
	counter uint64
	mu      sync.Mutex
}

func NewMockLamportClock() *MockLamportClock {
	return &MockLamportClock{counter: 0}
}

func (c *MockLamportClock) Tick() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter++
	return c.counter
}

func (c *MockLamportClock) Update(received uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if received > c.counter {
		c.counter = received
	}
	c.counter++
	return c.counter
}

func (c *MockLamportClock) Current() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counter
}

// MockReplicationHook implements the ReplicationHook interface for testing
type MockReplicationHook struct {
	mu              sync.Mutex
	entries         []*Entry
	batchEntries    [][]*Entry
	entriesReceived int
	batchesReceived int
}

func (m *MockReplicationHook) OnEntryWritten(entry *Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Make a deep copy of the entry to ensure tests are not affected by later modifications
	entryCopy := &Entry{
		SequenceNumber: entry.SequenceNumber,
		Type:           entry.Type,
		Key:            append([]byte{}, entry.Key...),
	}
	if entry.Value != nil {
		entryCopy.Value = append([]byte{}, entry.Value...)
	}
	
	m.entries = append(m.entries, entryCopy)
	m.entriesReceived++
}

func (m *MockReplicationHook) OnBatchWritten(entries []*Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Make a deep copy of all entries
	entriesCopy := make([]*Entry, len(entries))
	for i, entry := range entries {
		entriesCopy[i] = &Entry{
			SequenceNumber: entry.SequenceNumber,
			Type:           entry.Type,
			Key:            append([]byte{}, entry.Key...),
		}
		if entry.Value != nil {
			entriesCopy[i].Value = append([]byte{}, entry.Value...)
		}
	}
	
	m.batchEntries = append(m.batchEntries, entriesCopy)
	m.batchesReceived++
}

func (m *MockReplicationHook) GetEntries() []*Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries
}

func (m *MockReplicationHook) GetBatchEntries() [][]*Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.batchEntries
}

func (m *MockReplicationHook) GetStats() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entriesReceived, m.batchesReceived
}

func TestWALReplicationHook(t *testing.T) {
	// Create a temporary directory for the WAL
	dir, err := os.MkdirTemp("", "wal_replication_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a mock replication hook
	hook := &MockReplicationHook{}
	
	// Create a Lamport clock
	clock := NewMockLamportClock()
	
	// Create a WAL with the replication hook
	cfg := config.NewDefaultConfig(dir)
	wal, err := NewWALWithReplication(cfg, dir, clock, hook)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Test single entry writes
	key1 := []byte("key1")
	value1 := []byte("value1")
	
	seq1, err := wal.Append(OpTypePut, key1, value1)
	if err != nil {
		t.Fatalf("Failed to append to WAL: %v", err)
	}
	
	// Test that the hook received the entry
	entries := hook.GetEntries()
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}
	
	entry := entries[0]
	if entry.SequenceNumber != seq1 {
		t.Errorf("Expected sequence number %d, got %d", seq1, entry.SequenceNumber)
	}
	if entry.Type != OpTypePut {
		t.Errorf("Expected type %d, got %d", OpTypePut, entry.Type)
	}
	if string(entry.Key) != string(key1) {
		t.Errorf("Expected key %q, got %q", key1, entry.Key)
	}
	if string(entry.Value) != string(value1) {
		t.Errorf("Expected value %q, got %q", value1, entry.Value)
	}

	// Test batch writes
	key2 := []byte("key2")
	value2 := []byte("value2")
	key3 := []byte("key3")
	value3 := []byte("value3")
	
	batchEntries := []*Entry{
		{Type: OpTypePut, Key: key2, Value: value2},
		{Type: OpTypePut, Key: key3, Value: value3},
	}
	
	batchSeq, err := wal.AppendBatch(batchEntries)
	if err != nil {
		t.Fatalf("Failed to append batch to WAL: %v", err)
	}
	
	// Test that the hook received the batch
	batches := hook.GetBatchEntries()
	if len(batches) != 1 {
		t.Fatalf("Expected 1 batch, got %d", len(batches))
	}
	
	batch := batches[0]
	if len(batch) != 2 {
		t.Fatalf("Expected 2 entries in batch, got %d", len(batch))
	}
	
	// Check first entry in batch
	if batch[0].SequenceNumber != batchSeq {
		t.Errorf("Expected sequence number %d, got %d", batchSeq, batch[0].SequenceNumber)
	}
	if batch[0].Type != OpTypePut {
		t.Errorf("Expected type %d, got %d", OpTypePut, batch[0].Type)
	}
	if string(batch[0].Key) != string(key2) {
		t.Errorf("Expected key %q, got %q", key2, batch[0].Key)
	}
	if string(batch[0].Value) != string(value2) {
		t.Errorf("Expected value %q, got %q", value2, batch[0].Value)
	}
	
	// Check second entry in batch
	if batch[1].SequenceNumber != batchSeq+1 {
		t.Errorf("Expected sequence number %d, got %d", batchSeq+1, batch[1].SequenceNumber)
	}
	if batch[1].Type != OpTypePut {
		t.Errorf("Expected type %d, got %d", OpTypePut, batch[1].Type)
	}
	if string(batch[1].Key) != string(key3) {
		t.Errorf("Expected key %q, got %q", key3, batch[1].Key)
	}
	if string(batch[1].Value) != string(value3) {
		t.Errorf("Expected value %q, got %q", value3, batch[1].Value)
	}
	
	// Check call counts
	entriesReceived, batchesReceived := hook.GetStats()
	if entriesReceived != 1 {
		t.Errorf("Expected 1 single entry received, got %d", entriesReceived)
	}
	if batchesReceived != 1 {
		t.Errorf("Expected 1 batch received, got %d", batchesReceived)
	}
}

func TestWALWithLamportClock(t *testing.T) {
	// Create a temporary directory for the WAL
	dir, err := os.MkdirTemp("", "wal_lamport_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a Lamport clock
	clock := NewMockLamportClock()
	
	// Create a WAL with the Lamport clock but no hook
	cfg := config.NewDefaultConfig(dir)
	wal, err := NewWALWithReplication(cfg, dir, clock, nil)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Pre-tick the clock a few times to simulate a distributed system
	for i := 0; i < 5; i++ {
		clock.Tick()
	}
	
	// Current clock value should be 5
	if clock.Current() != 5 {
		t.Fatalf("Expected clock value 5, got %d", clock.Current())
	}

	// Test that the WAL uses the Lamport clock for sequence numbers
	key1 := []byte("key1")
	value1 := []byte("value1")
	
	seq1, err := wal.Append(OpTypePut, key1, value1)
	if err != nil {
		t.Fatalf("Failed to append to WAL: %v", err)
	}
	
	// Sequence number should be 6 (previous 5 + 1 for this operation)
	if seq1 != 6 {
		t.Errorf("Expected sequence number 6, got %d", seq1)
	}
	
	// Clock should have incremented
	if clock.Current() != 6 {
		t.Errorf("Expected clock value 6, got %d", clock.Current())
	}
	
	// Test with a batch
	entries := []*Entry{
		{Type: OpTypePut, Key: []byte("key2"), Value: []byte("value2")},
		{Type: OpTypePut, Key: []byte("key3"), Value: []byte("value3")},
	}
	
	batchSeq, err := wal.AppendBatch(entries)
	if err != nil {
		t.Fatalf("Failed to append batch to WAL: %v", err)
	}
	
	// Batch sequence should be 7
	if batchSeq != 7 {
		t.Errorf("Expected batch sequence number 7, got %d", batchSeq)
	}
	
	// Clock should have incremented again
	if clock.Current() != 7 {
		t.Errorf("Expected clock value 7, got %d", clock.Current())
	}
}

func TestWALHookAfterCreation(t *testing.T) {
	// Create a temporary directory for the WAL
	dir, err := os.MkdirTemp("", "wal_hook_after_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a WAL without hook initially
	cfg := config.NewDefaultConfig(dir)
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write an entry before adding a hook
	key1 := []byte("key1")
	value1 := []byte("value1")
	
	_, err = wal.Append(OpTypePut, key1, value1)
	if err != nil {
		t.Fatalf("Failed to append to WAL: %v", err)
	}
	
	// Create and add a hook after the fact
	hook := &MockReplicationHook{}
	wal.SetReplicationHook(hook)
	
	// Create and add a Lamport clock after the fact
	clock := NewMockLamportClock()
	wal.SetLamportClock(clock)
	
	// Write another entry, this should trigger the hook
	key2 := []byte("key2")
	value2 := []byte("value2")
	
	seq2, err := wal.Append(OpTypePut, key2, value2)
	if err != nil {
		t.Fatalf("Failed to append to WAL: %v", err)
	}
	
	// Verify hook received the entry
	entries := hook.GetEntries()
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry in hook, got %d", len(entries))
	}
	
	if entries[0].SequenceNumber != seq2 {
		t.Errorf("Expected sequence number %d, got %d", seq2, entries[0].SequenceNumber)
	}
	
	if string(entries[0].Key) != string(key2) {
		t.Errorf("Expected key %q, got %q", key2, entries[0].Key)
	}
	
	// Verify the clock was used
	if seq2 != 1 { // First tick of the clock
		t.Errorf("Expected sequence from clock to be 1, got %d", seq2)
	}
}