package wal

import (
	"os"
	"sync"
	"testing"

	"github.com/KevoDB/kevo/pkg/config"
)

// mockWALObserver implements WALEntryObserver for testing
type mockWALObserver struct {
	entries        []*Entry
	batches        [][]*Entry
	batchSeqs      []uint64
	syncs          []uint64
	entriesMu      sync.Mutex
	batchesMu      sync.Mutex
	syncsMu        sync.Mutex
	entryCallCount int
	batchCallCount int
	syncCallCount  int
}

func newMockWALObserver() *mockWALObserver {
	return &mockWALObserver{
		entries:   make([]*Entry, 0),
		batches:   make([][]*Entry, 0),
		batchSeqs: make([]uint64, 0),
		syncs:     make([]uint64, 0),
	}
}

func (m *mockWALObserver) OnWALEntryWritten(entry *Entry) {
	m.entriesMu.Lock()
	defer m.entriesMu.Unlock()
	m.entries = append(m.entries, entry)
	m.entryCallCount++
}

func (m *mockWALObserver) OnWALBatchWritten(startSeq uint64, entries []*Entry) {
	m.batchesMu.Lock()
	defer m.batchesMu.Unlock()
	m.batches = append(m.batches, entries)
	m.batchSeqs = append(m.batchSeqs, startSeq)
	m.batchCallCount++
}

func (m *mockWALObserver) OnWALSync(upToSeq uint64) {
	m.syncsMu.Lock()
	defer m.syncsMu.Unlock()
	m.syncs = append(m.syncs, upToSeq)
	m.syncCallCount++
}

func (m *mockWALObserver) getEntryCallCount() int {
	m.entriesMu.Lock()
	defer m.entriesMu.Unlock()
	return m.entryCallCount
}

func (m *mockWALObserver) getBatchCallCount() int {
	m.batchesMu.Lock()
	defer m.batchesMu.Unlock()
	return m.batchCallCount
}

func (m *mockWALObserver) getSyncCallCount() int {
	m.syncsMu.Lock()
	defer m.syncsMu.Unlock()
	return m.syncCallCount
}

func TestWALObserver(t *testing.T) {
	// Create a temporary directory for the WAL
	tempDir, err := os.MkdirTemp("", "wal_observer_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create WAL configuration
	cfg := config.NewDefaultConfig(tempDir)
	cfg.WALSyncMode = config.SyncNone // To control syncs manually

	// Create a new WAL
	w, err := NewWAL(cfg, tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Create a mock observer
	observer := newMockWALObserver()

	// Register the observer
	w.RegisterObserver("test", observer)

	// Test single entry
	t.Run("SingleEntry", func(t *testing.T) {
		key := []byte("key1")
		value := []byte("value1")
		seq, err := w.Append(OpTypePut, key, value)
		if err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}
		if seq != 1 {
			t.Errorf("Expected sequence number 1, got %d", seq)
		}

		// Check observer was notified
		if observer.getEntryCallCount() != 1 {
			t.Errorf("Expected entry call count to be 1, got %d", observer.getEntryCallCount())
		}
		if len(observer.entries) != 1 {
			t.Fatalf("Expected 1 entry, got %d", len(observer.entries))
		}
		if string(observer.entries[0].Key) != string(key) {
			t.Errorf("Expected key %s, got %s", key, observer.entries[0].Key)
		}
		if string(observer.entries[0].Value) != string(value) {
			t.Errorf("Expected value %s, got %s", value, observer.entries[0].Value)
		}
		if observer.entries[0].Type != OpTypePut {
			t.Errorf("Expected type %d, got %d", OpTypePut, observer.entries[0].Type)
		}
		if observer.entries[0].SequenceNumber != 1 {
			t.Errorf("Expected sequence number 1, got %d", observer.entries[0].SequenceNumber)
		}
	})

	// Test batch
	t.Run("Batch", func(t *testing.T) {
		batch := NewBatch()
		batch.Put([]byte("key2"), []byte("value2"))
		batch.Put([]byte("key3"), []byte("value3"))
		batch.Delete([]byte("key4"))

		entries := []*Entry{
			{
				Key:   []byte("key2"),
				Value: []byte("value2"),
				Type:  OpTypePut,
			},
			{
				Key:   []byte("key3"),
				Value: []byte("value3"),
				Type:  OpTypePut,
			},
			{
				Key:  []byte("key4"),
				Type: OpTypeDelete,
			},
		}

		startSeq, err := w.AppendBatch(entries)
		if err != nil {
			t.Fatalf("Failed to append batch: %v", err)
		}
		if startSeq != 2 {
			t.Errorf("Expected start sequence 2, got %d", startSeq)
		}

		// Check observer was notified for the batch
		if observer.getBatchCallCount() != 1 {
			t.Errorf("Expected batch call count to be 1, got %d", observer.getBatchCallCount())
		}
		if len(observer.batches) != 1 {
			t.Fatalf("Expected 1 batch, got %d", len(observer.batches))
		}
		if len(observer.batches[0]) != 3 {
			t.Errorf("Expected 3 entries in batch, got %d", len(observer.batches[0]))
		}
		if observer.batchSeqs[0] != 2 {
			t.Errorf("Expected batch sequence 2, got %d", observer.batchSeqs[0])
		}
	})

	// Test sync
	t.Run("Sync", func(t *testing.T) {
		err := w.Sync()
		if err != nil {
			t.Fatalf("Failed to sync WAL: %v", err)
		}

		// Check observer was notified about the sync
		if observer.getSyncCallCount() != 1 {
			t.Errorf("Expected sync call count to be 1, got %d", observer.getSyncCallCount())
		}
		if len(observer.syncs) != 1 {
			t.Fatalf("Expected 1 sync notification, got %d", len(observer.syncs))
		}
		// Should be 4 because we have written 1 + 3 entries
		if observer.syncs[0] != 4 {
			t.Errorf("Expected sync sequence 4, got %d", observer.syncs[0])
		}
	})

	// Test unregister
	t.Run("Unregister", func(t *testing.T) {
		// Unregister the observer
		w.UnregisterObserver("test")

		// Add a new entry and verify observer does not get notified
		prevEntryCount := observer.getEntryCallCount()
		_, err := w.Append(OpTypePut, []byte("key5"), []byte("value5"))
		if err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}

		// Observer should not be notified
		if observer.getEntryCallCount() != prevEntryCount {
			t.Errorf("Expected entry call count to remain %d, got %d", prevEntryCount, observer.getEntryCallCount())
		}

		// Re-register for cleanup
		w.RegisterObserver("test", observer)
	})
}

func TestWALObserverMultiple(t *testing.T) {
	// Create a temporary directory for the WAL
	tempDir, err := os.MkdirTemp("", "wal_observer_multi_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create WAL configuration
	cfg := config.NewDefaultConfig(tempDir)
	cfg.WALSyncMode = config.SyncNone

	// Create a new WAL
	w, err := NewWAL(cfg, tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Create multiple observers
	obs1 := newMockWALObserver()
	obs2 := newMockWALObserver()

	// Register the observers
	w.RegisterObserver("obs1", obs1)
	w.RegisterObserver("obs2", obs2)

	// Append an entry
	_, err = w.Append(OpTypePut, []byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	// Both observers should be notified
	if obs1.getEntryCallCount() != 1 {
		t.Errorf("Observer 1: Expected entry call count to be 1, got %d", obs1.getEntryCallCount())
	}
	if obs2.getEntryCallCount() != 1 {
		t.Errorf("Observer 2: Expected entry call count to be 1, got %d", obs2.getEntryCallCount())
	}

	// Unregister one observer
	w.UnregisterObserver("obs1")

	// Append another entry
	_, err = w.Append(OpTypePut, []byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Failed to append second entry: %v", err)
	}

	// Only obs2 should be notified about the second entry
	if obs1.getEntryCallCount() != 1 {
		t.Errorf("Observer 1: Expected entry call count to remain 1, got %d", obs1.getEntryCallCount())
	}
	if obs2.getEntryCallCount() != 2 {
		t.Errorf("Observer 2: Expected entry call count to be 2, got %d", obs2.getEntryCallCount())
	}
}