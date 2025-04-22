package memtable

import (
	"os"
	"testing"

	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/wal"
)

func setupTestWAL(t *testing.T) (string, *wal.WAL, func()) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "memtable_recovery_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Create config
	cfg := config.NewDefaultConfig(tmpDir)

	// Create WAL
	w, err := wal.NewWAL(cfg, tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create WAL: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		w.Close()
		os.RemoveAll(tmpDir)
	}

	return tmpDir, w, cleanup
}

func TestRecoverFromWAL(t *testing.T) {
	tmpDir, w, cleanup := setupTestWAL(t)
	defer cleanup()

	// Add entries to the WAL
	entries := []struct {
		opType uint8
		key    string
		value  string
	}{
		{wal.OpTypePut, "key1", "value1"},
		{wal.OpTypePut, "key2", "value2"},
		{wal.OpTypeDelete, "key1", ""},
		{wal.OpTypePut, "key3", "value3"},
	}

	for _, e := range entries {
		var seq uint64
		var err error

		if e.opType == wal.OpTypePut {
			seq, err = w.Append(e.opType, []byte(e.key), []byte(e.value))
		} else {
			seq, err = w.Append(e.opType, []byte(e.key), nil)
		}

		if err != nil {
			t.Fatalf("failed to append to WAL: %v", err)
		}
		t.Logf("Appended entry with seq %d", seq)
	}

	// Sync and close WAL
	if err := w.Sync(); err != nil {
		t.Fatalf("failed to sync WAL: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	// Create config for recovery
	cfg := config.NewDefaultConfig(tmpDir)
	cfg.WALDir = tmpDir
	cfg.MemTableSize = 1024 * 1024 // 1MB

	// Recover memtables from WAL
	memTables, maxSeq, err := RecoverFromWAL(cfg, nil)
	if err != nil {
		t.Fatalf("failed to recover from WAL: %v", err)
	}

	// Validate recovery results
	if len(memTables) == 0 {
		t.Fatalf("expected at least one memtable from recovery")
	}

	t.Logf("Recovered %d memtables with max sequence %d", len(memTables), maxSeq)

	// The max sequence number should be 4
	if maxSeq != 4 {
		t.Errorf("expected max sequence number 4, got %d", maxSeq)
	}

	// Validate content of the recovered memtable
	mt := memTables[0]

	// key1 should be deleted
	value, found := mt.Get([]byte("key1"))
	if !found {
		t.Errorf("expected key1 to be found (as deleted)")
	}
	if value != nil {
		t.Errorf("expected key1 to have nil value (deleted), got %v", value)
	}

	// key2 should have "value2"
	value, found = mt.Get([]byte("key2"))
	if !found {
		t.Errorf("expected key2 to be found")
	} else if string(value) != "value2" {
		t.Errorf("expected key2 to have value 'value2', got '%s'", string(value))
	}

	// key3 should have "value3"
	value, found = mt.Get([]byte("key3"))
	if !found {
		t.Errorf("expected key3 to be found")
	} else if string(value) != "value3" {
		t.Errorf("expected key3 to have value 'value3', got '%s'", string(value))
	}
}

func TestRecoveryWithMultipleMemTables(t *testing.T) {
	tmpDir, w, cleanup := setupTestWAL(t)
	defer cleanup()

	// Create a lot of large entries to force multiple memtables
	largeValue := make([]byte, 1000) // 1KB value
	for i := 0; i < 10; i++ {
		key := []byte{byte(i + 'a')}
		if _, err := w.Append(wal.OpTypePut, key, largeValue); err != nil {
			t.Fatalf("failed to append to WAL: %v", err)
		}
	}

	// Sync and close WAL
	if err := w.Sync(); err != nil {
		t.Fatalf("failed to sync WAL: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	// Create config for recovery with small memtable size
	cfg := config.NewDefaultConfig(tmpDir)
	cfg.WALDir = tmpDir
	cfg.MemTableSize = 5 * 1000 // 5KB - should fit about 5 entries
	cfg.MaxMemTables = 3        // Allow up to 3 memtables

	// Recover memtables from WAL
	memTables, _, err := RecoverFromWAL(cfg, nil)
	if err != nil {
		t.Fatalf("failed to recover from WAL: %v", err)
	}

	// Should have created multiple memtables
	if len(memTables) <= 1 {
		t.Errorf("expected multiple memtables due to size, got %d", len(memTables))
	}

	t.Logf("Recovered %d memtables", len(memTables))

	// All memtables except the last one should be immutable
	for i, mt := range memTables[:len(memTables)-1] {
		if !mt.IsImmutable() {
			t.Errorf("expected memtable %d to be immutable", i)
		}
	}

	// Verify all data was recovered across all memtables
	for i := 0; i < 10; i++ {
		key := []byte{byte(i + 'a')}
		found := false

		// Check each memtable for the key
		for _, mt := range memTables {
			if _, exists := mt.Get(key); exists {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("key %c not found in any memtable", i+'a')
		}
	}
}

func TestRecoveryWithBatchOperations(t *testing.T) {
	tmpDir, w, cleanup := setupTestWAL(t)
	defer cleanup()

	// Create a batch of operations
	batch := wal.NewBatch()
	batch.Put([]byte("batch_key1"), []byte("batch_value1"))
	batch.Put([]byte("batch_key2"), []byte("batch_value2"))
	batch.Delete([]byte("batch_key3"))

	// Write the batch to the WAL
	if err := batch.Write(w); err != nil {
		t.Fatalf("failed to write batch to WAL: %v", err)
	}

	// Add some individual operations too
	if _, err := w.Append(wal.OpTypePut, []byte("key4"), []byte("value4")); err != nil {
		t.Fatalf("failed to append to WAL: %v", err)
	}

	// Sync and close WAL
	if err := w.Sync(); err != nil {
		t.Fatalf("failed to sync WAL: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	// Create config for recovery
	cfg := config.NewDefaultConfig(tmpDir)
	cfg.WALDir = tmpDir

	// Recover memtables from WAL
	memTables, maxSeq, err := RecoverFromWAL(cfg, nil)
	if err != nil {
		t.Fatalf("failed to recover from WAL: %v", err)
	}

	if len(memTables) == 0 {
		t.Fatalf("expected at least one memtable from recovery")
	}

	// The max sequence number should account for batch operations
	if maxSeq < 3 { // At least 3 from batch + individual op
		t.Errorf("expected max sequence number >= 3, got %d", maxSeq)
	}

	// Validate content of the recovered memtable
	mt := memTables[0]

	// Check batch keys were recovered
	value, found := mt.Get([]byte("batch_key1"))
	if !found {
		t.Errorf("batch_key1 not found in recovered memtable")
	} else if string(value) != "batch_value1" {
		t.Errorf("expected batch_key1 to have value 'batch_value1', got '%s'", string(value))
	}

	value, found = mt.Get([]byte("batch_key2"))
	if !found {
		t.Errorf("batch_key2 not found in recovered memtable")
	} else if string(value) != "batch_value2" {
		t.Errorf("expected batch_key2 to have value 'batch_value2', got '%s'", string(value))
	}

	// batch_key3 should be marked as deleted
	value, found = mt.Get([]byte("batch_key3"))
	if !found {
		t.Errorf("expected batch_key3 to be found as deleted")
	}
	if value != nil {
		t.Errorf("expected batch_key3 to have nil value (deleted), got %v", value)
	}

	// Check individual operation was recovered
	value, found = mt.Get([]byte("key4"))
	if !found {
		t.Errorf("key4 not found in recovered memtable")
	} else if string(value) != "value4" {
		t.Errorf("expected key4 to have value 'value4', got '%s'", string(value))
	}
}
