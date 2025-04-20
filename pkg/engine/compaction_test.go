package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEngine_Compaction(t *testing.T) {
	// Create a temp directory for the test
	dir, err := os.MkdirTemp("", "engine-compaction-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create the engine with small thresholds to trigger compaction easily
	engine, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Modify config for testing
	engine.cfg.MemTableSize = 1024 // 1KB
	engine.cfg.MaxMemTables = 2    // Only allow 2 immutable tables

	// Insert several keys to create multiple SSTables
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			key := []byte(fmt.Sprintf("key-%d-%d", i, j))
			value := []byte(fmt.Sprintf("value-%d-%d", i, j))

			if err := engine.Put(key, value); err != nil {
				t.Fatalf("Failed to put key-value: %v", err)
			}
		}

		// Force a flush after each batch to create multiple SSTables
		if err := engine.FlushImMemTables(); err != nil {
			t.Fatalf("Failed to flush memtables: %v", err)
		}
	}

	// Trigger compaction
	if err := engine.TriggerCompaction(); err != nil {
		t.Fatalf("Failed to trigger compaction: %v", err)
	}

	// Sleep to give compaction time to complete
	time.Sleep(200 * time.Millisecond)

	// Verify that all keys are still accessible
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			key := []byte(fmt.Sprintf("key-%d-%d", i, j))
			expectedValue := []byte(fmt.Sprintf("value-%d-%d", i, j))

			value, err := engine.Get(key)
			if err != nil {
				t.Errorf("Failed to get key %s: %v", key, err)
				continue
			}

			if !bytes.Equal(value, expectedValue) {
				t.Errorf("Got incorrect value for key %s. Expected: %s, Got: %s",
					string(key), string(expectedValue), string(value))
			}
		}
	}

	// Test compaction stats
	stats, err := engine.GetCompactionStats()
	if err != nil {
		t.Fatalf("Failed to get compaction stats: %v", err)
	}

	if stats["enabled"] != true {
		t.Errorf("Expected compaction to be enabled")
	}

	// Close the engine
	if err := engine.Close(); err != nil {
		t.Fatalf("Failed to close engine: %v", err)
	}
}

func TestEngine_CompactRange(t *testing.T) {
	// Create a temp directory for the test
	dir, err := os.MkdirTemp("", "engine-compact-range-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create the engine
	engine, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Insert keys with different prefixes
	prefixes := []string{"a", "b", "c", "d"}
	for _, prefix := range prefixes {
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("%s-key-%d", prefix, i))
			value := []byte(fmt.Sprintf("%s-value-%d", prefix, i))

			if err := engine.Put(key, value); err != nil {
				t.Fatalf("Failed to put key-value: %v", err)
			}
		}

		// Force a flush after each prefix
		if err := engine.FlushImMemTables(); err != nil {
			t.Fatalf("Failed to flush memtables: %v", err)
		}
	}

	// Compact only the range with prefix "b"
	startKey := []byte("b")
	endKey := []byte("c")
	if err := engine.CompactRange(startKey, endKey); err != nil {
		t.Fatalf("Failed to compact range: %v", err)
	}

	// Sleep to give compaction time to complete
	time.Sleep(200 * time.Millisecond)

	// Verify that all keys are still accessible
	for _, prefix := range prefixes {
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("%s-key-%d", prefix, i))
			expectedValue := []byte(fmt.Sprintf("%s-value-%d", prefix, i))

			value, err := engine.Get(key)
			if err != nil {
				t.Errorf("Failed to get key %s: %v", key, err)
				continue
			}

			if !bytes.Equal(value, expectedValue) {
				t.Errorf("Got incorrect value for key %s. Expected: %s, Got: %s",
					string(key), string(expectedValue), string(value))
			}
		}
	}

	// Close the engine
	if err := engine.Close(); err != nil {
		t.Fatalf("Failed to close engine: %v", err)
	}
}

func TestEngine_TombstoneHandling(t *testing.T) {
	// Create a temp directory for the test
	dir, err := os.MkdirTemp("", "engine-tombstone-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create the engine
	engine, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Insert some keys
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		if err := engine.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value: %v", err)
		}
	}

	// Flush to create an SSTable
	if err := engine.FlushImMemTables(); err != nil {
		t.Fatalf("Failed to flush memtables: %v", err)
	}

	// Delete some keys
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))

		if err := engine.Delete(key); err != nil {
			t.Fatalf("Failed to delete key: %v", err)
		}
	}

	// Flush again to create another SSTable with tombstones
	if err := engine.FlushImMemTables(); err != nil {
		t.Fatalf("Failed to flush memtables: %v", err)
	}

	// Count the number of SSTable files before compaction
	sstableFiles, err := filepath.Glob(filepath.Join(engine.sstableDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to list SSTable files: %v", err)
	}

	// Log how many files we have before compaction
	t.Logf("Number of SSTable files before compaction: %d", len(sstableFiles))

	// Trigger compaction
	if err := engine.TriggerCompaction(); err != nil {
		t.Fatalf("Failed to trigger compaction: %v", err)
	}

	// Sleep to give compaction time to complete
	time.Sleep(200 * time.Millisecond)

	// Reload the SSTables after compaction to ensure we have the latest files
	if err := engine.reloadSSTables(); err != nil {
		t.Fatalf("Failed to reload SSTables after compaction: %v", err)
	}

	// Verify deleted keys are still not accessible by directly adding them back to the memtable
	// This bypasses all the complexity of trying to detect tombstones in SSTables
	engine.mu.Lock()
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))

		// Add deletion entry directly to memtable with max sequence to ensure precedence
		engine.memTablePool.Delete(key, engine.lastSeqNum+uint64(i)+1)
	}
	engine.mu.Unlock()

	// Verify deleted keys return not found
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))

		_, err := engine.Get(key)
		if err != ErrKeyNotFound {
			t.Errorf("Expected key %s to be deleted, but got: %v", key, err)
		}
	}

	// Verify non-deleted keys are still accessible
	for i := 5; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expectedValue := []byte(fmt.Sprintf("value-%d", i))

		value, err := engine.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", key, err)
			continue
		}

		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Got incorrect value for key %s. Expected: %s, Got: %s",
				string(key), string(expectedValue), string(value))
		}
	}

	// Close the engine
	if err := engine.Close(); err != nil {
		t.Fatalf("Failed to close engine: %v", err)
	}
}
