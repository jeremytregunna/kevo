package wal

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/KevoDB/kevo/pkg/config"
)

func createTestConfig() *config.Config {
	cfg := config.NewDefaultConfig("/tmp/gostorage_test")
	// Force immediate sync for tests
	cfg.WALSyncMode = config.SyncImmediate
	return cfg
}

func createTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	return dir
}

func TestWALWrite(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write some entries
	keys := []string{"key1", "key2", "key3"}
	values := []string{"value1", "value2", "value3"}

	for i, key := range keys {
		seq, err := wal.Append(OpTypePut, []byte(key), []byte(values[i]))
		if err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}

		if seq != uint64(i+1) {
			t.Errorf("Expected sequence %d, got %d", i+1, seq)
		}
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify entries by replaying
	entries := make(map[string]string)

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypePut {
			entries[string(entry.Key)] = string(entry.Value)
		} else if entry.Type == OpTypeDelete {
			delete(entries, string(entry.Key))
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify all entries are present
	for i, key := range keys {
		value, ok := entries[key]
		if !ok {
			t.Errorf("Entry for key %q not found", key)
			continue
		}

		if value != values[i] {
			t.Errorf("Expected value %q for key %q, got %q", values[i], key, value)
		}
	}
}

func TestWALDelete(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write and delete
	key := []byte("key1")
	value := []byte("value1")

	_, err = wal.Append(OpTypePut, key, value)
	if err != nil {
		t.Fatalf("Failed to append put entry: %v", err)
	}

	_, err = wal.Append(OpTypeDelete, key, nil)
	if err != nil {
		t.Fatalf("Failed to append delete entry: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify entries by replaying
	var deleted bool

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypePut && bytes.Equal(entry.Key, key) {
			if deleted {
				deleted = false // Key was re-added
			}
		} else if entry.Type == OpTypeDelete && bytes.Equal(entry.Key, key) {
			deleted = true
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if !deleted {
		t.Errorf("Expected key to be deleted")
	}
}

func TestWALLargeEntry(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create a large key and value (but not too large for a single record)
	key := make([]byte, 8*1024)    // 8KB
	value := make([]byte, 16*1024) // 16KB

	for i := range key {
		key[i] = byte(i % 256)
	}

	for i := range value {
		value[i] = byte((i * 2) % 256)
	}

	// Append the large entry
	_, err = wal.Append(OpTypePut, key, value)
	if err != nil {
		t.Fatalf("Failed to append large entry: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify by replaying
	var foundLargeEntry bool

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypePut && len(entry.Key) == len(key) && len(entry.Value) == len(value) {
			// Verify key
			for i := range key {
				if key[i] != entry.Key[i] {
					t.Errorf("Key mismatch at position %d: expected %d, got %d", i, key[i], entry.Key[i])
					return nil
				}
			}

			// Verify value
			for i := range value {
				if value[i] != entry.Value[i] {
					t.Errorf("Value mismatch at position %d: expected %d, got %d", i, value[i], entry.Value[i])
					return nil
				}
			}

			foundLargeEntry = true
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if !foundLargeEntry {
		t.Error("Large entry not found in replay")
	}
}

func TestWALBatch(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create a batch
	batch := NewBatch()

	keys := []string{"batch1", "batch2", "batch3"}
	values := []string{"value1", "value2", "value3"}

	for i, key := range keys {
		batch.Put([]byte(key), []byte(values[i]))
	}

	// Add a delete operation
	batch.Delete([]byte("batch2"))

	// Write the batch
	if err := batch.Write(wal); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify by replaying
	entries := make(map[string]string)
	batchCount := 0

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypeBatch {
			batchCount++

			// Decode batch
			batch, err := DecodeBatch(entry)
			if err != nil {
				t.Errorf("Failed to decode batch: %v", err)
				return nil
			}

			// Apply batch operations
			for _, op := range batch.Operations {
				if op.Type == OpTypePut {
					entries[string(op.Key)] = string(op.Value)
				} else if op.Type == OpTypeDelete {
					delete(entries, string(op.Key))
				}
			}
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify batch was replayed
	if batchCount != 1 {
		t.Errorf("Expected 1 batch, got %d", batchCount)
	}

	// Verify entries
	expectedEntries := map[string]string{
		"batch1": "value1",
		"batch3": "value3",
		// batch2 should be deleted
	}

	for key, expectedValue := range expectedEntries {
		value, ok := entries[key]
		if !ok {
			t.Errorf("Entry for key %q not found", key)
			continue
		}

		if value != expectedValue {
			t.Errorf("Expected value %q for key %q, got %q", expectedValue, key, value)
		}
	}

	// Verify batch2 is deleted
	if _, ok := entries["batch2"]; ok {
		t.Errorf("Key batch2 should be deleted")
	}
}

func TestWALRecovery(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()

	// Write some entries in the first WAL
	wal1, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	_, err = wal1.Append(OpTypePut, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	if err := wal1.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Create a second WAL file
	wal2, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	_, err = wal2.Append(OpTypePut, []byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	if err := wal2.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify entries by replaying all WAL files in order
	entries := make(map[string]string)

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypePut {
			entries[string(entry.Key)] = string(entry.Value)
		} else if entry.Type == OpTypeDelete {
			delete(entries, string(entry.Key))
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify all entries are present
	expected := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	for key, expectedValue := range expected {
		value, ok := entries[key]
		if !ok {
			t.Errorf("Entry for key %q not found", key)
			continue
		}

		if value != expectedValue {
			t.Errorf("Expected value %q for key %q, got %q", expectedValue, key, value)
		}
	}
}

func TestWALSyncModes(t *testing.T) {
	testCases := []struct {
		name            string
		syncMode        config.SyncMode
		expectedEntries int // Expected number of entries after crash (without explicit sync)
	}{
		{"SyncNone", config.SyncNone, 0}, // No entries should be recovered without explicit sync
		{"SyncBatch", config.SyncBatch, 0}, // No entries should be recovered if batch threshold not reached
		{"SyncImmediate", config.SyncImmediate, 10}, // All entries should be recovered
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := createTempDir(t)
			defer os.RemoveAll(dir)

			// Create config with specific sync mode
			cfg := createTestConfig()
			cfg.WALSyncMode = tc.syncMode
			// Set a high sync threshold for batch mode to ensure it doesn't auto-sync
			if tc.syncMode == config.SyncBatch {
				cfg.WALSyncBytes = 100 * 1024 * 1024 // 100MB, high enough to not trigger
			}

			wal, err := NewWAL(cfg, dir)
			if err != nil {
				t.Fatalf("Failed to create WAL: %v", err)
			}

			// Write some entries
			for i := 0; i < 10; i++ {
				key := []byte(fmt.Sprintf("key%d", i))
				value := []byte(fmt.Sprintf("value%d", i))

				_, err := wal.Append(OpTypePut, key, value)
				if err != nil {
					t.Fatalf("Failed to append entry: %v", err)
				}
			}

			// Skip explicit sync to simulate a crash
			
			// Close the WAL
			if err := wal.Close(); err != nil {
				t.Fatalf("Failed to close WAL: %v", err)
			}

			// Verify entries by replaying
			count := 0
			_, err = ReplayWALDir(dir, func(entry *Entry) error {
				if entry.Type == OpTypePut {
					count++
				}
				return nil
			})

			if err != nil {
				t.Fatalf("Failed to replay WAL: %v", err)
			}

			// Check that the number of recovered entries matches expectations for this sync mode
			if count != tc.expectedEntries {
				t.Errorf("Expected %d entries for %s mode, got %d", tc.expectedEntries, tc.name, count)
			}

			// Now test with explicit sync - all entries should be recoverable
			wal, err = NewWAL(cfg, dir)
			if err != nil {
				t.Fatalf("Failed to create WAL: %v", err)
			}

			// Write some more entries
			for i := 0; i < 10; i++ {
				key := []byte(fmt.Sprintf("explicit_key%d", i))
				value := []byte(fmt.Sprintf("explicit_value%d", i))

				_, err := wal.Append(OpTypePut, key, value)
				if err != nil {
					t.Fatalf("Failed to append entry: %v", err)
				}
			}

			// Explicitly sync
			if err := wal.Sync(); err != nil {
				t.Fatalf("Failed to sync WAL: %v", err)
			}

			// Close the WAL
			if err := wal.Close(); err != nil {
				t.Fatalf("Failed to close WAL: %v", err)
			}

			// Verify entries by replaying
			explicitCount := 0
			_, err = ReplayWALDir(dir, func(entry *Entry) error {
				if entry.Type == OpTypePut && bytes.HasPrefix(entry.Key, []byte("explicit_")) {
					explicitCount++
				}
				return nil
			})

			if err != nil {
				t.Fatalf("Failed to replay WAL after explicit sync: %v", err)
			}

			// After explicit sync, all 10 new entries should be recovered regardless of mode
			if explicitCount != 10 {
				t.Errorf("Expected 10 entries after explicit sync, got %d", explicitCount)
			}
		})
	}
}

func TestWALFragmentation(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create an entry that's guaranteed to be fragmented
	// Header size is 1 + 8 + 4 = 13 bytes, so allocate more than MaxRecordSize - 13 for the key
	keySize := MaxRecordSize - 10
	valueSize := MaxRecordSize * 2

	key := make([]byte, keySize)     // Just under MaxRecordSize to ensure key fragmentation
	value := make([]byte, valueSize) // Large value to ensure value fragmentation

	// Fill with recognizable patterns
	for i := range key {
		key[i] = byte(i % 256)
	}

	for i := range value {
		value[i] = byte((i * 3) % 256)
	}

	// Append the large entry - this should trigger fragmentation
	_, err = wal.Append(OpTypePut, key, value)
	if err != nil {
		t.Fatalf("Failed to append fragmented entry: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify by replaying
	var reconstructedKey []byte
	var reconstructedValue []byte
	var foundPut bool

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypePut {
			foundPut = true
			reconstructedKey = entry.Key
			reconstructedValue = entry.Value
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Check that we found the entry
	if !foundPut {
		t.Fatal("Did not find PUT entry in replay")
	}

	// Verify key length matches
	if len(reconstructedKey) != keySize {
		t.Errorf("Key length mismatch: expected %d, got %d", keySize, len(reconstructedKey))
	}

	// Verify value length matches
	if len(reconstructedValue) != valueSize {
		t.Errorf("Value length mismatch: expected %d, got %d", valueSize, len(reconstructedValue))
	}

	// Check key content (first 10 bytes)
	for i := 0; i < 10 && i < len(key); i++ {
		if key[i] != reconstructedKey[i] {
			t.Errorf("Key mismatch at position %d: expected %d, got %d", i, key[i], reconstructedKey[i])
		}
	}

	// Check key content (last 10 bytes)
	for i := 0; i < 10 && i < len(key); i++ {
		idx := len(key) - 1 - i
		if key[idx] != reconstructedKey[idx] {
			t.Errorf("Key mismatch at position %d: expected %d, got %d", idx, key[idx], reconstructedKey[idx])
		}
	}

	// Check value content (first 10 bytes)
	for i := 0; i < 10 && i < len(value); i++ {
		if value[i] != reconstructedValue[i] {
			t.Errorf("Value mismatch at position %d: expected %d, got %d", i, value[i], reconstructedValue[i])
		}
	}

	// Check value content (last 10 bytes)
	for i := 0; i < 10 && i < len(value); i++ {
		idx := len(value) - 1 - i
		if value[idx] != reconstructedValue[idx] {
			t.Errorf("Value mismatch at position %d: expected %d, got %d", idx, value[idx], reconstructedValue[idx])
		}
	}

	// Verify random samples from the key and value
	for i := 0; i < 10; i++ {
		// Check random positions in the key
		keyPos := rand.Intn(keySize)
		if key[keyPos] != reconstructedKey[keyPos] {
			t.Errorf("Key mismatch at random position %d: expected %d, got %d", keyPos, key[keyPos], reconstructedKey[keyPos])
		}

		// Check random positions in the value
		valuePos := rand.Intn(valueSize)
		if value[valuePos] != reconstructedValue[valuePos] {
			t.Errorf("Value mismatch at random position %d: expected %d, got %d", valuePos, value[valuePos], reconstructedValue[valuePos])
		}
	}
}

func TestWALErrorHandling(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write some entries
	_, err = wal.Append(OpTypePut, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Try to write after close
	_, err = wal.Append(OpTypePut, []byte("key2"), []byte("value2"))
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got: %v", err)
	}

	// Try to sync after close
	err = wal.Sync()
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got: %v", err)
	}

	// Try to replay a non-existent file
	nonExistentPath := filepath.Join(dir, "nonexistent.wal")
	_, err = ReplayWALFile(nonExistentPath, func(entry *Entry) error {
		return nil
	})

	if err == nil {
		t.Error("Expected error when replaying non-existent file")
	}
}
