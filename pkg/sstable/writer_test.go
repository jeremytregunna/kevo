package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestWriterBasics(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()
	sstablePath := filepath.Join(tempDir, "test.sst")

	// Create a new SSTable writer
	writer, err := NewWriter(sstablePath)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Add some key-value pairs
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)

		err := writer.Add([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	// Finish writing
	err = writer.Finish()
	if err != nil {
		t.Fatalf("Failed to finish SSTable: %v", err)
	}

	// Verify the file exists
	_, err = os.Stat(sstablePath)
	if os.IsNotExist(err) {
		t.Errorf("SSTable file %s does not exist after Finish()", sstablePath)
	}

	// Open the file to check it was created properly
	reader, err := OpenReader(sstablePath)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	// Verify the number of entries
	if reader.numEntries != uint32(numEntries) {
		t.Errorf("Expected %d entries, got %d", numEntries, reader.numEntries)
	}
}

func TestWriterAbort(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()
	sstablePath := filepath.Join(tempDir, "test.sst")

	// Create a new SSTable writer
	writer, err := NewWriter(sstablePath)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Add some key-value pairs
	for i := 0; i < 10; i++ {
		writer.Add([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("value%05d", i)))
	}

	// Get the temp file path
	tmpPath := filepath.Join(filepath.Dir(sstablePath), fmt.Sprintf(".%s.tmp", filepath.Base(sstablePath)))

	// Abort writing
	err = writer.Abort()
	if err != nil {
		t.Fatalf("Failed to abort SSTable: %v", err)
	}

	// Verify that the temp file has been deleted
	_, err = os.Stat(tmpPath)
	if !os.IsNotExist(err) {
		t.Errorf("Temp file %s still exists after abort", tmpPath)
	}

	// Verify that the final file doesn't exist
	_, err = os.Stat(sstablePath)
	if !os.IsNotExist(err) {
		t.Errorf("Final file %s exists after abort", sstablePath)
	}
}

func TestWriterTombstone(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()
	sstablePath := filepath.Join(tempDir, "test-tombstone.sst")

	// Create a new SSTable writer
	writer, err := NewWriter(sstablePath)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Add some normal key-value pairs
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		err := writer.Add([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	// Add some tombstones by using nil values
	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("key%05d", i)
		// Use AddTombstone which calls Add with nil value
		err := writer.AddTombstone([]byte(key))
		if err != nil {
			t.Fatalf("Failed to add tombstone: %v", err)
		}
	}

	// Finish writing
	err = writer.Finish()
	if err != nil {
		t.Fatalf("Failed to finish SSTable: %v", err)
	}

	// Open the SSTable for reading
	reader, err := OpenReader(sstablePath)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	// Test using the iterator
	iter := reader.NewIterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		keyNum := 0
		if n, err := fmt.Sscanf(key, "key%05d", &keyNum); n == 1 && err == nil {
			if keyNum >= 5 && keyNum < 10 {
				// This should be a tombstone - in the implementation,
				// tombstones are represented by empty slices, not nil values,
				// though the IsTombstone() method should still return true
				if len(iter.Value()) != 0 {
					t.Errorf("Tombstone key %s should have empty value, got %v", key, string(iter.Value()))
				}
			} else if keyNum < 5 {
				// Regular entry
				expectedValue := fmt.Sprintf("value%05d", keyNum)
				if string(iter.Value()) != expectedValue {
					t.Errorf("Expected value %s for key %s, got %s",
						expectedValue, key, string(iter.Value()))
				}
			}
		}
	}

	// Also test using direct Get method
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%05d", i)
		value, err := reader.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s: %v", key, err)
			continue
		}
		expectedValue := fmt.Sprintf("value%05d", i)
		if string(value) != expectedValue {
			t.Errorf("Value mismatch for key %s: expected %s, got %s",
				key, expectedValue, string(value))
		}
	}

	// Test retrieving tombstones - values should still be retrievable
	// but will be empty slices in the current implementation
	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("key%05d", i)
		value, err := reader.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get tombstone key %s: %v", key, err)
			continue
		}
		if len(value) != 0 {
			t.Errorf("Expected empty value for tombstone key %s, got %v", key, string(value))
		}
	}
}
