package sstable

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestIntegration performs a basic integration test between Writer and Reader
func TestIntegration(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()
	sstablePath := filepath.Join(tempDir, "test-integration.sst")

	// Create a new SSTable writer
	writer, err := NewWriter(sstablePath)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Add some key-value pairs
	numEntries := 100
	keyValues := make(map[string]string, numEntries)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		keyValues[key] = value

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

	// Open the SSTable for reading
	reader, err := OpenReader(sstablePath)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	// Verify the number of entries
	if reader.GetKeyCount() != numEntries {
		t.Errorf("Expected %d entries, got %d", numEntries, reader.GetKeyCount())
	}

	// Test GetKeyCount method
	if reader.GetKeyCount() != numEntries {
		t.Errorf("GetKeyCount returned %d, expected %d", reader.GetKeyCount(), numEntries)
	}

	// First test direct key retrieval
	missingKeys := 0
	for key, expectedValue := range keyValues {
		// Test direct Get
		value, err := reader.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s via Get(): %v", key, err)
			missingKeys++
			continue
		}

		if string(value) != expectedValue {
			t.Errorf("Value mismatch for key %s via Get(): expected %s, got %s",
				key, expectedValue, value)
		}
	}

	if missingKeys > 0 {
		t.Errorf("%d keys could not be retrieved via direct Get", missingKeys)
	}
}
