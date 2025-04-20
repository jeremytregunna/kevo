package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestIterator(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()
	sstablePath := filepath.Join(tempDir, "test-iterator.sst")

	// Ensure fresh directory by removing files from temp dir
	os.RemoveAll(tempDir)
	os.MkdirAll(tempDir, 0755)

	// Create a new SSTable writer
	writer, err := NewWriter(sstablePath)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Add some key-value pairs
	numEntries := 100
	orderedKeys := make([]string, 0, numEntries)
	keyValues := make(map[string]string, numEntries)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		orderedKeys = append(orderedKeys, key)
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

	// Print detailed information about the index
	t.Log("### SSTable Index Details ###")
	indexIter := reader.indexBlock.Iterator()
	indexCount := 0
	t.Log("Index entries (block offsets and sizes):")
	for indexIter.SeekToFirst(); indexIter.Valid(); indexIter.Next() {
		indexKey := string(indexIter.Key())
		locator, err := ParseBlockLocator(indexIter.Key(), indexIter.Value())
		if err != nil {
			t.Errorf("Failed to parse block locator: %v", err)
			continue
		}

		t.Logf("  Index entry %d: key=%s, offset=%d, size=%d",
			indexCount, indexKey, locator.Offset, locator.Size)

		// Read and verify each data block
		blockReader, err := reader.blockFetcher.FetchBlock(locator.Offset, locator.Size)
		if err != nil {
			t.Errorf("Failed to read data block at offset %d: %v", locator.Offset, err)
			continue
		}

		// Count keys in this block
		blockIter := blockReader.Iterator()
		blockKeyCount := 0
		for blockIter.SeekToFirst(); blockIter.Valid(); blockIter.Next() {
			blockKeyCount++
		}

		t.Logf("    Block contains %d keys", blockKeyCount)
		indexCount++
	}
	t.Logf("Total index entries: %d", indexCount)

	// Create an iterator
	iter := reader.NewIterator()

	// Verify we can read all keys
	foundKeys := make(map[string]bool)
	count := 0

	t.Log("### Testing SSTable Iterator ###")

	// DEBUG: Check if the index iterator is valid before we start
	debugIndexIter := reader.indexBlock.Iterator()
	debugIndexIter.SeekToFirst()
	t.Logf("Index iterator valid before test: %v", debugIndexIter.Valid())

	// Map of offsets to identify duplicates
	seenOffsets := make(map[uint64]*struct {
		offset uint64
		key    string
	})
	uniqueOffsetsInOrder := make([]uint64, 0, 10)

	// Collect unique offsets
	for debugIndexIter.SeekToFirst(); debugIndexIter.Valid(); debugIndexIter.Next() {
		locator, err := ParseBlockLocator(debugIndexIter.Key(), debugIndexIter.Value())
		if err != nil {
			t.Errorf("Failed to parse block locator: %v", err)
			continue
		}

		key := string(locator.Key)

		// Only add if we haven't seen this offset before
		if _, ok := seenOffsets[locator.Offset]; !ok {
			seenOffsets[locator.Offset] = &struct {
				offset uint64
				key    string
			}{locator.Offset, key}
			uniqueOffsetsInOrder = append(uniqueOffsetsInOrder, locator.Offset)
		}
	}

	// Log the unique offsets
	t.Log("Unique data block offsets:")
	for i, offset := range uniqueOffsetsInOrder {
		entry := seenOffsets[offset]
		t.Logf("  Block %d: offset=%d, first key=%s",
			i, entry.offset, entry.key)
	}

	// Get the first index entry for debugging
	debugIndexIter.SeekToFirst()
	if debugIndexIter.Valid() {
		locator, err := ParseBlockLocator(debugIndexIter.Key(), debugIndexIter.Value())
		if err != nil {
			t.Errorf("Failed to parse block locator: %v", err)
		} else {
			t.Logf("First index entry points to offset=%d, size=%d",
				locator.Offset, locator.Size)
		}
	}

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) == 0 {
			t.Log("Found empty key, skipping")
			continue // Skip empty keys
		}

		value := string(iter.Value())
		count++

		if count <= 20 || count%10 == 0 {
			t.Logf("Found key %d: %s, value: %s", count, key, value)
		}

		expectedValue, ok := keyValues[key]
		if !ok {
			t.Errorf("Found unexpected key: %s", key)
			continue
		}

		if value != expectedValue {
			t.Errorf("Value mismatch for key %s: expected %s, got %s",
				key, expectedValue, value)
		}

		foundKeys[key] = true

		// Debug: if we've read exactly 10 keys (the first block),
		// check the state of things before moving to next block
		if count == 10 {
			t.Log("### After reading first block (10 keys) ###")
			t.Log("Checking if there are more blocks available...")

			// Create new iterators for debugging
			debugIndexIter := reader.indexBlock.Iterator()
			debugIndexIter.SeekToFirst()
			if debugIndexIter.Next() {
				t.Log("There is a second entry in the index, so we should be able to read more blocks")
				locator, err := ParseBlockLocator(debugIndexIter.Key(), debugIndexIter.Value())
				if err != nil {
					t.Errorf("Failed to parse second index entry: %v", err)
				} else {
					t.Logf("Second index entry points to offset=%d, size=%d",
						locator.Offset, locator.Size)

					// Try reading the second block directly
					blockReader, err := reader.blockFetcher.FetchBlock(locator.Offset, locator.Size)
					if err != nil {
						t.Errorf("Failed to read second block: %v", err)
					} else {
						blockIter := blockReader.Iterator()
						blockKeyCount := 0
						t.Log("Keys in second block:")
						for blockIter.SeekToFirst(); blockIter.Valid() && blockKeyCount < 5; blockIter.Next() {
							t.Logf("  Key: %s", string(blockIter.Key()))
							blockKeyCount++
						}
						t.Logf("Found %d keys in second block", blockKeyCount)
					}
				}
			} else {
				t.Log("No second entry in index, which is unexpected")
			}
		}
	}

	t.Logf("Iterator found %d keys total", count)

	if err := iter.Error(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	// Make sure all keys were found
	if len(foundKeys) != numEntries {
		t.Errorf("Expected to find %d keys, got %d", numEntries, len(foundKeys))

		// List keys that were not found
		missingCount := 0
		for _, key := range orderedKeys {
			if !foundKeys[key] {
				if missingCount < 20 {
					t.Errorf("Key not found: %s", key)
				}
				missingCount++
			}
		}

		if missingCount > 20 {
			t.Errorf("... and %d more keys not found", missingCount-20)
		}
	}

	// Test seeking
	iter = reader.NewIterator()
	midKey := "key00050"
	found := iter.Seek([]byte(midKey))

	if found {
		key := string(iter.Key())
		_, ok := keyValues[key]
		if !ok {
			t.Errorf("Seek to %s returned invalid key: %s", midKey, key)
		}
	} else {
		t.Errorf("Failed to seek to %s", midKey)
	}
}

func TestIteratorSeekToFirst(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()
	sstablePath := filepath.Join(tempDir, "test-seek.sst")

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

	// Open the SSTable for reading
	reader, err := OpenReader(sstablePath)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	// Create an iterator
	iter := reader.NewIterator()

	// Test SeekToFirst
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatalf("Iterator is not valid after SeekToFirst")
	}

	expectedFirstKey := "key00000"
	actualFirstKey := string(iter.Key())
	if actualFirstKey != expectedFirstKey {
		t.Errorf("First key mismatch: expected %s, got %s", expectedFirstKey, actualFirstKey)
	}

	// Test SeekToLast
	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatalf("Iterator is not valid after SeekToLast")
	}

	expectedLastKey := "key00099"
	actualLastKey := string(iter.Key())
	if actualLastKey != expectedLastKey {
		t.Errorf("Last key mismatch: expected %s, got %s", expectedLastKey, actualLastKey)
	}
}
