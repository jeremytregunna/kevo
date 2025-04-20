package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestReaderBasics(t *testing.T) {
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
	if reader.numEntries != uint32(numEntries) {
		t.Errorf("Expected %d entries, got %d", numEntries, reader.numEntries)
	}

	// Print file information
	t.Logf("SSTable file size: %d bytes", reader.ioManager.GetFileSize())
	t.Logf("Index offset: %d", reader.indexOffset)
	t.Logf("Index size: %d", reader.indexSize)
	t.Logf("Entries in table: %d", reader.numEntries)

	// Check what's in the index
	indexIter := reader.indexBlock.Iterator()
	t.Log("Index entries:")
	count := 0
	for indexIter.SeekToFirst(); indexIter.Valid(); indexIter.Next() {
		if count < 10 { // Log the first 10 entries only
			indexValue := indexIter.Value()
			locator, err := ParseBlockLocator(indexIter.Key(), indexValue)
			if err != nil {
				t.Errorf("Failed to parse block locator: %v", err)
				continue
			}

			t.Logf("  Index key: %s, block offset: %d, block size: %d",
				string(locator.Key), locator.Offset, locator.Size)

			// Read the block and see what keys it contains
			blockReader, err := reader.blockFetcher.FetchBlock(locator.Offset, locator.Size)
			if err == nil {
				blockIter := blockReader.Iterator()
				t.Log("    Block contents:")
				keysInBlock := 0
				for blockIter.SeekToFirst(); blockIter.Valid() && keysInBlock < 10; blockIter.Next() {
					t.Logf("      Key: %s, Value: %s",
						string(blockIter.Key()), string(blockIter.Value()))
					keysInBlock++
				}
				if keysInBlock >= 10 {
					t.Logf("      ... and more keys")
				}
			}
		}
		count++
	}
	t.Logf("Total index entries: %d", count)

	// Read some keys
	for i := 0; i < numEntries; i += 10 {
		key := fmt.Sprintf("key%05d", i)
		expectedValue := keyValues[key]

		value, err := reader.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s: %v", key, err)
			continue
		}

		if string(value) != expectedValue {
			t.Errorf("Value mismatch for key %s: expected %s, got %s",
				key, expectedValue, value)
		}
	}

	// Try to read a non-existent key
	_, err = reader.Get([]byte("nonexistent"))
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound for non-existent key, got: %v", err)
	}
}

func TestReaderCorruption(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()
	sstablePath := filepath.Join(tempDir, "test.sst")

	// Create a new SSTable writer
	writer, err := NewWriter(sstablePath)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Add some key-value pairs
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))

		err := writer.Add(key, value)
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	// Finish writing
	err = writer.Finish()
	if err != nil {
		t.Fatalf("Failed to finish SSTable: %v", err)
	}

	// Corrupt the file
	file, err := os.OpenFile(sstablePath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("Failed to open file for corruption: %v", err)
	}

	// Write some garbage at the end to corrupt the footer
	_, err = file.Seek(-8, os.SEEK_END)
	if err != nil {
		t.Fatalf("Failed to seek: %v", err)
	}

	_, err = file.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	if err != nil {
		t.Fatalf("Failed to write garbage: %v", err)
	}

	file.Close()

	// Try to open the corrupted file
	_, err = OpenReader(sstablePath)
	if err == nil {
		t.Errorf("Expected error when opening corrupted file, but got none")
	}
}
