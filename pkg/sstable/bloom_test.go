package sstable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBasicBloomFilter(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "bloom_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	// Create an SSTable with bloom filters enabled
	sst := filepath.Join(tempDir, "test_bloom.sst")
	
	// Create the writer with bloom filters enabled
	options := DefaultWriterOptions()
	options.EnableBloomFilter = true
	writer, err := NewWriterWithOptions(sst, options)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	
	// Add just a few keys
	keys := []string{
		"apple",
		"banana",
		"cherry",
		"date",
		"elderberry",
	}
	
	for _, key := range keys {
		value := fmt.Sprintf("value-%s", key)
		if err := writer.Add([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to add key %s: %v", key, err)
		}
	}
	
	// Finish writing
	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish writer: %v", err)
	}
	
	// Open the reader
	reader, err := OpenReader(sst)
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()
	
	// Check that reader has bloom filters
	if !reader.hasBloomFilter {
		t.Errorf("Reader does not have bloom filters even though they were enabled")
	}
	
	// Check that all keys can be found
	for _, key := range keys {
		expectedValue := []byte(fmt.Sprintf("value-%s", key))
		value, err := reader.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to find key %s: %v", key, err)
			continue
		}
		
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Value mismatch for key %s: got %q, expected %q", key, value, expectedValue)
		} else {
			t.Logf("Successfully found key %s", key)
		}
	}
	
	// Check that non-existent keys are not found
	nonExistentKeys := []string{
		"fig",
		"grape",
		"honeydew",
	}
	
	for _, key := range nonExistentKeys {
		_, err := reader.Get([]byte(key))
		if err != ErrNotFound {
			t.Errorf("Expected ErrNotFound for key %s, got: %v", key, err)
		} else {
			t.Logf("Correctly reported key %s as not found", key)
		}
	}
}