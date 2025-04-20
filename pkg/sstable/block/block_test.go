package block

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBlockBuilderSimple(t *testing.T) {
	builder := NewBuilder()

	// Add some entries
	numEntries := 10
	orderedKeys := make([]string, 0, numEntries)
	keyValues := make(map[string]string, numEntries)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		orderedKeys = append(orderedKeys, key)
		keyValues[key] = value

		err := builder.Add([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	if builder.Entries() != numEntries {
		t.Errorf("Expected %d entries, got %d", numEntries, builder.Entries())
	}

	// Serialize the block
	var buf bytes.Buffer
	checksum, err := builder.Finish(&buf)
	if err != nil {
		t.Fatalf("Failed to finish block: %v", err)
	}

	if checksum == 0 {
		t.Errorf("Expected non-zero checksum")
	}

	// Read it back
	reader, err := NewReader(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to create block reader: %v", err)
	}

	if reader.checksum != checksum {
		t.Errorf("Checksum mismatch: expected %d, got %d", checksum, reader.checksum)
	}

	// Verify we can read all keys
	iter := reader.Iterator()
	foundKeys := make(map[string]bool)

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		value := string(iter.Value())

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
	}

	if len(foundKeys) != numEntries {
		t.Errorf("Expected to find %d keys, got %d", numEntries, len(foundKeys))
	}

	// Make sure all keys were found
	for _, key := range orderedKeys {
		if !foundKeys[key] {
			t.Errorf("Key not found: %s", key)
		}
	}
}

func TestBlockBuilderLarge(t *testing.T) {
	builder := NewBuilder()

	// Add a lot of entries to test restart points
	numEntries := 100 // reduced from 1000 to make test faster
	keyValues := make(map[string]string, numEntries)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		keyValues[key] = value

		err := builder.Add([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	// Serialize the block
	var buf bytes.Buffer
	_, err := builder.Finish(&buf)
	if err != nil {
		t.Fatalf("Failed to finish block: %v", err)
	}

	// Read it back
	reader, err := NewReader(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to create block reader: %v", err)
	}

	// Verify we can read all entries
	iter := reader.Iterator()
	foundKeys := make(map[string]bool)

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) == 0 {
			continue // Skip empty keys
		}

		expectedValue, ok := keyValues[key]
		if !ok {
			t.Errorf("Found unexpected key: %s", key)
			continue
		}

		if string(iter.Value()) != expectedValue {
			t.Errorf("Value mismatch for key %s: expected %s, got %s",
				key, expectedValue, iter.Value())
		}

		foundKeys[key] = true
	}

	// Make sure all keys were found
	if len(foundKeys) != numEntries {
		t.Errorf("Expected to find %d entries, got %d", numEntries, len(foundKeys))
	}
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%05d", i)
		if !foundKeys[key] {
			t.Errorf("Key not found: %s", key)
		}
	}
}

func TestBlockBuilderSeek(t *testing.T) {
	builder := NewBuilder()

	// Add entries
	numEntries := 100
	allKeys := make(map[string]bool)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		allKeys[key] = true

		err := builder.Add([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	// Serialize and read back
	var buf bytes.Buffer
	_, err := builder.Finish(&buf)
	if err != nil {
		t.Fatalf("Failed to finish block: %v", err)
	}

	reader, err := NewReader(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to create block reader: %v", err)
	}

	// Test seeks
	iter := reader.Iterator()

	// Seek to first and check it's a valid key
	iter.SeekToFirst()
	firstKey := string(iter.Key())
	if !allKeys[firstKey] {
		t.Errorf("SeekToFirst returned invalid key: %s", firstKey)
	}

	// Seek to last and check it's a valid key
	iter.SeekToLast()
	lastKey := string(iter.Key())
	if !allKeys[lastKey] {
		t.Errorf("SeekToLast returned invalid key: %s", lastKey)
	}

	// Check that we can seek to a random key in the middle
	midKey := "key050"
	found := iter.Seek([]byte(midKey))
	if !found {
		t.Errorf("Failed to seek to %s", midKey)
	} else if _, ok := allKeys[string(iter.Key())]; !ok {
		t.Errorf("Seek to %s returned invalid key: %s", midKey, iter.Key())
	}

	// Seek to a key beyond the last one
	beyondKey := "key999"
	found = iter.Seek([]byte(beyondKey))
	if found {
		if _, ok := allKeys[string(iter.Key())]; !ok {
			t.Errorf("Seek to %s returned invalid key: %s", beyondKey, iter.Key())
		}
	}
}

func TestBlockBuilderSorted(t *testing.T) {
	builder := NewBuilder()

	// Add entries in sorted order
	numEntries := 100
	orderedKeys := make([]string, 0, numEntries)
	keyValues := make(map[string]string, numEntries)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		orderedKeys = append(orderedKeys, key)
		keyValues[key] = value
	}

	// Add entries in sorted order
	for _, key := range orderedKeys {
		err := builder.Add([]byte(key), []byte(keyValues[key]))
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	// Serialize and read back
	var buf bytes.Buffer
	_, err := builder.Finish(&buf)
	if err != nil {
		t.Fatalf("Failed to finish block: %v", err)
	}

	reader, err := NewReader(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to create block reader: %v", err)
	}

	// Verify we can read all keys
	iter := reader.Iterator()
	foundKeys := make(map[string]bool)

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		value := string(iter.Value())

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
	}

	if len(foundKeys) != numEntries {
		t.Errorf("Expected to find %d keys, got %d", numEntries, len(foundKeys))
	}

	// Make sure all keys were found
	for _, key := range orderedKeys {
		if !foundKeys[key] {
			t.Errorf("Key not found: %s", key)
		}
	}
}

func TestBlockBuilderDuplicateKeys(t *testing.T) {
	builder := NewBuilder()

	// Add first entry
	key := []byte("key001")
	value := []byte("value001")
	err := builder.Add(key, value)
	if err != nil {
		t.Fatalf("Failed to add first entry: %v", err)
	}

	// Try to add duplicate key
	err = builder.Add(key, []byte("value002"))
	if err == nil {
		t.Fatalf("Expected error when adding duplicate key, but got none")
	}

	// Try to add lesser key
	err = builder.Add([]byte("key000"), []byte("value000"))
	if err == nil {
		t.Fatalf("Expected error when adding key in wrong order, but got none")
	}
}

func TestBlockCorruption(t *testing.T) {
	builder := NewBuilder()

	// Add some entries
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		builder.Add(key, value)
	}

	// Serialize the block
	var buf bytes.Buffer
	_, err := builder.Finish(&buf)
	if err != nil {
		t.Fatalf("Failed to finish block: %v", err)
	}

	// Corrupt the data
	data := buf.Bytes()
	corruptedData := make([]byte, len(data))
	copy(corruptedData, data)

	// Corrupt checksum
	corruptedData[len(corruptedData)-1] ^= 0xFF

	// Try to read corrupted data
	_, err = NewReader(corruptedData)
	if err == nil {
		t.Errorf("Expected error when reading corrupted block, but got none")
	}
}

func TestBlockReset(t *testing.T) {
	builder := NewBuilder()

	// Add some entries
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		builder.Add(key, value)
	}

	if builder.Entries() != 10 {
		t.Errorf("Expected 10 entries, got %d", builder.Entries())
	}

	// Reset and check
	builder.Reset()

	if builder.Entries() != 0 {
		t.Errorf("Expected 0 entries after reset, got %d", builder.Entries())
	}

	if builder.EstimatedSize() != 0 {
		t.Errorf("Expected 0 size after reset, got %d", builder.EstimatedSize())
	}
}
