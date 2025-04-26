package wal

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestBatchOperations(t *testing.T) {
	batch := NewBatch()

	// Test initially empty
	if batch.Count() != 0 {
		t.Errorf("Expected empty batch, got count %d", batch.Count())
	}

	// Add operations
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))
	batch.Delete([]byte("key3"))

	// Check count
	if batch.Count() != 3 {
		t.Errorf("Expected batch with 3 operations, got %d", batch.Count())
	}

	// Check size calculation
	expectedSize := BatchHeaderSize                         // count + seq
	expectedSize += 1 + 4 + 4 + len("key1") + len("value1") // type + keylen + vallen + key + value
	expectedSize += 1 + 4 + 4 + len("key2") + len("value2") // type + keylen + vallen + key + value
	expectedSize += 1 + 4 + len("key3")                     // type + keylen + key (no value for delete)

	if batch.Size() != expectedSize {
		t.Errorf("Expected batch size %d, got %d", expectedSize, batch.Size())
	}

	// Test reset
	batch.Reset()
	if batch.Count() != 0 {
		t.Errorf("Expected empty batch after reset, got count %d", batch.Count())
	}
}

func TestBatchEncoding(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()

	// Create a mock Lamport clock for the test
	clock := &MockLamportClock{counter: 0}

	wal, err := NewWALWithReplication(cfg, dir, clock, nil)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create and write a batch
	batch := NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))
	batch.Delete([]byte("key3"))

	if err := batch.Write(wal); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	// Check sequence
	if batch.Seq == 0 {
		t.Errorf("Batch sequence number not set")
	}

	// Close WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Replay and decode
	var decodedBatch *Batch

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypeBatch {
			var err error
			decodedBatch, err = DecodeBatch(entry)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if decodedBatch == nil {
		t.Fatal("No batch found in replay")
	}

	// Verify decoded batch
	if decodedBatch.Count() != 3 {
		t.Errorf("Expected 3 operations, got %d", decodedBatch.Count())
	}

	if decodedBatch.Seq != batch.Seq {
		t.Errorf("Expected sequence %d, got %d", batch.Seq, decodedBatch.Seq)
	}

	// Verify operations
	ops := decodedBatch.Operations

	if ops[0].Type != OpTypePut || !bytes.Equal(ops[0].Key, []byte("key1")) || !bytes.Equal(ops[0].Value, []byte("value1")) {
		t.Errorf("First operation mismatch")
	}

	if ops[1].Type != OpTypePut || !bytes.Equal(ops[1].Key, []byte("key2")) || !bytes.Equal(ops[1].Value, []byte("value2")) {
		t.Errorf("Second operation mismatch")
	}

	if ops[2].Type != OpTypeDelete || !bytes.Equal(ops[2].Key, []byte("key3")) {
		t.Errorf("Third operation mismatch")
	}
}

func TestEmptyBatch(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create empty batch
	batch := NewBatch()

	// Try to write empty batch
	err = batch.Write(wal)
	if err != ErrEmptyBatch {
		t.Errorf("Expected ErrEmptyBatch, got: %v", err)
	}

	// Close WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}
}

func TestLargeBatch(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create a batch that will exceed the maximum record size
	batch := NewBatch()

	// Add many large key-value pairs
	largeValue := make([]byte, 4096) // 4KB
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		batch.Put(key, largeValue)
	}

	// Verify the batch is too large
	if batch.Size() <= MaxRecordSize {
		t.Fatalf("Expected batch size > %d, got %d", MaxRecordSize, batch.Size())
	}

	// Try to write the large batch
	err = batch.Write(wal)
	if err == nil {
		t.Error("Expected error when writing large batch")
	}

	// Check that the error is ErrBatchTooLarge
	if err != nil && !bytes.Contains([]byte(err.Error()), []byte("batch too large")) {
		t.Errorf("Expected ErrBatchTooLarge, got: %v", err)
	}

	// Close WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}
}
