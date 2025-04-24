package engine

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestEngineFacade_BasicOperations(t *testing.T) {
	// Create a temp directory for the test
	dir, err := os.MkdirTemp("", "engine-facade-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a new facade-based engine
	eng, err := NewEngineFacade(dir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Test Put and Get operations
	testKey := []byte("test-key")
	testValue := []byte("test-value")

	// Put a key-value pair
	if err := eng.Put(testKey, testValue); err != nil {
		t.Fatalf("Failed to put key-value: %v", err)
	}

	// Retrieve the value
	value, err := eng.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if !bytes.Equal(value, testValue) {
		t.Fatalf("Got incorrect value. Expected: %s, Got: %s", testValue, value)
	}

	// Test Delete operation
	if err := eng.Delete(testKey); err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Verify key is deleted
	_, err = eng.Get(testKey)
	if err == nil {
		t.Fatalf("Expected key to be deleted, but it was found")
	}
}

func TestEngineFacade_Iterator(t *testing.T) {
	// Create a temp directory for the test
	dir, err := os.MkdirTemp("", "engine-facade-iterator-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a new facade-based engine
	eng, err := NewEngineFacade(dir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Insert several keys with a specific prefix
	numKeys := 10
	prefix := "test-key-"
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("%s%03d", prefix, i))
		value := []byte(fmt.Sprintf("value-%03d", i))

		if err := eng.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value: %v", err)
		}
	}

	// Test the iterator
	iter, err := eng.GetIterator()
	if err != nil {
		t.Fatalf("Failed to get iterator: %v", err)
	}

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		expectedKey := []byte(fmt.Sprintf("%s%03d", prefix, count))
		expectedValue := []byte(fmt.Sprintf("value-%03d", count))

		if !bytes.Equal(key, expectedKey) {
			t.Errorf("Iterator returned incorrect key. Expected: %s, Got: %s", expectedKey, key)
		}

		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Iterator returned incorrect value. Expected: %s, Got: %s", expectedValue, value)
		}

		count++
	}

	if count != numKeys {
		t.Errorf("Iterator returned wrong number of keys. Expected: %d, Got: %d", numKeys, count)
	}

	// Test range iterator
	startKey := []byte(fmt.Sprintf("%s%03d", prefix, 3))
	endKey := []byte(fmt.Sprintf("%s%03d", prefix, 7))

	rangeIter, err := eng.GetRangeIterator(startKey, endKey)
	if err != nil {
		t.Fatalf("Failed to get range iterator: %v", err)
	}

	count = 0
	expectedCount := 4 // Keys 3, 4, 5, 6 (exclusive of end key)
	for rangeIter.SeekToFirst(); rangeIter.Valid(); rangeIter.Next() {
		key := rangeIter.Key()
		idx := 3 + count // Start at index 3
		expectedKey := []byte(fmt.Sprintf("%s%03d", prefix, idx))

		if !bytes.Equal(key, expectedKey) {
			t.Errorf("Range iterator returned incorrect key. Expected: %s, Got: %s", expectedKey, key)
		}

		count++
	}

	if count != expectedCount {
		t.Errorf("Range iterator returned wrong number of keys. Expected: %d, Got: %d", expectedCount, count)
	}
}

func TestEngineFacade_Transactions(t *testing.T) {
	// Create a temp directory for the test
	dir, err := os.MkdirTemp("", "engine-facade-transaction-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a new facade-based engine
	eng, err := NewEngineFacade(dir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Test a successful transaction
	tx, err := eng.BeginTransaction(false) // Read-write transaction
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Perform some operations in the transaction
	if err := tx.Put([]byte("tx-key-1"), []byte("tx-value-1")); err != nil {
		t.Fatalf("Failed to put key in transaction: %v", err)
	}

	if err := tx.Put([]byte("tx-key-2"), []byte("tx-value-2")); err != nil {
		t.Fatalf("Failed to put key in transaction: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify keys are accessible after commit
	value, err := eng.Get([]byte("tx-key-1"))
	if err != nil {
		t.Fatalf("Failed to get key after transaction commit: %v", err)
	}
	if !bytes.Equal(value, []byte("tx-value-1")) {
		t.Errorf("Got incorrect value after transaction. Expected: tx-value-1, Got: %s", value)
	}

	// Test a rollback
	tx2, err := eng.BeginTransaction(false)
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	if err := tx2.Put([]byte("should-not-exist"), []byte("rollback-value")); err != nil {
		t.Fatalf("Failed to put key in transaction: %v", err)
	}

	// Rollback the transaction
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify key from rolled back transaction is not accessible
	_, err = eng.Get([]byte("should-not-exist"))
	if err == nil {
		t.Errorf("Key from rolled back transaction should not exist")
	}
}

func TestEngineFacade_Compaction(t *testing.T) {
	// Create a temp directory for the test
	dir, err := os.MkdirTemp("", "engine-facade-compaction-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a new facade-based engine
	eng, err := NewEngineFacade(dir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Insert data to trigger memtable flushes
	for i := 0; i < 5; i++ {
		// Insert a batch of keys
		for j := 0; j < 100; j++ {
			key := []byte(fmt.Sprintf("key-batch-%d-%03d", i, j))
			value := []byte(fmt.Sprintf("value-batch-%d-%03d", i, j))

			if err := eng.Put(key, value); err != nil {
				t.Fatalf("Failed to put key-value: %v", err)
			}
		}

		// Force a memtable flush
		if err := eng.FlushImMemTables(); err != nil {
			t.Fatalf("Failed to flush memtables: %v", err)
		}
	}

	// Trigger compaction explicitly
	if err := eng.TriggerCompaction(); err != nil {
		t.Fatalf("Failed to trigger compaction: %v", err)
	}

	// Give compaction time to run
	time.Sleep(300 * time.Millisecond)

	// Get compaction stats
	stats, err := eng.GetCompactionStats()
	if err != nil {
		t.Fatalf("Failed to get compaction stats: %v", err)
	}

	// Check stats
	if stats["enabled"] != true {
		t.Errorf("Expected compaction to be enabled")
	}

	// Verify all keys are still accessible after compaction
	for i := 0; i < 5; i++ {
		// Check a few keys from each batch
		for j := 0; j < 100; j += 10 {
			key := []byte(fmt.Sprintf("key-batch-%d-%03d", i, j))
			expectedValue := []byte(fmt.Sprintf("value-batch-%d-%03d", i, j))

			value, err := eng.Get(key)
			if err != nil {
				t.Errorf("Failed to get key after compaction: %v", err)
				continue
			}

			if !bytes.Equal(value, expectedValue) {
				t.Errorf("Got incorrect value after compaction. Key: %s, Expected: %s, Got: %s",
					key, expectedValue, value)
			}
		}
	}

	// Clean up
	if err := eng.Close(); err != nil {
		t.Fatalf("Failed to close engine: %v", err)
	}
}