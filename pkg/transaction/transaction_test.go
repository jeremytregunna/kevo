package transaction

import (
	"bytes"
	"os"
	"testing"

	"github.com/KevoDB/kevo/pkg/engine"
)

func setupTestEngine(t *testing.T) (*engine.Engine, string) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "transaction_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a new engine
	eng, err := engine.NewEngine(tempDir)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create engine: %v", err)
	}

	return eng, tempDir
}

func TestReadOnlyTransaction(t *testing.T) {
	eng, tempDir := setupTestEngine(t)
	defer os.RemoveAll(tempDir)
	defer eng.Close()

	// Add some data directly to the engine
	if err := eng.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	if err := eng.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}

	// Create a read-only transaction
	tx, err := NewTransaction(eng, ReadOnly)
	if err != nil {
		t.Fatalf("Failed to create read-only transaction: %v", err)
	}

	// Test Get functionality
	value, err := tx.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if !bytes.Equal(value, []byte("value1")) {
		t.Errorf("Expected 'value1' but got '%s'", value)
	}

	// Test read-only constraints
	err = tx.Put([]byte("key3"), []byte("value3"))
	if err != ErrReadOnlyTransaction {
		t.Errorf("Expected ErrReadOnlyTransaction but got: %v", err)
	}

	err = tx.Delete([]byte("key1"))
	if err != ErrReadOnlyTransaction {
		t.Errorf("Expected ErrReadOnlyTransaction but got: %v", err)
	}

	// Test iterator
	iter := tx.NewIterator()
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 keys but found %d", count)
	}

	// Test commit (which for read-only just releases resources)
	if err := tx.Commit(); err != nil {
		t.Errorf("Failed to commit read-only transaction: %v", err)
	}

	// Transaction should be closed now
	_, err = tx.Get([]byte("key1"))
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed but got: %v", err)
	}
}

func TestReadWriteTransaction(t *testing.T) {
	eng, tempDir := setupTestEngine(t)
	defer os.RemoveAll(tempDir)
	defer eng.Close()

	// Add initial data
	if err := eng.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}

	// Create a read-write transaction
	tx, err := NewTransaction(eng, ReadWrite)
	if err != nil {
		t.Fatalf("Failed to create read-write transaction: %v", err)
	}

	// Add more data through the transaction
	if err := tx.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	if err := tx.Put([]byte("key3"), []byte("value3")); err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}

	// Delete a key
	if err := tx.Delete([]byte("key1")); err != nil {
		t.Fatalf("Failed to delete key1: %v", err)
	}

	// Verify the changes are visible in the transaction but not in the engine yet
	// Check via transaction
	value, err := tx.Get([]byte("key2"))
	if err != nil {
		t.Errorf("Failed to get key2 from transaction: %v", err)
	}
	if !bytes.Equal(value, []byte("value2")) {
		t.Errorf("Expected 'value2' but got '%s'", value)
	}

	// Check deleted key
	_, err = tx.Get([]byte("key1"))
	if err == nil {
		t.Errorf("key1 should be deleted in transaction")
	}

	// Check directly in engine - changes shouldn't be visible yet
	value, err = eng.Get([]byte("key2"))
	if err == nil {
		t.Errorf("key2 should not be visible in engine yet")
	}

	value, err = eng.Get([]byte("key1"))
	if err != nil {
		t.Errorf("key1 should still be visible in engine: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Now check engine again - changes should be visible
	value, err = eng.Get([]byte("key2"))
	if err != nil {
		t.Errorf("key2 should be visible in engine after commit: %v", err)
	}
	if !bytes.Equal(value, []byte("value2")) {
		t.Errorf("Expected 'value2' but got '%s'", value)
	}

	// Deleted key should be gone
	value, err = eng.Get([]byte("key1"))
	if err == nil {
		t.Errorf("key1 should be deleted in engine after commit")
	}

	// Transaction should be closed
	_, err = tx.Get([]byte("key2"))
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed but got: %v", err)
	}
}

func TestTransactionRollback(t *testing.T) {
	eng, tempDir := setupTestEngine(t)
	defer os.RemoveAll(tempDir)
	defer eng.Close()

	// Add initial data
	if err := eng.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}

	// Create a read-write transaction
	tx, err := NewTransaction(eng, ReadWrite)
	if err != nil {
		t.Fatalf("Failed to create read-write transaction: %v", err)
	}

	// Add and modify data
	if err := tx.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	if err := tx.Delete([]byte("key1")); err != nil {
		t.Fatalf("Failed to delete key1: %v", err)
	}

	// Rollback the transaction
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Changes should not be visible in the engine
	value, err := eng.Get([]byte("key1"))
	if err != nil {
		t.Errorf("key1 should still exist after rollback: %v", err)
	}
	if !bytes.Equal(value, []byte("value1")) {
		t.Errorf("Expected 'value1' but got '%s'", value)
	}

	// key2 should not exist
	_, err = eng.Get([]byte("key2"))
	if err == nil {
		t.Errorf("key2 should not exist after rollback")
	}

	// Transaction should be closed
	_, err = tx.Get([]byte("key1"))
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed but got: %v", err)
	}
}

func TestTransactionIterator(t *testing.T) {
	eng, tempDir := setupTestEngine(t)
	defer os.RemoveAll(tempDir)
	defer eng.Close()

	// Add initial data
	if err := eng.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	if err := eng.Put([]byte("key3"), []byte("value3")); err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}
	if err := eng.Put([]byte("key5"), []byte("value5")); err != nil {
		t.Fatalf("Failed to put key5: %v", err)
	}

	// Create a read-write transaction
	tx, err := NewTransaction(eng, ReadWrite)
	if err != nil {
		t.Fatalf("Failed to create read-write transaction: %v", err)
	}

	// Add and modify data in transaction
	if err := tx.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	if err := tx.Put([]byte("key4"), []byte("value4")); err != nil {
		t.Fatalf("Failed to put key4: %v", err)
	}
	if err := tx.Delete([]byte("key3")); err != nil {
		t.Fatalf("Failed to delete key3: %v", err)
	}

	// Use iterator to check order and content
	iter := tx.NewIterator()
	expected := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key4", "value4"},
		{"key5", "value5"},
	}

	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if i >= len(expected) {
			t.Errorf("Too many keys in iterator")
			break
		}

		if !bytes.Equal(iter.Key(), []byte(expected[i].key)) {
			t.Errorf("Expected key '%s' but got '%s'", expected[i].key, string(iter.Key()))
		}
		if !bytes.Equal(iter.Value(), []byte(expected[i].value)) {
			t.Errorf("Expected value '%s' but got '%s'", expected[i].value, string(iter.Value()))
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Expected %d keys but found %d", len(expected), i)
	}

	// Test range iterator
	rangeIter := tx.NewRangeIterator([]byte("key2"), []byte("key5"))
	expected = []struct {
		key   string
		value string
	}{
		{"key2", "value2"},
		{"key4", "value4"},
	}

	i = 0
	for rangeIter.SeekToFirst(); rangeIter.Valid(); rangeIter.Next() {
		if i >= len(expected) {
			t.Errorf("Too many keys in range iterator")
			break
		}

		if !bytes.Equal(rangeIter.Key(), []byte(expected[i].key)) {
			t.Errorf("Expected key '%s' but got '%s'", expected[i].key, string(rangeIter.Key()))
		}
		if !bytes.Equal(rangeIter.Value(), []byte(expected[i].value)) {
			t.Errorf("Expected value '%s' but got '%s'", expected[i].value, string(rangeIter.Value()))
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Expected %d keys in range but found %d", len(expected), i)
	}

	// Commit and verify results
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

func TestTransactionPutDeletePutSequence(t *testing.T) {
	eng, tempDir := setupTestEngine(t)
	defer os.RemoveAll(tempDir)
	defer eng.Close()

	// Create a read-write transaction
	tx, err := NewTransaction(eng, ReadWrite)
	if err != nil {
		t.Fatalf("Failed to create read-write transaction: %v", err)
	}

	// Define key and values
	key := []byte("transaction-sequence-key")
	initialValue := []byte("initial-transaction-value")
	newValue := []byte("new-transaction-value-after-delete")

	// 1. Put the initial value within the transaction
	if err := tx.Put(key, initialValue); err != nil {
		t.Fatalf("Failed to put initial value in transaction: %v", err)
	}

	// 2. Get and verify the initial value within the transaction
	val, err := tx.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key after initial put in transaction: %v", err)
	}
	if !bytes.Equal(val, initialValue) {
		t.Errorf("Got incorrect value after initial put. Expected: %s, Got: %s",
			initialValue, val)
	}

	// 3. Delete the key within the transaction
	if err := tx.Delete(key); err != nil {
		t.Fatalf("Failed to delete key in transaction: %v", err)
	}

	// 4. Verify the key is deleted within the transaction
	_, err = tx.Get(key)
	if err == nil {
		t.Error("Expected error after deleting key in transaction, got nil")
	}

	// 5. Put a new value for the same key within the transaction
	if err := tx.Put(key, newValue); err != nil {
		t.Fatalf("Failed to put new value after delete in transaction: %v", err)
	}

	// 6. Get and verify the new value within the transaction
	val, err = tx.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key after put-delete-put sequence in transaction: %v", err)
	}
	if !bytes.Equal(val, newValue) {
		t.Errorf("Got incorrect value after put-delete-put sequence. Expected: %s, Got: %s",
			newValue, val)
	}

	// 7. Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// 8. Verify the final state is correctly persisted to the engine
	val, err = eng.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key from engine after commit: %v", err)
	}
	if !bytes.Equal(val, newValue) {
		t.Errorf("Got incorrect value from engine after commit. Expected: %s, Got: %s",
			newValue, val)
	}

	// 9. Create a new transaction to verify the data is still correct
	tx2, err := NewTransaction(eng, ReadOnly)
	if err != nil {
		t.Fatalf("Failed to create second transaction: %v", err)
	}

	val, err = tx2.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key in second transaction: %v", err)
	}
	if !bytes.Equal(val, newValue) {
		t.Errorf("Got incorrect value in second transaction. Expected: %s, Got: %s",
			newValue, val)
	}

	tx2.Rollback()
}
