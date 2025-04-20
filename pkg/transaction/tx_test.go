package transaction

import (
	"bytes"
	"os"
	"testing"

	"github.com/jer/kevo/pkg/engine"
)

func setupTest(t *testing.T) (*engine.Engine, func()) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "transaction-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create the engine
	e, err := engine.NewEngine(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		e.Close()
		os.RemoveAll(dir)
	}

	return e, cleanup
}

func TestTransaction_BasicOperations(t *testing.T) {
	e, cleanup := setupTest(t)
	defer cleanup()

	// Get transaction statistics before starting
	stats := e.GetStats()
	txStarted := stats["tx_started"].(uint64)

	// Begin a read-write transaction
	tx, err := e.BeginTransaction(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Verify transaction started count increased
	stats = e.GetStats()
	if stats["tx_started"].(uint64) != txStarted+1 {
		t.Errorf("Expected tx_started to be %d, got: %d", txStarted+1, stats["tx_started"].(uint64))
	}

	// Put a value in the transaction
	err = tx.Put([]byte("tx-key1"), []byte("tx-value1"))
	if err != nil {
		t.Fatalf("Failed to put value in transaction: %v", err)
	}

	// Get the value from the transaction
	val, err := tx.Get([]byte("tx-key1"))
	if err != nil {
		t.Fatalf("Failed to get value from transaction: %v", err)
	}
	if !bytes.Equal(val, []byte("tx-value1")) {
		t.Errorf("Expected value 'tx-value1', got: %s", string(val))
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify transaction completed count increased
	stats = e.GetStats()
	if stats["tx_completed"].(uint64) != 1 {
		t.Errorf("Expected tx_completed to be 1, got: %d", stats["tx_completed"].(uint64))
	}
	if stats["tx_aborted"].(uint64) != 0 {
		t.Errorf("Expected tx_aborted to be 0, got: %d", stats["tx_aborted"].(uint64))
	}

	// Verify the value is accessible from the engine
	val, err = e.Get([]byte("tx-key1"))
	if err != nil {
		t.Fatalf("Failed to get value from engine: %v", err)
	}
	if !bytes.Equal(val, []byte("tx-value1")) {
		t.Errorf("Expected value 'tx-value1', got: %s", string(val))
	}
}

func TestTransaction_Rollback(t *testing.T) {
	e, cleanup := setupTest(t)
	defer cleanup()

	// Begin a read-write transaction
	tx, err := e.BeginTransaction(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Put a value in the transaction
	err = tx.Put([]byte("tx-key2"), []byte("tx-value2"))
	if err != nil {
		t.Fatalf("Failed to put value in transaction: %v", err)
	}

	// Get the value from the transaction
	val, err := tx.Get([]byte("tx-key2"))
	if err != nil {
		t.Fatalf("Failed to get value from transaction: %v", err)
	}
	if !bytes.Equal(val, []byte("tx-value2")) {
		t.Errorf("Expected value 'tx-value2', got: %s", string(val))
	}

	// Rollback the transaction
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify transaction aborted count increased
	stats := e.GetStats()
	if stats["tx_completed"].(uint64) != 0 {
		t.Errorf("Expected tx_completed to be 0, got: %d", stats["tx_completed"].(uint64))
	}
	if stats["tx_aborted"].(uint64) != 1 {
		t.Errorf("Expected tx_aborted to be 1, got: %d", stats["tx_aborted"].(uint64))
	}

	// Verify the value is not accessible from the engine
	_, err = e.Get([]byte("tx-key2"))
	if err != engine.ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got: %v", err)
	}
}

func TestTransaction_ReadOnly(t *testing.T) {
	e, cleanup := setupTest(t)
	defer cleanup()

	// Add some data to the engine
	if err := e.Put([]byte("key-ro"), []byte("value-ro")); err != nil {
		t.Fatalf("Failed to put value in engine: %v", err)
	}

	// Begin a read-only transaction
	tx, err := e.BeginTransaction(true)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if !tx.IsReadOnly() {
		t.Errorf("Expected transaction to be read-only")
	}

	// Read the value
	val, err := tx.Get([]byte("key-ro"))
	if err != nil {
		t.Fatalf("Failed to get value from transaction: %v", err)
	}
	if !bytes.Equal(val, []byte("value-ro")) {
		t.Errorf("Expected value 'value-ro', got: %s", string(val))
	}

	// Attempt to write (should fail)
	err = tx.Put([]byte("new-key"), []byte("new-value"))
	if err == nil {
		t.Errorf("Expected error when putting value in read-only transaction")
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify transaction completed count increased
	stats := e.GetStats()
	if stats["tx_completed"].(uint64) != 1 {
		t.Errorf("Expected tx_completed to be 1, got: %d", stats["tx_completed"].(uint64))
	}
}
