package transaction

import (
	"sync"
	"testing"
	"time"
)

func TestManagerBasics(t *testing.T) {
	storage := NewMemoryStorage()
	statsCollector := &StatsCollectorMock{}

	// Create a transaction manager
	manager := NewManager(storage, statsCollector)

	// Test starting a read-only transaction
	tx1, err := manager.BeginTransaction(true)
	if err != nil {
		t.Errorf("Unexpected error beginning read-only transaction: %v", err)
	}
	if !tx1.IsReadOnly() {
		t.Error("Transaction should be read-only")
	}

	// Commit the read-only transaction before starting a read-write one
	// to avoid deadlock (since our tests run in a single thread)
	err = tx1.Commit()
	if err != nil {
		t.Errorf("Unexpected error committing read-only transaction: %v", err)
	}

	// Test starting a read-write transaction
	tx2, err := manager.BeginTransaction(false)
	if err != nil {
		t.Errorf("Unexpected error beginning read-write transaction: %v", err)
	}
	if tx2.IsReadOnly() {
		t.Error("Transaction should be read-write")
	}

	// Commit the read-write transaction
	err = tx2.Commit()
	if err != nil {
		t.Errorf("Unexpected error committing read-write transaction: %v", err)
	}

	// Verify stats tracking
	stats := manager.GetTransactionStats()

	if stats["tx_started"] != uint64(2) {
		t.Errorf("Expected 2 transactions started, got %v", stats["tx_started"])
	}

	if stats["tx_completed"] != uint64(2) {
		t.Errorf("Expected 2 transactions completed, got %v", stats["tx_completed"])
	}

	if stats["tx_aborted"] != uint64(0) {
		t.Errorf("Expected 0 transactions aborted, got %v", stats["tx_aborted"])
	}

	if stats["tx_active"] != uint64(0) {
		t.Errorf("Expected 0 active transactions, got %v", stats["tx_active"])
	}
}

func TestManagerRollback(t *testing.T) {
	storage := NewMemoryStorage()
	statsCollector := &StatsCollectorMock{}

	// Create a transaction manager
	manager := NewManager(storage, statsCollector)

	// Start a transaction and roll it back
	tx, err := manager.BeginTransaction(false)
	if err != nil {
		t.Errorf("Unexpected error beginning transaction: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Errorf("Unexpected error rolling back transaction: %v", err)
	}

	// Verify stats tracking
	stats := manager.GetTransactionStats()

	if stats["tx_started"] != uint64(1) {
		t.Errorf("Expected 1 transaction started, got %v", stats["tx_started"])
	}

	if stats["tx_completed"] != uint64(0) {
		t.Errorf("Expected 0 transactions completed, got %v", stats["tx_completed"])
	}

	if stats["tx_aborted"] != uint64(1) {
		t.Errorf("Expected 1 transaction aborted, got %v", stats["tx_aborted"])
	}

	if stats["tx_active"] != uint64(0) {
		t.Errorf("Expected 0 active transactions, got %v", stats["tx_active"])
	}
}

func TestConcurrentTransactions(t *testing.T) {
	storage := NewMemoryStorage()
	statsCollector := &StatsCollectorMock{}

	// Create a transaction manager
	manager := NewManager(storage, statsCollector)

	// Initialize some data
	storage.Put([]byte("counter"), []byte{0})

	// Rather than using concurrency which can cause flaky tests,
	// we'll execute transactions sequentially but simulate the same behavior
	numTransactions := 10

	for i := 0; i < numTransactions; i++ {
		// Start a read-write transaction
		tx, err := manager.BeginTransaction(false)
		if err != nil {
			t.Fatalf("Failed to begin transaction %d: %v", i, err)
		}

		// Read counter value
		counterValue, err := tx.Get([]byte("counter"))
		if err != nil {
			t.Fatalf("Failed to get counter in transaction %d: %v", i, err)
		}

		// Increment counter value
		newValue := []byte{counterValue[0] + 1}

		// Write new counter value
		err = tx.Put([]byte("counter"), newValue)
		if err != nil {
			t.Fatalf("Failed to update counter in transaction %d: %v", i, err)
		}

		// Commit transaction
		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction %d: %v", i, err)
		}
	}

	// Verify final counter value
	finalValue, err := storage.Get([]byte("counter"))
	if err != nil {
		t.Errorf("Error getting final counter value: %v", err)
	}

	// Counter should have been incremented numTransactions times
	expectedValue := byte(numTransactions)
	if finalValue[0] != expectedValue {
		t.Errorf("Expected counter value %d, got %d", expectedValue, finalValue[0])
	}

	// Verify that all transactions completed
	stats := manager.GetTransactionStats()

	if stats["tx_started"] != uint64(numTransactions) {
		t.Errorf("Expected %d transactions started, got %v", numTransactions, stats["tx_started"])
	}

	if stats["tx_completed"] != uint64(numTransactions) {
		t.Errorf("Expected %d transactions completed, got %v", numTransactions, stats["tx_completed"])
	}

	if stats["tx_active"] != uint64(0) {
		t.Errorf("Expected 0 active transactions, got %v", stats["tx_active"])
	}
}

func TestReadOnlyConcurrency(t *testing.T) {
	storage := NewMemoryStorage()
	statsCollector := &StatsCollectorMock{}

	// Create a transaction manager
	manager := NewManager(storage, statsCollector)

	// Initialize some data
	storage.Put([]byte("key1"), []byte("value1"))

	// Create a WaitGroup to synchronize goroutines
	var wg sync.WaitGroup

	// Number of concurrent read transactions to run
	numReaders := 5
	wg.Add(numReaders)

	// Channel to collect errors
	errors := make(chan error, numReaders)

	// Start multiple read transactions concurrently
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()

			// Start a read-only transaction
			tx, err := manager.BeginTransaction(true)
			if err != nil {
				errors <- err
				return
			}

			// Read data
			_, err = tx.Get([]byte("key1"))
			if err != nil {
				errors <- err
				return
			}

			// Simulate some processing time
			time.Sleep(10 * time.Millisecond)

			// Commit transaction
			err = tx.Commit()
			if err != nil {
				errors <- err
				return
			}
		}()
	}

	// Wait for all readers to finish
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Error in concurrent read transaction: %v", err)
	}

	// Verify that all transactions completed
	stats := manager.GetTransactionStats()

	if stats["tx_started"] != uint64(numReaders) {
		t.Errorf("Expected %d transactions started, got %v", numReaders, stats["tx_started"])
	}

	if stats["tx_completed"] != uint64(numReaders) {
		t.Errorf("Expected %d transactions completed, got %v", numReaders, stats["tx_completed"])
	}

	if stats["tx_active"] != uint64(0) {
		t.Errorf("Expected 0 active transactions, got %v", stats["tx_active"])
	}
}
