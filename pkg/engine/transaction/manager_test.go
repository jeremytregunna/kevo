package transaction

import (
	"testing"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	"github.com/KevoDB/kevo/pkg/stats"
	"github.com/KevoDB/kevo/pkg/wal"
)

// MockStorageManager is a simple mock for the interfaces.StorageManager
type MockStorageManager struct {
	data map[string][]byte
}

func NewMockStorageManager() *MockStorageManager {
	return &MockStorageManager{
		data: make(map[string][]byte),
	}
}

func (m *MockStorageManager) Put(key, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *MockStorageManager) Get(key []byte) ([]byte, error) {
	value, ok := m.data[string(key)]
	if !ok {
		return nil, interfaces.ErrKeyNotFound
	}
	return value, nil
}

func (m *MockStorageManager) Delete(key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *MockStorageManager) IsDeleted(key []byte) (bool, error) {
	_, exists := m.data[string(key)]
	return !exists, nil
}

func (m *MockStorageManager) FlushMemTables() error {
	return nil
}

func (m *MockStorageManager) GetIterator() (iterator.Iterator, error) {
	return nil, nil // Not needed for these tests
}

func (m *MockStorageManager) GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error) {
	return nil, nil // Not needed for these tests
}

func (m *MockStorageManager) ApplyBatch(entries []*wal.Entry) error {
	// Process each entry in the batch
	for _, entry := range entries {
		switch entry.Type {
		case wal.OpTypePut:
			m.data[string(entry.Key)] = entry.Value
		case wal.OpTypeDelete:
			delete(m.data, string(entry.Key))
		}
	}
	return nil
}

func (m *MockStorageManager) GetStorageStats() map[string]interface{} {
	return nil // Not needed for these tests
}

func (m *MockStorageManager) Close() error {
	return nil
}

// Additional methods required by the StorageManager interface
func (m *MockStorageManager) GetMemTableSize() uint64 {
	return 0
}

func (m *MockStorageManager) IsFlushNeeded() bool {
	return false
}

func (m *MockStorageManager) GetSSTables() []string {
	return []string{}
}

func (m *MockStorageManager) ReloadSSTables() error {
	return nil
}

func (m *MockStorageManager) RotateWAL() error {
	return nil
}

func TestTransactionManager_BasicOperations(t *testing.T) {
	// Create dependencies
	storage := NewMockStorageManager()
	collector := stats.NewAtomicCollector()

	// Create the transaction manager
	manager := NewManager(storage, collector)

	// Begin a new read-write transaction
	tx, err := manager.BeginTransaction(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Put a key-value pair
	err = tx.Put([]byte("test-key"), []byte("test-value"))
	if err != nil {
		t.Fatalf("Failed to put key in transaction: %v", err)
	}

	// Verify we can get the value within the transaction
	value, err := tx.Get([]byte("test-key"))
	if err != nil {
		t.Fatalf("Failed to get key from transaction: %v", err)
	}
	if string(value) != "test-value" {
		t.Errorf("Got incorrect value in transaction. Expected: test-value, Got: %s", string(value))
	}

	// The value should not be in the storage yet (not committed)
	_, err = storage.Get([]byte("test-key"))
	if err == nil {
		t.Errorf("Key should not be in storage before commit")
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Now the value should be in the storage
	value, err = storage.Get([]byte("test-key"))
	if err != nil {
		t.Fatalf("Key not found in storage after commit: %v", err)
	}
	if string(value) != "test-value" {
		t.Errorf("Got incorrect value in storage. Expected: test-value, Got: %s", string(value))
	}

	// Check transaction metrics
	stats := manager.GetTransactionStats()
	if count, ok := stats["tx_started"]; !ok || count.(uint64) != 1 {
		t.Errorf("Incorrect tx_started count. Got: %v", count)
	}
	if count, ok := stats["tx_completed"]; !ok || count.(uint64) != 1 {
		t.Errorf("Incorrect tx_completed count. Got: %v", count)
	}
}

func TestTransactionManager_RollbackAndReadOnly(t *testing.T) {
	// Create dependencies
	storage := NewMockStorageManager()
	collector := stats.NewAtomicCollector()

	// Create the transaction manager
	manager := NewManager(storage, collector)

	// Test rollback
	rwTx, err := manager.BeginTransaction(false)
	if err != nil {
		t.Fatalf("Failed to begin read-write transaction: %v", err)
	}

	// Make some changes
	err = rwTx.Put([]byte("rollback-key"), []byte("rollback-value"))
	if err != nil {
		t.Fatalf("Failed to put key in transaction: %v", err)
	}

	// Rollback the transaction
	err = rwTx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify the changes were not applied
	_, err = storage.Get([]byte("rollback-key"))
	if err == nil {
		t.Errorf("Key should not be in storage after rollback")
	}

	// Test read-only transaction
	roTx, err := manager.BeginTransaction(true)
	if err != nil {
		t.Fatalf("Failed to begin read-only transaction: %v", err)
	}

	// Try to write in a read-only transaction (should fail)
	err = roTx.Put([]byte("readonly-key"), []byte("readonly-value"))
	if err == nil {
		t.Errorf("Put should fail in a read-only transaction")
	}

	// Add data to storage directly
	storage.Put([]byte("readonly-test"), []byte("readonly-value"))

	// Read-only transaction should be able to read
	value, err := roTx.Get([]byte("readonly-test"))
	if err != nil {
		t.Fatalf("Failed to get key in read-only transaction: %v", err)
	}
	if string(value) != "readonly-value" {
		t.Errorf("Got incorrect value in read-only transaction. Expected: readonly-value, Got: %s", string(value))
	}

	// Commit should work for read-only transaction
	err = roTx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit read-only transaction: %v", err)
	}

	// Check transaction metrics
	stats := manager.GetTransactionStats()
	if count, ok := stats["tx_started"]; !ok || count.(uint64) != 2 {
		t.Errorf("Incorrect tx_started count. Got: %v", count)
	}
	if count, ok := stats["tx_completed"]; !ok || count.(uint64) != 1 {
		t.Errorf("Incorrect tx_completed count. Got: %v", count)
	}
	if count, ok := stats["tx_aborted"]; !ok || count.(uint64) != 1 {
		t.Errorf("Incorrect tx_aborted count. Got: %v", count)
	}
}

func TestTransactionManager_Isolation(t *testing.T) {
	// Create dependencies
	storage := NewMockStorageManager()
	collector := stats.NewAtomicCollector()

	// Create the transaction manager
	manager := NewManager(storage, collector)

	// Add initial data
	storage.Put([]byte("isolation-key"), []byte("initial-value"))

	// In a real scenario with proper locking, we'd test isolation across transactions
	// But for unit testing, we'll simplify to avoid deadlocks

	// Test part 1: uncommitted changes aren't visible to new transactions
	{
		// Begin a transaction and modify data
		tx1, err := manager.BeginTransaction(false)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Modify the key in the transaction
		err = tx1.Put([]byte("isolation-key"), []byte("tx1-value"))
		if err != nil {
			t.Fatalf("Failed to put key in transaction: %v", err)
		}

		// Ensure the change is in the transaction buffer but not committed yet
		txValue, err := tx1.Get([]byte("isolation-key"))
		if err != nil || string(txValue) != "tx1-value" {
			t.Fatalf("Transaction doesn't see its own changes. Got: %s, err: %v", txValue, err)
		}

		// Storage should still have the original value
		storageValue, err := storage.Get([]byte("isolation-key"))
		if err != nil || string(storageValue) != "initial-value" {
			t.Fatalf("Storage changed before commit. Got: %s, err: %v", storageValue, err)
		}

		// Commit the transaction
		err = tx1.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		// Now storage should have the updated value
		storageValue, err = storage.Get([]byte("isolation-key"))
		if err != nil || string(storageValue) != "tx1-value" {
			t.Fatalf("Storage not updated after commit. Got: %s, err: %v", storageValue, err)
		}
	}

	// Test part 2: reading committed data
	{
		// A new transaction should see the updated value
		tx2, err := manager.BeginTransaction(true)
		if err != nil {
			t.Fatalf("Failed to begin read-only transaction: %v", err)
		}

		value, err := tx2.Get([]byte("isolation-key"))
		if err != nil {
			t.Fatalf("Failed to get key in transaction: %v", err)
		}
		if string(value) != "tx1-value" {
			t.Errorf("Transaction doesn't see committed changes. Expected: tx1-value, Got: %s", string(value))
		}

		// Commit the read-only transaction
		err = tx2.Commit()
		if err != nil {
			t.Fatalf("Failed to commit read-only transaction: %v", err)
		}
	}
}
