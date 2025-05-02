package transaction

import (
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/stats"
	"github.com/KevoDB/kevo/pkg/wal"
)

// MockStorage implements the StorageManager interface for testing
type MockStorage struct{}

func (m *MockStorage) Get(key []byte) ([]byte, error) {
	return []byte("value"), nil
}

func (m *MockStorage) Put(key, value []byte) error {
	return nil
}

func (m *MockStorage) Delete(key []byte) error {
	return nil
}

func (m *MockStorage) IsDeleted(key []byte) (bool, error) {
	return false, nil
}

func (m *MockStorage) GetIterator() (iterator.Iterator, error) {
	return &MockIterator{}, nil
}

func (m *MockStorage) GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error) {
	return &MockIterator{}, nil
}

func (m *MockStorage) ApplyBatch(entries []*wal.Entry) error {
	return nil
}

func (m *MockStorage) FlushMemTables() error {
	return nil
}

func (m *MockStorage) Close() error {
	return nil
}

func (m *MockStorage) GetMemTableSize() uint64 {
	return 0
}

func (m *MockStorage) IsFlushNeeded() bool {
	return false
}

func (m *MockStorage) GetSSTables() []string {
	return []string{}
}

func (m *MockStorage) ReloadSSTables() error {
	return nil
}

func (m *MockStorage) RotateWAL() error {
	return nil
}

func (m *MockStorage) GetStorageStats() map[string]interface{} {
	return map[string]interface{}{}
}

// MockIterator is a simple iterator for testing
type MockIterator struct{}

func (it *MockIterator) SeekToFirst() {}
func (it *MockIterator) SeekToLast()  {}
func (it *MockIterator) Seek(key []byte) bool {
	return false
}
func (it *MockIterator) Next() bool {
	return false
}
func (it *MockIterator) Key() []byte {
	return nil
}
func (it *MockIterator) Value() []byte {
	return nil
}
func (it *MockIterator) Valid() bool {
	return false
}
func (it *MockIterator) IsTombstone() bool {
	return false
}

// MockStatsCollector implements the stats.Collector interface for testing
type MockStatsCollector struct{}

func (m *MockStatsCollector) GetStats() map[string]interface{} {
	return nil
}

func (m *MockStatsCollector) GetStatsFiltered(prefix string) map[string]interface{} {
	return nil
}

func (m *MockStatsCollector) TrackOperation(op stats.OperationType) {}

func (m *MockStatsCollector) TrackOperationWithLatency(op stats.OperationType, latencyNs uint64) {}

func (m *MockStatsCollector) TrackError(errorType string) {}

func (m *MockStatsCollector) TrackBytes(isWrite bool, bytes uint64) {}

func (m *MockStatsCollector) TrackMemTableSize(size uint64) {}

func (m *MockStatsCollector) TrackFlush() {}

func (m *MockStatsCollector) TrackCompaction() {}

func (m *MockStatsCollector) StartRecovery() time.Time {
	return time.Now()
}

func (m *MockStatsCollector) FinishRecovery(startTime time.Time, filesRecovered, entriesRecovered, corruptedEntries uint64) {
}

func TestForwardingLayer(t *testing.T) {
	// Create mocks
	storage := &MockStorage{}
	statsCollector := &MockStatsCollector{}

	// Create the manager through the forwarding layer
	manager := NewManager(storage, statsCollector)

	// Verify the manager was created
	if manager == nil {
		t.Fatal("Expected manager to be created, got nil")
	}

	// Get the RWLock
	rwLock := manager.GetRWLock()
	if rwLock == nil {
		t.Fatal("Expected non-nil RWLock")
	}

	// Test transaction creation
	tx, err := manager.BeginTransaction(true)
	if err != nil {
		t.Fatalf("Unexpected error beginning transaction: %v", err)
	}

	// Verify it's a read-only transaction
	if !tx.IsReadOnly() {
		t.Error("Expected read-only transaction")
	}

	// Test some operations
	_, err = tx.Get([]byte("key"))
	if err != nil {
		t.Errorf("Unexpected error in Get: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Errorf("Unexpected error committing transaction: %v", err)
	}

	// Create a read-write transaction
	tx, err = manager.BeginTransaction(false)
	if err != nil {
		t.Fatalf("Unexpected error beginning transaction: %v", err)
	}

	// Verify it's a read-write transaction
	if tx.IsReadOnly() {
		t.Error("Expected read-write transaction")
	}

	// Test put operation
	err = tx.Put([]byte("key"), []byte("value"))
	if err != nil {
		t.Errorf("Unexpected error in Put: %v", err)
	}

	// Test delete operation
	err = tx.Delete([]byte("key"))
	if err != nil {
		t.Errorf("Unexpected error in Delete: %v", err)
	}

	// Test iterator
	it := tx.NewIterator()
	if it == nil {
		t.Error("Expected non-nil iterator")
	}

	// Test range iterator
	rangeIt := tx.NewRangeIterator([]byte("a"), []byte("z"))
	if rangeIt == nil {
		t.Error("Expected non-nil range iterator")
	}

	// Rollback the transaction
	err = tx.Rollback()
	if err != nil {
		t.Errorf("Unexpected error rolling back transaction: %v", err)
	}

	// Verify IncrementTxCompleted and IncrementTxAborted are working
	manager.IncrementTxCompleted()
	manager.IncrementTxAborted()

	// Test the registry creation
	registry := NewRegistry()
	if registry == nil {
		t.Fatal("Expected registry to be created, got nil")
	}
}
