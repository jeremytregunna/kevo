package replication

import (
	"errors"
	"sync"
	"testing"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/wal"
)

// MockStorage implements a simple mock storage for testing
type MockStorage struct {
	mu             sync.Mutex
	data           map[string][]byte
	putFail        bool
	deleteFail     bool
	putCount       int
	deleteCount    int
	lastPutKey     []byte
	lastPutValue   []byte
	lastDeleteKey  []byte
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		data: make(map[string][]byte),
	}
}

func (m *MockStorage) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.putFail {
		return errors.New("simulated put failure")
	}
	
	m.putCount++
	m.lastPutKey = append([]byte{}, key...)
	m.lastPutValue = append([]byte{}, value...)
	m.data[string(key)] = append([]byte{}, value...)
	return nil
}

func (m *MockStorage) Get(key []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	value, ok := m.data[string(key)]
	if !ok {
		return nil, errors.New("key not found")
	}
	return value, nil
}

func (m *MockStorage) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.deleteFail {
		return errors.New("simulated delete failure")
	}
	
	m.deleteCount++
	m.lastDeleteKey = append([]byte{}, key...)
	delete(m.data, string(key))
	return nil
}

// Stub implementations for the rest of the interface
func (m *MockStorage) Close() error { return nil }
func (m *MockStorage) IsDeleted(key []byte) (bool, error) { return false, nil }
func (m *MockStorage) GetIterator() (iterator.Iterator, error) { return nil, nil }
func (m *MockStorage) GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error) { return nil, nil }
func (m *MockStorage) ApplyBatch(entries []*wal.Entry) error { return nil }
func (m *MockStorage) FlushMemTables() error { return nil }
func (m *MockStorage) GetMemTableSize() uint64 { return 0 }
func (m *MockStorage) IsFlushNeeded() bool { return false }
func (m *MockStorage) GetSSTables() []string { return nil }
func (m *MockStorage) ReloadSSTables() error { return nil }
func (m *MockStorage) RotateWAL() error { return nil }
func (m *MockStorage) GetStorageStats() map[string]interface{} { return nil }

func TestWALApplierBasic(t *testing.T) {
	storage := NewMockStorage()
	applier := NewWALApplier(storage)
	defer applier.Close()
	
	// Create test entries
	entries := []*wal.Entry{
		{
			SequenceNumber: 1,
			Type:           wal.OpTypePut,
			Key:            []byte("key1"),
			Value:          []byte("value1"),
		},
		{
			SequenceNumber: 2,
			Type:           wal.OpTypePut,
			Key:            []byte("key2"),
			Value:          []byte("value2"),
		},
		{
			SequenceNumber: 3,
			Type:           wal.OpTypeDelete,
			Key:            []byte("key1"),
		},
	}
	
	// Apply entries one by one
	for i, entry := range entries {
		applied, err := applier.Apply(entry)
		if err != nil {
			t.Fatalf("Error applying entry %d: %v", i, err)
		}
		if !applied {
			t.Errorf("Entry %d should have been applied", i)
		}
	}
	
	// Check state
	if got := applier.GetHighestApplied(); got != 3 {
		t.Errorf("Expected highest applied 3, got %d", got)
	}
	
	// Check storage state
	if value, _ := storage.Get([]byte("key2")); string(value) != "value2" {
		t.Errorf("Expected key2=value2 in storage, got %q", value)
	}
	
	// key1 should be deleted
	if _, err := storage.Get([]byte("key1")); err == nil {
		t.Errorf("Expected key1 to be deleted")
	}
	
	// Check stats
	stats := applier.GetStats()
	if stats["appliedCount"] != 3 {
		t.Errorf("Expected appliedCount=3, got %d", stats["appliedCount"])
	}
	if stats["pendingCount"] != 0 {
		t.Errorf("Expected pendingCount=0, got %d", stats["pendingCount"])
	}
}

func TestWALApplierOutOfOrder(t *testing.T) {
	storage := NewMockStorage()
	applier := NewWALApplier(storage)
	defer applier.Close()
	
	// Apply entries out of order
	entries := []*wal.Entry{
		{
			SequenceNumber: 2,
			Type:           wal.OpTypePut,
			Key:            []byte("key2"),
			Value:          []byte("value2"),
		},
		{
			SequenceNumber: 3,
			Type:           wal.OpTypePut,
			Key:            []byte("key3"),
			Value:          []byte("value3"),
		},
		{
			SequenceNumber: 1,
			Type:           wal.OpTypePut,
			Key:            []byte("key1"),
			Value:          []byte("value1"),
		},
	}
	
	// Apply entry with sequence 2 - should be stored as pending
	applied, err := applier.Apply(entries[0])
	if err != nil {
		t.Fatalf("Error applying entry: %v", err)
	}
	if applied {
		t.Errorf("Entry with seq 2 should not have been applied yet")
	}
	
	// Apply entry with sequence 3 - should be stored as pending
	applied, err = applier.Apply(entries[1])
	if err != nil {
		t.Fatalf("Error applying entry: %v", err)
	}
	if applied {
		t.Errorf("Entry with seq 3 should not have been applied yet")
	}
	
	// Check pending count
	if pending := applier.PendingEntryCount(); pending != 2 {
		t.Errorf("Expected 2 pending entries, got %d", pending)
	}
	
	// Now apply entry with sequence 1 - should trigger all entries to be applied
	applied, err = applier.Apply(entries[2])
	if err != nil {
		t.Fatalf("Error applying entry: %v", err)
	}
	if !applied {
		t.Errorf("Entry with seq 1 should have been applied")
	}
	
	// Check state - all entries should be applied now
	if got := applier.GetHighestApplied(); got != 3 {
		t.Errorf("Expected highest applied 3, got %d", got)
	}
	
	// Pending count should be 0
	if pending := applier.PendingEntryCount(); pending != 0 {
		t.Errorf("Expected 0 pending entries, got %d", pending)
	}
	
	// Check storage contains all values
	values := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}
	
	for _, v := range values {
		if val, err := storage.Get([]byte(v.key)); err != nil || string(val) != v.value {
			t.Errorf("Expected %s=%s in storage, got %s, err=%v", v.key, v.value, val, err)
		}
	}
}

func TestWALApplierBatch(t *testing.T) {
	storage := NewMockStorage()
	applier := NewWALApplier(storage)
	defer applier.Close()
	
	// Create a batch of entries
	batch := []*wal.Entry{
		{
			SequenceNumber: 3,
			Type:           wal.OpTypePut,
			Key:            []byte("key3"),
			Value:          []byte("value3"),
		},
		{
			SequenceNumber: 1,
			Type:           wal.OpTypePut,
			Key:            []byte("key1"),
			Value:          []byte("value1"),
		},
		{
			SequenceNumber: 2,
			Type:           wal.OpTypePut,
			Key:            []byte("key2"),
			Value:          []byte("value2"),
		},
	}
	
	// Apply batch - entries should be sorted by sequence number
	applied, err := applier.ApplyBatch(batch)
	if err != nil {
		t.Fatalf("Error applying batch: %v", err)
	}
	
	// All 3 entries should be applied
	if applied != 3 {
		t.Errorf("Expected 3 entries applied, got %d", applied)
	}
	
	// Check highest applied
	if got := applier.GetHighestApplied(); got != 3 {
		t.Errorf("Expected highest applied 3, got %d", got)
	}
	
	// Check all values in storage
	values := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}
	
	for _, v := range values {
		if val, err := storage.Get([]byte(v.key)); err != nil || string(val) != v.value {
			t.Errorf("Expected %s=%s in storage, got %s, err=%v", v.key, v.value, val, err)
		}
	}
}

func TestWALApplierAlreadyApplied(t *testing.T) {
	storage := NewMockStorage()
	applier := NewWALApplier(storage)
	defer applier.Close()
	
	// Apply an entry
	entry := &wal.Entry{
		SequenceNumber: 1,
		Type:           wal.OpTypePut,
		Key:            []byte("key1"),
		Value:          []byte("value1"),
	}
	
	applied, err := applier.Apply(entry)
	if err != nil {
		t.Fatalf("Error applying entry: %v", err)
	}
	if !applied {
		t.Errorf("Entry should have been applied")
	}
	
	// Try to apply the same entry again
	applied, err = applier.Apply(entry)
	if err != nil {
		t.Fatalf("Error applying entry: %v", err)
	}
	if applied {
		t.Errorf("Entry should not have been applied a second time")
	}
	
	// Check stats
	stats := applier.GetStats()
	if stats["appliedCount"] != 1 {
		t.Errorf("Expected appliedCount=1, got %d", stats["appliedCount"])
	}
	if stats["skippedCount"] != 1 {
		t.Errorf("Expected skippedCount=1, got %d", stats["skippedCount"])
	}
}

func TestWALApplierError(t *testing.T) {
	storage := NewMockStorage()
	storage.putFail = true
	applier := NewWALApplier(storage)
	defer applier.Close()
	
	entry := &wal.Entry{
		SequenceNumber: 1,
		Type:           wal.OpTypePut,
		Key:            []byte("key1"),
		Value:          []byte("value1"),
	}
	
	// Apply should return an error
	_, err := applier.Apply(entry)
	if err == nil {
		t.Errorf("Expected error from Apply, got nil")
	}
	
	// Check error count
	stats := applier.GetStats()
	if stats["errorCount"] != 1 {
		t.Errorf("Expected errorCount=1, got %d", stats["errorCount"])
	}
	
	// Fix storage and try again
	storage.putFail = false
	
	// Apply should succeed
	applied, err := applier.Apply(entry)
	if err != nil {
		t.Fatalf("Error applying entry: %v", err)
	}
	if !applied {
		t.Errorf("Entry should have been applied")
	}
}

func TestWALApplierInvalidType(t *testing.T) {
	storage := NewMockStorage()
	applier := NewWALApplier(storage)
	defer applier.Close()
	
	entry := &wal.Entry{
		SequenceNumber: 1,
		Type:           99, // Invalid type
		Key:            []byte("key1"),
		Value:          []byte("value1"),
	}
	
	// Apply should return an error
	_, err := applier.Apply(entry)
	if err == nil || !errors.Is(err, ErrInvalidEntryType) {
		t.Errorf("Expected invalid entry type error, got %v", err)
	}
}

func TestWALApplierClose(t *testing.T) {
	storage := NewMockStorage()
	applier := NewWALApplier(storage)
	
	// Apply an entry
	entry := &wal.Entry{
		SequenceNumber: 1,
		Type:           wal.OpTypePut,
		Key:            []byte("key1"),
		Value:          []byte("value1"),
	}
	
	applied, err := applier.Apply(entry)
	if err != nil {
		t.Fatalf("Error applying entry: %v", err)
	}
	if !applied {
		t.Errorf("Entry should have been applied")
	}
	
	// Close the applier
	if err := applier.Close(); err != nil {
		t.Fatalf("Error closing applier: %v", err)
	}
	
	// Try to apply another entry
	_, err = applier.Apply(&wal.Entry{
		SequenceNumber: 2,
		Type:           wal.OpTypePut,
		Key:            []byte("key2"),
		Value:          []byte("value2"),
	})
	
	if err == nil || !errors.Is(err, ErrApplierClosed) {
		t.Errorf("Expected applier closed error, got %v", err)
	}
}

func TestWALApplierResetHighest(t *testing.T) {
	storage := NewMockStorage()
	applier := NewWALApplier(storage)
	defer applier.Close()
	
	// Manually set the highest applied to 10
	applier.ResetHighestApplied(10)
	
	// Check value
	if got := applier.GetHighestApplied(); got != 10 {
		t.Errorf("Expected highest applied 10, got %d", got)
	}
	
	// Try to apply an entry with sequence 10
	applied, err := applier.Apply(&wal.Entry{
		SequenceNumber: 10,
		Type:           wal.OpTypePut,
		Key:            []byte("key10"),
		Value:          []byte("value10"),
	})
	
	if err != nil {
		t.Fatalf("Error applying entry: %v", err)
	}
	if applied {
		t.Errorf("Entry with seq 10 should have been skipped")
	}
	
	// Apply an entry with sequence 11
	applied, err = applier.Apply(&wal.Entry{
		SequenceNumber: 11,
		Type:           wal.OpTypePut,
		Key:            []byte("key11"),
		Value:          []byte("value11"),
	})
	
	if err != nil {
		t.Fatalf("Error applying entry: %v", err)
	}
	if !applied {
		t.Errorf("Entry with seq 11 should have been applied")
	}
	
	// Check new highest
	if got := applier.GetHighestApplied(); got != 11 {
		t.Errorf("Expected highest applied 11, got %d", got)
	}
}

func TestWALApplierHasEntry(t *testing.T) {
	storage := NewMockStorage()
	applier := NewWALApplier(storage)
	defer applier.Close()
	
	// Apply an entry with sequence 1
	applied, err := applier.Apply(&wal.Entry{
		SequenceNumber: 1,
		Type:           wal.OpTypePut,
		Key:            []byte("key1"),
		Value:          []byte("value1"),
	})
	
	if err != nil {
		t.Fatalf("Error applying entry: %v", err)
	}
	if !applied {
		t.Errorf("Entry should have been applied")
	}
	
	// Add a pending entry with sequence 3
	_, err = applier.Apply(&wal.Entry{
		SequenceNumber: 3,
		Type:           wal.OpTypePut,
		Key:            []byte("key3"),
		Value:          []byte("value3"),
	})
	
	if err != nil {
		t.Fatalf("Error applying entry: %v", err)
	}
	
	// Check has entry
	testCases := []struct {
		timestamp uint64
		expected  bool
	}{
		{0, true},
		{1, true},
		{2, false},
		{3, true},
		{4, false},
	}
	
	for _, tc := range testCases {
		if got := applier.HasEntry(tc.timestamp); got != tc.expected {
			t.Errorf("HasEntry(%d) = %v, want %v", tc.timestamp, got, tc.expected)
		}
	}
}