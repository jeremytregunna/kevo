package memtable

import (
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/wal"
)

func TestMemTableBasicOperations(t *testing.T) {
	mt := NewMemTable()

	// Test Put and Get
	mt.Put([]byte("key1"), []byte("value1"), 1)

	value, found := mt.Get([]byte("key1"))
	if !found {
		t.Fatalf("expected to find key1, but got not found")
	}
	if string(value) != "value1" {
		t.Errorf("expected value1, got %s", string(value))
	}

	// Test not found
	_, found = mt.Get([]byte("nonexistent"))
	if found {
		t.Errorf("expected key 'nonexistent' to not be found")
	}

	// Test Delete
	mt.Delete([]byte("key1"), 2)

	value, found = mt.Get([]byte("key1"))
	if !found {
		t.Fatalf("expected tombstone to be found for key1")
	}
	if value != nil {
		t.Errorf("expected nil value for deleted key, got %v", value)
	}

	// Test Contains
	if !mt.Contains([]byte("key1")) {
		t.Errorf("expected Contains to return true for deleted key")
	}
	if mt.Contains([]byte("nonexistent")) {
		t.Errorf("expected Contains to return false for nonexistent key")
	}
}

func TestMemTableSequenceNumbers(t *testing.T) {
	mt := NewMemTable()

	// Add entries with sequence numbers
	mt.Put([]byte("key"), []byte("value1"), 1)
	mt.Put([]byte("key"), []byte("value2"), 3)
	mt.Put([]byte("key"), []byte("value3"), 2)

	// Should get the latest by sequence number (value2)
	value, found := mt.Get([]byte("key"))
	if !found {
		t.Fatalf("expected to find key, but got not found")
	}
	if string(value) != "value2" {
		t.Errorf("expected value2 (highest seq), got %s", string(value))
	}

	// The next sequence number should be one more than the highest seen
	if nextSeq := mt.GetNextSequenceNumber(); nextSeq != 4 {
		t.Errorf("expected next sequence number to be 4, got %d", nextSeq)
	}
}

func TestMemTableImmutability(t *testing.T) {
	mt := NewMemTable()

	// Add initial data
	mt.Put([]byte("key"), []byte("value"), 1)

	// Mark as immutable
	mt.SetImmutable()
	if !mt.IsImmutable() {
		t.Errorf("expected IsImmutable to return true after SetImmutable")
	}

	// Attempts to modify should have no effect
	mt.Put([]byte("key2"), []byte("value2"), 2)
	mt.Delete([]byte("key"), 3)

	// Verify no changes occurred
	_, found := mt.Get([]byte("key2"))
	if found {
		t.Errorf("expected key2 to not be added to immutable memtable")
	}

	value, found := mt.Get([]byte("key"))
	if !found {
		t.Fatalf("expected to still find key after delete on immutable table")
	}
	if string(value) != "value" {
		t.Errorf("expected value to remain unchanged, got %s", string(value))
	}
}

func TestMemTableAge(t *testing.T) {
	mt := NewMemTable()

	// A new memtable should have a very small age
	if age := mt.Age(); age > 1.0 {
		t.Errorf("expected new memtable to have age < 1.0s, got %.2fs", age)
	}

	// Sleep to increase age
	time.Sleep(10 * time.Millisecond)

	if age := mt.Age(); age <= 0.0 {
		t.Errorf("expected memtable age to be > 0, got %.6fs", age)
	}
}

func TestMemTableWALIntegration(t *testing.T) {
	mt := NewMemTable()

	// Create WAL entries
	entries := []*wal.Entry{
		{SequenceNumber: 1, Type: wal.OpTypePut, Key: []byte("key1"), Value: []byte("value1")},
		{SequenceNumber: 2, Type: wal.OpTypeDelete, Key: []byte("key2"), Value: nil},
		{SequenceNumber: 3, Type: wal.OpTypePut, Key: []byte("key3"), Value: []byte("value3")},
	}

	// Process entries
	for _, entry := range entries {
		if err := mt.ProcessWALEntry(entry); err != nil {
			t.Fatalf("failed to process WAL entry: %v", err)
		}
	}

	// Verify entries were processed correctly
	testCases := []struct {
		key      string
		expected string
		found    bool
	}{
		{"key1", "value1", true},
		{"key2", "", true}, // Deleted key
		{"key3", "value3", true},
		{"key4", "", false}, // Non-existent key
	}

	for _, tc := range testCases {
		value, found := mt.Get([]byte(tc.key))

		if found != tc.found {
			t.Errorf("key %s: expected found=%v, got %v", tc.key, tc.found, found)
			continue
		}

		if found && tc.expected != "" {
			if string(value) != tc.expected {
				t.Errorf("key %s: expected value '%s', got '%s'", tc.key, tc.expected, string(value))
			}
		}
	}

	// Verify next sequence number
	if nextSeq := mt.GetNextSequenceNumber(); nextSeq != 4 {
		t.Errorf("expected next sequence number to be 4, got %d", nextSeq)
	}
}

func TestMemTableIterator(t *testing.T) {
	mt := NewMemTable()

	// Add entries in non-sorted order
	entries := []struct {
		key   string
		value string
		seq   uint64
	}{
		{"banana", "yellow", 1},
		{"apple", "red", 2},
		{"cherry", "red", 3},
		{"date", "brown", 4},
	}

	for _, e := range entries {
		mt.Put([]byte(e.key), []byte(e.value), e.seq)
	}

	// Use iterator to verify keys are returned in sorted order
	it := mt.NewIterator()
	it.SeekToFirst()

	expected := []string{"apple", "banana", "cherry", "date"}

	for i := 0; it.Valid() && i < len(expected); i++ {
		key := string(it.Key())
		if key != expected[i] {
			t.Errorf("position %d: expected key %s, got %s", i, expected[i], key)
		}
		it.Next()
	}
}
