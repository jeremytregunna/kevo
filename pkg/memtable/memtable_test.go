package memtable

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
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

// TestConcurrentReadWrite tests that concurrent reads and writes work as expected
func TestConcurrentReadWrite(t *testing.T) {
	mt := NewMemTable()
	
	// Create some initial data
	const initialKeys = 1000
	for i := 0; i < initialKeys; i++ {
		key := []byte(fmt.Sprintf("initial-key-%d", i))
		value := []byte(fmt.Sprintf("initial-value-%d", i))
		mt.Put(key, value, uint64(i))
	}
	
	// Perform concurrent reads and writes
	const (
		numReaders = 4
		numWriters = 2
		opsPerGoroutine = 1000
	)
	
	var wg sync.WaitGroup
	wg.Add(numReaders + numWriters)
	
	// Start reader goroutines
	for r := 0; r < numReaders; r++ {
		go func(id int) {
			defer wg.Done()
			// Each reader has its own random source
			rnd := rand.New(rand.NewSource(int64(id)))
			
			for i := 0; i < opsPerGoroutine; i++ {
				// Read an existing key (one we know exists)
				idx := rnd.Intn(initialKeys)
				key := []byte(fmt.Sprintf("initial-key-%d", idx))
				expectedValue := fmt.Sprintf("initial-value-%d", idx)
				
				value, found := mt.Get(key)
				if !found {
					t.Errorf("Reader %d: expected to find key %s but it wasn't found", id, string(key))
					continue
				}
				
				// Due to concurrent writes, the value might have been updated or deleted
				// but we at least expect to find the key
				if value != nil && string(value) != expectedValue {
					// This is ok - it means a writer updated this key while we were reading
					// Just ensure it follows the expected pattern for a writer
					if !strings.HasPrefix(string(value), "updated-value-") {
						t.Errorf("Reader %d: unexpected value for key %s: %s", id, string(key), string(value))
					}
				}
			}
		}(r)
	}
	
	// Start writer goroutines
	for w := 0; w < numWriters; w++ {
		go func(id int) {
			defer wg.Done()
			// Each writer has its own random source
			rnd := rand.New(rand.NewSource(int64(id + numReaders)))
			
			for i := 0; i < opsPerGoroutine; i++ {
				// Pick an operation: 50% updates, 25% inserts, 25% deletes
				op := rnd.Intn(4)
				var key []byte
				
				if op < 2 {
					// Update an existing key
					idx := rnd.Intn(initialKeys)
					key = []byte(fmt.Sprintf("initial-key-%d", idx))
					value := []byte(fmt.Sprintf("updated-value-%d-%d-%d", id, i, idx))
					mt.Put(key, value, uint64(initialKeys + id*opsPerGoroutine + i))
				} else if op == 2 {
					// Insert a new key
					key = []byte(fmt.Sprintf("new-key-%d-%d", id, i))
					value := []byte(fmt.Sprintf("new-value-%d-%d", id, i))
					mt.Put(key, value, uint64(initialKeys + id*opsPerGoroutine + i))
				} else {
					// Delete a key
					idx := rnd.Intn(initialKeys)
					key = []byte(fmt.Sprintf("initial-key-%d", idx))
					mt.Delete(key, uint64(initialKeys + id*opsPerGoroutine + i))
				}
			}
		}(w)
	}
	
	// Wait for all goroutines to finish
	wg.Wait()
	
	// Verify the memtable is in a consistent state
	verifyInitialKeys := 0
	verifyNewKeys := 0
	verifyUpdatedKeys := 0
	verifyDeletedKeys := 0
	
	for i := 0; i < initialKeys; i++ {
		key := []byte(fmt.Sprintf("initial-key-%d", i))
		value, found := mt.Get(key)
		
		if !found {
			// This key should always be found, but it might be deleted
			t.Errorf("expected to find key %s, but it wasn't found", string(key))
			continue
		}
		
		if value == nil {
			// This key was deleted
			verifyDeletedKeys++
		} else if strings.HasPrefix(string(value), "initial-value-") {
			// This key still has its original value
			verifyInitialKeys++
		} else if strings.HasPrefix(string(value), "updated-value-") {
			// This key was updated
			verifyUpdatedKeys++
		}
	}
	
	// Check for new keys that were inserted
	for w := 0; w < numWriters; w++ {
		for i := 0; i < opsPerGoroutine; i++ {
			key := []byte(fmt.Sprintf("new-key-%d-%d", w, i))
			_, found := mt.Get(key)
			if found {
				verifyNewKeys++
			}
		}
	}
	
	// Log the statistics of what happened
	t.Logf("Verified keys after concurrent operations:")
	t.Logf("  - Original keys remaining: %d", verifyInitialKeys)
	t.Logf("  - Updated keys: %d", verifyUpdatedKeys)
	t.Logf("  - Deleted keys: %d", verifyDeletedKeys)
	t.Logf("  - New keys inserted: %d", verifyNewKeys)
	
	// Make sure the counts add up correctly
	if verifyInitialKeys + verifyUpdatedKeys + verifyDeletedKeys != initialKeys {
		t.Errorf("Key count mismatch: %d + %d + %d != %d", 
			verifyInitialKeys, verifyUpdatedKeys, verifyDeletedKeys, initialKeys)
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
