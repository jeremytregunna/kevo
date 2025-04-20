package memtable

import (
	"bytes"
	"testing"
)

func TestSkipListBasicOperations(t *testing.T) {
	sl := NewSkipList()

	// Test insertion
	e1 := newEntry([]byte("key1"), []byte("value1"), TypeValue, 1)
	e2 := newEntry([]byte("key2"), []byte("value2"), TypeValue, 2)
	e3 := newEntry([]byte("key3"), []byte("value3"), TypeValue, 3)

	sl.Insert(e1)
	sl.Insert(e2)
	sl.Insert(e3)

	// Test lookup
	found := sl.Find([]byte("key2"))
	if found == nil {
		t.Fatalf("expected to find key2, but got nil")
	}
	if string(found.value) != "value2" {
		t.Errorf("expected value to be 'value2', got '%s'", string(found.value))
	}

	// Test lookup of non-existent key
	notFound := sl.Find([]byte("key4"))
	if notFound != nil {
		t.Errorf("expected nil for non-existent key, got %v", notFound)
	}
}

func TestSkipListSequenceNumbers(t *testing.T) {
	sl := NewSkipList()

	// Insert same key with different sequence numbers
	e1 := newEntry([]byte("key"), []byte("value1"), TypeValue, 1)
	e2 := newEntry([]byte("key"), []byte("value2"), TypeValue, 2)
	e3 := newEntry([]byte("key"), []byte("value3"), TypeValue, 3)

	// Insert in reverse order to test ordering
	sl.Insert(e3)
	sl.Insert(e2)
	sl.Insert(e1)

	// Find should return the entry with the highest sequence number
	found := sl.Find([]byte("key"))
	if found == nil {
		t.Fatalf("expected to find key, but got nil")
	}
	if string(found.value) != "value3" {
		t.Errorf("expected value to be 'value3' (highest seq num), got '%s'", string(found.value))
	}
	if found.seqNum != 3 {
		t.Errorf("expected sequence number to be 3, got %d", found.seqNum)
	}
}

func TestSkipListIterator(t *testing.T) {
	sl := NewSkipList()

	// Insert entries
	entries := []struct {
		key   string
		value string
		seq   uint64
	}{
		{"apple", "red", 1},
		{"banana", "yellow", 2},
		{"cherry", "red", 3},
		{"date", "brown", 4},
		{"elderberry", "purple", 5},
	}

	for _, e := range entries {
		sl.Insert(newEntry([]byte(e.key), []byte(e.value), TypeValue, e.seq))
	}

	// Test iteration
	it := sl.NewIterator()
	it.SeekToFirst()

	count := 0
	for it.Valid() {
		if count >= len(entries) {
			t.Fatalf("iterator returned more entries than expected")
		}

		expectedKey := entries[count].key
		expectedValue := entries[count].value

		if string(it.Key()) != expectedKey {
			t.Errorf("at position %d, expected key '%s', got '%s'", count, expectedKey, string(it.Key()))
		}
		if string(it.Value()) != expectedValue {
			t.Errorf("at position %d, expected value '%s', got '%s'", count, expectedValue, string(it.Value()))
		}

		it.Next()
		count++
	}

	if count != len(entries) {
		t.Errorf("expected to iterate through %d entries, but got %d", len(entries), count)
	}
}

func TestSkipListSeek(t *testing.T) {
	sl := NewSkipList()

	// Insert entries
	entries := []struct {
		key   string
		value string
		seq   uint64
	}{
		{"apple", "red", 1},
		{"banana", "yellow", 2},
		{"cherry", "red", 3},
		{"date", "brown", 4},
		{"elderberry", "purple", 5},
	}

	for _, e := range entries {
		sl.Insert(newEntry([]byte(e.key), []byte(e.value), TypeValue, e.seq))
	}

	testCases := []struct {
		seek     string
		expected string
		valid    bool
	}{
		// Before first entry
		{"a", "apple", true},
		// Exact match
		{"cherry", "cherry", true},
		// Between entries
		{"blueberry", "cherry", true},
		// After last entry
		{"zebra", "", false},
	}

	for _, tc := range testCases {
		t.Run(tc.seek, func(t *testing.T) {
			it := sl.NewIterator()
			it.Seek([]byte(tc.seek))

			if it.Valid() != tc.valid {
				t.Errorf("expected Valid() to be %v, got %v", tc.valid, it.Valid())
			}

			if tc.valid {
				if string(it.Key()) != tc.expected {
					t.Errorf("expected key '%s', got '%s'", tc.expected, string(it.Key()))
				}
			}
		})
	}
}

func TestEntryComparison(t *testing.T) {
	testCases := []struct {
		e1, e2   *entry
		expected int
	}{
		// Different keys
		{
			newEntry([]byte("a"), []byte("val"), TypeValue, 1),
			newEntry([]byte("b"), []byte("val"), TypeValue, 1),
			-1,
		},
		{
			newEntry([]byte("b"), []byte("val"), TypeValue, 1),
			newEntry([]byte("a"), []byte("val"), TypeValue, 1),
			1,
		},
		// Same key, different sequence numbers (higher seq should be "less")
		{
			newEntry([]byte("same"), []byte("val1"), TypeValue, 2),
			newEntry([]byte("same"), []byte("val2"), TypeValue, 1),
			-1,
		},
		{
			newEntry([]byte("same"), []byte("val1"), TypeValue, 1),
			newEntry([]byte("same"), []byte("val2"), TypeValue, 2),
			1,
		},
		// Same key, same sequence number
		{
			newEntry([]byte("same"), []byte("val"), TypeValue, 1),
			newEntry([]byte("same"), []byte("val"), TypeValue, 1),
			0,
		},
	}

	for i, tc := range testCases {
		result := tc.e1.compareWithEntry(tc.e2)
		expected := tc.expected
		// We just care about the sign
		if (result < 0 && expected >= 0) || (result > 0 && expected <= 0) || (result == 0 && expected != 0) {
			t.Errorf("case %d: expected comparison result %d, got %d", i, expected, result)
		}
	}
}

func TestSkipListApproximateSize(t *testing.T) {
	sl := NewSkipList()

	// Initial size should be 0
	if size := sl.ApproximateSize(); size != 0 {
		t.Errorf("expected initial size to be 0, got %d", size)
	}

	// Add some entries
	e1 := newEntry([]byte("key1"), []byte("value1"), TypeValue, 1)
	e2 := newEntry([]byte("key2"), bytes.Repeat([]byte("v"), 100), TypeValue, 2)

	sl.Insert(e1)
	expectedSize := int64(e1.size())
	if size := sl.ApproximateSize(); size != expectedSize {
		t.Errorf("expected size to be %d after first insert, got %d", expectedSize, size)
	}

	sl.Insert(e2)
	expectedSize += int64(e2.size())
	if size := sl.ApproximateSize(); size != expectedSize {
		t.Errorf("expected size to be %d after second insert, got %d", expectedSize, size)
	}
}
