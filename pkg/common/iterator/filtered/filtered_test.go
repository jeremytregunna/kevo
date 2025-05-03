package filtered

import (
	"bytes"
	"testing"

	"github.com/KevoDB/kevo/pkg/common/iterator"
)

// MockEntry represents a single entry in the mock iterator
type MockEntry struct {
	Key       []byte
	Value     []byte
	Tombstone bool
}

// MockIterator is a simple in-memory iterator for testing
type MockIterator struct {
	entries    []MockEntry
	currentIdx int
}

// NewMockIterator creates a new mock iterator with the given entries
func NewMockIterator(entries []MockEntry) *MockIterator {
	return &MockIterator{
		entries:    entries,
		currentIdx: -1, // Start before the first entry
	}
}

// SeekToFirst positions at the first entry
func (mi *MockIterator) SeekToFirst() {
	if len(mi.entries) > 0 {
		mi.currentIdx = 0
	} else {
		mi.currentIdx = -1
	}
}

// SeekToLast positions at the last entry
func (mi *MockIterator) SeekToLast() {
	if len(mi.entries) > 0 {
		mi.currentIdx = len(mi.entries) - 1
	} else {
		mi.currentIdx = -1
	}
}

// Seek positions at the first entry with key >= target
func (mi *MockIterator) Seek(target []byte) bool {
	for i, entry := range mi.entries {
		if bytes.Compare(entry.Key, target) >= 0 {
			mi.currentIdx = i
			return true
		}
	}
	mi.currentIdx = len(mi.entries)
	return false
}

// Next advances to the next entry
func (mi *MockIterator) Next() bool {
	if mi.currentIdx < len(mi.entries)-1 {
		mi.currentIdx++
		return true
	}
	mi.currentIdx = len(mi.entries)
	return false
}

// Key returns the current key
func (mi *MockIterator) Key() []byte {
	if mi.Valid() {
		return mi.entries[mi.currentIdx].Key
	}
	return nil
}

// Value returns the current value
func (mi *MockIterator) Value() []byte {
	if mi.Valid() {
		return mi.entries[mi.currentIdx].Value
	}
	return nil
}

// Valid returns true if positioned at a valid entry
func (mi *MockIterator) Valid() bool {
	return mi.currentIdx >= 0 && mi.currentIdx < len(mi.entries)
}

// IsTombstone returns whether the current entry is a tombstone
func (mi *MockIterator) IsTombstone() bool {
	if mi.Valid() {
		return mi.entries[mi.currentIdx].Tombstone
	}
	return false
}

// Verify the MockIterator implements Iterator
var _ iterator.Iterator = (*MockIterator)(nil)

// Test the FilteredIterator with a simple filter
func TestFilteredIterator(t *testing.T) {
	entries := []MockEntry{
		{Key: []byte("a1"), Value: []byte("val1"), Tombstone: false},
		{Key: []byte("b2"), Value: []byte("val2"), Tombstone: false},
		{Key: []byte("a3"), Value: []byte("val3"), Tombstone: true},
		{Key: []byte("c4"), Value: []byte("val4"), Tombstone: false},
		{Key: []byte("a5"), Value: []byte("val5"), Tombstone: false},
	}

	baseIter := NewMockIterator(entries)
	
	// Filter for keys starting with 'a'
	filter := func(key []byte) bool {
		return bytes.HasPrefix(key, []byte("a"))
	}
	
	filtered := NewFilteredIterator(baseIter, filter)
	
	// Test SeekToFirst and Next
	filtered.SeekToFirst()
	
	if !filtered.Valid() {
		t.Fatal("Expected valid position after SeekToFirst")
	}
	
	if string(filtered.Key()) != "a1" {
		t.Errorf("Expected key 'a1', got '%s'", string(filtered.Key()))
	}
	
	if string(filtered.Value()) != "val1" {
		t.Errorf("Expected value 'val1', got '%s'", string(filtered.Value()))
	}
	
	if filtered.IsTombstone() {
		t.Error("Expected non-tombstone for first entry")
	}
	
	// Advance to next matching entry
	if !filtered.Next() {
		t.Fatal("Expected successful Next() call")
	}
	
	if string(filtered.Key()) != "a3" {
		t.Errorf("Expected key 'a3', got '%s'", string(filtered.Key()))
	}
	
	if !filtered.IsTombstone() {
		t.Error("Expected tombstone for second entry")
	}
	
	// Advance again
	if !filtered.Next() {
		t.Fatal("Expected successful Next() call")
	}
	
	if string(filtered.Key()) != "a5" {
		t.Errorf("Expected key 'a5', got '%s'", string(filtered.Key()))
	}
	
	// No more entries
	if filtered.Next() {
		t.Fatal("Expected end of iteration")
	}
	
	if filtered.Valid() {
		t.Fatal("Expected invalid position at end of iteration")
	}
}

// Test the PrefixIterator
func TestPrefixIterator(t *testing.T) {
	entries := []MockEntry{
		{Key: []byte("apple1"), Value: []byte("val1"), Tombstone: false},
		{Key: []byte("banana2"), Value: []byte("val2"), Tombstone: false},
		{Key: []byte("apple3"), Value: []byte("val3"), Tombstone: true},
		{Key: []byte("cherry4"), Value: []byte("val4"), Tombstone: false},
		{Key: []byte("apple5"), Value: []byte("val5"), Tombstone: false},
	}

	baseIter := NewMockIterator(entries)
	prefixIter := NewPrefixIterator(baseIter, []byte("apple"))
	
	// Count matching entries
	prefixIter.SeekToFirst()
	
	count := 0
	for prefixIter.Valid() {
		count++
		prefixIter.Next()
	}
	
	if count != 3 {
		t.Errorf("Expected 3 entries with prefix 'apple', got %d", count)
	}
	
	// Test Seek
	prefixIter.Seek([]byte("apple3"))
	
	if !prefixIter.Valid() {
		t.Fatal("Expected valid position after Seek")
	}
	
	if string(prefixIter.Key()) != "apple3" {
		t.Errorf("Expected key 'apple3', got '%s'", string(prefixIter.Key()))
	}
}

// Test the SuffixIterator
func TestSuffixIterator(t *testing.T) {
	entries := []MockEntry{
		{Key: []byte("key1_suffix"), Value: []byte("val1"), Tombstone: false},
		{Key: []byte("key2_other"), Value: []byte("val2"), Tombstone: false},
		{Key: []byte("key3_suffix"), Value: []byte("val3"), Tombstone: true},
		{Key: []byte("key4_test"), Value: []byte("val4"), Tombstone: false},
		{Key: []byte("key5_suffix"), Value: []byte("val5"), Tombstone: false},
	}

	baseIter := NewMockIterator(entries)
	suffixIter := NewSuffixIterator(baseIter, []byte("_suffix"))
	
	// Count matching entries
	suffixIter.SeekToFirst()
	
	count := 0
	for suffixIter.Valid() {
		count++
		suffixIter.Next()
	}
	
	if count != 3 {
		t.Errorf("Expected 3 entries with suffix '_suffix', got %d", count)
	}
	
	// Test seeking to find entries with suffix
	suffixIter.Seek([]byte("key3"))
	
	if !suffixIter.Valid() {
		t.Fatal("Expected valid position after Seek")
	}
	
	if string(suffixIter.Key()) != "key3_suffix" {
		t.Errorf("Expected key 'key3_suffix', got '%s'", string(suffixIter.Key()))
	}
}