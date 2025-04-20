package composite

import (
	"bytes"
	"testing"

	"github.com/jeremytregunna/kevo/pkg/common/iterator"
)

// mockIterator is a simple in-memory iterator for testing
type mockIterator struct {
	pairs []struct {
		key, value []byte
	}
	index     int
	tombstone int // index of entry that should be a tombstone, -1 if none
}

func newMockIterator(data map[string]string, tombstone string) *mockIterator {
	m := &mockIterator{
		pairs:     make([]struct{ key, value []byte }, 0, len(data)),
		index:     -1,
		tombstone: -1,
	}

	// Collect keys for sorting
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	// Sort keys
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	// Add sorted key-value pairs
	for i, k := range keys {
		m.pairs = append(m.pairs, struct{ key, value []byte }{
			key:   []byte(k),
			value: []byte(data[k]),
		})
		if k == tombstone {
			m.tombstone = i
		}
	}

	return m
}

func (m *mockIterator) SeekToFirst() {
	if len(m.pairs) > 0 {
		m.index = 0
	} else {
		m.index = -1
	}
}

func (m *mockIterator) SeekToLast() {
	if len(m.pairs) > 0 {
		m.index = len(m.pairs) - 1
	} else {
		m.index = -1
	}
}

func (m *mockIterator) Seek(target []byte) bool {
	for i, p := range m.pairs {
		if bytes.Compare(p.key, target) >= 0 {
			m.index = i
			return true
		}
	}
	m.index = -1
	return false
}

func (m *mockIterator) Next() bool {
	if m.index >= 0 && m.index < len(m.pairs)-1 {
		m.index++
		return true
	}
	m.index = -1
	return false
}

func (m *mockIterator) Key() []byte {
	if m.index >= 0 && m.index < len(m.pairs) {
		return m.pairs[m.index].key
	}
	return nil
}

func (m *mockIterator) Value() []byte {
	if m.index >= 0 && m.index < len(m.pairs) {
		if m.index == m.tombstone {
			return nil // tombstone
		}
		return m.pairs[m.index].value
	}
	return nil
}

func (m *mockIterator) Valid() bool {
	return m.index >= 0 && m.index < len(m.pairs)
}

func (m *mockIterator) IsTombstone() bool {
	return m.Valid() && m.index == m.tombstone
}

func TestHierarchicalIterator_SeekToFirst(t *testing.T) {
	// Create mock iterators
	iter1 := newMockIterator(map[string]string{
		"a": "v1a",
		"c": "v1c",
		"e": "v1e",
	}, "")

	iter2 := newMockIterator(map[string]string{
		"b": "v2b",
		"c": "v2c", // Should be hidden by iter1's "c"
		"d": "v2d",
	}, "")

	// Create hierarchical iterator with iter1 being newer than iter2
	hierIter := NewHierarchicalIterator([]iterator.Iterator{iter1, iter2})

	// Test SeekToFirst
	hierIter.SeekToFirst()
	if !hierIter.Valid() {
		t.Fatal("Expected iterator to be valid after SeekToFirst")
	}

	// Should be at "a" from iter1
	if string(hierIter.Key()) != "a" {
		t.Errorf("Expected key 'a', got '%s'", string(hierIter.Key()))
	}
	if string(hierIter.Value()) != "v1a" {
		t.Errorf("Expected value 'v1a', got '%s'", string(hierIter.Value()))
	}

	// Test order of keys is merged correctly
	expected := []struct {
		key, value string
	}{
		{"a", "v1a"},
		{"b", "v2b"},
		{"c", "v1c"}, // From iter1, not iter2
		{"d", "v2d"},
		{"e", "v1e"},
	}

	for i, exp := range expected {
		if !hierIter.Valid() {
			t.Fatalf("Iterator should be valid at position %d", i)
		}

		if string(hierIter.Key()) != exp.key {
			t.Errorf("Position %d: Expected key '%s', got '%s'", i, exp.key, string(hierIter.Key()))
		}

		if string(hierIter.Value()) != exp.value {
			t.Errorf("Position %d: Expected value '%s', got '%s'", i, exp.value, string(hierIter.Value()))
		}

		if i < len(expected)-1 {
			if !hierIter.Next() {
				t.Fatalf("Next() should return true at position %d", i)
			}
		}
	}

	// After all elements, Next should return false
	if hierIter.Next() {
		t.Error("Expected Next() to return false after all elements")
	}
}

func TestHierarchicalIterator_SeekToLast(t *testing.T) {
	// Create mock iterators
	iter1 := newMockIterator(map[string]string{
		"a": "v1a",
		"c": "v1c",
		"e": "v1e",
	}, "")

	iter2 := newMockIterator(map[string]string{
		"b": "v2b",
		"d": "v2d",
		"f": "v2f",
	}, "")

	// Create hierarchical iterator with iter1 being newer than iter2
	hierIter := NewHierarchicalIterator([]iterator.Iterator{iter1, iter2})

	// Test SeekToLast
	hierIter.SeekToLast()
	if !hierIter.Valid() {
		t.Fatal("Expected iterator to be valid after SeekToLast")
	}

	// Should be at "f" from iter2
	if string(hierIter.Key()) != "f" {
		t.Errorf("Expected key 'f', got '%s'", string(hierIter.Key()))
	}
	if string(hierIter.Value()) != "v2f" {
		t.Errorf("Expected value 'v2f', got '%s'", string(hierIter.Value()))
	}
}

func TestHierarchicalIterator_Seek(t *testing.T) {
	// Create mock iterators
	iter1 := newMockIterator(map[string]string{
		"a": "v1a",
		"c": "v1c",
		"e": "v1e",
	}, "")

	iter2 := newMockIterator(map[string]string{
		"b": "v2b",
		"d": "v2d",
		"f": "v2f",
	}, "")

	// Create hierarchical iterator with iter1 being newer than iter2
	hierIter := NewHierarchicalIterator([]iterator.Iterator{iter1, iter2})

	// Test Seek
	tests := []struct {
		target      string
		expectValid bool
		expectKey   string
		expectValue string
	}{
		{"a", true, "a", "v1a"},  // Exact match from iter1
		{"b", true, "b", "v2b"},  // Exact match from iter2
		{"c", true, "c", "v1c"},  // Exact match from iter1
		{"c1", true, "d", "v2d"}, // Between c and d
		{"x", false, "", ""},     // Beyond last key
		{"", true, "a", "v1a"},   // Before first key
	}

	for i, test := range tests {
		found := hierIter.Seek([]byte(test.target))
		if found != test.expectValid {
			t.Errorf("Test %d: Seek(%s) returned %v, expected %v",
				i, test.target, found, test.expectValid)
		}

		if test.expectValid {
			if string(hierIter.Key()) != test.expectKey {
				t.Errorf("Test %d: Seek(%s) key is '%s', expected '%s'",
					i, test.target, string(hierIter.Key()), test.expectKey)
			}
			if string(hierIter.Value()) != test.expectValue {
				t.Errorf("Test %d: Seek(%s) value is '%s', expected '%s'",
					i, test.target, string(hierIter.Value()), test.expectValue)
			}
		}
	}
}

func TestHierarchicalIterator_Tombstone(t *testing.T) {
	// Create mock iterators with tombstone
	iter1 := newMockIterator(map[string]string{
		"a": "v1a",
		"c": "v1c",
	}, "c") // c is a tombstone in iter1

	iter2 := newMockIterator(map[string]string{
		"b": "v2b",
		"c": "v2c", // This should be hidden by iter1's tombstone
		"d": "v2d",
	}, "")

	// Create hierarchical iterator with iter1 being newer than iter2
	hierIter := NewHierarchicalIterator([]iterator.Iterator{iter1, iter2})

	// Test that the tombstone is correctly identified
	hierIter.SeekToFirst() // Should be at "a"
	if hierIter.IsTombstone() {
		t.Error("Key 'a' should not be a tombstone")
	}

	hierIter.Next() // Should be at "b"
	if hierIter.IsTombstone() {
		t.Error("Key 'b' should not be a tombstone")
	}

	hierIter.Next() // Should be at "c" (which is a tombstone in iter1)
	if !hierIter.IsTombstone() {
		t.Error("Key 'c' should be a tombstone")
	}

	if hierIter.Value() != nil {
		t.Error("Tombstone value should be nil")
	}

	hierIter.Next() // Should be at "d"
	if hierIter.IsTombstone() {
		t.Error("Key 'd' should not be a tombstone")
	}
}

func TestHierarchicalIterator_CompositeInterface(t *testing.T) {
	// Create mock iterators
	iter1 := newMockIterator(map[string]string{"a": "1"}, "")
	iter2 := newMockIterator(map[string]string{"b": "2"}, "")

	// Create the composite iterator
	hierIter := NewHierarchicalIterator([]iterator.Iterator{iter1, iter2})

	// Test CompositeIterator interface methods
	if hierIter.NumSources() != 2 {
		t.Errorf("Expected NumSources() to return 2, got %d", hierIter.NumSources())
	}

	sources := hierIter.GetSourceIterators()
	if len(sources) != 2 {
		t.Errorf("Expected GetSourceIterators() to return 2 sources, got %d", len(sources))
	}

	// Verify that the sources are correct
	if sources[0] != iter1 || sources[1] != iter2 {
		t.Error("Source iterators don't match the original iterators")
	}
}
