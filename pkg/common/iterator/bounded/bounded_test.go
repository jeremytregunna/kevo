package bounded

import (
	"testing"
)

// mockIterator is a simple in-memory iterator for testing
type mockIterator struct {
	data  map[string]string
	keys  []string
	index int
}

func newMockIterator(data map[string]string) *mockIterator {
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

	return &mockIterator{
		data:  data,
		keys:  keys,
		index: -1,
	}
}

func (m *mockIterator) SeekToFirst() {
	if len(m.keys) > 0 {
		m.index = 0
	} else {
		m.index = -1
	}
}

func (m *mockIterator) SeekToLast() {
	if len(m.keys) > 0 {
		m.index = len(m.keys) - 1
	} else {
		m.index = -1
	}
}

func (m *mockIterator) Seek(target []byte) bool {
	targetStr := string(target)
	for i, key := range m.keys {
		if key >= targetStr {
			m.index = i
			return true
		}
	}
	m.index = -1
	return false
}

func (m *mockIterator) Next() bool {
	if m.index >= 0 && m.index < len(m.keys)-1 {
		m.index++
		return true
	}
	m.index = -1
	return false
}

func (m *mockIterator) Key() []byte {
	if m.index >= 0 && m.index < len(m.keys) {
		return []byte(m.keys[m.index])
	}
	return nil
}

func (m *mockIterator) Value() []byte {
	if m.index >= 0 && m.index < len(m.keys) {
		key := m.keys[m.index]
		return []byte(m.data[key])
	}
	return nil
}

func (m *mockIterator) Valid() bool {
	return m.index >= 0 && m.index < len(m.keys)
}

func (m *mockIterator) IsTombstone() bool {
	return false
}

func TestBoundedIterator_NoBounds(t *testing.T) {
	// Create a mock iterator with some data
	mockIter := newMockIterator(map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": "4",
		"e": "5",
	})

	// Create bounded iterator with no bounds
	boundedIter := NewBoundedIterator(mockIter, nil, nil)

	// Test SeekToFirst
	boundedIter.SeekToFirst()
	if !boundedIter.Valid() {
		t.Fatal("Expected iterator to be valid after SeekToFirst")
	}

	// Should be at "a"
	if string(boundedIter.Key()) != "a" {
		t.Errorf("Expected key 'a', got '%s'", string(boundedIter.Key()))
	}

	// Test iterating through all keys
	expected := []string{"a", "b", "c", "d", "e"}
	for i, exp := range expected {
		if !boundedIter.Valid() {
			t.Fatalf("Iterator should be valid at position %d", i)
		}

		if string(boundedIter.Key()) != exp {
			t.Errorf("Position %d: Expected key '%s', got '%s'", i, exp, string(boundedIter.Key()))
		}

		if i < len(expected)-1 {
			if !boundedIter.Next() {
				t.Fatalf("Next() should return true at position %d", i)
			}
		}
	}

	// After all elements, Next should return false
	if boundedIter.Next() {
		t.Error("Expected Next() to return false after all elements")
	}

	// Test SeekToLast
	boundedIter.SeekToLast()
	if !boundedIter.Valid() {
		t.Fatal("Expected iterator to be valid after SeekToLast")
	}

	// Should be at "e"
	if string(boundedIter.Key()) != "e" {
		t.Errorf("Expected key 'e', got '%s'", string(boundedIter.Key()))
	}
}

func TestBoundedIterator_WithBounds(t *testing.T) {
	// Create a mock iterator with some data
	mockIter := newMockIterator(map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": "4",
		"e": "5",
	})

	// Create bounded iterator with bounds b to d (inclusive b, exclusive d)
	boundedIter := NewBoundedIterator(mockIter, []byte("b"), []byte("d"))

	// Test SeekToFirst
	boundedIter.SeekToFirst()
	if !boundedIter.Valid() {
		t.Fatal("Expected iterator to be valid after SeekToFirst")
	}

	// Should be at "b" (start of range)
	if string(boundedIter.Key()) != "b" {
		t.Errorf("Expected key 'b', got '%s'", string(boundedIter.Key()))
	}

	// Test iterating through the range
	expected := []string{"b", "c"}
	for i, exp := range expected {
		if !boundedIter.Valid() {
			t.Fatalf("Iterator should be valid at position %d", i)
		}

		if string(boundedIter.Key()) != exp {
			t.Errorf("Position %d: Expected key '%s', got '%s'", i, exp, string(boundedIter.Key()))
		}

		if i < len(expected)-1 {
			if !boundedIter.Next() {
				t.Fatalf("Next() should return true at position %d", i)
			}
		}
	}

	// After last element in range, Next should return false
	if boundedIter.Next() {
		t.Error("Expected Next() to return false after last element in range")
	}

	// Test SeekToLast
	boundedIter.SeekToLast()
	if !boundedIter.Valid() {
		t.Fatal("Expected iterator to be valid after SeekToLast")
	}

	// Should be at "c" (last element in range)
	if string(boundedIter.Key()) != "c" {
		t.Errorf("Expected key 'c', got '%s'", string(boundedIter.Key()))
	}
}

func TestBoundedIterator_Seek(t *testing.T) {
	// Create a mock iterator with some data
	mockIter := newMockIterator(map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": "4",
		"e": "5",
	})

	// Create bounded iterator with bounds b to d (inclusive b, exclusive d)
	boundedIter := NewBoundedIterator(mockIter, []byte("b"), []byte("d"))

	// Test seeking within bounds
	tests := []struct {
		target      string
		expectValid bool
		expectKey   string
	}{
		{"a", true, "b"},  // Before range, should go to start bound
		{"b", true, "b"},  // At range start
		{"bc", true, "c"}, // Between b and c
		{"c", true, "c"},  // Within range
		{"d", false, ""},  // At range end (exclusive)
		{"e", false, ""},  // After range
	}

	for i, test := range tests {
		found := boundedIter.Seek([]byte(test.target))
		if found != test.expectValid {
			t.Errorf("Test %d: Seek(%s) returned %v, expected %v",
				i, test.target, found, test.expectValid)
		}

		if test.expectValid {
			if string(boundedIter.Key()) != test.expectKey {
				t.Errorf("Test %d: Seek(%s) key is '%s', expected '%s'",
					i, test.target, string(boundedIter.Key()), test.expectKey)
			}
		}
	}
}

func TestBoundedIterator_SetBounds(t *testing.T) {
	// Create a mock iterator with some data
	mockIter := newMockIterator(map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
		"d": "4",
		"e": "5",
	})

	// Create bounded iterator with no initial bounds
	boundedIter := NewBoundedIterator(mockIter, nil, nil)

	// Position at 'c'
	boundedIter.Seek([]byte("c"))

	// Set bounds that include 'c'
	boundedIter.SetBounds([]byte("b"), []byte("e"))

	// Iterator should still be valid at 'c'
	if !boundedIter.Valid() {
		t.Fatal("Iterator should remain valid after setting bounds that include current position")
	}

	if string(boundedIter.Key()) != "c" {
		t.Errorf("Expected key to remain 'c', got '%s'", string(boundedIter.Key()))
	}

	// Set bounds that exclude 'c'
	boundedIter.SetBounds([]byte("d"), []byte("f"))

	// Iterator should no longer be valid
	if boundedIter.Valid() {
		t.Fatal("Iterator should be invalid after setting bounds that exclude current position")
	}

	// SeekToFirst should position at 'd'
	boundedIter.SeekToFirst()
	if !boundedIter.Valid() {
		t.Fatal("Iterator should be valid after SeekToFirst")
	}

	if string(boundedIter.Key()) != "d" {
		t.Errorf("Expected key 'd', got '%s'", string(boundedIter.Key()))
	}
}
