package transaction

import (
	"bytes"
	"testing"
)

func TestBufferBasicOperations(t *testing.T) {
	b := NewBuffer()

	// Test initial state
	if b.Size() != 0 {
		t.Errorf("Expected empty buffer, got size %d", b.Size())
	}

	// Test Put operation
	key1 := []byte("key1")
	value1 := []byte("value1")
	b.Put(key1, value1)

	if b.Size() != 1 {
		t.Errorf("Expected buffer size 1, got %d", b.Size())
	}

	// Test Get operation
	val, found := b.Get(key1)
	if !found {
		t.Errorf("Expected to find key %s, but it was not found", key1)
	}
	if !bytes.Equal(val, value1) {
		t.Errorf("Expected value %s, got %s", value1, val)
	}

	// Test overwriting a key
	newValue1 := []byte("new_value1")
	b.Put(key1, newValue1)

	if b.Size() != 1 {
		t.Errorf("Expected buffer size to remain 1 after overwrite, got %d", b.Size())
	}

	val, found = b.Get(key1)
	if !found {
		t.Errorf("Expected to find key %s after overwrite, but it was not found", key1)
	}
	if !bytes.Equal(val, newValue1) {
		t.Errorf("Expected updated value %s, got %s", newValue1, val)
	}

	// Test Delete operation
	b.Delete(key1)

	if b.Size() != 1 {
		t.Errorf("Expected buffer size to remain 1 after delete, got %d", b.Size())
	}

	val, found = b.Get(key1)
	if !found {
		t.Errorf("Expected to find key %s after delete op, but it was not found", key1)
	}
	if val != nil {
		t.Errorf("Expected nil value after delete, got %s", val)
	}

	// Test Clear operation
	b.Clear()

	if b.Size() != 0 {
		t.Errorf("Expected empty buffer after clear, got size %d", b.Size())
	}
}

func TestBufferOperationsMethod(t *testing.T) {
	b := NewBuffer()

	// Add multiple operations
	keys := [][]byte{
		[]byte("c"),
		[]byte("a"),
		[]byte("b"),
	}
	values := [][]byte{
		[]byte("value_c"),
		[]byte("value_a"),
		[]byte("value_b"),
	}

	b.Put(keys[0], values[0])
	b.Put(keys[1], values[1])
	b.Put(keys[2], values[2])

	// Test Operations() returns operations sorted by key
	ops := b.Operations()

	if len(ops) != 3 {
		t.Errorf("Expected 3 operations, got %d", len(ops))
	}

	// Check the order (should be sorted by key: a, b, c)
	expected := [][]byte{keys[1], keys[2], keys[0]}
	for i, op := range ops {
		if !bytes.Equal(op.Key, expected[i]) {
			t.Errorf("Expected key %s at position %d, got %s", expected[i], i, op.Key)
		}
	}

	// Test with delete operations
	b.Clear()
	b.Put(keys[0], values[0])
	b.Delete(keys[1])

	ops = b.Operations()

	if len(ops) != 2 {
		t.Errorf("Expected 2 operations, got %d", len(ops))
	}

	// The first should be a delete for 'a', the second a put for 'c'
	if !bytes.Equal(ops[0].Key, keys[1]) || !ops[0].IsDelete {
		t.Errorf("Expected delete operation for key %s, got %v", keys[1], ops[0])
	}

	if !bytes.Equal(ops[1].Key, keys[0]) || ops[1].IsDelete {
		t.Errorf("Expected put operation for key %s, got %v", keys[0], ops[1])
	}
}

func TestBufferIterator(t *testing.T) {
	b := NewBuffer()

	// Add multiple operations in non-sorted order
	keys := [][]byte{
		[]byte("c"),
		[]byte("a"),
		[]byte("b"),
	}
	values := [][]byte{
		[]byte("value_c"),
		[]byte("value_a"),
		[]byte("value_b"),
	}

	for i := range keys {
		b.Put(keys[i], values[i])
	}

	// Test iterator
	it := b.NewIterator()

	// Test Seek behavior
	if !it.Seek([]byte("b")) {
		t.Error("Expected Seek('b') to return true")
	}

	if !bytes.Equal(it.Key(), []byte("b")) {
		t.Errorf("Expected key 'b', got %s", it.Key())
	}

	if !bytes.Equal(it.Value(), []byte("value_b")) {
		t.Errorf("Expected value 'value_b', got %s", it.Value())
	}

	// Test seeking to a key that should exist
	if !it.Seek([]byte("a")) {
		t.Error("Expected Seek('a') to return true")
	}

	// Test seeking to a key that doesn't exist but is within range
	if !it.Seek([]byte("bb")) {
		t.Error("Expected Seek('bb') to return true")
	}

	if !bytes.Equal(it.Key(), []byte("c")) {
		t.Errorf("Expected key 'c' (next key after 'bb'), got %s", it.Key())
	}

	// Test seeking past the end
	if it.Seek([]byte("d")) {
		t.Error("Expected Seek('d') to return false")
	}

	if it.Valid() {
		t.Error("Expected iterator to be invalid after seeking past end")
	}

	// Test SeekToFirst
	it.SeekToFirst()

	if !it.Valid() {
		t.Error("Expected iterator to be valid after SeekToFirst")
	}

	if !bytes.Equal(it.Key(), []byte("a")) {
		t.Errorf("Expected first key to be 'a', got %s", it.Key())
	}

	// Test Next
	if !it.Next() {
		t.Error("Expected Next() to return true")
	}

	if !bytes.Equal(it.Key(), []byte("b")) {
		t.Errorf("Expected second key to be 'b', got %s", it.Key())
	}

	if !it.Next() {
		t.Error("Expected Next() to return true for the third key")
	}

	if !bytes.Equal(it.Key(), []byte("c")) {
		t.Errorf("Expected third key to be 'c', got %s", it.Key())
	}

	// Should be at the end now
	if it.Next() {
		t.Error("Expected Next() to return false after last key")
	}

	if it.Valid() {
		t.Error("Expected iterator to be invalid after iterating past end")
	}

	// Test SeekToLast
	it.SeekToLast()

	if !it.Valid() {
		t.Error("Expected iterator to be valid after SeekToLast")
	}

	if !bytes.Equal(it.Key(), []byte("c")) {
		t.Errorf("Expected last key to be 'c', got %s", it.Key())
	}

	// Test with delete operations
	b.Clear()
	b.Put([]byte("key1"), []byte("value1"))
	b.Delete([]byte("key2"))

	it = b.NewIterator()
	it.SeekToFirst()

	// First key should be key1
	if !bytes.Equal(it.Key(), []byte("key1")) {
		t.Errorf("Expected first key to be 'key1', got %s", it.Key())
	}

	if it.IsTombstone() {
		t.Error("Expected key1 not to be a tombstone")
	}

	// Next key should be key2
	it.Next()

	if !bytes.Equal(it.Key(), []byte("key2")) {
		t.Errorf("Expected second key to be 'key2', got %s", it.Key())
	}

	if !it.IsTombstone() {
		t.Error("Expected key2 to be a tombstone")
	}

	// Test empty iterator
	b.Clear()
	it = b.NewIterator()

	if it.Valid() {
		t.Error("Expected iterator to be invalid for empty buffer")
	}

	it.SeekToFirst()

	if it.Valid() {
		t.Error("Expected iterator to be invalid after SeekToFirst on empty buffer")
	}

	it.SeekToLast()

	if it.Valid() {
		t.Error("Expected iterator to be invalid after SeekToLast on empty buffer")
	}

	if it.Seek([]byte("any")) {
		t.Error("Expected Seek to return false on empty buffer")
	}
}
