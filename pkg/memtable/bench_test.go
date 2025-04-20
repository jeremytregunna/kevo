package memtable

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

func BenchmarkSkipListInsert(b *testing.B) {
	sl := NewSkipList()

	// Create random keys ahead of time
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		values[i] = []byte(fmt.Sprintf("value-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := newEntry(keys[i], values[i], TypeValue, uint64(i))
		sl.Insert(e)
	}
}

func BenchmarkSkipListFind(b *testing.B) {
	sl := NewSkipList()

	// Insert entries first
	const numEntries = 100000
	keys := make([][]byte, numEntries)
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		keys[i] = key
		sl.Insert(newEntry(key, value, TypeValue, uint64(i)))
	}

	// Create random keys for lookup
	lookupKeys := make([][]byte, b.N)
	r := rand.New(rand.NewSource(42)) // Use fixed seed for reproducibility
	for i := 0; i < b.N; i++ {
		idx := r.Intn(numEntries)
		lookupKeys[i] = keys[idx]
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Find(lookupKeys[i])
	}
}

func BenchmarkMemTablePut(b *testing.B) {
	mt := NewMemTable()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		value := []byte("value-" + strconv.Itoa(i))
		mt.Put(key, value, uint64(i))
	}
}

func BenchmarkMemTableGet(b *testing.B) {
	mt := NewMemTable()

	// Insert entries first
	const numEntries = 100000
	keys := make([][]byte, numEntries)
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		keys[i] = key
		mt.Put(key, value, uint64(i))
	}

	// Create random keys for lookup
	lookupKeys := make([][]byte, b.N)
	r := rand.New(rand.NewSource(42)) // Use fixed seed for reproducibility
	for i := 0; i < b.N; i++ {
		idx := r.Intn(numEntries)
		lookupKeys[i] = keys[idx]
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mt.Get(lookupKeys[i])
	}
}

func BenchmarkMemPoolGet(b *testing.B) {
	cfg := createTestConfig()
	cfg.MemTableSize = 1024 * 1024 * 32 // 32MB for benchmark
	pool := NewMemTablePool(cfg)

	// Create multiple memtables with entries
	const entriesPerTable = 50000
	const numTables = 3
	keys := make([][]byte, entriesPerTable*numTables)

	// Fill tables
	for t := 0; t < numTables; t++ {
		// Fill a table
		for i := 0; i < entriesPerTable; i++ {
			idx := t*entriesPerTable + i
			key := []byte(fmt.Sprintf("key-%d", idx))
			value := []byte(fmt.Sprintf("value-%d", idx))
			keys[idx] = key
			pool.Put(key, value, uint64(idx))
		}

		// Switch to a new memtable (except for last one)
		if t < numTables-1 {
			pool.SwitchToNewMemTable()
		}
	}

	// Create random keys for lookup
	lookupKeys := make([][]byte, b.N)
	r := rand.New(rand.NewSource(42)) // Use fixed seed for reproducibility
	for i := 0; i < b.N; i++ {
		idx := r.Intn(entriesPerTable * numTables)
		lookupKeys[i] = keys[idx]
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Get(lookupKeys[i])
	}
}
