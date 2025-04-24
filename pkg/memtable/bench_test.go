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

func BenchmarkMemTableDelete(b *testing.B) {
	mt := NewMemTable()

	// Prepare keys ahead of time
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}

	// Insert entries first
	for i := 0; i < b.N; i++ {
		value := []byte(fmt.Sprintf("value-%d", i))
		mt.Put(keys[i], value, uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mt.Delete(keys[i], uint64(i+b.N))
	}
}

func BenchmarkImmutableMemTableGet(b *testing.B) {
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

	// Mark memtable as immutable
	mt.SetImmutable()

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

func BenchmarkConcurrentMemTableGet(b *testing.B) {
	// This benchmark tests concurrent read performance on a mutable memtable
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
	r := rand.New(rand.NewSource(42)) // Use fixed seed for reproducibility
	lookupKeys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		idx := r.Intn(numEntries)
		lookupKeys[i] = keys[idx]
	}
	
	// Set up for parallel benchmark
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine needs its own random sequence
		localRand := rand.New(rand.NewSource(rand.Int63()))
		i := 0
		for pb.Next() {
			// Pick a random key from our prepared list
			idx := localRand.Intn(len(lookupKeys))
			mt.Get(lookupKeys[idx])
			i++
		}
	})
}

func BenchmarkConcurrentImmutableMemTableGet(b *testing.B) {
	// This benchmark tests concurrent read performance on an immutable memtable
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
	
	// Mark memtable as immutable
	mt.SetImmutable()
	
	// Create random keys for lookup
	r := rand.New(rand.NewSource(42)) // Use fixed seed for reproducibility
	lookupKeys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		idx := r.Intn(numEntries)
		lookupKeys[i] = keys[idx]
	}
	
	// Set up for parallel benchmark
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine needs its own random sequence
		localRand := rand.New(rand.NewSource(rand.Int63()))
		i := 0
		for pb.Next() {
			// Pick a random key from our prepared list
			idx := localRand.Intn(len(lookupKeys))
			mt.Get(lookupKeys[idx])
			i++
		}
	})
}

func BenchmarkMixedWorkload(b *testing.B) {
	// Skip very long benchmarks if testing with -short flag
	if testing.Short() {
		b.Skip("Skipping mixed workload benchmark in short mode")
	}
	
	// This benchmark tests a mixed workload with concurrent reads and writes
	mt := NewMemTable()
	
	// Pre-populate with some data
	const initialEntries = 50000
	keys := make([][]byte, initialEntries)
	for i := 0; i < initialEntries; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		keys[i] = key
		mt.Put(key, value, uint64(i))
	}
	
	// Prepare random operations
	readRatio := 0.8 // 80% reads, 20% writes
	
	b.ResetTimer()
	
	// Run the benchmark in parallel mode
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own random number generator
		r := rand.New(rand.NewSource(rand.Int63()))
		localCount := 0
		
		// Continue until the benchmark is done
		for pb.Next() {
			// Determine operation: read or write
			op := r.Float64()
			if op < readRatio {
				// Read operation
				idx := r.Intn(initialEntries)
				mt.Get(keys[idx])
			} else {
				// Write operation (alternating put and delete)
				if localCount%2 == 0 {
					// Put
					newKey := []byte(fmt.Sprintf("key-new-%d", localCount))
					newValue := []byte(fmt.Sprintf("value-new-%d", localCount))
					mt.Put(newKey, newValue, uint64(initialEntries+localCount))
				} else {
					// Delete (use an existing key)
					idx := r.Intn(initialEntries)
					mt.Delete(keys[idx], uint64(initialEntries+localCount))
				}
			}
			localCount++
		}
	})
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
