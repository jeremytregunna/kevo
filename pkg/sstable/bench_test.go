package sstable

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// keyForIndex generates a consistent key format for a given index
func keyForIndex(idx int) []byte {
	return []byte(fmt.Sprintf("key-%08d", idx))
}

// setupBenchmarkSSTable creates a temporary SSTable file with benchmark data
func setupBenchmarkSSTable(b *testing.B, numEntries int) (string, error) {
	// Create a temporary directory
	dir, err := os.MkdirTemp("", "sstable-bench-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Create a temporary SSTable file
	path := filepath.Join(dir, "benchmark.sst")
	
	// Create a writer
	writer, err := NewWriter(path)
	if err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("failed to create SSTable writer: %w", err)
	}

	// Add entries
	for i := 0; i < numEntries; i++ {
		key := keyForIndex(i)
		value := []byte(fmt.Sprintf("value%08d", i))
		if err := writer.Add(key, value); err != nil {
			writer.Finish() // Ignore error here as we're already in an error path
			os.RemoveAll(dir)
			return "", fmt.Errorf("failed to add entry: %w", err)
		}
	}

	// Finalize the SSTable
	if err := writer.Finish(); err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("failed to finalize SSTable: %w", err)
	}

	return path, nil
}

// cleanup removes the temporary directory and SSTable file
func cleanup(path string) {
	dir := filepath.Dir(path)
	os.RemoveAll(dir)
}

// Basic test to check if SSTable read/write works at all
func TestSSTableBasicOps(t *testing.T) {
	// Create a temporary directory
	dir, err := os.MkdirTemp("", "sstable-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a temporary SSTable file
	path := filepath.Join(dir, "basic.sst")
	
	// Create a writer
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Add a single entry
	key := []byte("test-key")
	value := []byte("test-value")
	if err := writer.Add(key, value); err != nil {
		writer.Finish() // Ignore error here as we're already in an error path
		t.Fatalf("Failed to add entry: %v", err)
	}

	// Finalize the SSTable
	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finalize SSTable: %v", err)
	}

	// Open the SSTable reader
	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("Failed to open SSTable reader: %v", err)
	}
	defer reader.Close()

	// Try to get the key
	result, err := reader.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	// Verify the value
	if string(result) != string(value) {
		t.Fatalf("Expected value %q, got %q", value, result)
	}
	
	t.Logf("Basic SSTable operations work correctly!")
}

// BenchmarkSSTableGet benchmarks the Get operation
func BenchmarkSSTableGet(b *testing.B) {
	// Test with and without bloom filters
	for _, enableBloomFilter := range []bool{false, true} {
		name := "WithoutBloomFilter"
		if enableBloomFilter {
			name = "WithBloomFilter"
		}

		b.Run(name, func(b *testing.B) {
			// Create a temporary directory
			dir, err := os.MkdirTemp("", "sstable-bench-*")
			if err != nil {
				b.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(dir)

			// Create a temporary SSTable file
			path := filepath.Join(dir, fmt.Sprintf("benchmark_%s.sst", name))
			
			// Create writer with appropriate options
			options := DefaultWriterOptions()
			options.EnableBloomFilter = enableBloomFilter
			
			writer, err := NewWriterWithOptions(path, options)
			if err != nil {
				b.Fatalf("Failed to create SSTable writer: %v", err)
			}

			// Add entries (larger number for a more realistic benchmark)
			const numEntries = 10000
			for i := 0; i < numEntries; i++ {
				key := []byte(fmt.Sprintf("key%08d", i))
				value := []byte(fmt.Sprintf("value%08d", i))
				if err := writer.Add(key, value); err != nil {
					b.Fatalf("Failed to add entry: %v", err)
				}
			}

			// Finalize the SSTable
			if err := writer.Finish(); err != nil {
				b.Fatalf("Failed to finalize SSTable: %v", err)
			}

			// Open the SSTable reader
			reader, err := OpenReader(path)
			if err != nil {
				b.Fatalf("Failed to open SSTable reader: %v", err)
			}
			defer reader.Close()

			// Verify that we can read a key
			testKey := []byte("key00000000")
			_, err = reader.Get(testKey)
			if err != nil {
				b.Fatalf("Failed to get test key %q: %v", testKey, err)
			}

			// Set up keys for benchmarking
			var key []byte
			r := rand.New(rand.NewSource(42)) // Use fixed seed for reproducibility

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Alternate between existing and non-existing keys
				// This highlights the benefit of bloom filters for negative lookups
				if i%2 == 0 {
					// Existing key
					idx := r.Intn(numEntries)
					key = []byte(fmt.Sprintf("key%08d", idx))
					
					// Perform the Get operation
					_, err := reader.Get(key)
					if err != nil {
						b.Fatalf("Failed to get key %s: %v", key, err)
					}
				} else {
					// Non-existing key
					key = []byte(fmt.Sprintf("nonexistent%08d", i))
					
					// Perform the Get operation (expect not found)
					_, err := reader.Get(key)
					if err != ErrNotFound {
						b.Fatalf("Expected ErrNotFound for key %s, got: %v", key, err)
					}
				}
			}
		})
	}
}

// BenchmarkSSTableIterator benchmarks iterator operations
func BenchmarkSSTableIterator(b *testing.B) {
	// Create a temporary directory
	dir, err := os.MkdirTemp("", "sstable-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a temporary SSTable file with just 100 entries
	path := filepath.Join(dir, "benchmark.sst")
	writer, err := NewWriter(path)
	if err != nil {
		b.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Add entries
	const numEntries = 100
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d", i))
		if err := writer.Add(key, value); err != nil {
			b.Fatalf("Failed to add entry: %v", err)
		}
	}

	// Finalize the SSTable
	if err := writer.Finish(); err != nil {
		b.Fatalf("Failed to finalize SSTable: %v", err)
	}

	// Open the SSTable reader
	reader, err := OpenReader(path)
	if err != nil {
		b.Fatalf("Failed to open SSTable reader: %v", err)
	}
	defer reader.Close()

	b.ResetTimer()
	b.Run("SeekFirst", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := reader.NewIterator()
			iter.SeekToFirst()
			if !iter.Valid() {
				b.Fatal("Iterator should be valid after SeekToFirst")
			}
		}
	})

	b.Run("FullScan", func(b *testing.B) {
		// Do a test scan to determine the actual number of entries
		testIter := reader.NewIterator()
		actualCount := 0
		for testIter.SeekToFirst(); testIter.Valid(); testIter.Next() {
			actualCount++
		}
		
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			iter := reader.NewIterator()
			b.StartTimer()
			
			count := 0
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				// Just access the key and value to ensure they're loaded
				_ = iter.Key()
				_ = iter.Value()
				count++
			}
			
			if count != actualCount {
				b.Fatalf("Expected %d entries, got %d", actualCount, count)
			}
		}
	})

	b.Run("RandomSeek", func(b *testing.B) {
		// Use a fixed iterator for all seeks
		iter := reader.NewIterator()
		r := rand.New(rand.NewSource(42))
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Generate a key to seek
			idx := r.Intn(numEntries)
			key := []byte(fmt.Sprintf("key-%05d", idx))
			
			// Perform the seek
			found := iter.Seek(key)
			if !found || !iter.Valid() {
				b.Fatalf("Failed to seek to key %s", key)
			}
		}
	})
}

// BenchmarkConcurrentSSTableGet benchmarks concurrent Get operations
func BenchmarkConcurrentSSTableGet(b *testing.B) {
	// Create a temporary directory
	dir, err := os.MkdirTemp("", "sstable-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a temporary SSTable file with just 100 entries
	path := filepath.Join(dir, "benchmark.sst")
	writer, err := NewWriter(path)
	if err != nil {
		b.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Add entries
	const numEntries = 100
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d", i))
		if err := writer.Add(key, value); err != nil {
			b.Fatalf("Failed to add entry: %v", err)
		}
	}

	// Finalize the SSTable
	if err := writer.Finish(); err != nil {
		b.Fatalf("Failed to finalize SSTable: %v", err)
	}

	// Open the SSTable reader
	reader, err := OpenReader(path)
	if err != nil {
		b.Fatalf("Failed to open SSTable reader: %v", err)
	}
	defer reader.Close()

	// Verify that we can read a key
	testKey := []byte("key-00000")
	_, err = reader.Get(testKey)
	if err != nil {
		b.Fatalf("Failed to get test key %q: %v", testKey, err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own random number generator
		r := rand.New(rand.NewSource(rand.Int63()))
		var key []byte
		
		for pb.Next() {
			// Use a random key from our range
			idx := r.Intn(numEntries)
			key = []byte(fmt.Sprintf("key-%05d", idx))
			
			// Perform the Get operation
			_, err := reader.Get(key)
			if err != nil {
				b.Fatalf("Failed to get key %s: %v", key, err)
			}
		}
	})
}

// BenchmarkConcurrentSSTableIterators benchmarks concurrent iterators
func BenchmarkConcurrentSSTableIterators(b *testing.B) {
	// Use a smaller number of entries to make test setup faster
	const numEntries = 10000
	path, err := setupBenchmarkSSTable(b, numEntries)
	if err != nil {
		b.Fatalf("Failed to set up benchmark SSTable: %v", err)
	}
	defer cleanup(path)

	// Open the SSTable reader
	reader, err := OpenReader(path)
	if err != nil {
		b.Fatalf("Failed to open SSTable reader: %v", err)
	}
	defer reader.Close()

	// Create a pool of random keys for seeking
	seekKeys := make([][]byte, 1000)
	r := rand.New(rand.NewSource(42))
	for i := 0; i < len(seekKeys); i++ {
		idx := r.Intn(numEntries)
		seekKeys[i] = keyForIndex(idx)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own iterator and random number generator
		iter := reader.NewIterator()
		localRand := rand.New(rand.NewSource(rand.Int63()))
		
		for pb.Next() {
			// Choose a random operation type: 
			// 0 = Seek, 1 = SeekToFirst, 2 = Next
			op := localRand.Intn(3)
			
			switch op {
			case 0:
				// Random seek
				keyIdx := localRand.Intn(len(seekKeys))
				found := iter.Seek(seekKeys[keyIdx])
				if !found {
					// It's okay if we occasionally don't find a key
					// (e.g., if we seek past the end)
					continue
				}
				
				// Access the key/value to ensure they're loaded
				if iter.Valid() {
					_ = iter.Key()
					_ = iter.Value()
				}
				
			case 1:
				// Seek to first and read a few entries
				iter.SeekToFirst()
				if iter.Valid() {
					_ = iter.Key()
					_ = iter.Value()
					
					// Read a few more entries
					count := 0
					max := localRand.Intn(10) + 1 // 1-10 entries
					for count < max && iter.Next() {
						_ = iter.Key()
						_ = iter.Value()
						count++
					}
				}
				
			case 2:
				// If we have a valid position, move to next
				if iter.Valid() {
					iter.Next()
					if iter.Valid() {
						_ = iter.Key()
						_ = iter.Value()
					}
				} else {
					// If not valid, seek to first
					iter.SeekToFirst()
					if iter.Valid() {
						_ = iter.Key()
						_ = iter.Value()
					}
				}
			}
		}
	})
}

// BenchmarkMultipleSSTableReaders benchmarks operations with multiple SSTable readers
func BenchmarkMultipleSSTableReaders(b *testing.B) {
	// Create multiple SSTable files
	const numSSTables = 5
	const entriesPerTable = 5000
	
	paths := make([]string, numSSTables)
	for i := 0; i < numSSTables; i++ {
		path, err := setupBenchmarkSSTable(b, entriesPerTable)
		if err != nil {
			// Clean up any created files
			for j := 0; j < i; j++ {
				cleanup(paths[j])
			}
			b.Fatalf("Failed to set up benchmark SSTable %d: %v", i, err)
		}
		paths[i] = path
	}
	
	// Make sure we clean up all files
	defer func() {
		for _, path := range paths {
			cleanup(path)
		}
	}()
	
	// Open readers for all the SSTable files
	readers := make([]*Reader, numSSTables)
	for i, path := range paths {
		reader, err := OpenReader(path)
		if err != nil {
			b.Fatalf("Failed to open SSTable reader for %s: %v", path, err)
		}
		readers[i] = reader
		defer reader.Close()
	}
	
	// Prepare random keys for lookup
	keys := make([][]byte, b.N)
	tableIdx := make([]int, b.N)
	r := rand.New(rand.NewSource(42))
	
	for i := 0; i < b.N; i++ {
		// Pick a random SSTable and a random key in that table
		tableIdx[i] = r.Intn(numSSTables)
		keyIdx := r.Intn(entriesPerTable)
		keys[i] = keyForIndex(keyIdx)
	}
	
	b.ResetTimer()
	b.Run("SerialGet", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := readers[tableIdx[i]].Get(keys[i])
			if err != nil {
				b.Fatalf("Failed to get key %s from SSTable %d: %v", 
					keys[i], tableIdx[i], err)
			}
		}
	})
	
	b.Run("ConcurrentGet", func(b *testing.B) {
		var wg sync.WaitGroup
		// Use 10 goroutines
		numWorkers := 10
		batchSize := b.N / numWorkers
		
		if batchSize == 0 {
			batchSize = 1
		}
		
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				start := workerID * batchSize
				end := (workerID + 1) * batchSize
				if end > b.N {
					end = b.N
				}
				
				for i := start; i < end; i++ {
					_, err := readers[tableIdx[i]].Get(keys[i])
					if err != nil {
						b.Fatalf("Worker %d: Failed to get key %s from SSTable %d: %v", 
							workerID, keys[i], tableIdx[i], err)
					}
				}
			}(w)
		}
		
		wg.Wait()
	})
}