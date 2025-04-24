package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkBloomFilterGet(b *testing.B) {
	// Test with and without bloom filters
	for _, enableBloomFilter := range []bool{false, true} {
		name := "WithoutBloomFilter"
		if enableBloomFilter {
			name = "WithBloomFilter"
		}

		b.Run(name, func(b *testing.B) {
			// Create temporary directory for the test
			tmpDir, err := os.MkdirTemp("", "sstable_bloom_benchmark")
			if err != nil {
				b.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			// Create SSTable file path
			tablePath := filepath.Join(tmpDir, fmt.Sprintf("bench_%s.sst", name))

			// Create writer with or without bloom filters
			options := DefaultWriterOptions()
			options.EnableBloomFilter = enableBloomFilter
			writer, err := NewWriterWithOptions(tablePath, options)
			if err != nil {
				b.Fatalf("Failed to create writer: %v", err)
			}

			// Insert some known keys
			// Use fewer keys for faster benchmarking
			const numKeys = 1000
			
			// Create sorted keys (SSTable requires sorted keys)
			keys := make([]string, numKeys)
			for i := 0; i < numKeys; i++ {
				keys[i] = fmt.Sprintf("key%08d", i)
			}
			
			// Add them to the SSTable
			for _, key := range keys {
				value := []byte(fmt.Sprintf("val-%s", key))
				if err := writer.Add([]byte(key), value); err != nil {
					b.Fatalf("Failed to add key %s: %v", key, err)
				}
			}

			// Finish writing
			if err := writer.Finish(); err != nil {
				b.Fatalf("Failed to finish writer: %v", err)
			}

			// Open reader
			reader, err := OpenReader(tablePath)
			if err != nil {
				b.Fatalf("Failed to open reader: %v", err)
			}
			defer reader.Close()
			
			// Test a few specific lookups to ensure the table was written correctly
			for i := 0; i < 5; i++ {
				testKey := []byte(fmt.Sprintf("key%08d", i))
				expectedValue := []byte(fmt.Sprintf("val-key%08d", i))
				
				val, err := reader.Get(testKey)
				if err != nil {
					b.Fatalf("Verification failed: couldn't find key %s: %v", testKey, err)
				}
				
				if string(val) != string(expectedValue) {
					b.Fatalf("Value mismatch for key %s: got %q, expected %q", testKey, val, expectedValue)
				}
				
				b.Logf("Successfully verified key: %s", testKey)
			}

			// Reset timer for the benchmark
			b.ResetTimer()

			// Run benchmark - alternate between existing and non-existing keys
			for i := 0; i < b.N; i++ {
				var key []byte
				if i%2 == 0 {
					// Existing key
					keyIdx := i % numKeys
					key = []byte(fmt.Sprintf("key%08d", keyIdx))
					
					// Should find this key
					_, err := reader.Get(key)
					if err != nil {
						b.Fatalf("Failed to find existing key %s: %v", key, err)
					}
				} else {
					// Non-existing key - this is where bloom filters really help
					key = []byte(fmt.Sprintf("nonexistent%08d", i))
					
					// Should not find this key
					_, err := reader.Get(key)
					if err != ErrNotFound {
						b.Fatalf("Expected ErrNotFound for key %s, got: %v", key, err)
					}
				}
			}
		})
	}
}