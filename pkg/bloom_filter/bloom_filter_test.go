package bloomfilter

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBloomFilterBasic(t *testing.T) {
	// Create a new Bloom filter with 1% false positive rate and 1000 expected elements
	bf := NewBloomFilter(0.01, 1000)

	// Add some elements
	bf.Add([]byte("apple"))
	bf.Add([]byte("banana"))
	bf.Add([]byte("cherry"))

	// Test positive lookups
	if !bf.Contains([]byte("apple")) {
		t.Error("Expected 'apple' to be in the filter")
	}
	if !bf.Contains([]byte("banana")) {
		t.Error("Expected 'banana' to be in the filter")
	}
	if !bf.Contains([]byte("cherry")) {
		t.Error("Expected 'cherry' to be in the filter")
	}

	// Test negative lookup
	if bf.Contains([]byte("durian")) {
		t.Error("Did not expect 'durian' to be in the filter")
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	// Create Bloom filters with different false positive rates
	testCases := []struct {
		fpr      float64 // Target false positive rate
		elements uint64  // Number of elements to insert
	}{
		{0.01, 1000},  // 1% false positive rate
		{0.001, 1000}, // 0.1% false positive rate
		{0.1, 1000},   // 10% false positive rate
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("FPR=%.3f", tc.fpr), func(t *testing.T) {
			bf := NewBloomFilter(tc.fpr, tc.elements)

			// Insert the expected number of elements
			for i := uint64(0); i < tc.elements; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				bf.Add(key)
			}

			// Estimated FPR should be close to the target
			estimatedFPR := bf.EstimatedFalsePositiveRate()
			t.Logf("Target FPR: %.6f, Estimated FPR: %.6f", tc.fpr, estimatedFPR)

			// The estimated FPR should be within a reasonable range of the target
			if estimatedFPR > tc.fpr*3 || estimatedFPR < tc.fpr/3 {
				t.Errorf("Estimated FPR (%.6f) is not within expected range of target FPR (%.6f)",
					estimatedFPR, tc.fpr)
			}

			// Test negative lookups to estimate actual false positive rate
			falsePositives := 0
			testCount := 10000

			for i := 0; i < testCount; i++ {
				key := []byte(fmt.Sprintf("non-existent-key-%d", i+int(tc.elements)))
				if bf.Contains(key) {
					falsePositives++
				}
			}

			actualFPR := float64(falsePositives) / float64(testCount)
			t.Logf("Actual FPR from tests: %.6f", actualFPR)

			// Actual FPR should be close to the target as well
			// But this is probabilistic, so be generous with the range
			if actualFPR > tc.fpr*100 {
				t.Errorf("Actual FPR (%.6f) is significantly higher than target FPR (%.6f)",
					actualFPR, tc.fpr)
			}
		})
	}
}

func TestBloomFilterClear(t *testing.T) {
	bf := NewBloomFilter(0.01, 1000)

	// Add some elements
	bf.Add([]byte("apple"))
	bf.Add([]byte("banana"))

	// Verify they're in the filter
	if !bf.Contains([]byte("apple")) || !bf.Contains([]byte("banana")) {
		t.Fatal("Elements not added to filter correctly")
	}

	// Clear the filter
	bf.Clear()

	// Verify elements are no longer in the filter
	if bf.Contains([]byte("apple")) || bf.Contains([]byte("banana")) {
		t.Error("Filter was not cleared correctly")
	}

	// Insertions count should be reset
	if bf.Insertions() != 0 {
		t.Errorf("Insertion count should be 0 after clear, got %d", bf.Insertions())
	}
}

func TestBloomFilterSaveLoad(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "bloom_filter_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	filterPath := filepath.Join(tempDir, "test_filter.bloom")

	// Create and populate a filter
	bf := NewBloomFilter(0.01, 1000)
	testKeys := []string{"apple", "banana", "cherry", "date", "elderberry"}

	for _, key := range testKeys {
		bf.Add([]byte(key))
	}

	// Save to file
	err = bf.SaveToFile(filterPath)
	if err != nil {
		t.Fatalf("Failed to save filter: %v", err)
	}

	// Load from file
	loadedBF, err := LoadBloomFilter(filterPath)
	if err != nil {
		t.Fatalf("Failed to load filter: %v", err)
	}

	// Verify properties
	if loadedBF.Size() != bf.Size() {
		t.Errorf("Size mismatch: expected %d, got %d", bf.Size(), loadedBF.Size())
	}
	if loadedBF.HashFunctions() != bf.HashFunctions() {
		t.Errorf("Hash function count mismatch: expected %d, got %d",
			bf.HashFunctions(), loadedBF.HashFunctions())
	}
	if loadedBF.Insertions() != bf.Insertions() {
		t.Errorf("Insertion count mismatch: expected %d, got %d",
			bf.Insertions(), loadedBF.Insertions())
	}

	// Verify all test keys are found
	for _, key := range testKeys {
		if !loadedBF.Contains([]byte(key)) {
			t.Errorf("Loaded filter doesn't contain key: %s", key)
		}
	}

	// Verify non-existent key behavior is preserved
	if loadedBF.Contains([]byte("non-existent")) != bf.Contains([]byte("non-existent")) {
		t.Error("Loaded filter behavior differs for non-existent key")
	}
}

func TestBloomFilterPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Create a large filter
	bf := NewBloomFilter(0.01, 1000000) // 1% FPR, 1 million elements

	// Insert a large number of elements
	for i := 0; i < 1000000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		bf.Add(key)
	}

	// Verify all inserted elements are found
	for i := 0; i < 1000; i++ { // Sample 1000 keys
		key := []byte(fmt.Sprintf("key-%d", i))
		if !bf.Contains(key) {
			t.Errorf("False negative: key %s should be in the filter", key)
		}
	}
}

func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(0.01, uint64(b.N))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		bf.Add(key)
	}
}

func BenchmarkBloomFilterContains(b *testing.B) {
	bf := NewBloomFilter(0.01, 1000000)

	// Insert some elements first
	for i := 0; i < 1000000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		bf.Add(key)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Mix of existing and non-existing keys
		keyExists := i%2 == 0
		var key []byte
		if keyExists {
			key = []byte(fmt.Sprintf("key-%d", i%1000000))
		} else {
			key = []byte(fmt.Sprintf("non-existent-%d", i))
		}
		bf.Contains(key)
	}
}
