package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/engine"
)

// CompactionBenchmarkOptions configures the compaction benchmark
type CompactionBenchmarkOptions struct {
	DataDir       string
	NumKeys       int
	ValueSize     int
	WriteInterval time.Duration
	TotalDuration time.Duration
}

// CompactionBenchmarkResult contains the results of a compaction benchmark
type CompactionBenchmarkResult struct {
	TotalKeys            int
	TotalBytes           int64
	WriteDuration        time.Duration
	CompactionDuration   time.Duration
	WriteOpsPerSecond    float64
	CompactionThroughput float64 // MB/s
	MemoryUsage          uint64  // Peak memory usage
	SSTableCount         int     // Number of SSTables created
	CompactionCount      int     // Number of compactions performed
}

// RunCompactionBenchmark runs a benchmark focused on compaction performance
func RunCompactionBenchmark(opts CompactionBenchmarkOptions) (*CompactionBenchmarkResult, error) {
	fmt.Println("Starting Compaction Benchmark...")

	// Create clean directory
	dataDir := opts.DataDir
	os.RemoveAll(dataDir)
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create benchmark directory: %v", err)
	}

	// Create the engine
	e, err := engine.NewEngine(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage engine: %v", err)
	}
	defer e.Close()

	// Prepare value
	value := make([]byte, opts.ValueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	result := &CompactionBenchmarkResult{
		TotalKeys:  opts.NumKeys,
		TotalBytes: int64(opts.NumKeys) * int64(opts.ValueSize),
	}

	// Create a stop channel for ending the metrics collection
	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	// Start metrics collection in a goroutine
	wg.Add(1)
	var peakMemory uint64
	var lastStats map[string]interface{}

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Get memory usage
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.Alloc > peakMemory {
					peakMemory = m.Alloc
				}

				// Get engine stats
				lastStats = e.GetStats()
			case <-stopChan:
				return
			}
		}
	}()

	// Start writing data with pauses to allow compaction to happen
	fmt.Println("Writing data with pauses to trigger compaction...")
	writeStart := time.Now()

	var keyCounter int
	writeDeadline := writeStart.Add(opts.TotalDuration)

	for time.Now().Before(writeDeadline) {
		// Write a batch of keys
		batchStart := time.Now()
		batchDeadline := batchStart.Add(opts.WriteInterval)

		var batchCount int
		for time.Now().Before(batchDeadline) && keyCounter < opts.NumKeys {
			key := []byte(fmt.Sprintf("compaction-key-%010d", keyCounter))
			if err := e.Put(key, value); err != nil {
				fmt.Fprintf(os.Stderr, "Write error: %v\n", err)
				break
			}
			keyCounter++
			batchCount++

			// Small pause between writes to simulate real-world write rate
			if batchCount%100 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}

		// Pause between batches to let compaction catch up
		fmt.Printf("Wrote %d keys, pausing to allow compaction...\n", batchCount)
		time.Sleep(2 * time.Second)

		// If we've written all the keys, break
		if keyCounter >= opts.NumKeys {
			break
		}
	}

	result.WriteDuration = time.Since(writeStart)
	result.WriteOpsPerSecond = float64(keyCounter) / result.WriteDuration.Seconds()

	// Wait a bit longer for any pending compactions to finish
	fmt.Println("Waiting for compactions to complete...")
	time.Sleep(5 * time.Second)

	// Stop metrics collection
	close(stopChan)
	wg.Wait()

	// Update result with final metrics
	result.MemoryUsage = peakMemory

	if lastStats != nil {
		// Extract compaction information from engine stats
		if sstCount, ok := lastStats["sstable_count"].(int); ok {
			result.SSTableCount = sstCount
		}

		var compactionCount int
		var compactionTimeNano int64

		// Look for compaction-related statistics
		for k, v := range lastStats {
			if k == "compaction_count" {
				if count, ok := v.(uint64); ok {
					compactionCount = int(count)
				}
			} else if k == "compaction_time_ns" {
				if timeNs, ok := v.(uint64); ok {
					compactionTimeNano = int64(timeNs)
				}
			}
		}

		result.CompactionCount = compactionCount
		result.CompactionDuration = time.Duration(compactionTimeNano)

		// Calculate compaction throughput in MB/s if we have duration
		if result.CompactionDuration > 0 {
			throughputBytes := float64(result.TotalBytes) / result.CompactionDuration.Seconds()
			result.CompactionThroughput = throughputBytes / (1024 * 1024) // Convert to MB/s
		}
	}

	// Print summary
	fmt.Println("\nCompaction Benchmark Summary:")
	fmt.Printf("  Total Keys: %d\n", result.TotalKeys)
	fmt.Printf("  Total Data: %.2f MB\n", float64(result.TotalBytes)/(1024*1024))
	fmt.Printf("  Write Duration: %.2f seconds\n", result.WriteDuration.Seconds())
	fmt.Printf("  Write Throughput: %.2f ops/sec\n", result.WriteOpsPerSecond)
	fmt.Printf("  Peak Memory Usage: %.2f MB\n", float64(result.MemoryUsage)/(1024*1024))
	fmt.Printf("  SSTable Count: %d\n", result.SSTableCount)
	fmt.Printf("  Compaction Count: %d\n", result.CompactionCount)

	if result.CompactionDuration > 0 {
		fmt.Printf("  Compaction Duration: %.2f seconds\n", result.CompactionDuration.Seconds())
		fmt.Printf("  Compaction Throughput: %.2f MB/s\n", result.CompactionThroughput)
	} else {
		fmt.Println("  Compaction Duration: Unknown (no compaction metrics available)")
	}

	return result, nil
}

// RunCompactionBenchmarkWithDefaults runs the compaction benchmark with default settings
func RunCompactionBenchmarkWithDefaults(dataDir string) error {
	opts := CompactionBenchmarkOptions{
		DataDir:       dataDir,
		NumKeys:       500000,
		ValueSize:     1024, // 1KB values
		WriteInterval: 5 * time.Second,
		TotalDuration: 2 * time.Minute,
	}

	// Run the benchmark
	_, err := RunCompactionBenchmark(opts)
	return err
}

// CustomCompactionBenchmark allows running a compaction benchmark from the command line
func CustomCompactionBenchmark(numKeys, valueSize int, duration time.Duration) error {
	// Create a dedicated directory for this benchmark
	dataDir := filepath.Join(*dataDir, fmt.Sprintf("compaction-bench-%d", time.Now().Unix()))

	opts := CompactionBenchmarkOptions{
		DataDir:       dataDir,
		NumKeys:       numKeys,
		ValueSize:     valueSize,
		WriteInterval: 5 * time.Second,
		TotalDuration: duration,
	}

	// Run the benchmark
	_, err := RunCompactionBenchmark(opts)
	return err
}
