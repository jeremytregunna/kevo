package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/KevoDB/kevo/pkg/engine"
)

const (
	defaultValueSize = 100
	defaultKeyCount  = 100000
)

var (
	// Command line flags
	benchmarkType = flag.String("type", "all", "Type of benchmark to run (write, read, scan, mixed, tune, or all)")
	duration      = flag.Duration("duration", 10*time.Second, "Duration to run the benchmark")
	numKeys       = flag.Int("keys", defaultKeyCount, "Number of keys to use")
	valueSize     = flag.Int("value-size", defaultValueSize, "Size of values in bytes")
	dataDir       = flag.String("data-dir", "./benchmark-data", "Directory to store benchmark data")
	sequential    = flag.Bool("sequential", false, "Use sequential keys instead of random")
	cpuProfile    = flag.String("cpu-profile", "", "Write CPU profile to file")
	memProfile    = flag.String("mem-profile", "", "Write memory profile to file")
	resultsFile   = flag.String("results", "", "File to write results to (in addition to stdout)")
	tuneParams    = flag.Bool("tune", false, "Run configuration tuning benchmarks")
)

func main() {
	flag.Parse()

	// Set up CPU profiling if requested
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not create CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "Could not start CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	// Remove any existing benchmark data before starting
	if _, err := os.Stat(*dataDir); err == nil {
		fmt.Println("Cleaning previous benchmark data...")
		if err := os.RemoveAll(*dataDir); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to clean benchmark directory: %v\n", err)
		}
	}

	// Create benchmark directory
	err := os.MkdirAll(*dataDir, 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create benchmark directory: %v\n", err)
		os.Exit(1)
	}

	// Open storage engine
	e, err := engine.NewEngine(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create storage engine: %v\n", err)
		os.Exit(1)
	}
	defer e.Close()

	// Prepare result output
	var results []string
	results = append(results, fmt.Sprintf("Benchmark Report (%s)", time.Now().Format(time.RFC3339)))
	results = append(results, fmt.Sprintf("Keys: %d, Value Size: %d bytes, Duration: %s, Mode: %s",
		*numKeys, *valueSize, *duration, keyMode()))

	// Run the specified benchmarks
	// Check if we should run the tuning benchmark
	if *tuneParams {
		fmt.Println("Running configuration tuning benchmarks...")
		if err := RunFullTuningBenchmark(); err != nil {
			fmt.Fprintf(os.Stderr, "Tuning failed: %v\n", err)
			os.Exit(1)
		}
		return // Exit after tuning
	}

	types := strings.Split(*benchmarkType, ",")
	for _, typ := range types {
		switch strings.ToLower(typ) {
		case "write":
			result := runWriteBenchmark(e)
			results = append(results, result)
		case "read":
			result := runReadBenchmark(e)
			results = append(results, result)
		case "scan":
			result := runScanBenchmark(e)
			results = append(results, result)
		case "mixed":
			result := runMixedBenchmark(e)
			results = append(results, result)
		case "tune":
			fmt.Println("Running configuration tuning benchmarks...")
			if err := RunFullTuningBenchmark(); err != nil {
				fmt.Fprintf(os.Stderr, "Tuning failed: %v\n", err)
				continue
			}
			return // Exit after tuning
		case "all":
			results = append(results, runWriteBenchmark(e))
			results = append(results, runReadBenchmark(e))
			results = append(results, runScanBenchmark(e))
			results = append(results, runMixedBenchmark(e))
		default:
			fmt.Fprintf(os.Stderr, "Unknown benchmark type: %s\n", typ)
			os.Exit(1)
		}
	}

	// Print results
	for _, result := range results {
		fmt.Println(result)
	}

	// Write results to file if requested
	if *resultsFile != "" {
		err := os.WriteFile(*resultsFile, []byte(strings.Join(results, "\n")), 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write results to file: %v\n", err)
		}
	}

	// Write memory profile if requested
	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not create memory profile: %v\n", err)
		} else {
			defer f.Close()
			runtime.GC() // Run GC before taking memory profile
			if err := pprof.WriteHeapProfile(f); err != nil {
				fmt.Fprintf(os.Stderr, "Could not write memory profile: %v\n", err)
			}
		}
	}
}

// keyMode returns a string describing the key generation mode
func keyMode() string {
	if *sequential {
		return "Sequential"
	}
	return "Random"
}

// runWriteBenchmark benchmarks write performance
func runWriteBenchmark(e *engine.Engine) string {
	fmt.Println("Running Write Benchmark...")

	// Determine reasonable batch size based on value size
	// Smaller values can be written in larger batches
	batchSize := 1000
	if *valueSize > 1024 {
		batchSize = 500
	} else if *valueSize > 4096 {
		batchSize = 100
	}

	start := time.Now()
	deadline := start.Add(*duration)

	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	var opsCount int
	var consecutiveErrors int
	maxConsecutiveErrors := 10

	for time.Now().Before(deadline) {
		// Process in batches
		for i := 0; i < batchSize && time.Now().Before(deadline); i++ {
			key := generateKey(opsCount)
			if err := e.Put(key, value); err != nil {
				if err == engine.ErrEngineClosed {
					fmt.Fprintf(os.Stderr, "Engine closed, stopping benchmark\n")
					consecutiveErrors++
					if consecutiveErrors >= maxConsecutiveErrors {
						goto benchmarkEnd
					}
					time.Sleep(10 * time.Millisecond) // Wait a bit for possible background operations
					continue
				}

				fmt.Fprintf(os.Stderr, "Write error (key #%d): %v\n", opsCount, err)
				consecutiveErrors++
				if consecutiveErrors >= maxConsecutiveErrors {
					fmt.Fprintf(os.Stderr, "Too many consecutive errors, stopping benchmark\n")
					goto benchmarkEnd
				}
				continue
			}

			consecutiveErrors = 0 // Reset error counter on successful writes
			opsCount++
		}

		// Pause between batches to give background operations time to complete
		time.Sleep(5 * time.Millisecond)
	}

benchmarkEnd:
	elapsed := time.Since(start)
	opsPerSecond := float64(opsCount) / elapsed.Seconds()
	mbPerSecond := float64(opsCount) * float64(*valueSize) / (1024 * 1024) / elapsed.Seconds()

	// If we hit errors due to WAL rotation, note that in results
	var status string
	if consecutiveErrors >= maxConsecutiveErrors {
		status = "COMPLETED WITH ERRORS (expected during WAL rotation)"
	} else {
		status = "COMPLETED SUCCESSFULLY"
	}

	result := fmt.Sprintf("\nWrite Benchmark Results:")
	result += fmt.Sprintf("\n  Status: %s", status)
	result += fmt.Sprintf("\n  Operations: %d", opsCount)
	result += fmt.Sprintf("\n  Data Written: %.2f MB", float64(opsCount)*float64(*valueSize)/(1024*1024))
	result += fmt.Sprintf("\n  Time: %.2f seconds", elapsed.Seconds())
	result += fmt.Sprintf("\n  Throughput: %.2f ops/sec (%.2f MB/sec)", opsPerSecond, mbPerSecond)
	result += fmt.Sprintf("\n  Latency: %.3f µs/op", 1000000.0/opsPerSecond)
	result += fmt.Sprintf("\n  Note: Errors related to WAL are expected when the memtable is flushed during benchmark")

	return result
}

// runReadBenchmark benchmarks read performance
func runReadBenchmark(e *engine.Engine) string {
	fmt.Println("Preparing data for Read Benchmark...")

	// First, write data to read
	actualNumKeys := *numKeys
	if actualNumKeys > 100000 {
		// Limit number of keys for preparation to avoid overwhelming
		actualNumKeys = 100000
		fmt.Println("Limiting to 100,000 keys for preparation phase")
	}

	keys := make([][]byte, actualNumKeys)
	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	for i := 0; i < actualNumKeys; i++ {
		keys[i] = generateKey(i)
		if err := e.Put(keys[i], value); err != nil {
			if err == engine.ErrEngineClosed {
				fmt.Fprintf(os.Stderr, "Engine closed during preparation\n")
				return "Read Benchmark Failed: Engine closed"
			}
			fmt.Fprintf(os.Stderr, "Write error during preparation: %v\n", err)
			return "Read Benchmark Failed: Error preparing data"
		}

		// Add small pause every 1000 keys
		if i > 0 && i%1000 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	fmt.Println("Running Read Benchmark...")
	start := time.Now()
	deadline := start.Add(*duration)

	var opsCount, hitCount int
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for time.Now().Before(deadline) {
		// Use smaller batches
		batchSize := 100
		for i := 0; i < batchSize; i++ {
			// Read a random key from our set
			idx := r.Intn(actualNumKeys)
			key := keys[idx]

			val, err := e.Get(key)
			if err == engine.ErrEngineClosed {
				fmt.Fprintf(os.Stderr, "Engine closed, stopping benchmark\n")
				goto benchmarkEnd
			}
			if err == nil && val != nil {
				hitCount++
			}
			opsCount++
		}

		// Small pause to prevent overwhelming the engine
		time.Sleep(1 * time.Millisecond)
	}

benchmarkEnd:
	elapsed := time.Since(start)
	opsPerSecond := float64(opsCount) / elapsed.Seconds()
	hitRate := float64(hitCount) / float64(opsCount) * 100

	result := fmt.Sprintf("\nRead Benchmark Results:")
	result += fmt.Sprintf("\n  Operations: %d", opsCount)
	result += fmt.Sprintf("\n  Hit Rate: %.2f%%", hitRate)
	result += fmt.Sprintf("\n  Time: %.2f seconds", elapsed.Seconds())
	result += fmt.Sprintf("\n  Throughput: %.2f ops/sec", opsPerSecond)
	result += fmt.Sprintf("\n  Latency: %.3f µs/op", 1000000.0/opsPerSecond)

	return result
}

// runScanBenchmark benchmarks range scan performance
func runScanBenchmark(e *engine.Engine) string {
	fmt.Println("Preparing data for Scan Benchmark...")

	// First, write data to scan
	actualNumKeys := *numKeys
	if actualNumKeys > 50000 {
		// Limit number of keys for scan to avoid overwhelming
		actualNumKeys = 50000
		fmt.Println("Limiting to 50,000 keys for scan benchmark")
	}

	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	for i := 0; i < actualNumKeys; i++ {
		// Use sequential keys for scanning
		key := []byte(fmt.Sprintf("key-%06d", i))
		if err := e.Put(key, value); err != nil {
			if err == engine.ErrEngineClosed {
				fmt.Fprintf(os.Stderr, "Engine closed during preparation\n")
				return "Scan Benchmark Failed: Engine closed"
			}
			fmt.Fprintf(os.Stderr, "Write error during preparation: %v\n", err)
			return "Scan Benchmark Failed: Error preparing data"
		}

		// Add small pause every 1000 keys
		if i > 0 && i%1000 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	fmt.Println("Running Scan Benchmark...")
	start := time.Now()
	deadline := start.Add(*duration)

	var opsCount, entriesScanned int
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	const scanSize = 100 // Scan 100 entries at a time

	for time.Now().Before(deadline) {
		// Pick a random starting point for the scan
		maxStart := actualNumKeys - scanSize
		if maxStart <= 0 {
			maxStart = 1
		}
		startIdx := r.Intn(maxStart)
		startKey := []byte(fmt.Sprintf("key-%06d", startIdx))
		endKey := []byte(fmt.Sprintf("key-%06d", startIdx+scanSize))

		iter, err := e.GetRangeIterator(startKey, endKey)
		if err != nil {
			if err == engine.ErrEngineClosed {
				fmt.Fprintf(os.Stderr, "Engine closed, stopping benchmark\n")
				goto benchmarkEnd
			}
			fmt.Fprintf(os.Stderr, "Failed to create iterator: %v\n", err)
			continue
		}

		// Perform the scan
		var scanned int
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			// Access the key and value to simulate real usage
			_ = iter.Key()
			_ = iter.Value()
			scanned++
		}

		entriesScanned += scanned
		opsCount++

		// Small pause between scans
		time.Sleep(5 * time.Millisecond)
	}

benchmarkEnd:
	elapsed := time.Since(start)
	scansPerSecond := float64(opsCount) / elapsed.Seconds()
	entriesPerSecond := float64(entriesScanned) / elapsed.Seconds()

	result := fmt.Sprintf("\nScan Benchmark Results:")
	result += fmt.Sprintf("\n  Scan Operations: %d", opsCount)
	result += fmt.Sprintf("\n  Entries Scanned: %d", entriesScanned)
	result += fmt.Sprintf("\n  Time: %.2f seconds", elapsed.Seconds())
	result += fmt.Sprintf("\n  Throughput: %.2f scans/sec", scansPerSecond)
	result += fmt.Sprintf("\n  Entry Throughput: %.2f entries/sec", entriesPerSecond)
	result += fmt.Sprintf("\n  Latency: %.3f ms/scan", 1000.0/scansPerSecond)

	return result
}

// runMixedBenchmark benchmarks a mix of read and write operations
func runMixedBenchmark(e *engine.Engine) string {
	fmt.Println("Preparing data for Mixed Benchmark...")

	// First, write some initial data
	actualNumKeys := *numKeys / 2 // Start with half the keys
	if actualNumKeys > 50000 {
		// Limit number of keys for preparation
		actualNumKeys = 50000
		fmt.Println("Limiting to 50,000 initial keys for mixed benchmark")
	}

	keys := make([][]byte, actualNumKeys)
	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	for i := 0; i < len(keys); i++ {
		keys[i] = generateKey(i)
		if err := e.Put(keys[i], value); err != nil {
			if err == engine.ErrEngineClosed {
				fmt.Fprintf(os.Stderr, "Engine closed during preparation\n")
				return "Mixed Benchmark Failed: Engine closed"
			}
			fmt.Fprintf(os.Stderr, "Write error during preparation: %v\n", err)
			return "Mixed Benchmark Failed: Error preparing data"
		}

		// Add small pause every 1000 keys
		if i > 0 && i%1000 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	fmt.Println("Running Mixed Benchmark (75% reads, 25% writes)...")
	start := time.Now()
	deadline := start.Add(*duration)

	var readOps, writeOps int
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	keyCounter := len(keys)

	for time.Now().Before(deadline) {
		// Process smaller batches
		batchSize := 100
		for i := 0; i < batchSize; i++ {
			// Decide operation: 75% reads, 25% writes
			if r.Float64() < 0.75 {
				// Read operation - random existing key
				idx := r.Intn(len(keys))
				key := keys[idx]

				_, err := e.Get(key)
				if err == engine.ErrEngineClosed {
					fmt.Fprintf(os.Stderr, "Engine closed, stopping benchmark\n")
					goto benchmarkEnd
				}
				readOps++
			} else {
				// Write operation - new key
				key := generateKey(keyCounter)
				keyCounter++

				if err := e.Put(key, value); err != nil {
					if err == engine.ErrEngineClosed {
						fmt.Fprintf(os.Stderr, "Engine closed, stopping benchmark\n")
						goto benchmarkEnd
					}
					fmt.Fprintf(os.Stderr, "Write error: %v\n", err)
					continue
				}
				writeOps++
			}
		}

		// Small pause to prevent overwhelming the engine
		time.Sleep(1 * time.Millisecond)
	}

benchmarkEnd:
	elapsed := time.Since(start)
	totalOps := readOps + writeOps
	opsPerSecond := float64(totalOps) / elapsed.Seconds()
	readRatio := float64(readOps) / float64(totalOps) * 100
	writeRatio := float64(writeOps) / float64(totalOps) * 100

	result := fmt.Sprintf("\nMixed Benchmark Results:")
	result += fmt.Sprintf("\n  Total Operations: %d", totalOps)
	result += fmt.Sprintf("\n  Read Operations: %d (%.1f%%)", readOps, readRatio)
	result += fmt.Sprintf("\n  Write Operations: %d (%.1f%%)", writeOps, writeRatio)
	result += fmt.Sprintf("\n  Time: %.2f seconds", elapsed.Seconds())
	result += fmt.Sprintf("\n  Throughput: %.2f ops/sec", opsPerSecond)
	result += fmt.Sprintf("\n  Latency: %.3f µs/op", 1000000.0/opsPerSecond)

	return result
}

// generateKey generates a key based on the counter and mode
func generateKey(counter int) []byte {
	if *sequential {
		return []byte(fmt.Sprintf("key-%010d", counter))
	}
	// Random key with counter to ensure uniqueness
	return []byte(fmt.Sprintf("key-%s-%010d",
		strconv.FormatUint(rand.Uint64(), 16), counter))
}
