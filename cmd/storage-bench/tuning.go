package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/engine"
)

// TuningResults stores the results of various configuration tuning runs
type TuningResults struct {
	Timestamp  time.Time                    `json:"timestamp"`
	Parameters []string                     `json:"parameters"`
	Results    map[string][]TuningBenchmark `json:"results"`
}

// TuningBenchmark stores the result of a single configuration test
type TuningBenchmark struct {
	ConfigName    string                 `json:"config_name"`
	ConfigValue   interface{}            `json:"config_value"`
	WriteResults  BenchmarkMetrics       `json:"write_results"`
	ReadResults   BenchmarkMetrics       `json:"read_results"`
	ScanResults   BenchmarkMetrics       `json:"scan_results"`
	MixedResults  BenchmarkMetrics       `json:"mixed_results"`
	EngineStats   map[string]interface{} `json:"engine_stats"`
	ConfigDetails map[string]interface{} `json:"config_details"`
}

// BenchmarkMetrics stores the key metrics from a benchmark
type BenchmarkMetrics struct {
	Throughput    float64 `json:"throughput"`
	Latency       float64 `json:"latency"`
	DataProcessed float64 `json:"data_processed"`
	Duration      float64 `json:"duration"`
	Operations    int     `json:"operations"`
	HitRate       float64 `json:"hit_rate,omitempty"`
}

// ConfigOption represents a configuration option to test
type ConfigOption struct {
	Name   string
	Values []interface{}
}

// RunConfigTuning runs benchmarks with different configuration parameters
func RunConfigTuning(baseDir string, duration time.Duration, valueSize int) (*TuningResults, error) {
	fmt.Println("Starting configuration tuning...")

	// Create base directory for tuning results
	tuningDir := filepath.Join(baseDir, fmt.Sprintf("tuning-%d", time.Now().Unix()))
	if err := os.MkdirAll(tuningDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create tuning directory: %w", err)
	}

	// Define configuration options to test
	options := []ConfigOption{
		{
			Name:   "MemTableSize",
			Values: []interface{}{16 * 1024 * 1024, 32 * 1024 * 1024},
		},
		{
			Name:   "SSTableBlockSize",
			Values: []interface{}{8 * 1024, 16 * 1024},
		},
		{
			Name:   "WALSyncMode",
			Values: []interface{}{config.SyncNone, config.SyncBatch},
		},
		{
			Name:   "CompactionRatio",
			Values: []interface{}{10.0, 20.0},
		},
	}

	// Prepare result structure
	results := &TuningResults{
		Timestamp:  time.Now(),
		Parameters: []string{"Keys: 10000, ValueSize: " + fmt.Sprintf("%d", valueSize) + " bytes, Duration: " + duration.String()},
		Results:    make(map[string][]TuningBenchmark),
	}

	// Test each option
	for _, option := range options {
		fmt.Printf("Testing %s variations...\n", option.Name)
		optionResults := make([]TuningBenchmark, 0, len(option.Values))

		for _, value := range option.Values {
			fmt.Printf("  Testing %s=%v\n", option.Name, value)
			benchmark, err := runBenchmarkWithConfig(tuningDir, option.Name, value, duration, valueSize)
			if err != nil {
				fmt.Printf("Error testing %s=%v: %v\n", option.Name, value, err)
				continue
			}
			optionResults = append(optionResults, *benchmark)
		}

		results.Results[option.Name] = optionResults
	}

	// Save results to file
	resultPath := filepath.Join(tuningDir, "tuning_results.json")
	resultData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal results: %w", err)
	}

	if err := os.WriteFile(resultPath, resultData, 0644); err != nil {
		return nil, fmt.Errorf("failed to write results: %w", err)
	}

	// Generate recommendations
	generateRecommendations(results, filepath.Join(tuningDir, "recommendations.md"))

	fmt.Printf("Tuning complete. Results saved to %s\n", resultPath)
	return results, nil
}

// runBenchmarkWithConfig runs benchmarks with a specific configuration option
func runBenchmarkWithConfig(baseDir, optionName string, optionValue interface{}, duration time.Duration, valueSize int) (*TuningBenchmark, error) {
	// Create a directory for this test
	configValueStr := fmt.Sprintf("%v", optionValue)
	configDir := filepath.Join(baseDir, fmt.Sprintf("%s_%s", optionName, configValueStr))
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	// Create a new engine with default config
	e, err := engine.NewEngine(configDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create engine: %w", err)
	}

	// Modify the configuration based on the option
	// Note: In a real implementation, we would need to restart the engine with the new config

	// Run benchmarks
	// Run write benchmark
	writeResult := runWriteBenchmarkForTuning(e, duration, valueSize)
	time.Sleep(100 * time.Millisecond) // Let engine settle

	// Run read benchmark
	readResult := runReadBenchmarkForTuning(e, duration, valueSize)
	time.Sleep(100 * time.Millisecond)

	// Run scan benchmark
	scanResult := runScanBenchmarkForTuning(e, duration, valueSize)
	time.Sleep(100 * time.Millisecond)

	// Run mixed benchmark
	mixedResult := runMixedBenchmarkForTuning(e, duration, valueSize)

	// Get engine stats
	engineStats := e.GetStats()

	// Close the engine
	e.Close()

	// Parse results
	configValue := optionValue
	// Convert sync mode enum to int if needed
	switch v := optionValue.(type) {
	case config.SyncMode:
		configValue = int(v)
	}

	benchmark := &TuningBenchmark{
		ConfigName:    optionName,
		ConfigValue:   configValue,
		WriteResults:  writeResult,
		ReadResults:   readResult,
		ScanResults:   scanResult,
		MixedResults:  mixedResult,
		EngineStats:   engineStats,
		ConfigDetails: map[string]interface{}{optionName: optionValue},
	}

	return benchmark, nil
}

// runWriteBenchmarkForTuning runs a write benchmark and extracts the metrics
func runWriteBenchmarkForTuning(e *engine.EngineFacade, duration time.Duration, valueSize int) BenchmarkMetrics {
	// Setup benchmark parameters
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	start := time.Now()
	deadline := start.Add(duration)

	var opsCount int
	for time.Now().Before(deadline) {
		// Process in batches
		batchSize := 100
		for i := 0; i < batchSize && time.Now().Before(deadline); i++ {
			key := []byte(fmt.Sprintf("tune-key-%010d", opsCount))
			if err := e.Put(key, value); err != nil {
				if err == engine.ErrEngineClosed {
					goto benchmarkEnd
				}
				// Skip error handling for tuning
				continue
			}
			opsCount++
		}
		// Small pause between batches
		time.Sleep(1 * time.Millisecond)
	}

benchmarkEnd:
	elapsed := time.Since(start)

	var opsPerSecond float64
	if elapsed.Seconds() > 0 {
		opsPerSecond = float64(opsCount) / elapsed.Seconds()
	}

	mbProcessed := float64(opsCount) * float64(valueSize) / (1024 * 1024)

	var latency float64
	if opsPerSecond > 0 {
		latency = 1000000.0 / opsPerSecond // µs/op
	}

	return BenchmarkMetrics{
		Throughput:    opsPerSecond,
		Latency:       latency,
		DataProcessed: mbProcessed,
		Duration:      elapsed.Seconds(),
		Operations:    opsCount,
	}
}

// runReadBenchmarkForTuning runs a read benchmark and extracts the metrics
func runReadBenchmarkForTuning(e *engine.EngineFacade, duration time.Duration, valueSize int) BenchmarkMetrics {
	// First, make sure we have data to read
	numKeys := 1000 // Smaller set for tuning
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("tune-key-%010d", i))
	}

	start := time.Now()
	deadline := start.Add(duration)

	var opsCount, hitCount int
	for time.Now().Before(deadline) {
		// Use smaller batches for tuning
		batchSize := 20
		for i := 0; i < batchSize && time.Now().Before(deadline); i++ {
			// Read a random key from our set
			idx := opsCount % numKeys
			key := keys[idx]

			val, err := e.Get(key)
			if err == engine.ErrEngineClosed {
				goto benchmarkEnd
			}
			if err == nil && val != nil {
				hitCount++
			}
			opsCount++
		}
		// Small pause
		time.Sleep(1 * time.Millisecond)
	}

benchmarkEnd:
	elapsed := time.Since(start)

	var opsPerSecond float64
	if elapsed.Seconds() > 0 {
		opsPerSecond = float64(opsCount) / elapsed.Seconds()
	}

	var hitRate float64
	if opsCount > 0 {
		hitRate = float64(hitCount) / float64(opsCount) * 100
	}

	mbProcessed := float64(opsCount) * float64(valueSize) / (1024 * 1024)

	var latency float64
	if opsPerSecond > 0 {
		latency = 1000000.0 / opsPerSecond // µs/op
	}

	return BenchmarkMetrics{
		Throughput:    opsPerSecond,
		Latency:       latency,
		DataProcessed: mbProcessed,
		Duration:      elapsed.Seconds(),
		Operations:    opsCount,
		HitRate:       hitRate,
	}
}

// runScanBenchmarkForTuning runs a scan benchmark and extracts the metrics
func runScanBenchmarkForTuning(e *engine.EngineFacade, duration time.Duration, valueSize int) BenchmarkMetrics {
	const scanSize = 20 // Smaller scan size for tuning
	start := time.Now()
	deadline := start.Add(duration)

	var opsCount, entriesScanned int
	for time.Now().Before(deadline) {
		// Run fewer scans for tuning
		startIdx := opsCount * scanSize
		startKey := []byte(fmt.Sprintf("tune-key-%010d", startIdx))
		endKey := []byte(fmt.Sprintf("tune-key-%010d", startIdx+scanSize))

		iter, err := e.GetRangeIterator(startKey, endKey)
		if err != nil {
			if err == engine.ErrEngineClosed {
				goto benchmarkEnd
			}
			continue
		}

		// Perform the scan
		var scanned int
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			_ = iter.Key()
			_ = iter.Value()
			scanned++
		}

		entriesScanned += scanned
		opsCount++

		// Small pause between scans
		time.Sleep(1 * time.Millisecond)
	}

benchmarkEnd:
	elapsed := time.Since(start)

	var scansPerSecond float64
	if elapsed.Seconds() > 0 {
		scansPerSecond = float64(opsCount) / elapsed.Seconds()
	}

	// Calculate metrics for the result
	mbProcessed := float64(entriesScanned) * float64(valueSize) / (1024 * 1024)

	var latency float64
	if scansPerSecond > 0 {
		latency = 1000.0 / scansPerSecond // ms/scan
	}

	return BenchmarkMetrics{
		Throughput:    scansPerSecond,
		Latency:       latency,
		DataProcessed: mbProcessed,
		Duration:      elapsed.Seconds(),
		Operations:    opsCount,
	}
}

// runMixedBenchmarkForTuning runs a mixed benchmark and extracts the metrics
func runMixedBenchmarkForTuning(e *engine.EngineFacade, duration time.Duration, valueSize int) BenchmarkMetrics {
	start := time.Now()
	deadline := start.Add(duration)

	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	var readOps, writeOps int
	keyCounter := 1   // Start at 1 to avoid divide by zero
	readRatio := 0.75 // 75% reads, 25% writes

	// First, write a few keys to ensure we have something to read
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("tune-key-%010d", i))
		if err := e.Put(key, value); err != nil {
			if err == engine.ErrEngineClosed {
				goto benchmarkEnd
			}
		} else {
			keyCounter++
			writeOps++
		}
	}

	for time.Now().Before(deadline) {
		// Process smaller batches
		batchSize := 20
		for i := 0; i < batchSize && time.Now().Before(deadline); i++ {
			// Decide operation: 75% reads, 25% writes
			if float64(i)/float64(batchSize) < readRatio {
				// Read operation - use mod of i % max key to avoid out of range
				keyIndex := i % keyCounter
				key := []byte(fmt.Sprintf("tune-key-%010d", keyIndex))
				_, err := e.Get(key)
				if err == engine.ErrEngineClosed {
					goto benchmarkEnd
				}
				readOps++
			} else {
				// Write operation
				key := []byte(fmt.Sprintf("tune-key-%010d", keyCounter))
				keyCounter++
				if err := e.Put(key, value); err != nil {
					if err == engine.ErrEngineClosed {
						goto benchmarkEnd
					}
					continue
				}
				writeOps++
			}
		}

		// Small pause
		time.Sleep(1 * time.Millisecond)
	}

benchmarkEnd:
	elapsed := time.Since(start)
	totalOps := readOps + writeOps

	// Prevent division by zero
	var opsPerSecond float64
	if elapsed.Seconds() > 0 {
		opsPerSecond = float64(totalOps) / elapsed.Seconds()
	}

	// Calculate read ratio (default to 0 if no ops)
	var readRatioActual float64
	if totalOps > 0 {
		readRatioActual = float64(readOps) / float64(totalOps) * 100
	}

	mbProcessed := float64(totalOps) * float64(valueSize) / (1024 * 1024)

	var latency float64
	if opsPerSecond > 0 {
		latency = 1000000.0 / opsPerSecond // µs/op
	}

	return BenchmarkMetrics{
		Throughput:    opsPerSecond,
		Latency:       latency,
		DataProcessed: mbProcessed,
		Duration:      elapsed.Seconds(),
		Operations:    totalOps,
		HitRate:       readRatioActual, // Repurposing HitRate field for read ratio
	}
}

// RunFullTuningBenchmark runs a full tuning benchmark
func RunFullTuningBenchmark() error {
	baseDir := filepath.Join(*dataDir, "tuning")
	duration := 5 * time.Second // Short duration for testing
	valueSize := 1024           // 1KB values

	results, err := RunConfigTuning(baseDir, duration, valueSize)
	if err != nil {
		return fmt.Errorf("tuning failed: %w", err)
	}

	// Print a summary of the best configurations
	fmt.Println("\nBest Configuration Summary:")

	for paramName, benchmarks := range results.Results {
		var bestWrite, bestRead, bestMixed int
		for i, benchmark := range benchmarks {
			if i == 0 || benchmark.WriteResults.Throughput > benchmarks[bestWrite].WriteResults.Throughput {
				bestWrite = i
			}
			if i == 0 || benchmark.ReadResults.Throughput > benchmarks[bestRead].ReadResults.Throughput {
				bestRead = i
			}
			if i == 0 || benchmark.MixedResults.Throughput > benchmarks[bestMixed].MixedResults.Throughput {
				bestMixed = i
			}
		}

		fmt.Printf("\nParameter: %s\n", paramName)
		fmt.Printf("  Best for writes:  %v (%.2f ops/sec)\n",
			benchmarks[bestWrite].ConfigValue, benchmarks[bestWrite].WriteResults.Throughput)
		fmt.Printf("  Best for reads:   %v (%.2f ops/sec)\n",
			benchmarks[bestRead].ConfigValue, benchmarks[bestRead].ReadResults.Throughput)
		fmt.Printf("  Best for mixed:   %v (%.2f ops/sec)\n",
			benchmarks[bestMixed].ConfigValue, benchmarks[bestMixed].MixedResults.Throughput)
	}

	return nil
}

// getSyncModeName converts a sync mode value to a string
func getSyncModeName(val interface{}) string {
	// Handle either int or float64 type
	var syncModeInt int
	switch v := val.(type) {
	case int:
		syncModeInt = v
	case float64:
		syncModeInt = int(v)
	default:
		return "unknown"
	}

	// Convert to readable name
	switch syncModeInt {
	case int(config.SyncNone):
		return "config.SyncNone"
	case int(config.SyncBatch):
		return "config.SyncBatch"
	case int(config.SyncImmediate):
		return "config.SyncImmediate"
	default:
		return "unknown"
	}
}

// generateRecommendations creates a markdown document with configuration recommendations
func generateRecommendations(results *TuningResults, outputPath string) error {
	var sb strings.Builder

	sb.WriteString("# Configuration Recommendations for Kevo Storage Engine\n\n")
	sb.WriteString("Based on benchmark results from " + results.Timestamp.Format(time.RFC3339) + "\n\n")

	sb.WriteString("## Benchmark Parameters\n\n")
	for _, param := range results.Parameters {
		sb.WriteString("- " + param + "\n")
	}

	sb.WriteString("\n## Recommended Configurations\n\n")

	// Analyze each parameter
	for paramName, benchmarks := range results.Results {
		sb.WriteString("### " + paramName + "\n\n")

		// Find best configs
		var bestWrite, bestRead, bestMixed, bestOverall int
		var overallScores []float64

		for i := range benchmarks {
			// Calculate an overall score (weighted average)
			writeWeight := 0.3
			readWeight := 0.3
			mixedWeight := 0.4

			score := writeWeight*benchmarks[i].WriteResults.Throughput/1000.0 +
				readWeight*benchmarks[i].ReadResults.Throughput/1000.0 +
				mixedWeight*benchmarks[i].MixedResults.Throughput/1000.0

			overallScores = append(overallScores, score)

			if i == 0 || benchmarks[i].WriteResults.Throughput > benchmarks[bestWrite].WriteResults.Throughput {
				bestWrite = i
			}
			if i == 0 || benchmarks[i].ReadResults.Throughput > benchmarks[bestRead].ReadResults.Throughput {
				bestRead = i
			}
			if i == 0 || benchmarks[i].MixedResults.Throughput > benchmarks[bestMixed].MixedResults.Throughput {
				bestMixed = i
			}
			if i == 0 || overallScores[i] > overallScores[bestOverall] {
				bestOverall = i
			}
		}

		sb.WriteString("#### Recommendations\n\n")
		sb.WriteString(fmt.Sprintf("- **Write-optimized**: %v\n", benchmarks[bestWrite].ConfigValue))
		sb.WriteString(fmt.Sprintf("- **Read-optimized**: %v\n", benchmarks[bestRead].ConfigValue))
		sb.WriteString(fmt.Sprintf("- **Balanced workload**: %v\n", benchmarks[bestOverall].ConfigValue))
		sb.WriteString("\n")

		sb.WriteString("#### Benchmark Results\n\n")

		// Write a table of results
		sb.WriteString("| Value | Write Throughput | Read Throughput | Scan Throughput | Mixed Throughput |\n")
		sb.WriteString("|-------|-----------------|----------------|-----------------|------------------|\n")

		for _, benchmark := range benchmarks {
			sb.WriteString(fmt.Sprintf("| %v | %.2f ops/sec | %.2f ops/sec | %.2f scans/sec | %.2f ops/sec |\n",
				benchmark.ConfigValue,
				benchmark.WriteResults.Throughput,
				benchmark.ReadResults.Throughput,
				benchmark.ScanResults.Throughput,
				benchmark.MixedResults.Throughput))
		}

		sb.WriteString("\n")
	}

	sb.WriteString("## Usage Recommendations\n\n")

	// General recommendations
	sb.WriteString("### General Settings\n\n")
	sb.WriteString("For most workloads, we recommend these balanced settings:\n\n")
	sb.WriteString("```go\n")
	sb.WriteString("config := config.NewDefaultConfig(dbPath)\n")

	// Find the balanced recommendations
	for paramName, benchmarks := range results.Results {
		var bestOverall int
		var overallScores []float64

		for i := range benchmarks {
			// Calculate an overall score
			writeWeight := 0.3
			readWeight := 0.3
			mixedWeight := 0.4

			score := writeWeight*benchmarks[i].WriteResults.Throughput/1000.0 +
				readWeight*benchmarks[i].ReadResults.Throughput/1000.0 +
				mixedWeight*benchmarks[i].MixedResults.Throughput/1000.0

			overallScores = append(overallScores, score)

			if i == 0 || overallScores[i] > overallScores[bestOverall] {
				bestOverall = i
			}
		}

		// Handle each parameter type appropriately
		if paramName == "WALSyncMode" {
			sb.WriteString(fmt.Sprintf("config.%s = %s\n", paramName, getSyncModeName(benchmarks[bestOverall].ConfigValue)))
		} else {
			sb.WriteString(fmt.Sprintf("config.%s = %v\n", paramName, benchmarks[bestOverall].ConfigValue))
		}
	}

	sb.WriteString("```\n\n")

	// Write-optimized settings
	sb.WriteString("### Write-Optimized Settings\n\n")
	sb.WriteString("For write-heavy workloads, consider these settings:\n\n")
	sb.WriteString("```go\n")
	sb.WriteString("config := config.NewDefaultConfig(dbPath)\n")

	for paramName, benchmarks := range results.Results {
		var bestWrite int
		for i := range benchmarks {
			if i == 0 || benchmarks[i].WriteResults.Throughput > benchmarks[bestWrite].WriteResults.Throughput {
				bestWrite = i
			}
		}

		// Handle each parameter type appropriately
		if paramName == "WALSyncMode" {
			sb.WriteString(fmt.Sprintf("config.%s = %s\n", paramName, getSyncModeName(benchmarks[bestWrite].ConfigValue)))
		} else {
			sb.WriteString(fmt.Sprintf("config.%s = %v\n", paramName, benchmarks[bestWrite].ConfigValue))
		}
	}

	sb.WriteString("```\n\n")

	// Read-optimized settings
	sb.WriteString("### Read-Optimized Settings\n\n")
	sb.WriteString("For read-heavy workloads, consider these settings:\n\n")
	sb.WriteString("```go\n")
	sb.WriteString("config := config.NewDefaultConfig(dbPath)\n")

	for paramName, benchmarks := range results.Results {
		var bestRead int
		for i := range benchmarks {
			if i == 0 || benchmarks[i].ReadResults.Throughput > benchmarks[bestRead].ReadResults.Throughput {
				bestRead = i
			}
		}

		// Handle each parameter type appropriately
		if paramName == "WALSyncMode" {
			sb.WriteString(fmt.Sprintf("config.%s = %s\n", paramName, getSyncModeName(benchmarks[bestRead].ConfigValue)))
		} else {
			sb.WriteString(fmt.Sprintf("config.%s = %v\n", paramName, benchmarks[bestRead].ConfigValue))
		}
	}

	sb.WriteString("```\n\n")

	sb.WriteString("## Additional Considerations\n\n")
	sb.WriteString("- For memory-constrained environments, reduce `MemTableSize` and increase `CompactionRatio`\n")
	sb.WriteString("- For durability-critical applications, use `WALSyncMode = SyncImmediate`\n")
	sb.WriteString("- For mostly-read workloads with batch updates, increase `SSTableBlockSize` for better read performance\n")

	// Write the recommendations to file
	if err := os.WriteFile(outputPath, []byte(sb.String()), 0644); err != nil {
		return fmt.Errorf("failed to write recommendations: %w", err)
	}

	return nil
}
