package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	pb "github.com/jeremytregunna/kevo/proto/kevo"
)

// BenchmarkOptions defines the options for gRPC benchmarking
type BenchmarkOptions struct {
	Address     string
	Connections int
	Iterations  int
	KeySize     int
	ValueSize   int
	Parallelism int
	UseTLS      bool
	TLSConfig   *TLSConfig
}

// BenchmarkResult holds the results of a benchmark run
type BenchmarkResult struct {
	Operation       string
	TotalTime       time.Duration
	RequestsPerSec  float64
	AvgLatency      time.Duration
	MinLatency      time.Duration
	MaxLatency      time.Duration
	P90Latency      time.Duration
	P99Latency      time.Duration
	TotalBytes      int64
	BytesPerSecond  float64
	ErrorRate       float64
	TotalOperations int
	FailedOps       int
}

// Benchmark runs a performance benchmark on the gRPC transport
func Benchmark(ctx context.Context, opts *BenchmarkOptions) (map[string]*BenchmarkResult, error) {
	if opts.Connections <= 0 {
		opts.Connections = 10
	}
	if opts.Iterations <= 0 {
		opts.Iterations = 10000
	}
	if opts.KeySize <= 0 {
		opts.KeySize = 16
	}
	if opts.ValueSize <= 0 {
		opts.ValueSize = 100
	}
	if opts.Parallelism <= 0 {
		opts.Parallelism = 8
	}

	// Create TLS config if requested
	var tlsConfig *tls.Config
	var err error
	if opts.UseTLS && opts.TLSConfig != nil {
		tlsConfig, err = LoadClientTLSConfig(opts.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS config: %w", err)
		}
	}

	// Create transport manager
	transportOpts := &GRPCTransportOptions{
		TLSConfig: tlsConfig,
	}
	manager, err := NewGRPCTransportManager(transportOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport manager: %w", err)
	}

	// Create connection pool
	poolManager := NewConnectionPoolManager(manager, opts.Connections/2, opts.Connections, 5*time.Minute)
	pool := poolManager.GetPool(opts.Address)
	defer poolManager.CloseAll()

	// Create client
	client := NewClient(pool, 3, 100*time.Millisecond)

	// Generate test data
	testKey := make([]byte, opts.KeySize)
	testValue := make([]byte, opts.ValueSize)
	for i := 0; i < opts.KeySize; i++ {
		testKey[i] = byte('a' + (i % 26))
	}
	for i := 0; i < opts.ValueSize; i++ {
		testValue[i] = byte('A' + (i % 26))
	}

	// Run benchmarks for different operations
	results := make(map[string]*BenchmarkResult)

	// Benchmark Put operation
	putResult, err := benchmarkPut(ctx, client, testKey, testValue, opts)
	if err != nil {
		return nil, fmt.Errorf("put benchmark failed: %w", err)
	}
	results["put"] = putResult

	// Benchmark Get operation
	getResult, err := benchmarkGet(ctx, client, testKey, opts)
	if err != nil {
		return nil, fmt.Errorf("get benchmark failed: %w", err)
	}
	results["get"] = getResult

	// Benchmark Delete operation
	deleteResult, err := benchmarkDelete(ctx, client, testKey, opts)
	if err != nil {
		return nil, fmt.Errorf("delete benchmark failed: %w", err)
	}
	results["delete"] = deleteResult

	// Benchmark BatchWrite operation
	batchResult, err := benchmarkBatch(ctx, client, testKey, testValue, opts)
	if err != nil {
		return nil, fmt.Errorf("batch benchmark failed: %w", err)
	}
	results["batch"] = batchResult

	return results, nil
}

// benchmarkPut benchmarks the Put operation
func benchmarkPut(ctx context.Context, client *Client, baseKey, value []byte, opts *BenchmarkOptions) (*BenchmarkResult, error) {
	result := &BenchmarkResult{
		Operation:       "Put",
		MinLatency:      time.Hour, // Start with a large value to find minimum
		TotalOperations: opts.Iterations,
	}

	var totalBytes int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, opts.Iterations)
	errorCount := 0

	// Use a semaphore to limit parallelism
	sem := make(chan struct{}, opts.Parallelism)

	startTime := time.Now()

	for i := 0; i < opts.Iterations; i++ {
		sem <- struct{}{} // Acquire semaphore
		wg.Add(1)

		go func(idx int) {
			defer func() {
				<-sem // Release semaphore
				wg.Done()
			}()

			// Create unique key for this iteration
			key := make([]byte, len(baseKey))
			copy(key, baseKey)
			// Append index to make key unique
			idxBytes := []byte(fmt.Sprintf("_%d", idx))
			for j := 0; j < len(idxBytes) && j < len(key); j++ {
				key[len(key)-j-1] = idxBytes[len(idxBytes)-j-1]
			}

			// Measure latency of this operation
			opStart := time.Now()

			_, err := client.Execute(ctx, func(ctx context.Context, c interface{}) (interface{}, error) {
				client := c.(pb.KevoServiceClient)
				return client.Put(ctx, &pb.PutRequest{
					Key:   key,
					Value: value,
					Sync:  false,
				})
			})

			opLatency := time.Since(opStart)

			// Update results with mutex protection
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errorCount++
				return
			}

			latencies = append(latencies, opLatency)
			totalBytes += int64(len(key) + len(value))

			if opLatency < result.MinLatency {
				result.MinLatency = opLatency
			}
			if opLatency > result.MaxLatency {
				result.MaxLatency = opLatency
			}
		}(i)
	}

	wg.Wait()
	totalTime := time.Since(startTime)

	// Calculate statistics
	result.TotalTime = totalTime
	result.RequestsPerSec = float64(opts.Iterations-errorCount) / totalTime.Seconds()
	result.TotalBytes = totalBytes
	result.BytesPerSecond = float64(totalBytes) / totalTime.Seconds()
	result.ErrorRate = float64(errorCount) / float64(opts.Iterations)
	result.FailedOps = errorCount

	// Sort latencies to calculate percentiles
	if len(latencies) > 0 {
		// Calculate average latency
		var totalLatency time.Duration
		for _, lat := range latencies {
			totalLatency += lat
		}
		result.AvgLatency = totalLatency / time.Duration(len(latencies))

		// Sort latencies for percentile calculation
		sortDurations(latencies)

		// Calculate P90 and P99 latencies
		p90Index := int(float64(len(latencies)) * 0.9)
		p99Index := int(float64(len(latencies)) * 0.99)
		
		if p90Index < len(latencies) {
			result.P90Latency = latencies[p90Index]
		}
		if p99Index < len(latencies) {
			result.P99Latency = latencies[p99Index]
		}
	}

	return result, nil
}

// benchmarkGet benchmarks the Get operation
func benchmarkGet(ctx context.Context, client *Client, baseKey []byte, opts *BenchmarkOptions) (*BenchmarkResult, error) {
	// Similar implementation to benchmarkPut, but for Get operation
	result := &BenchmarkResult{
		Operation:       "Get",
		MinLatency:      time.Hour,
		TotalOperations: opts.Iterations,
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, opts.Iterations)
	errorCount := 0
	
	sem := make(chan struct{}, opts.Parallelism)
	startTime := time.Now()

	for i := 0; i < opts.Iterations; i++ {
		sem <- struct{}{}
		wg.Add(1)

		go func(idx int) {
			defer func() {
				<-sem
				wg.Done()
			}()

			key := make([]byte, len(baseKey))
			copy(key, baseKey)
			idxBytes := []byte(fmt.Sprintf("_%d", idx))
			for j := 0; j < len(idxBytes) && j < len(key); j++ {
				key[len(key)-j-1] = idxBytes[len(idxBytes)-j-1]
			}

			opStart := time.Now()

			_, err := client.Execute(ctx, func(ctx context.Context, c interface{}) (interface{}, error) {
				client := c.(pb.KevoServiceClient)
				return client.Get(ctx, &pb.GetRequest{
					Key: key,
				})
			})

			opLatency := time.Since(opStart)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errorCount++
				return
			}

			latencies = append(latencies, opLatency)

			if opLatency < result.MinLatency {
				result.MinLatency = opLatency
			}
			if opLatency > result.MaxLatency {
				result.MaxLatency = opLatency
			}
		}(i)
	}

	wg.Wait()
	totalTime := time.Since(startTime)

	result.TotalTime = totalTime
	result.RequestsPerSec = float64(opts.Iterations-errorCount) / totalTime.Seconds()
	result.ErrorRate = float64(errorCount) / float64(opts.Iterations)
	result.FailedOps = errorCount

	if len(latencies) > 0 {
		var totalLatency time.Duration
		for _, lat := range latencies {
			totalLatency += lat
		}
		result.AvgLatency = totalLatency / time.Duration(len(latencies))

		sortDurations(latencies)

		p90Index := int(float64(len(latencies)) * 0.9)
		p99Index := int(float64(len(latencies)) * 0.99)
		
		if p90Index < len(latencies) {
			result.P90Latency = latencies[p90Index]
		}
		if p99Index < len(latencies) {
			result.P99Latency = latencies[p99Index]
		}
	}

	return result, nil
}

// benchmarkDelete benchmarks the Delete operation (implementation similar to above)
func benchmarkDelete(ctx context.Context, client *Client, baseKey []byte, opts *BenchmarkOptions) (*BenchmarkResult, error) {
	// Similar implementation to the Get benchmark
	result := &BenchmarkResult{
		Operation:       "Delete",
		MinLatency:      time.Hour,
		TotalOperations: opts.Iterations,
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, opts.Iterations)
	errorCount := 0
	
	sem := make(chan struct{}, opts.Parallelism)
	startTime := time.Now()

	for i := 0; i < opts.Iterations; i++ {
		sem <- struct{}{}
		wg.Add(1)

		go func(idx int) {
			defer func() {
				<-sem
				wg.Done()
			}()

			key := make([]byte, len(baseKey))
			copy(key, baseKey)
			idxBytes := []byte(fmt.Sprintf("_%d", idx))
			for j := 0; j < len(idxBytes) && j < len(key); j++ {
				key[len(key)-j-1] = idxBytes[len(idxBytes)-j-1]
			}

			opStart := time.Now()

			_, err := client.Execute(ctx, func(ctx context.Context, c interface{}) (interface{}, error) {
				client := c.(pb.KevoServiceClient)
				return client.Delete(ctx, &pb.DeleteRequest{
					Key:  key,
					Sync: false,
				})
			})

			opLatency := time.Since(opStart)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errorCount++
				return
			}

			latencies = append(latencies, opLatency)

			if opLatency < result.MinLatency {
				result.MinLatency = opLatency
			}
			if opLatency > result.MaxLatency {
				result.MaxLatency = opLatency
			}
		}(i)
	}

	wg.Wait()
	totalTime := time.Since(startTime)

	result.TotalTime = totalTime
	result.RequestsPerSec = float64(opts.Iterations-errorCount) / totalTime.Seconds()
	result.ErrorRate = float64(errorCount) / float64(opts.Iterations)
	result.FailedOps = errorCount

	if len(latencies) > 0 {
		var totalLatency time.Duration
		for _, lat := range latencies {
			totalLatency += lat
		}
		result.AvgLatency = totalLatency / time.Duration(len(latencies))

		sortDurations(latencies)

		p90Index := int(float64(len(latencies)) * 0.9)
		p99Index := int(float64(len(latencies)) * 0.99)
		
		if p90Index < len(latencies) {
			result.P90Latency = latencies[p90Index]
		}
		if p99Index < len(latencies) {
			result.P99Latency = latencies[p99Index]
		}
	}

	return result, nil
}

// benchmarkBatch benchmarks batch operations
func benchmarkBatch(ctx context.Context, client *Client, baseKey, value []byte, opts *BenchmarkOptions) (*BenchmarkResult, error) {
	// Similar to other benchmarks but creates batch operations
	batchSize := 10 // Number of operations per batch
	
	result := &BenchmarkResult{
		Operation:       "BatchWrite",
		MinLatency:      time.Hour,
		TotalOperations: opts.Iterations,
	}

	var totalBytes int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, opts.Iterations)
	errorCount := 0
	
	sem := make(chan struct{}, opts.Parallelism)
	startTime := time.Now()

	for i := 0; i < opts.Iterations; i++ {
		sem <- struct{}{}
		wg.Add(1)

		go func(idx int) {
			defer func() {
				<-sem
				wg.Done()
			}()

			// Create batch operations
			operations := make([]*pb.Operation, batchSize)
			batchBytes := int64(0)
			
			for j := 0; j < batchSize; j++ {
				key := make([]byte, len(baseKey))
				copy(key, baseKey)
				// Make each key unique within the batch
				idxBytes := []byte(fmt.Sprintf("_%d_%d", idx, j))
				for k := 0; k < len(idxBytes) && k < len(key); k++ {
					key[len(key)-k-1] = idxBytes[len(idxBytes)-k-1]
				}
				
				operations[j] = &pb.Operation{
					Type:  pb.Operation_PUT,
					Key:   key,
					Value: value,
				}
				
				batchBytes += int64(len(key) + len(value))
			}

			opStart := time.Now()

			_, err := client.Execute(ctx, func(ctx context.Context, c interface{}) (interface{}, error) {
				client := c.(pb.KevoServiceClient)
				return client.BatchWrite(ctx, &pb.BatchWriteRequest{
					Operations: operations,
					Sync:       false,
				})
			})

			opLatency := time.Since(opStart)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errorCount++
				return
			}

			latencies = append(latencies, opLatency)
			totalBytes += batchBytes

			if opLatency < result.MinLatency {
				result.MinLatency = opLatency
			}
			if opLatency > result.MaxLatency {
				result.MaxLatency = opLatency
			}
		}(i)
	}

	wg.Wait()
	totalTime := time.Since(startTime)

	result.TotalTime = totalTime
	result.RequestsPerSec = float64(opts.Iterations-errorCount) / totalTime.Seconds()
	result.TotalBytes = totalBytes
	result.BytesPerSecond = float64(totalBytes) / totalTime.Seconds()
	result.ErrorRate = float64(errorCount) / float64(opts.Iterations)
	result.FailedOps = errorCount

	if len(latencies) > 0 {
		var totalLatency time.Duration
		for _, lat := range latencies {
			totalLatency += lat
		}
		result.AvgLatency = totalLatency / time.Duration(len(latencies))

		sortDurations(latencies)

		p90Index := int(float64(len(latencies)) * 0.9)
		p99Index := int(float64(len(latencies)) * 0.99)
		
		if p90Index < len(latencies) {
			result.P90Latency = latencies[p90Index]
		}
		if p99Index < len(latencies) {
			result.P99Latency = latencies[p99Index]
		}
	}

	return result, nil
}

// sortDurations sorts a slice of durations in ascending order
func sortDurations(durations []time.Duration) {
	for i := 0; i < len(durations); i++ {
		for j := i + 1; j < len(durations); j++ {
			if durations[i] > durations[j] {
				durations[i], durations[j] = durations[j], durations[i]
			}
		}
	}
}