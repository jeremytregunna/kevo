package stats

import (
	"sync"
	"testing"
	"time"
)

func TestCollector_TrackOperation(t *testing.T) {
	collector := NewAtomicCollector()

	// Track operations
	collector.TrackOperation(OpPut)
	collector.TrackOperation(OpPut)
	collector.TrackOperation(OpGet)

	// Get stats
	stats := collector.GetStats()

	// Verify counts
	if stats["put_ops"].(uint64) != 2 {
		t.Errorf("Expected 2 put operations, got %v", stats["put_ops"])
	}

	if stats["get_ops"].(uint64) != 1 {
		t.Errorf("Expected 1 get operation, got %v", stats["get_ops"])
	}

	// Verify last operation times exist
	if _, exists := stats["last_put_time"]; !exists {
		t.Errorf("Expected last_put_time to exist in stats")
	}

	if _, exists := stats["last_get_time"]; !exists {
		t.Errorf("Expected last_get_time to exist in stats")
	}
}

func TestCollector_TrackOperationWithLatency(t *testing.T) {
	collector := NewAtomicCollector()

	// Track operations with latency
	collector.TrackOperationWithLatency(OpGet, 100)
	collector.TrackOperationWithLatency(OpGet, 200)
	collector.TrackOperationWithLatency(OpGet, 300)

	// Get stats
	stats := collector.GetStats()

	// Check latency stats
	latencyStats, ok := stats["get_latency"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected get_latency to be a map, got %T", stats["get_latency"])
	}

	if count := latencyStats["count"].(uint64); count != 3 {
		t.Errorf("Expected 3 latency records, got %v", count)
	}

	if avg := latencyStats["avg_ns"].(uint64); avg != 200 {
		t.Errorf("Expected average latency 200ns, got %v", avg)
	}

	if min := latencyStats["min_ns"].(uint64); min != 100 {
		t.Errorf("Expected min latency 100ns, got %v", min)
	}

	if max := latencyStats["max_ns"].(uint64); max != 300 {
		t.Errorf("Expected max latency 300ns, got %v", max)
	}
}

func TestCollector_ConcurrentAccess(t *testing.T) {
	collector := NewAtomicCollector()
	const numGoroutines = 10
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch goroutines to track operations concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				// Mix different operations
				switch j % 3 {
				case 0:
					collector.TrackOperation(OpPut)
				case 1:
					collector.TrackOperation(OpGet)
				case 2:
					collector.TrackOperationWithLatency(OpDelete, uint64(j))
				}
			}
		}(i)
	}

	wg.Wait()

	// Get stats
	stats := collector.GetStats()

	// There should be approximately opsPerGoroutine * numGoroutines / 3 operations of each type
	expectedOps := uint64(numGoroutines * opsPerGoroutine / 3)

	// Allow for small variations due to concurrent execution
	// Use 99% of expected as minimum threshold
	minThreshold := expectedOps * 99 / 100

	if ops := stats["put_ops"].(uint64); ops < minThreshold {
		t.Errorf("Expected approximately %d put operations, got %v (below threshold %d)",
			expectedOps, ops, minThreshold)
	}

	if ops := stats["get_ops"].(uint64); ops < minThreshold {
		t.Errorf("Expected approximately %d get operations, got %v (below threshold %d)",
			expectedOps, ops, minThreshold)
	}

	if ops := stats["delete_ops"].(uint64); ops < minThreshold {
		t.Errorf("Expected approximately %d delete operations, got %v (below threshold %d)",
			expectedOps, ops, minThreshold)
	}
}

func TestCollector_GetStatsFiltered(t *testing.T) {
	collector := NewAtomicCollector()

	// Track different operations
	collector.TrackOperation(OpPut)
	collector.TrackOperation(OpGet)
	collector.TrackOperation(OpGet)
	collector.TrackOperation(OpDelete)
	collector.TrackError("io_error")
	collector.TrackError("network_error")

	// Filter by "get" prefix
	getStats := collector.GetStatsFiltered("get")

	// Should only contain get_ops and related stats
	if len(getStats) == 0 {
		t.Errorf("Expected non-empty filtered stats")
	}

	if _, exists := getStats["get_ops"]; !exists {
		t.Errorf("Expected get_ops in filtered stats")
	}

	if _, exists := getStats["put_ops"]; exists {
		t.Errorf("Did not expect put_ops in get-filtered stats")
	}

	// Filter by "error" prefix
	errorStats := collector.GetStatsFiltered("error")

	if _, exists := errorStats["errors"]; !exists {
		t.Errorf("Expected errors in error-filtered stats")
	}
}

func TestCollector_TrackBytes(t *testing.T) {
	collector := NewAtomicCollector()

	// Track read and write bytes
	collector.TrackBytes(true, 1000) // write
	collector.TrackBytes(false, 500) // read

	stats := collector.GetStats()

	if bytesWritten := stats["total_bytes_written"].(uint64); bytesWritten != 1000 {
		t.Errorf("Expected 1000 bytes written, got %v", bytesWritten)
	}

	if bytesRead := stats["total_bytes_read"].(uint64); bytesRead != 500 {
		t.Errorf("Expected 500 bytes read, got %v", bytesRead)
	}
}

func TestCollector_TrackMemTableSize(t *testing.T) {
	collector := NewAtomicCollector()

	// Track memtable size
	collector.TrackMemTableSize(2048)

	stats := collector.GetStats()

	if size := stats["memtable_size"].(uint64); size != 2048 {
		t.Errorf("Expected memtable size 2048, got %v", size)
	}

	// Update memtable size
	collector.TrackMemTableSize(4096)

	stats = collector.GetStats()

	if size := stats["memtable_size"].(uint64); size != 4096 {
		t.Errorf("Expected updated memtable size 4096, got %v", size)
	}
}

func TestCollector_RecoveryStats(t *testing.T) {
	collector := NewAtomicCollector()

	// Start recovery
	startTime := collector.StartRecovery()

	// Simulate some work
	time.Sleep(10 * time.Millisecond)

	// Finish recovery
	collector.FinishRecovery(startTime, 5, 1000, 2)

	stats := collector.GetStats()
	recoveryStats, ok := stats["recovery"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected recovery stats to be a map")
	}

	if filesRecovered := recoveryStats["wal_files_recovered"].(uint64); filesRecovered != 5 {
		t.Errorf("Expected 5 files recovered, got %v", filesRecovered)
	}

	if entriesRecovered := recoveryStats["wal_entries_recovered"].(uint64); entriesRecovered != 1000 {
		t.Errorf("Expected 1000 entries recovered, got %v", entriesRecovered)
	}

	if corruptedEntries := recoveryStats["wal_corrupted_entries"].(uint64); corruptedEntries != 2 {
		t.Errorf("Expected 2 corrupted entries, got %v", corruptedEntries)
	}

	if _, exists := recoveryStats["wal_recovery_duration_ms"]; !exists {
		t.Errorf("Expected recovery duration to be recorded")
	}
}
