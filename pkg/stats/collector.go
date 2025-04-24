package stats

import (
	"sync"
	"sync/atomic"
	"time"
)

// OperationType defines the type of operation being tracked
type OperationType string

// Common operation types
const (
	OpPut        OperationType = "put"
	OpGet        OperationType = "get"
	OpDelete     OperationType = "delete"
	OpTxBegin    OperationType = "tx_begin"
	OpTxCommit   OperationType = "tx_commit"
	OpTxRollback OperationType = "tx_rollback"
	OpFlush      OperationType = "flush"
	OpCompact    OperationType = "compact"
	OpSeek       OperationType = "seek"
	OpScan       OperationType = "scan"
	OpScanRange  OperationType = "scan_range"
)

// AtomicCollector provides centralized statistics collection with minimal contention
// using atomic operations for thread safety
type AtomicCollector struct {
	// Operation counters using atomic values
	counts       map[OperationType]*atomic.Uint64
	countsMu     sync.RWMutex // Only used when creating new counter entries

	// Timing measurements for last operation timestamps
	lastOpTime   map[OperationType]time.Time
	lastOpTimeMu sync.RWMutex // Only used for timestamp updates

	// Usage metrics
	memTableSize      atomic.Uint64
	totalBytesRead    atomic.Uint64
	totalBytesWritten atomic.Uint64

	// Error tracking
	errors     map[string]*atomic.Uint64
	errorsMu   sync.RWMutex // Only used when creating new error entries

	// Performance metrics
	flushCount        atomic.Uint64
	compactionCount   atomic.Uint64

	// Recovery statistics
	recoveryStats RecoveryStats
	
	// Latency tracking 
	latencies    map[OperationType]*LatencyTracker
	latenciesMu  sync.RWMutex // Only used when creating new latency trackers
}

// RecoveryStats tracks statistics related to WAL recovery
type RecoveryStats struct {
	WALFilesRecovered    atomic.Uint64
	WALEntriesRecovered  atomic.Uint64
	WALCorruptedEntries  atomic.Uint64
	WALRecoveryDuration  atomic.Int64 // nanoseconds
}

// LatencyTracker maintains running statistics about operation latencies
type LatencyTracker struct {
	count   atomic.Uint64
	sum     atomic.Uint64 // sum in nanoseconds
	max     atomic.Uint64 // max in nanoseconds
	min     atomic.Uint64 // min in nanoseconds (initialized to max uint64)
}

// NewCollector creates a new statistics collector
func NewCollector() *AtomicCollector {
	return &AtomicCollector{
		counts:     make(map[OperationType]*atomic.Uint64),
		lastOpTime: make(map[OperationType]time.Time),
		errors:     make(map[string]*atomic.Uint64),
		latencies:  make(map[OperationType]*LatencyTracker),
	}
}

// NewAtomicCollector creates a new atomic statistics collector
// This is the recommended collector implementation for production use
func NewAtomicCollector() *AtomicCollector {
	return &AtomicCollector{
		counts:     make(map[OperationType]*atomic.Uint64),
		lastOpTime: make(map[OperationType]time.Time),
		errors:     make(map[string]*atomic.Uint64),
		latencies:  make(map[OperationType]*LatencyTracker),
	}
}

// TrackOperation increments the counter for the specified operation type
func (c *AtomicCollector) TrackOperation(op OperationType) {
	counter := c.getOrCreateCounter(op)
	counter.Add(1)

	// Update last operation time (less critical, can use mutex)
	c.lastOpTimeMu.Lock()
	c.lastOpTime[op] = time.Now()
	c.lastOpTimeMu.Unlock()
}

// TrackOperationWithLatency tracks an operation and its latency
func (c *AtomicCollector) TrackOperationWithLatency(op OperationType, latencyNs uint64) {
	// Track operation count
	counter := c.getOrCreateCounter(op)
	counter.Add(1)

	// Update last operation time
	c.lastOpTimeMu.Lock()
	c.lastOpTime[op] = time.Now()
	c.lastOpTimeMu.Unlock()

	// Update latency statistics
	tracker := c.getOrCreateLatencyTracker(op)
	tracker.count.Add(1)
	tracker.sum.Add(latencyNs)

	// Update max (using compare-and-swap pattern)
	for {
		current := tracker.max.Load()
		if latencyNs <= current {
			break // No need to update
		}
		if tracker.max.CompareAndSwap(current, latencyNs) {
			break // Successfully updated
		}
		// If we get here, someone else updated it between our load and CAS, so we retry
	}

	// Update min (using compare-and-swap pattern)
	for {
		current := tracker.min.Load()
		if current == 0 {
			// First value
			if tracker.min.CompareAndSwap(0, latencyNs) {
				break
			}
			continue // Race condition, try again
		}
		if latencyNs >= current {
			break // No need to update
		}
		if tracker.min.CompareAndSwap(current, latencyNs) {
			break // Successfully updated
		}
		// If we get here, someone else updated it between our load and CAS, so we retry
	}
}

// TrackError increments the counter for the specified error type
func (c *AtomicCollector) TrackError(errorType string) {
	c.errorsMu.RLock()
	counter, exists := c.errors[errorType]
	c.errorsMu.RUnlock()

	if !exists {
		c.errorsMu.Lock()
		if counter, exists = c.errors[errorType]; !exists {
			counter = &atomic.Uint64{}
			c.errors[errorType] = counter
		}
		c.errorsMu.Unlock()
	}

	counter.Add(1)
}

// TrackBytes adds the specified number of bytes to the read or write counter
func (c *AtomicCollector) TrackBytes(isWrite bool, bytes uint64) {
	if isWrite {
		c.totalBytesWritten.Add(bytes)
	} else {
		c.totalBytesRead.Add(bytes)
	}
}

// TrackMemTableSize records the current memtable size
func (c *AtomicCollector) TrackMemTableSize(size uint64) {
	c.memTableSize.Store(size)
}

// TrackFlush increments the flush counter
func (c *AtomicCollector) TrackFlush() {
	c.flushCount.Add(1)
}

// TrackCompaction increments the compaction counter
func (c *AtomicCollector) TrackCompaction() {
	c.compactionCount.Add(1)
}

// StartRecovery initializes recovery statistics
func (c *AtomicCollector) StartRecovery() time.Time {
	// Reset recovery stats
	c.recoveryStats.WALFilesRecovered.Store(0)
	c.recoveryStats.WALEntriesRecovered.Store(0)
	c.recoveryStats.WALCorruptedEntries.Store(0)
	c.recoveryStats.WALRecoveryDuration.Store(0)
	
	return time.Now()
}

// FinishRecovery completes recovery statistics
func (c *AtomicCollector) FinishRecovery(startTime time.Time, filesRecovered, entriesRecovered, corruptedEntries uint64) {
	c.recoveryStats.WALFilesRecovered.Store(filesRecovered)
	c.recoveryStats.WALEntriesRecovered.Store(entriesRecovered)
	c.recoveryStats.WALCorruptedEntries.Store(corruptedEntries)
	c.recoveryStats.WALRecoveryDuration.Store(time.Since(startTime).Nanoseconds())
}

// GetStats returns all statistics as a map
func (c *AtomicCollector) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// Add operation counters
	c.countsMu.RLock()
	for op, counter := range c.counts {
		stats[string(op)+"_ops"] = counter.Load()
	}
	c.countsMu.RUnlock()

	// Add timing information
	c.lastOpTimeMu.RLock()
	for op, timestamp := range c.lastOpTime {
		stats["last_"+string(op)+"_time"] = timestamp.UnixNano()
	}
	c.lastOpTimeMu.RUnlock()

	// Add performance metrics
	stats["memtable_size"] = c.memTableSize.Load()
	stats["total_bytes_read"] = c.totalBytesRead.Load()
	stats["total_bytes_written"] = c.totalBytesWritten.Load()
	stats["flush_count"] = c.flushCount.Load()
	stats["compaction_count"] = c.compactionCount.Load()

	// Add error statistics
	c.errorsMu.RLock()
	errorStats := make(map[string]uint64)
	for errType, counter := range c.errors {
		errorStats[errType] = counter.Load()
	}
	c.errorsMu.RUnlock()
	stats["errors"] = errorStats

	// Add recovery statistics
	recoveryStats := map[string]interface{}{
		"wal_files_recovered":    c.recoveryStats.WALFilesRecovered.Load(),
		"wal_entries_recovered":  c.recoveryStats.WALEntriesRecovered.Load(),
		"wal_corrupted_entries":  c.recoveryStats.WALCorruptedEntries.Load(),
	}
	
	recoveryDuration := c.recoveryStats.WALRecoveryDuration.Load()
	if recoveryDuration > 0 {
		recoveryStats["wal_recovery_duration_ms"] = recoveryDuration / int64(time.Millisecond)
	}
	stats["recovery"] = recoveryStats

	// Add latency statistics
	c.latenciesMu.RLock()
	for op, tracker := range c.latencies {
		count := tracker.count.Load()
		if count == 0 {
			continue
		}

		latencyStats := map[string]interface{}{
			"count": count,
			"avg_ns": tracker.sum.Load() / count,
		}

		// Only include min/max if we have values
		if min := tracker.min.Load(); min != 0 {
			latencyStats["min_ns"] = min
		}
		if max := tracker.max.Load(); max != 0 {
			latencyStats["max_ns"] = max
		}

		stats[string(op)+"_latency"] = latencyStats
	}
	c.latenciesMu.RUnlock()

	return stats
}

// GetStatsFiltered returns statistics filtered by prefix
func (c *AtomicCollector) GetStatsFiltered(prefix string) map[string]interface{} {
	allStats := c.GetStats()
	filtered := make(map[string]interface{})

	for key, value := range allStats {
		// Add entries that start with the prefix
		if len(prefix) == 0 || startsWith(key, prefix) {
			filtered[key] = value
		}
	}

	return filtered
}

// getOrCreateCounter gets or creates an atomic counter for the operation
func (c *AtomicCollector) getOrCreateCounter(op OperationType) *atomic.Uint64 {
	// Try read lock first (fast path)
	c.countsMu.RLock()
	counter, exists := c.counts[op]
	c.countsMu.RUnlock()

	if !exists {
		// Slow path with write lock
		c.countsMu.Lock()
		if counter, exists = c.counts[op]; !exists {
			counter = &atomic.Uint64{}
			c.counts[op] = counter
		}
		c.countsMu.Unlock()
	}

	return counter
}

// getOrCreateLatencyTracker gets or creates a latency tracker for the operation
func (c *AtomicCollector) getOrCreateLatencyTracker(op OperationType) *LatencyTracker {
	// Try read lock first (fast path)
	c.latenciesMu.RLock()
	tracker, exists := c.latencies[op]
	c.latenciesMu.RUnlock()

	if !exists {
		// Slow path with write lock
		c.latenciesMu.Lock()
		if tracker, exists = c.latencies[op]; !exists {
			tracker = &LatencyTracker{}
			c.latencies[op] = tracker
		}
		c.latenciesMu.Unlock()
	}

	return tracker
}

// startsWith checks if a string starts with a prefix
func startsWith(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}