# Statistics Package Documentation

The `stats` package implements a comprehensive, atomic, thread-safe statistics collection system for the Kevo engine. It provides a centralized way to track metrics across all components with minimal performance impact and contention.

## Overview

Statistics collection is a critical aspect of database monitoring, performance tuning, and debugging. The stats package is designed to collect and provide access to various metrics with minimal overhead, even in highly concurrent environments.

Key responsibilities of the stats package include:
- Tracking operation counts (puts, gets, deletes, etc.)
- Measuring operation latencies (min, max, average)
- Recording byte counts for I/O operations
- Tracking error occurrences by category
- Maintaining timestamps for the last operations
- Collecting WAL recovery statistics
- Providing a thread-safe, unified interface for all metrics

## Architecture

### Core Components

The statistics system consists of several well-defined components:

```
┌───────────────────────────────────────────┐
│            AtomicCollector                │
├───────────────┬──────────────┬────────────┤
│  Operation    │  Latency     │  Error     │
│  Counters     │  Trackers    │  Counters  │
└───────────────┴──────────────┴────────────┘
```

1. **AtomicCollector**: Thread-safe implementation of the Collector interface
2. **OperationType**: Type definition for various operation categories
3. **LatencyTracker**: Component for tracking operation latencies 
4. **RecoveryStats**: Specialized structure for WAL recovery metrics

## Implementation Details

### AtomicCollector

The `AtomicCollector` is the core component and implements the `Collector` interface:

```go
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
```

The collector uses atomic variables and minimal locking to ensure thread safety while maintaining high performance.

### Operation Types

The package defines standard operation types as constants:

```go
type OperationType string

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
```

These standardized types enable consistent tracking across all engine components.

### Latency Tracking

The `LatencyTracker` maintains runtime statistics about operation latencies:

```go
type LatencyTracker struct {
    count   atomic.Uint64
    sum     atomic.Uint64 // sum in nanoseconds
    max     atomic.Uint64 // max in nanoseconds
    min     atomic.Uint64 // min in nanoseconds (initialized to max uint64)
}
```

It tracks:
- Count of operations
- Sum of all latencies (for calculating averages)
- Maximum latency observed
- Minimum latency observed

All fields use atomic operations to ensure thread safety.

### Recovery Statistics

Recovery statistics are tracked in a specialized structure:

```go
type RecoveryStats struct {
    WALFilesRecovered    atomic.Uint64
    WALEntriesRecovered  atomic.Uint64
    WALCorruptedEntries  atomic.Uint64
    WALRecoveryDuration  atomic.Int64 // nanoseconds
}
```

These metrics provide insights into the recovery process after engine startup.

## Key Operations

### Operation Tracking

The `TrackOperation` method increments the counter for the specified operation type:

```go
func (c *AtomicCollector) TrackOperation(op OperationType) {
    counter := c.getOrCreateCounter(op)
    counter.Add(1)

    // Update last operation time
    c.lastOpTimeMu.Lock()
    c.lastOpTime[op] = time.Now()
    c.lastOpTimeMu.Unlock()
}
```

This method is used for basic operation counting without latency tracking.

### Latency Tracking

The `TrackOperationWithLatency` method not only counts operations but also records their duration:

```go
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
    // ...

    // Update min (using compare-and-swap pattern)
    // ...
}
```

This provides detailed timing metrics for performance analysis.

### Error Tracking

Errors are tracked by category using the `TrackError` method:

```go
func (c *AtomicCollector) TrackError(errorType string) {
    // Get or create error counter
    // ...
    
    counter.Add(1)
}
```

This helps identify problematic areas in the engine.

### Byte Tracking

Data volumes are tracked with the `TrackBytes` method:

```go
func (c *AtomicCollector) TrackBytes(isWrite bool, bytes uint64) {
    if isWrite {
        c.totalBytesWritten.Add(bytes)
    } else {
        c.totalBytesRead.Add(bytes)
    }
}
```

This distinguishes between read and write operations.

### Recovery Tracking

Recovery statistics are managed through specialized methods:

```go
func (c *AtomicCollector) StartRecovery() time.Time {
    // Reset recovery stats
    c.recoveryStats.WALFilesRecovered.Store(0)
    c.recoveryStats.WALEntriesRecovered.Store(0)
    c.recoveryStats.WALCorruptedEntries.Store(0)
    c.recoveryStats.WALRecoveryDuration.Store(0)
    
    return time.Now()
}

func (c *AtomicCollector) FinishRecovery(startTime time.Time, filesRecovered, entriesRecovered, corruptedEntries uint64) {
    c.recoveryStats.WALFilesRecovered.Store(filesRecovered)
    c.recoveryStats.WALEntriesRecovered.Store(entriesRecovered)
    c.recoveryStats.WALCorruptedEntries.Store(corruptedEntries)
    c.recoveryStats.WALRecoveryDuration.Store(time.Since(startTime).Nanoseconds())
}
```

These provide structured insight into the startup recovery process.

## Retrieving Statistics

### Full Statistics Retrieval

The `GetStats` method returns a complete map of all collected statistics:

```go
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
    // ...

    // Add latency statistics
    // ...

    return stats
}
```

This provides a comprehensive view of the engine's operations and performance.

### Filtered Statistics

For targeted analysis, the `GetStatsFiltered` method allows retrieving only statistics with a specific prefix:

```go
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
```

This is useful for examining specific types of operations or components.

## Performance Considerations

### Atomic Operations

The statistics collector uses atomic operations extensively to minimize contention:

1. **Lock-Free Counters**:
   - Most increments and reads use atomic operations
   - No locking during normal operation

2. **Limited Lock Scope**:
   - Locks are only used when creating new entries
   - Read locks for retrieving complete statistics

3. **Read-Write Locks**:
   - Uses `sync.RWMutex` to allow concurrent reads
   - Writes (rare in this context) obtain exclusive access

### Memory Efficiency

The collector is designed to be memory-efficient:

1. **Lazy Initialization**:
   - Counters are created only when needed
   - No pre-allocation of unused statistics

2. **Map-Based Storage**:
   - Only tracks operations that actually occur
   - Compact representation for sparse metrics

3. **Fixed Overhead**:
   - Predictable memory usage regardless of operation volume
   - Low per-operation overhead

## Integration with the Engine

The statistics collector is integrated throughout the engine's operations:

1. **EngineFacade Integration**:
   - Central collector instance in the EngineFacade
   - All operations tracked through the facade

2. **Manager-Specific Statistics**:
   - Each manager contributes component-specific stats
   - Combined by the facade for a complete view

3. **Centralized Reporting**:
   - The `GetStats()` method merges all statistics
   - Provides a unified view for monitoring

## Common Usage Patterns

### Tracking Operations

```go
// Track a basic operation
collector.TrackOperation(stats.OpPut)

// Track an operation with latency
startTime := time.Now()
// ... perform operation ...
latencyNs := uint64(time.Since(startTime).Nanoseconds())
collector.TrackOperationWithLatency(stats.OpGet, latencyNs)

// Track bytes processed
collector.TrackBytes(true, uint64(len(key)+len(value))) // write
collector.TrackBytes(false, uint64(len(value))) // read

// Track errors
if err != nil {
    collector.TrackError("read_error")
}
```

### Retrieving Statistics

```go
// Get all statistics
allStats := collector.GetStats()
fmt.Printf("Put operations: %d\n", allStats["put_ops"])
fmt.Printf("Total bytes written: %d\n", allStats["total_bytes_written"])

// Get filtered statistics
txStats := collector.GetStatsFiltered("tx_")
for k, v := range txStats {
    fmt.Printf("%s: %v\n", k, v)
}
```

## Limitations and Future Enhancements

### Current Limitations

1. **Fixed Metric Types**:
   - Predefined operation types
   - No dynamic metric definition at runtime

2. **Simple Aggregation**:
   - Basic counters and min/max/avg latencies
   - No percentiles or histograms

3. **In-Memory Only**:
   - No persistence of historical metrics
   - Resets on engine restart

### Potential Enhancements

1. **Advanced Metrics**:
   - Latency percentiles (e.g., p95, p99)
   - Histograms for distribution analysis
   - Moving averages for trend detection

2. **Time Series Support**:
   - Time-bucketed statistics
   - Historical metrics retention
   - Rate calculations (operations per second)

3. **Metric Export**:
   - Prometheus integration
   - Structured logging with metrics
   - Periodic stat dumping to files