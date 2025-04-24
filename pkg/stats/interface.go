package stats

import "time"

// Provider defines the interface for components that provide statistics
type Provider interface {
	// GetStats returns all statistics
	GetStats() map[string]interface{}
	
	// GetStatsFiltered returns statistics filtered by prefix
	GetStatsFiltered(prefix string) map[string]interface{}
}

// Collector interface defines methods for collecting statistics
type Collector interface {
	Provider
	
	// TrackOperation records a single operation
	TrackOperation(op OperationType)
	
	// TrackOperationWithLatency records an operation with its latency
	TrackOperationWithLatency(op OperationType, latencyNs uint64)
	
	// TrackError increments the counter for the specified error type
	TrackError(errorType string)
	
	// TrackBytes adds the specified number of bytes to the read or write counter
	TrackBytes(isWrite bool, bytes uint64)
	
	// TrackMemTableSize records the current memtable size
	TrackMemTableSize(size uint64)
	
	// TrackFlush increments the flush counter
	TrackFlush()
	
	// TrackCompaction increments the compaction counter
	TrackCompaction()
	
	// StartRecovery initializes recovery statistics
	StartRecovery() time.Time
	
	// FinishRecovery completes recovery statistics
	FinishRecovery(startTime time.Time, filesRecovered, entriesRecovered, corruptedEntries uint64)
}

// Ensure AtomicCollector implements the Collector interface
var _ Collector = (*AtomicCollector)(nil)