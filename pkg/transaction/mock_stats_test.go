package transaction

import (
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/stats"
)

// StatsCollectorMock is a simple stats collector for testing
type StatsCollectorMock struct {
	txCompleted atomic.Int64
	txAborted   atomic.Int64
}

// GetStats returns all statistics
func (s *StatsCollectorMock) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"tx_completed": s.txCompleted.Load(),
		"tx_aborted":   s.txAborted.Load(),
	}
}

// GetStatsFiltered returns statistics filtered by prefix
func (s *StatsCollectorMock) GetStatsFiltered(prefix string) map[string]interface{} {
	return s.GetStats() // No filtering in mock
}

// TrackOperation records a single operation
func (s *StatsCollectorMock) TrackOperation(op stats.OperationType) {
	// No-op for the mock
}

// TrackOperationWithLatency records an operation with its latency
func (s *StatsCollectorMock) TrackOperationWithLatency(op stats.OperationType, latencyNs uint64) {
	// No-op for the mock
}

// TrackError increments the counter for the specified error type
func (s *StatsCollectorMock) TrackError(errorType string) {
	// No-op for the mock
}

// TrackBytes adds the specified number of bytes to the read or write counter
func (s *StatsCollectorMock) TrackBytes(isWrite bool, bytes uint64) {
	// No-op for the mock
}

// TrackMemTableSize records the current memtable size
func (s *StatsCollectorMock) TrackMemTableSize(size uint64) {
	// No-op for the mock
}

// TrackFlush increments the flush counter
func (s *StatsCollectorMock) TrackFlush() {
	// No-op for the mock
}

// TrackCompaction increments the compaction counter
func (s *StatsCollectorMock) TrackCompaction() {
	// No-op for the mock
}

// StartRecovery initializes recovery statistics
func (s *StatsCollectorMock) StartRecovery() time.Time {
	return time.Now()
}

// FinishRecovery completes recovery statistics
func (s *StatsCollectorMock) FinishRecovery(startTime time.Time, filesRecovered, entriesRecovered, corruptedEntries uint64) {
	// No-op for the mock
}

// IncrementTxCompleted increments the completed transaction counter
func (s *StatsCollectorMock) IncrementTxCompleted() {
	s.txCompleted.Add(1)
}

// IncrementTxAborted increments the aborted transaction counter
func (s *StatsCollectorMock) IncrementTxAborted() {
	s.txAborted.Add(1)
}
