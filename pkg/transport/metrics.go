package transport

import (
	"sync"
	"sync/atomic"
	"time"
)

// MetricsCollector collects metrics for transport operations
type MetricsCollector interface {
	// RecordRequest records metrics for a request
	RecordRequest(requestType string, startTime time.Time, err error)

	// RecordSend records metrics for bytes sent
	RecordSend(bytes int)

	// RecordReceive records metrics for bytes received
	RecordReceive(bytes int)

	// RecordConnection records a connection event
	RecordConnection(successful bool)

	// GetMetrics returns the current metrics
	GetMetrics() Metrics
}

// Metrics represents transport metrics
type Metrics struct {
	TotalRequests      uint64
	SuccessfulRequests uint64
	FailedRequests     uint64
	BytesSent          uint64
	BytesReceived      uint64
	Connections        uint64
	ConnectionFailures uint64
	AvgLatencyByType   map[string]time.Duration
}

// BasicMetricsCollector is a simple implementation of MetricsCollector
type BasicMetricsCollector struct {
	mu                 sync.RWMutex
	totalRequests      uint64
	successfulRequests uint64
	failedRequests     uint64
	bytesSent          uint64
	bytesReceived      uint64
	connections        uint64
	connectionFailures uint64

	// Track average latency and count for each request type
	avgLatencyByType   map[string]time.Duration
	requestCountByType map[string]uint64
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() MetricsCollector {
	return &BasicMetricsCollector{
		avgLatencyByType:   make(map[string]time.Duration),
		requestCountByType: make(map[string]uint64),
	}
}

// RecordRequest records metrics for a request
func (c *BasicMetricsCollector) RecordRequest(requestType string, startTime time.Time, err error) {
	atomic.AddUint64(&c.totalRequests, 1)

	if err == nil {
		atomic.AddUint64(&c.successfulRequests, 1)
	} else {
		atomic.AddUint64(&c.failedRequests, 1)
	}

	// Update average latency for request type
	latency := time.Since(startTime)

	c.mu.Lock()
	defer c.mu.Unlock()

	currentAvg, exists := c.avgLatencyByType[requestType]
	currentCount, _ := c.requestCountByType[requestType]

	if exists {
		// Update running average - the common case for better branch prediction
		// new_avg = (old_avg * count + new_value) / (count + 1)
		totalDuration := currentAvg*time.Duration(currentCount) + latency
		newCount := currentCount + 1
		c.avgLatencyByType[requestType] = totalDuration / time.Duration(newCount)
		c.requestCountByType[requestType] = newCount
	} else {
		// First request of this type
		c.avgLatencyByType[requestType] = latency
		c.requestCountByType[requestType] = 1
	}
}

// RecordSend records metrics for bytes sent
func (c *BasicMetricsCollector) RecordSend(bytes int) {
	atomic.AddUint64(&c.bytesSent, uint64(bytes))
}

// RecordReceive records metrics for bytes received
func (c *BasicMetricsCollector) RecordReceive(bytes int) {
	atomic.AddUint64(&c.bytesReceived, uint64(bytes))
}

// RecordConnection records a connection event
func (c *BasicMetricsCollector) RecordConnection(successful bool) {
	if successful {
		atomic.AddUint64(&c.connections, 1)
	} else {
		atomic.AddUint64(&c.connectionFailures, 1)
	}
}

// GetMetrics returns the current metrics
func (c *BasicMetricsCollector) GetMetrics() Metrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a copy of the average latency map
	avgLatencyByType := make(map[string]time.Duration, len(c.avgLatencyByType))
	for k, v := range c.avgLatencyByType {
		avgLatencyByType[k] = v
	}

	return Metrics{
		TotalRequests:      atomic.LoadUint64(&c.totalRequests),
		SuccessfulRequests: atomic.LoadUint64(&c.successfulRequests),
		FailedRequests:     atomic.LoadUint64(&c.failedRequests),
		BytesSent:          atomic.LoadUint64(&c.bytesSent),
		BytesReceived:      atomic.LoadUint64(&c.bytesReceived),
		Connections:        atomic.LoadUint64(&c.connections),
		ConnectionFailures: atomic.LoadUint64(&c.connectionFailures),
		AvgLatencyByType:   avgLatencyByType,
	}
}
