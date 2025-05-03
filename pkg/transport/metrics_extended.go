package transport

import (
	"sync/atomic"
	"time"
)

// Metrics struct extensions for server metrics
type ServerMetrics struct {
	Metrics
	ServerStarted uint64
	ServerErrored uint64
	ServerStopped uint64
}

// ConnectionStatus represents the status of a connection
type ConnectionStatus struct {
	Connected    bool
	LastActivity time.Time
	ErrorCount   int
	RequestCount int
	LatencyAvg   time.Duration
}

// ExtendedMetricsCollector extends the basic metrics collector with server metrics
type ExtendedMetricsCollector struct {
	BasicMetricsCollector
	serverStarted uint64
	serverErrored uint64
	serverStopped uint64
}

// NewMetrics creates a new extended metrics collector with a given transport name
func NewMetrics(transport string) *ExtendedMetricsCollector {
	return &ExtendedMetricsCollector{
		BasicMetricsCollector: BasicMetricsCollector{
			avgLatencyByType:   make(map[string]time.Duration),
			requestCountByType: make(map[string]uint64),
		},
	}
}

// ServerStarted increments the server started counter
func (c *ExtendedMetricsCollector) ServerStarted() {
	atomic.AddUint64(&c.serverStarted, 1)
}

// ServerErrored increments the server errored counter
func (c *ExtendedMetricsCollector) ServerErrored() {
	atomic.AddUint64(&c.serverErrored, 1)
}

// ServerStopped increments the server stopped counter
func (c *ExtendedMetricsCollector) ServerStopped() {
	atomic.AddUint64(&c.serverStopped, 1)
}

// ConnectionOpened records a connection opened event
func (c *ExtendedMetricsCollector) ConnectionOpened() {
	atomic.AddUint64(&c.connections, 1)
}

// ConnectionFailed records a connection failed event
func (c *ExtendedMetricsCollector) ConnectionFailed() {
	atomic.AddUint64(&c.connectionFailures, 1)
}

// ConnectionClosed records a connection closed event
func (c *ExtendedMetricsCollector) ConnectionClosed() {
	atomic.AddUint64(&c.connections, ^uint64(0)) // Decrement active connections count
}

// GetExtendedMetrics returns the current extended metrics
func (c *ExtendedMetricsCollector) GetExtendedMetrics() ServerMetrics {
	baseMetrics := c.GetMetrics()

	return ServerMetrics{
		Metrics:       baseMetrics,
		ServerStarted: atomic.LoadUint64(&c.serverStarted),
		ServerErrored: atomic.LoadUint64(&c.serverErrored),
		ServerStopped: atomic.LoadUint64(&c.serverStopped),
	}
}
