package transport

import (
	"errors"
	"testing"
	"time"
)

func TestBasicMetricsCollector(t *testing.T) {
	collector := NewMetricsCollector()

	// Test initial state
	metrics := collector.GetMetrics()
	if metrics.TotalRequests != 0 ||
		metrics.SuccessfulRequests != 0 ||
		metrics.FailedRequests != 0 ||
		metrics.BytesSent != 0 ||
		metrics.BytesReceived != 0 ||
		metrics.Connections != 0 ||
		metrics.ConnectionFailures != 0 ||
		len(metrics.AvgLatencyByType) != 0 {
		t.Errorf("Initial metrics not initialized correctly: %+v", metrics)
	}

	// Test recording successful request
	startTime := time.Now().Add(-100 * time.Millisecond) // Simulate 100ms request
	collector.RecordRequest("get", startTime, nil)

	metrics = collector.GetMetrics()
	if metrics.TotalRequests != 1 {
		t.Errorf("Expected TotalRequests to be 1, got %d", metrics.TotalRequests)
	}
	if metrics.SuccessfulRequests != 1 {
		t.Errorf("Expected SuccessfulRequests to be 1, got %d", metrics.SuccessfulRequests)
	}
	if metrics.FailedRequests != 0 {
		t.Errorf("Expected FailedRequests to be 0, got %d", metrics.FailedRequests)
	}

	// Check average latency
	if avgLatency, exists := metrics.AvgLatencyByType["get"]; !exists {
		t.Error("Expected 'get' latency to exist")
	} else if avgLatency < 100*time.Millisecond {
		t.Errorf("Expected latency to be at least 100ms, got %v", avgLatency)
	}

	// Test recording failed request
	startTime = time.Now().Add(-200 * time.Millisecond) // Simulate 200ms request
	collector.RecordRequest("get", startTime, errors.New("test error"))

	metrics = collector.GetMetrics()
	if metrics.TotalRequests != 2 {
		t.Errorf("Expected TotalRequests to be 2, got %d", metrics.TotalRequests)
	}
	if metrics.SuccessfulRequests != 1 {
		t.Errorf("Expected SuccessfulRequests to be 1, got %d", metrics.SuccessfulRequests)
	}
	if metrics.FailedRequests != 1 {
		t.Errorf("Expected FailedRequests to be 1, got %d", metrics.FailedRequests)
	}

	// Test average latency calculation for multiple requests
	startTime = time.Now().Add(-300 * time.Millisecond)
	collector.RecordRequest("put", startTime, nil)

	startTime = time.Now().Add(-500 * time.Millisecond)
	collector.RecordRequest("put", startTime, nil)

	metrics = collector.GetMetrics()
	avgPutLatency := metrics.AvgLatencyByType["put"]

	// Expected avg is around (300ms + 500ms) / 2 = 400ms
	if avgPutLatency < 390*time.Millisecond || avgPutLatency > 410*time.Millisecond {
		t.Errorf("Expected average 'put' latency to be around 400ms, got %v", avgPutLatency)
	}

	// Test byte tracking
	collector.RecordSend(1000)
	collector.RecordReceive(2000)

	metrics = collector.GetMetrics()
	if metrics.BytesSent != 1000 {
		t.Errorf("Expected BytesSent to be 1000, got %d", metrics.BytesSent)
	}
	if metrics.BytesReceived != 2000 {
		t.Errorf("Expected BytesReceived to be 2000, got %d", metrics.BytesReceived)
	}

	// Test connection tracking
	collector.RecordConnection(true)
	collector.RecordConnection(false)
	collector.RecordConnection(true)

	metrics = collector.GetMetrics()
	if metrics.Connections != 2 {
		t.Errorf("Expected Connections to be 2, got %d", metrics.Connections)
	}
	if metrics.ConnectionFailures != 1 {
		t.Errorf("Expected ConnectionFailures to be 1, got %d", metrics.ConnectionFailures)
	}
}
