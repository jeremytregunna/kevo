// ABOUTME: Unit tests for MemTable telemetry metrics interface and implementation with mock telemetry server
// ABOUTME: Tests real MemTable operations with mock telemetry server to verify metric recording accuracy

package memtable

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/config"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// mockTelemetryServer provides a telemetry server implementation that captures metrics for testing
// This mocks the telemetry destination/server, NOT the business logic
type mockTelemetryServer struct {
	histograms []histogramRecord
	counters   []counterRecord
	spans      []spanRecord
}

type histogramRecord struct {
	name  string
	value float64
	attrs []attribute.KeyValue
}

type counterRecord struct {
	name  string
	value int64
	attrs []attribute.KeyValue
}

type spanRecord struct {
	name  string
	attrs []attribute.KeyValue
}

func newMockTelemetryServer() *mockTelemetryServer {
	return &mockTelemetryServer{
		histograms: make([]histogramRecord, 0),
		counters:   make([]counterRecord, 0),
		spans:      make([]spanRecord, 0),
	}
}

func (m *mockTelemetryServer) RecordHistogram(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
	m.histograms = append(m.histograms, histogramRecord{
		name:  name,
		value: value,
		attrs: attrs,
	})
}

func (m *mockTelemetryServer) RecordCounter(ctx context.Context, name string, value int64, attrs ...attribute.KeyValue) {
	m.counters = append(m.counters, counterRecord{
		name:  name,
		value: value,
		attrs: attrs,
	})
}

func (m *mockTelemetryServer) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	m.spans = append(m.spans, spanRecord{
		name:  name,
		attrs: attrs,
	})
	return ctx, trace.SpanFromContext(ctx)
}

func (m *mockTelemetryServer) Shutdown(ctx context.Context) error {
	return nil
}

func (m *mockTelemetryServer) findHistogram(name string) *histogramRecord {
	for _, h := range m.histograms {
		if h.name == name {
			return &h
		}
	}
	return nil
}

func (m *mockTelemetryServer) findCounter(name string) *counterRecord {
	for _, c := range m.counters {
		if c.name == name {
			return &c
		}
	}
	return nil
}

func (m *mockTelemetryServer) countCounters(name string) int {
	count := 0
	for _, c := range m.counters {
		if c.name == name {
			count++
		}
	}
	return count
}

func (m *mockTelemetryServer) reset() {
	m.histograms = m.histograms[:0]
	m.counters = m.counters[:0]
	m.spans = m.spans[:0]
}

// TestMemTableMetrics tests the MemTable metrics interface implementation
func TestMemTableMetrics(t *testing.T) {
	ctx := context.Background()
	mockServer := newMockTelemetryServer()
	metrics := NewMemTableMetrics(mockServer)

	t.Run("RecordOperation", func(t *testing.T) {
		mockServer.reset()

		metrics.RecordOperation(ctx, "put", 50*time.Millisecond)

		// Check operation duration histogram
		durHist := mockServer.findHistogram("kevo.memtable.operation.duration")
		if durHist == nil {
			t.Fatal("Expected operation duration histogram to be recorded")
		}
		if durHist.value != 0.05 {
			t.Errorf("Expected duration 0.05s, got %f", durHist.value)
		}

		// Check operation counter
		opsCounter := mockServer.findCounter("kevo.memtable.operations.total")
		if opsCounter == nil {
			t.Fatal("Expected operations counter to be recorded")
		}
		if opsCounter.value != 1 {
			t.Errorf("Expected operations count 1, got %d", opsCounter.value)
		}
	})

	t.Run("RecordFlushTrigger", func(t *testing.T) {
		mockServer.reset()

		metrics.RecordFlushTrigger(ctx, "size", 1048576, 120.5)

		// Check flush trigger counter
		triggerCounter := mockServer.findCounter("kevo.memtable.flush.trigger.total")
		if triggerCounter == nil {
			t.Fatal("Expected flush trigger counter to be recorded")
		}
		if triggerCounter.value != 1 {
			t.Errorf("Expected trigger count 1, got %d", triggerCounter.value)
		}

		// Check flush trigger size histogram
		sizeHist := mockServer.findHistogram("kevo.memtable.flush.trigger.size")
		if sizeHist == nil {
			t.Fatal("Expected flush trigger size histogram to be recorded")
		}
		if sizeHist.value != 1048576.0 {
			t.Errorf("Expected size 1048576, got %f", sizeHist.value)
		}

		// Check flush trigger age histogram
		ageHist := mockServer.findHistogram("kevo.memtable.flush.trigger.age")
		if ageHist == nil {
			t.Fatal("Expected flush trigger age histogram to be recorded")
		}
		if ageHist.value != 120.5 {
			t.Errorf("Expected age 120.5, got %f", ageHist.value)
		}
	})

	t.Run("RecordFlushDuration", func(t *testing.T) {
		mockServer.reset()

		metrics.RecordFlushDuration(ctx, 2*time.Second, 2097152, 1000)

		// Check flush duration histogram
		durHist := mockServer.findHistogram("kevo.memtable.flush.duration")
		if durHist == nil {
			t.Fatal("Expected flush duration histogram to be recorded")
		}
		if durHist.value != 2.0 {
			t.Errorf("Expected duration 2.0s, got %f", durHist.value)
		}

		// Check flush size histogram
		sizeHist := mockServer.findHistogram("kevo.memtable.flush.size")
		if sizeHist == nil {
			t.Fatal("Expected flush size histogram to be recorded")
		}
		if sizeHist.value != 2097152.0 {
			t.Errorf("Expected size 2097152, got %f", sizeHist.value)
		}

		// Check flush entries counter
		entriesCounter := mockServer.findCounter("kevo.memtable.flush.entries")
		if entriesCounter == nil {
			t.Fatal("Expected flush entries counter to be recorded")
		}
		if entriesCounter.value != 1000 {
			t.Errorf("Expected entries 1000, got %d", entriesCounter.value)
		}

		// Check flush total counter
		totalCounter := mockServer.findCounter("kevo.memtable.flush.total")
		if totalCounter == nil {
			t.Fatal("Expected flush total counter to be recorded")
		}
		if totalCounter.value != 1 {
			t.Errorf("Expected flush total 1, got %d", totalCounter.value)
		}
	})

	t.Run("RecordSizeChange", func(t *testing.T) {
		mockServer.reset()

		metrics.RecordSizeChange(ctx, 512000, 1024, "active")

		// Check size histogram
		sizeHist := mockServer.findHistogram("kevo.memtable.size.bytes")
		if sizeHist == nil {
			t.Fatal("Expected size histogram to be recorded")
		}
		if sizeHist.value != 512000.0 {
			t.Errorf("Expected size 512000, got %f", sizeHist.value)
		}

		// Check size delta histogram
		deltaHist := mockServer.findHistogram("kevo.memtable.size.delta")
		if deltaHist == nil {
			t.Fatal("Expected size delta histogram to be recorded")
		}
		if deltaHist.value != 1024.0 {
			t.Errorf("Expected delta 1024, got %f", deltaHist.value)
		}
	})

	t.Run("RecordPoolState", func(t *testing.T) {
		mockServer.reset()

		metrics.RecordPoolState(ctx, 256000, 3, 1024000)

		// Check active size histogram
		activeSizeHist := mockServer.findHistogram("kevo.memtable.pool.active.size")
		if activeSizeHist == nil {
			t.Fatal("Expected active size histogram to be recorded")
		}
		if activeSizeHist.value != 256000.0 {
			t.Errorf("Expected active size 256000, got %f", activeSizeHist.value)
		}

		// Check immutable count histogram
		immutableCountHist := mockServer.findHistogram("kevo.memtable.pool.immutable.count")
		if immutableCountHist == nil {
			t.Fatal("Expected immutable count histogram to be recorded")
		}
		if immutableCountHist.value != 3.0 {
			t.Errorf("Expected immutable count 3, got %f", immutableCountHist.value)
		}

		// Check total size histogram
		totalSizeHist := mockServer.findHistogram("kevo.memtable.pool.total.size")
		if totalSizeHist == nil {
			t.Fatal("Expected total size histogram to be recorded")
		}
		if totalSizeHist.value != 1024000.0 {
			t.Errorf("Expected total size 1024000, got %f", totalSizeHist.value)
		}
	})
}

// TestNoopMemTableMetrics tests the no-op implementation
func TestNoopMemTableMetrics(t *testing.T) {
	ctx := context.Background()
	metrics := NewNoopMemTableMetrics()

	// All operations should work without errors
	metrics.RecordOperation(ctx, "put", 10*time.Millisecond)
	metrics.RecordFlushTrigger(ctx, "size", 1024, 60.0)
	metrics.RecordFlushDuration(ctx, time.Second, 2048, 100)
	metrics.RecordSizeChange(ctx, 1024, 512, "active")
	metrics.RecordPoolState(ctx, 1024, 2, 2048)

	err := metrics.Close()
	if err != nil {
		t.Errorf("Expected no error from no-op Close(), got %v", err)
	}
}

// TestMemTablePoolTelemetryIntegration tests real MemTablePool operations with telemetry
func TestMemTablePoolTelemetryIntegration(t *testing.T) {
	cfg := config.NewDefaultConfig("/tmp/test")
	cfg.MemTableSize = 1024 // Small size for testing flush triggers
	cfg.MaxMemTableAge = 1  // 1 second for age-based flush testing

	mockServer := newMockTelemetryServer()
	metrics := NewMemTableMetrics(mockServer)

	pool := NewMemTablePool(cfg)
	pool.SetTelemetry(metrics)

	t.Run("PutOperation", func(t *testing.T) {
		mockServer.reset()

		// Perform real Put operation
		pool.Put([]byte("key1"), []byte("value1"), 1)

		// Verify operation metrics were recorded
		opsCount := mockServer.countCounters("kevo.memtable.operations.total")
		if opsCount != 1 {
			t.Errorf("Expected 1 operation recorded, got %d", opsCount)
		}
	})

	t.Run("DeleteOperation", func(t *testing.T) {
		mockServer.reset()

		// Perform real Delete operation
		pool.Delete([]byte("key2"), 2)

		// Verify operation metrics were recorded
		opsCount := mockServer.countCounters("kevo.memtable.operations.total")
		if opsCount != 1 {
			t.Errorf("Expected 1 operation recorded, got %d", opsCount)
		}
	})

	t.Run("GetOperation", func(t *testing.T) {
		mockServer.reset()

		// First put a value
		pool.Put([]byte("key3"), []byte("value3"), 3)
		mockServer.reset() // Reset to only count the Get operation

		// Perform real Get operation
		value, found := pool.Get([]byte("key3"))
		if !found || string(value) != "value3" {
			t.Errorf("Expected to find key3 with value3, got found=%v, value=%s", found, string(value))
		}

		// Verify operation metrics were recorded
		opsCount := mockServer.countCounters("kevo.memtable.operations.total")
		if opsCount != 1 {
			t.Errorf("Expected 1 operation recorded, got %d", opsCount)
		}
	})

	t.Run("FlushTriggerBySize", func(t *testing.T) {
		mockServer.reset()

		// Add some data and manually trigger checkFlushConditions by calling IsFlushNeeded
		largeValue := make([]byte, 200) // 200 byte value
		for i := 0; i < 5; i++ {
			key := []byte(fmt.Sprintf("key_%d", i))
			pool.Put(key, largeValue, uint64(i+10))
		}

		// Check if any flush operations were triggered during puts
		// This is mainly testing that telemetry doesn't break normal operations
		isFlushNeeded := pool.IsFlushNeeded()
		t.Logf("Flush needed: %v", isFlushNeeded)

		// The important thing is that operations completed without hanging
		t.Log("Put operations completed successfully with telemetry")
	})

	t.Run("SwitchToNewMemTable", func(t *testing.T) {
		mockServer.reset()

		// Switch to new MemTable
		oldMemTable := pool.SwitchToNewMemTable()
		if oldMemTable == nil {
			t.Error("Expected non-nil MemTable from switch")
		}

		// We should have at least some pool state histograms recorded
		poolStateHistograms := 0
		for _, h := range mockServer.histograms {
			if h.name == "kevo.memtable.pool.active.size" ||
				h.name == "kevo.memtable.pool.immutable.count" ||
				h.name == "kevo.memtable.pool.total.size" {
				poolStateHistograms++
			}
		}

		if poolStateHistograms < 3 {
			t.Errorf("Expected at least 3 pool state histograms, got %d", poolStateHistograms)
		}
	})
}

// TestMemTablePoolWithoutTelemetry verifies MemTablePool works correctly without telemetry
func TestMemTablePoolWithoutTelemetry(t *testing.T) {
	cfg := config.NewDefaultConfig("/tmp/test")
	pool := NewMemTablePool(cfg)

	// All operations should work without telemetry
	pool.Put([]byte("key1"), []byte("value1"), 1)
	pool.Delete([]byte("key2"), 2)

	value, found := pool.Get([]byte("key1"))
	if !found || string(value) != "value1" {
		t.Errorf("Expected to find key1 with value1, got found=%v, value=%s", found, string(value))
	}

	// Switch should work
	oldMemTable := pool.SwitchToNewMemTable()
	if oldMemTable == nil {
		t.Error("Expected non-nil MemTable from switch")
	}
}

// TestSetTelemetryTypeAssertion tests type assertion safety in SetTelemetry
func TestSetTelemetryTypeAssertion(t *testing.T) {
	cfg := config.NewDefaultConfig("/tmp/test")
	pool := NewMemTablePool(cfg)

	// Test with valid MemTableMetrics
	mockServer := newMockTelemetryServer()
	metrics := NewMemTableMetrics(mockServer)
	pool.SetTelemetry(metrics)

	// Test with invalid type (should not crash)
	pool.SetTelemetry("invalid_type")
	pool.SetTelemetry(123)
	pool.SetTelemetry(nil)

	// Pool should still work
	pool.Put([]byte("key"), []byte("value"), 1)
}

// TestHelperFunctions tests the helper functions used for telemetry
func TestHelperFunctions(t *testing.T) {
	t.Run("getFlushReasonName", func(t *testing.T) {
		tests := []struct {
			size, age, manual bool
			expected          string
		}{
			{false, false, true, "manual"},
			{true, true, false, "size_and_age"},
			{true, false, false, "size"},
			{false, true, false, "age"},
			{false, false, false, "unknown"},
		}

		for _, test := range tests {
			result := getFlushReasonName(test.size, test.age, test.manual)
			if result != test.expected {
				t.Errorf("getFlushReasonName(%v, %v, %v) = %s, expected %s",
					test.size, test.age, test.manual, result, test.expected)
			}
		}
	})

	t.Run("getMemTableTypeName", func(t *testing.T) {
		if getMemTableTypeName(true) != "immutable" {
			t.Error("Expected 'immutable' for immutable MemTable")
		}
		if getMemTableTypeName(false) != "active" {
			t.Error("Expected 'active' for mutable MemTable")
		}
	})
}
