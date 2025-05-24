// ABOUTME: Comprehensive tests for engine-level telemetry coordination and resource monitoring
// ABOUTME: Tests all EngineMetrics interface methods with mock telemetry infrastructure validation

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// mockTelemetryServer captures telemetry calls for validation (infrastructure mocking only)
type mockTelemetryServer struct {
	histograms []mockHistogramCall
	counters   []mockCounterCall
	spans      []mockSpanCall
}

type mockHistogramCall struct {
	name  string
	value float64
	attrs []attribute.KeyValue
}

type mockCounterCall struct {
	name  string
	value int64
	attrs []attribute.KeyValue
}

type mockSpanCall struct {
	name  string
	attrs []attribute.KeyValue
}

func (m *mockTelemetryServer) RecordHistogram(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
	m.histograms = append(m.histograms, mockHistogramCall{
		name:  name,
		value: value,
		attrs: attrs,
	})
}

func (m *mockTelemetryServer) RecordCounter(ctx context.Context, name string, value int64, attrs ...attribute.KeyValue) {
	m.counters = append(m.counters, mockCounterCall{
		name:  name,
		value: value,
		attrs: attrs,
	})
}

func (m *mockTelemetryServer) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	m.spans = append(m.spans, mockSpanCall{
		name:  name,
		attrs: attrs,
	})
	return ctx, nil
}

func (m *mockTelemetryServer) Shutdown(ctx context.Context) error {
	return nil
}

func (m *mockTelemetryServer) reset() {
	m.histograms = nil
	m.counters = nil
	m.spans = nil
}

func (m *mockTelemetryServer) hasHistogram(name string) bool {
	for _, h := range m.histograms {
		if h.name == name {
			return true
		}
	}
	return false
}

func (m *mockTelemetryServer) hasCounter(name string) bool {
	for _, c := range m.counters {
		if c.name == name {
			return true
		}
	}
	return false
}

func (m *mockTelemetryServer) getHistogramValue(name string) float64 {
	for _, h := range m.histograms {
		if h.name == name {
			return h.value
		}
	}
	return 0
}

func (m *mockTelemetryServer) getCounterValue(name string) int64 {
	for _, c := range m.counters {
		if c.name == name {
			return c.value
		}
	}
	return 0
}

func (m *mockTelemetryServer) getHistogramAttrs(name string) []attribute.KeyValue {
	for _, h := range m.histograms {
		if h.name == name {
			return h.attrs
		}
	}
	return nil
}

func (m *mockTelemetryServer) getCounterAttrs(name string) []attribute.KeyValue {
	for _, c := range m.counters {
		if c.name == name {
			return c.attrs
		}
	}
	return nil
}

func TestNewEngineMetrics(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)

	if metrics == nil {
		t.Fatal("NewEngineMetrics returned nil")
	}

	// Test that it's the correct implementation
	if _, ok := metrics.(*engineMetrics); !ok {
		t.Errorf("Expected *engineMetrics, got %T", metrics)
	}
}

func TestNewNoopEngineMetrics(t *testing.T) {
	metrics := NewNoopEngineMetrics()

	if metrics == nil {
		t.Fatal("NewNoopEngineMetrics returned nil")
	}

	// Test that it's the correct implementation
	if _, ok := metrics.(*noopEngineMetrics); !ok {
		t.Errorf("Expected *noopEngineMetrics, got %T", metrics)
	}
}

func TestEngineMetrics_RecordMemoryUsage(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)
	ctx := context.Background()

	// Test memory usage recording
	metrics.RecordMemoryUsage(ctx, "heap", 1024*1024)

	// Verify counter was recorded
	if !mockTel.hasCounter("kevo.engine.memory.usage.bytes") {
		t.Error("Expected memory usage counter to be recorded")
	}

	value := mockTel.getCounterValue("kevo.engine.memory.usage.bytes")
	if value != 1024*1024 {
		t.Errorf("Expected memory usage value 1048576, got %d", value)
	}

	// Verify attributes
	attrs := mockTel.getCounterAttrs("kevo.engine.memory.usage.bytes")
	if len(attrs) != 2 {
		t.Errorf("Expected 2 attributes, got %d", len(attrs))
	}
}

func TestEngineMetrics_RecordFileDescriptors(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)
	ctx := context.Background()

	// Test file descriptor recording
	metrics.RecordFileDescriptors(ctx, 100, 1024)

	// Verify counters were recorded
	if !mockTel.hasCounter("kevo.engine.file.descriptors.count") {
		t.Error("Expected file descriptor count counter to be recorded")
	}

	// Verify histogram for utilization
	if !mockTel.hasHistogram("kevo.engine.file.descriptors.utilization") {
		t.Error("Expected file descriptor utilization histogram to be recorded")
	}

	// Check utilization calculation
	utilizationValue := mockTel.getHistogramValue("kevo.engine.file.descriptors.utilization")
	expectedUtilization := float64(100) / float64(1024) * 100.0
	if utilizationValue != expectedUtilization {
		t.Errorf("Expected utilization %f, got %f", expectedUtilization, utilizationValue)
	}
}

func TestEngineMetrics_RecordDiskUsage(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)
	ctx := context.Background()

	// Test disk usage recording
	metrics.RecordDiskUsage(ctx, "sstables", 5*1024*1024*1024) // 5GB

	// Verify counter was recorded
	if !mockTel.hasCounter("kevo.engine.disk.usage.bytes") {
		t.Error("Expected disk usage counter to be recorded")
	}

	value := mockTel.getCounterValue("kevo.engine.disk.usage.bytes")
	if value != 5*1024*1024*1024 {
		t.Errorf("Expected disk usage value %d, got %d", 5*1024*1024*1024, value)
	}
}

func TestEngineMetrics_RecordEngineOperation(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)
	ctx := context.Background()

	// Test successful operation
	duration := 50 * time.Millisecond
	metrics.RecordEngineOperation(ctx, "put", duration, true)

	// Verify histogram and counter were recorded
	if !mockTel.hasHistogram("kevo.engine.operation.duration") {
		t.Error("Expected operation duration histogram to be recorded")
	}

	if !mockTel.hasCounter("kevo.engine.operation.count") {
		t.Error("Expected operation count counter to be recorded")
	}

	// Verify duration value
	durationValue := mockTel.getHistogramValue("kevo.engine.operation.duration")
	expectedDuration := duration.Seconds()
	if durationValue != expectedDuration {
		t.Errorf("Expected duration %f, got %f", expectedDuration, durationValue)
	}

	// Verify count value
	countValue := mockTel.getCounterValue("kevo.engine.operation.count")
	if countValue != 1 {
		t.Errorf("Expected count 1, got %d", countValue)
	}
}

func TestEngineMetrics_RecordOperationLatency(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)
	ctx := context.Background()

	// Test latency recording
	duration := 25 * time.Millisecond
	metrics.RecordOperationLatency(ctx, "get", duration)

	// Verify histogram was recorded
	if !mockTel.hasHistogram("kevo.engine.latency.percentiles") {
		t.Error("Expected latency percentiles histogram to be recorded")
	}

	value := mockTel.getHistogramValue("kevo.engine.latency.percentiles")
	if value != duration.Seconds() {
		t.Errorf("Expected latency %f, got %f", duration.Seconds(), value)
	}
}

func TestEngineMetrics_RecordOperationThroughput(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)
	ctx := context.Background()

	// Test throughput recording
	bytesPerSecond := 1024.5
	metrics.RecordOperationThroughput(ctx, "put", bytesPerSecond)

	// Verify histogram was recorded
	if !mockTel.hasHistogram("kevo.engine.throughput.bytes_per_second") {
		t.Error("Expected throughput histogram to be recorded")
	}

	value := mockTel.getHistogramValue("kevo.engine.throughput.bytes_per_second")
	if value != bytesPerSecond {
		t.Errorf("Expected throughput %f, got %f", bytesPerSecond, value)
	}
}

func TestEngineMetrics_RecordComponentInitialization(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)
	ctx := context.Background()

	// Test component initialization recording
	duration := 100 * time.Millisecond
	metrics.RecordComponentInitialization(ctx, "storage", duration, true)

	// Verify histogram was recorded
	if !mockTel.hasHistogram("kevo.engine.component.initialization.duration") {
		t.Error("Expected component initialization histogram to be recorded")
	}

	value := mockTel.getHistogramValue("kevo.engine.component.initialization.duration")
	if value != duration.Seconds() {
		t.Errorf("Expected initialization duration %f, got %f", duration.Seconds(), value)
	}

	// Verify attributes include success status
	attrs := mockTel.getHistogramAttrs("kevo.engine.component.initialization.duration")
	hasSuccessAttr := false
	for _, attr := range attrs {
		if attr.Key == telemetry.AttrSuccess {
			hasSuccessAttr = true
			break
		}
	}
	if !hasSuccessAttr {
		t.Error("Expected success attribute in component initialization")
	}
}

func TestEngineMetrics_RecordStartupMetrics(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)
	ctx := context.Background()

	// Test startup metrics recording
	totalStartupTime := 500 * time.Millisecond
	componentCount := int64(4)
	metrics.RecordStartupMetrics(ctx, totalStartupTime, componentCount)

	// Verify histogram and counter were recorded
	if !mockTel.hasHistogram("kevo.engine.startup.duration") {
		t.Error("Expected startup duration histogram to be recorded")
	}

	if !mockTel.hasCounter("kevo.engine.startup.components") {
		t.Error("Expected startup components counter to be recorded")
	}

	// Verify values
	durationValue := mockTel.getHistogramValue("kevo.engine.startup.duration")
	if durationValue != totalStartupTime.Seconds() {
		t.Errorf("Expected startup duration %f, got %f", totalStartupTime.Seconds(), durationValue)
	}

	countValue := mockTel.getCounterValue("kevo.engine.startup.components")
	if countValue != componentCount {
		t.Errorf("Expected component count %d, got %d", componentCount, countValue)
	}
}

func TestEngineMetrics_RecordError(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)
	ctx := context.Background()

	// Test error recording
	metrics.RecordError(ctx, "put_error", "storage", "error")

	// Verify counter was recorded
	if !mockTel.hasCounter("kevo.engine.errors.total") {
		t.Error("Expected error counter to be recorded")
	}

	value := mockTel.getCounterValue("kevo.engine.errors.total")
	if value != 1 {
		t.Errorf("Expected error count 1, got %d", value)
	}

	// Verify attributes
	attrs := mockTel.getCounterAttrs("kevo.engine.errors.total")
	if len(attrs) != 3 {
		t.Errorf("Expected 3 attributes for error recording, got %d", len(attrs))
	}
}

func TestEngineMetrics_RecordSystemHealth(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)
	ctx := context.Background()

	// Test system health recording
	uptime := 1 * time.Hour
	metrics.RecordSystemHealth(ctx, "healthy", uptime)

	// Verify counter and histogram were recorded
	if !mockTel.hasCounter("kevo.engine.health.status") {
		t.Error("Expected health status counter to be recorded")
	}

	if !mockTel.hasHistogram("kevo.engine.uptime.seconds") {
		t.Error("Expected uptime histogram to be recorded")
	}

	// Verify uptime value
	uptimeValue := mockTel.getHistogramValue("kevo.engine.uptime.seconds")
	if uptimeValue != uptime.Seconds() {
		t.Errorf("Expected uptime %f, got %f", uptime.Seconds(), uptimeValue)
	}
}

func TestEngineMetrics_Close(t *testing.T) {
	mockTel := &mockTelemetryServer{}
	metrics := NewEngineMetrics(mockTel)

	// Test close operation
	err := metrics.Close()
	if err != nil {
		t.Errorf("Expected no error from Close(), got %v", err)
	}
}

func TestNoopEngineMetrics_AllMethods(t *testing.T) {
	metrics := NewNoopEngineMetrics()
	ctx := context.Background()

	// Test that all methods can be called without panicking
	metrics.RecordMemoryUsage(ctx, "heap", 1024)
	metrics.RecordFileDescriptors(ctx, 100, 1024)
	metrics.RecordDiskUsage(ctx, "sstables", 1024*1024)
	metrics.RecordEngineOperation(ctx, "put", time.Millisecond, true)
	metrics.RecordOperationLatency(ctx, "get", time.Millisecond)
	metrics.RecordOperationThroughput(ctx, "put", 1024.0)
	metrics.RecordComponentInitialization(ctx, "storage", time.Millisecond, true)
	metrics.RecordStartupMetrics(ctx, time.Second, 4)
	metrics.RecordError(ctx, "test_error", "component", "error")
	metrics.RecordSystemHealth(ctx, "healthy", time.Hour)

	err := metrics.Close()
	if err != nil {
		t.Errorf("Expected no error from noop Close(), got %v", err)
	}
}

func TestGetMemoryStats(t *testing.T) {
	heapAlloc, heapSys, stackInuse := GetMemoryStats()

	// Verify that memory stats are reasonable values
	if heapAlloc < 0 {
		t.Errorf("Expected non-negative heapAlloc, got %d", heapAlloc)
	}

	if heapSys < heapAlloc {
		t.Errorf("Expected heapSys >= heapAlloc, got heapSys=%d, heapAlloc=%d", heapSys, heapAlloc)
	}

	if stackInuse < 0 {
		t.Errorf("Expected non-negative stackInuse, got %d", stackInuse)
	}
}

func TestBoolToString(t *testing.T) {
	if boolToString(true) != "true" {
		t.Errorf("Expected 'true', got %s", boolToString(true))
	}

	if boolToString(false) != "false" {
		t.Errorf("Expected 'false', got %s", boolToString(false))
	}
}

func TestEngineMetrics_PanicRecovery(t *testing.T) {
	// Test that telemetry panics are recovered gracefully
	panicTel := &panicTelemetryServer{}
	metrics := NewEngineMetrics(panicTel)
	ctx := context.Background()

	// These should not panic the test even though the underlying telemetry panics
	metrics.RecordMemoryUsage(ctx, "heap", 1024)
	metrics.RecordEngineOperation(ctx, "put", time.Millisecond, true)
	metrics.RecordError(ctx, "test_error", "component", "error")
}

// panicTelemetryServer panics on all method calls to test panic recovery
type panicTelemetryServer struct{}

func (p *panicTelemetryServer) RecordHistogram(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
	panic("telemetry panic")
}

func (p *panicTelemetryServer) RecordCounter(ctx context.Context, name string, value int64, attrs ...attribute.KeyValue) {
	panic("telemetry panic")
}

func (p *panicTelemetryServer) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	panic("telemetry panic")
}

func (p *panicTelemetryServer) Shutdown(ctx context.Context) error {
	panic("telemetry panic")
}
