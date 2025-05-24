// ABOUTME: Compaction telemetry metrics tests with mock telemetry server and real compaction operations
// ABOUTME: Provides comprehensive test coverage for compaction metrics interface and integration tests

package compaction

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// mockTelemetryServer captures metrics for testing compaction telemetry (infrastructure mocking only)
type mockTelemetryServer struct {
	mu         sync.Mutex
	histograms map[string][]mockHistogramValue
	counters   map[string][]mockCounterValue
}

type mockHistogramValue struct {
	value      float64
	attributes []attribute.KeyValue
}

type mockCounterValue struct {
	value      int64
	attributes []attribute.KeyValue
}

func newMockTelemetryServer() *mockTelemetryServer {
	return &mockTelemetryServer{
		histograms: make(map[string][]mockHistogramValue),
		counters:   make(map[string][]mockCounterValue),
	}
}

func (m *mockTelemetryServer) RecordHistogram(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.histograms[name] = append(m.histograms[name], mockHistogramValue{
		value:      value,
		attributes: attrs,
	})
}

func (m *mockTelemetryServer) RecordCounter(ctx context.Context, name string, value int64, attrs ...attribute.KeyValue) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name] = append(m.counters[name], mockCounterValue{
		value:      value,
		attributes: attrs,
	})
}

func (m *mockTelemetryServer) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return ctx, trace.SpanFromContext(ctx)
}

func (m *mockTelemetryServer) Shutdown(ctx context.Context) error {
	return nil
}

func (m *mockTelemetryServer) getHistogramCount(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.histograms[name])
}

func (m *mockTelemetryServer) getCounterCount(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.counters[name])
}

func (m *mockTelemetryServer) getHistogramValues(name string) []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := make([]float64, len(m.histograms[name]))
	for i, v := range m.histograms[name] {
		values[i] = v.value
	}
	return values
}

func (m *mockTelemetryServer) getCounterValues(name string) []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := make([]int64, len(m.counters[name]))
	for i, v := range m.counters[name] {
		values[i] = v.value
	}
	return values
}

func (m *mockTelemetryServer) getMetricNames() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	names := make([]string, 0)
	for name := range m.histograms {
		names = append(names, name)
	}
	for name := range m.counters {
		names = append(names, name)
	}

	sort.Strings(names)
	return names
}

// TestCompactionMetricsInterface tests all methods of the CompactionMetrics interface
func TestCompactionMetricsInterface(t *testing.T) {
	mockTel := newMockTelemetryServer()
	metrics := NewCompactionMetrics(mockTel)
	ctx := context.Background()

	// Test RecordCompactionStart
	metrics.RecordCompactionStart(ctx, 1, "tiered", 3, 1024000)

	if count := mockTel.getCounterCount("kevo.compaction.start.count"); count != 1 {
		t.Errorf("Expected 1 compaction start record, got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.compaction.input.files"); count != 1 {
		t.Errorf("Expected 1 input files record, got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.compaction.input.bytes"); count != 1 {
		t.Errorf("Expected 1 input bytes record, got %d", count)
	}

	// Test RecordCompactionComplete
	metrics.RecordCompactionComplete(ctx, 150*time.Millisecond, 1024000, 512000, 50, true)

	if count := mockTel.getHistogramCount("kevo.compaction.execution.duration"); count != 1 {
		t.Errorf("Expected 1 execution duration record, got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.compaction.output.bytes"); count != 1 {
		t.Errorf("Expected 1 output bytes record, got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.compaction.tombstones.removed"); count != 1 {
		t.Errorf("Expected 1 tombstones removed record, got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.compaction.space.reclaimed.bytes"); count != 1 {
		t.Errorf("Expected 1 space reclaimed record, got %d", count)
	}

	if count := mockTel.getHistogramCount("kevo.compaction.compression.ratio"); count != 1 {
		t.Errorf("Expected 1 compression ratio record, got %d", count)
	}

	// Test RecordLevelTransition
	metrics.RecordLevelTransition(ctx, 0, 1, 512000)

	if count := mockTel.getCounterCount("kevo.compaction.level.transition.bytes"); count != 1 {
		t.Errorf("Expected 1 level transition bytes record, got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.compaction.level.transition.count"); count != 1 {
		t.Errorf("Expected 1 level transition count record, got %d", count)
	}

	// Test RecordTombstoneCleanup
	metrics.RecordTombstoneCleanup(ctx, 25, 1)

	if count := mockTel.getCounterCount("kevo.compaction.tombstones.cleaned"); count != 1 {
		t.Errorf("Expected 1 tombstones cleaned record, got %d", count)
	}

	// Test RecordStrategyDecision
	levelSizes := map[int]int64{0: 2048000, 1: 1024000}
	metrics.RecordStrategyDecision(ctx, "tiered", "size_ratio", "high", levelSizes)

	if count := mockTel.getCounterCount("kevo.compaction.strategy.decision.count"); count != 1 {
		t.Errorf("Expected 1 strategy decision record, got %d", count)
	}

	if count := mockTel.getHistogramCount("kevo.compaction.level.size.bytes"); count != 2 {
		t.Errorf("Expected 2 level size records, got %d", count)
	}

	// Test RecordFileOperations
	metrics.RecordFileOperations(ctx, "create", 2, 512000, 1)

	if count := mockTel.getCounterCount("kevo.compaction.file.operations.count"); count != 1 {
		t.Errorf("Expected 1 file operations count record, got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.compaction.file.operations.bytes"); count != 1 {
		t.Errorf("Expected 1 file operations bytes record, got %d", count)
	}

	// Test RecordCompactionEfficiency
	metrics.RecordCompactionEfficiency(ctx, 0.8, 256000, 10)

	if count := mockTel.getHistogramCount("kevo.compaction.efficiency.compression_ratio"); count != 1 {
		t.Errorf("Expected 1 compression ratio record, got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.compaction.efficiency.space_reclaimed"); count != 1 {
		t.Errorf("Expected 1 space reclaimed record, got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.compaction.efficiency.duplicates_removed"); count != 1 {
		t.Errorf("Expected 1 duplicates removed record, got %d", count)
	}

	// Test RecordLevelStats
	metrics.RecordLevelStats(ctx, 1, 5, 1024000, 1000)

	if count := mockTel.getHistogramCount("kevo.compaction.level.file_count"); count != 1 {
		t.Errorf("Expected 1 level file count record, got %d", count)
	}

	if count := mockTel.getHistogramCount("kevo.compaction.level.total_size"); count != 1 {
		t.Errorf("Expected 1 level total size record, got %d", count)
	}

	if count := mockTel.getHistogramCount("kevo.compaction.level.key_count"); count != 1 {
		t.Errorf("Expected 1 level key count record, got %d", count)
	}

	// Test RecordCompactionQueue
	metrics.RecordCompactionQueue(ctx, 3, 2048000)

	if count := mockTel.getHistogramCount("kevo.compaction.queue.depth"); count != 1 {
		t.Errorf("Expected 1 queue depth record, got %d", count)
	}

	if count := mockTel.getHistogramCount("kevo.compaction.queue.pending_bytes"); count != 1 {
		t.Errorf("Expected 1 pending bytes record, got %d", count)
	}

	// Test Close
	if err := metrics.Close(); err != nil {
		t.Errorf("Expected nil error from Close(), got %v", err)
	}
}

// TestNoopCompactionMetrics verifies that no-op implementation works correctly
func TestNoopCompactionMetrics(t *testing.T) {
	metrics := NewNoopCompactionMetrics()
	ctx := context.Background()

	// All calls should succeed without panics
	metrics.RecordCompactionStart(ctx, 1, "tiered", 3, 1024000)
	metrics.RecordCompactionComplete(ctx, 150*time.Millisecond, 1024000, 512000, 50, true)
	metrics.RecordLevelTransition(ctx, 0, 1, 512000)
	metrics.RecordTombstoneCleanup(ctx, 25, 1)

	levelSizes := map[int]int64{0: 2048000, 1: 1024000}
	metrics.RecordStrategyDecision(ctx, "tiered", "size_ratio", "high", levelSizes)

	metrics.RecordFileOperations(ctx, "create", 2, 512000, 1)
	metrics.RecordCompactionEfficiency(ctx, 0.8, 256000, 10)
	metrics.RecordLevelStats(ctx, 1, 5, 1024000, 1000)
	metrics.RecordCompactionQueue(ctx, 3, 2048000)

	if err := metrics.Close(); err != nil {
		t.Errorf("Expected nil error from no-op Close(), got %v", err)
	}
}

// TestCompactionCoordinatorTelemetryIntegration tests coordinator with telemetry integration
func TestCompactionCoordinatorTelemetryIntegration(t *testing.T) {
	// Test basic telemetry integration without complex setup
	mockTel := newMockTelemetryServer()
	metrics := NewCompactionMetrics(mockTel)

	// Test coordinator SetTelemetry
	coordinator := &DefaultCompactionCoordinator{
		metrics: NewNoopCompactionMetrics(),
	}
	coordinator.SetTelemetry(metrics)

	// Verify metrics were set (indirectly by using them)
	ctx := context.Background()
	coordinator.recordQueueStateMetrics(ctx)

	if count := mockTel.getHistogramCount("kevo.compaction.queue.depth"); count != 1 {
		t.Errorf("Expected 1 queue depth record after SetTelemetry, got %d", count)
	}
}

// TestCompactionExecutorTelemetryIntegration tests executor with telemetry integration
func TestCompactionExecutorTelemetryIntegration(t *testing.T) {
	mockTel := newMockTelemetryServer()
	metrics := NewCompactionMetrics(mockTel)

	// Test executor SetTelemetry
	executor := &DefaultCompactionExecutor{
		metrics: NewNoopCompactionMetrics(),
	}
	executor.SetTelemetry(metrics)

	// Verify metrics were set (indirectly by using them)
	ctx := context.Background()
	executor.recordFileOperationsMetrics(ctx, "test", 1, 1024, 1)

	if count := mockTel.getCounterCount("kevo.compaction.file.operations.count"); count != 1 {
		t.Errorf("Expected 1 file operations record after SetTelemetry, got %d", count)
	}
}

// TestCompactionStrategyTelemetryIntegration tests strategy with telemetry integration
func TestCompactionStrategyTelemetryIntegration(t *testing.T) {
	mockTel := newMockTelemetryServer()
	metrics := NewCompactionMetrics(mockTel)

	// Test strategy SetTelemetry
	strategy := &TieredCompactionStrategy{
		BaseCompactionStrategy: &BaseCompactionStrategy{
			levels: make(map[int][]*SSTableInfo),
		},
		metrics: NewNoopCompactionMetrics(),
	}
	strategy.SetTelemetry(metrics)

	// Verify metrics were set (indirectly by using them)
	ctx := context.Background()
	levelSizes := map[int]int64{0: 1024, 1: 2048}
	strategy.recordStrategyDecisionMetrics(ctx, "tiered", "test", "low", levelSizes)

	if count := mockTel.getCounterCount("kevo.compaction.strategy.decision.count"); count != 1 {
		t.Errorf("Expected 1 strategy decision record after SetTelemetry, got %d", count)
	}
}

// TestCompactionWithoutTelemetry tests backward compatibility without telemetry
func TestCompactionWithoutTelemetry(t *testing.T) {
	// Test coordinator without telemetry (should work with no-op metrics)
	coordinator := &DefaultCompactionCoordinator{
		metrics: NewNoopCompactionMetrics(),
	}

	ctx := context.Background()
	coordinator.recordQueueStateMetrics(ctx) // Should not panic

	// Test executor without telemetry
	executor := &DefaultCompactionExecutor{
		metrics: NewNoopCompactionMetrics(),
	}

	executor.recordFileOperationsMetrics(ctx, "test", 1, 1024, 1) // Should not panic

	// Test strategy without telemetry
	strategy := &TieredCompactionStrategy{
		BaseCompactionStrategy: &BaseCompactionStrategy{
			levels: make(map[int][]*SSTableInfo),
		},
		metrics: NewNoopCompactionMetrics(),
	}

	levelSizes := map[int]int64{0: 1024, 1: 2048}
	strategy.recordStrategyDecisionMetrics(ctx, "tiered", "test", "low", levelSizes) // Should not panic
}

// TestSetTelemetryTypeSafety tests that SetTelemetry only accepts correct types
func TestSetTelemetryTypeSafety(t *testing.T) {
	coordinator := &DefaultCompactionCoordinator{
		metrics: NewNoopCompactionMetrics(),
	}
	executor := &DefaultCompactionExecutor{
		metrics: NewNoopCompactionMetrics(),
	}
	strategy := &TieredCompactionStrategy{
		metrics: NewNoopCompactionMetrics(),
	}

	// Test with correct type
	mockTel := newMockTelemetryServer()
	metrics := NewCompactionMetrics(mockTel)

	coordinator.SetTelemetry(metrics)
	executor.SetTelemetry(metrics)
	strategy.SetTelemetry(metrics)

	// Test with incorrect types (should be ignored)
	coordinator.SetTelemetry("not a metrics interface")
	executor.SetTelemetry(123)
	strategy.SetTelemetry(nil)

	// Components should still work after invalid SetTelemetry calls
	ctx := context.Background()
	coordinator.recordQueueStateMetrics(ctx)
	executor.recordFileOperationsMetrics(ctx, "test", 1, 1024, 1)

	levelSizes := map[int]int64{0: 1024}
	strategy.recordStrategyDecisionMetrics(ctx, "tiered", "test", "low", levelSizes)
}

// TestHelperFunctions tests the helper functions for telemetry attributes
func TestHelperFunctions(t *testing.T) {
	// Test statusToString
	if statusToString(true) != "success" {
		t.Errorf("Expected 'success', got %s", statusToString(true))
	}
	if statusToString(false) != "error" {
		t.Errorf("Expected 'error', got %s", statusToString(false))
	}

	// Test strategyToString
	if strategyToString("tiered") != "tiered" {
		t.Errorf("Expected 'tiered', got %s", strategyToString("tiered"))
	}
	if strategyToString("leveled") != "leveled" {
		t.Errorf("Expected 'leveled', got %s", strategyToString("leveled"))
	}
	if strategyToString("invalid") != "unknown" {
		t.Errorf("Expected 'unknown', got %s", strategyToString("invalid"))
	}

	// Test triggerToString
	if triggerToString("size") != "size" {
		t.Errorf("Expected 'size', got %s", triggerToString("size"))
	}
	if triggerToString("file_count") != "file_count" {
		t.Errorf("Expected 'file_count', got %s", triggerToString("file_count"))
	}
	if triggerToString("invalid") != "unknown" {
		t.Errorf("Expected 'unknown', got %s", triggerToString("invalid"))
	}

	// Test urgencyToString
	if urgencyToString("low") != "low" {
		t.Errorf("Expected 'low', got %s", urgencyToString("low"))
	}
	if urgencyToString("high") != "high" {
		t.Errorf("Expected 'high', got %s", urgencyToString("high"))
	}
	if urgencyToString("invalid") != "unknown" {
		t.Errorf("Expected 'unknown', got %s", urgencyToString("invalid"))
	}
}
