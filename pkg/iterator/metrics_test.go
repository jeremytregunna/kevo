// ABOUTME: Iterator telemetry metrics tests with mock telemetry server and real iterator operations
// ABOUTME: Provides comprehensive test coverage for iterator metrics interface and integration tests

package iterator

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// mockTelemetryServer captures metrics for testing iterator telemetry (infrastructure mocking only)
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
	values := make([]float64, 0, len(m.histograms[name]))
	for _, v := range m.histograms[name] {
		values = append(values, v.value)
	}
	return values
}

func (m *mockTelemetryServer) getCounterSum(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var sum int64
	for _, v := range m.counters[name] {
		sum += v.value
	}
	return sum
}

// Simple test iterator for testing iterator metrics
type testIterator struct {
	keys   []string
	values []string
	pos    int
	valid  bool
}

func newTestIterator(keyValues map[string]string) iterator.Iterator {
	keys := make([]string, 0, len(keyValues))
	for k := range keyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	values := make([]string, len(keys))
	for i, k := range keys {
		values[i] = keyValues[k]
	}

	return &testIterator{
		keys:   keys,
		values: values,
		pos:    -1,
		valid:  false,
	}
}

func (t *testIterator) SeekToFirst() {
	if len(t.keys) > 0 {
		t.pos = 0
		t.valid = true
	} else {
		t.pos = -1
		t.valid = false
	}
}

func (t *testIterator) SeekToLast() {
	if len(t.keys) > 0 {
		t.pos = len(t.keys) - 1
		t.valid = true
	} else {
		t.pos = -1
		t.valid = false
	}
}

func (t *testIterator) Seek(target []byte) bool {
	targetStr := string(target)
	for i, k := range t.keys {
		if k >= targetStr {
			t.pos = i
			t.valid = true
			return true
		}
	}
	t.pos = -1
	t.valid = false
	return false
}

func (t *testIterator) Next() bool {
	if t.pos < len(t.keys)-1 {
		t.pos++
		t.valid = true
		return true
	}
	t.pos = -1
	t.valid = false
	return false
}

func (t *testIterator) Key() []byte {
	if !t.valid || t.pos < 0 || t.pos >= len(t.keys) {
		return nil
	}
	return []byte(t.keys[t.pos])
}

func (t *testIterator) Value() []byte {
	if !t.valid || t.pos < 0 || t.pos >= len(t.values) {
		return nil
	}
	return []byte(t.values[t.pos])
}

func (t *testIterator) Valid() bool {
	return t.valid && t.pos >= 0 && t.pos < len(t.keys)
}

func (t *testIterator) IsTombstone() bool {
	if !t.Valid() {
		return false
	}
	return t.values[t.pos] == ""
}

// Test all IteratorMetrics interface methods with mock server verification
func TestIteratorMetrics_AllMethods(t *testing.T) {
	server := newMockTelemetryServer()
	metrics := NewIteratorMetrics(server)

	ctx := context.Background()
	duration := 100 * time.Millisecond

	// Test RecordSeek
	metrics.RecordSeek(ctx, duration, true, 5)
	if count := server.getHistogramCount("kevo.iterator.seek.duration"); count != 1 {
		t.Errorf("Expected 1 seek duration metric, got %d", count)
	}
	if count := server.getCounterCount("kevo.iterator.operations.total"); count != 1 {
		t.Errorf("Expected 1 operation count metric, got %d", count)
	}

	// Test RecordNext
	metrics.RecordNext(ctx, duration, true)
	if count := server.getHistogramCount("kevo.iterator.next.duration"); count != 1 {
		t.Errorf("Expected 1 next duration metric, got %d", count)
	}
	if count := server.getCounterCount("kevo.iterator.operations.total"); count != 2 {
		t.Errorf("Expected 2 operation count metrics, got %d", count)
	}

	// Test RecordRangeScan
	metrics.RecordRangeScan(ctx, duration, 10, []byte("start"), []byte("end"))
	if count := server.getHistogramCount("kevo.iterator.range_scan.duration"); count != 1 {
		t.Errorf("Expected 1 range scan duration metric, got %d", count)
	}

	// Test RecordHierarchicalMerge
	metrics.RecordHierarchicalMerge(ctx, 3, duration)
	if count := server.getHistogramCount("kevo.iterator.hierarchical.merge.duration"); count != 1 {
		t.Errorf("Expected 1 hierarchical merge duration metric, got %d", count)
	}

	// Test RecordIteratorType
	metrics.RecordIteratorType(ctx, "hierarchical", 3)
	if count := server.getCounterCount("kevo.iterator.type.instantiated"); count != 1 {
		t.Errorf("Expected 1 iterator type metric, got %d", count)
	}

	// Test RecordBoundedOperation
	metrics.RecordBoundedOperation(ctx, "seek", duration, true)
	if count := server.getHistogramCount("kevo.iterator.bounded.operation.duration"); count != 1 {
		t.Errorf("Expected 1 bounded operation duration metric, got %d", count)
	}

	// Test RecordFilteredOperation
	metrics.RecordFilteredOperation(ctx, "next", duration, true)
	if count := server.getHistogramCount("kevo.iterator.filtered.operation.duration"); count != 1 {
		t.Errorf("Expected 1 filtered operation duration metric, got %d", count)
	}

	// Test Close
	if err := metrics.Close(); err != nil {
		t.Errorf("Expected nil error from Close(), got %v", err)
	}
}

// Test no-op metrics implementation
func TestNoopIteratorMetrics(t *testing.T) {
	metrics := NewNoopIteratorMetrics()
	ctx := context.Background()
	duration := 100 * time.Millisecond

	// All operations should be no-op and not panic
	metrics.RecordSeek(ctx, duration, true, 5)
	metrics.RecordNext(ctx, duration, true)
	metrics.RecordRangeScan(ctx, duration, 10, []byte("start"), []byte("end"))
	metrics.RecordHierarchicalMerge(ctx, 3, duration)
	metrics.RecordIteratorType(ctx, "hierarchical", 3)
	metrics.RecordBoundedOperation(ctx, "seek", duration, true)
	metrics.RecordFilteredOperation(ctx, "next", duration, true)

	if err := metrics.Close(); err != nil {
		t.Errorf("Expected nil error from Close(), got %v", err)
	}
}

// Test NewIteratorMetrics with nil telemetry returns no-op
func TestNewIteratorMetrics_NilTelemetry(t *testing.T) {
	metrics := NewIteratorMetrics(nil)
	if metrics == nil {
		t.Fatal("Expected non-nil metrics")
	}

	// Should be no-op implementation
	ctx := context.Background()
	duration := 100 * time.Millisecond
	metrics.RecordSeek(ctx, duration, true, 5) // Should not panic
}

// Integration test with real HierarchicalIterator operations
func TestHierarchicalIterator_SeekTelemetry(t *testing.T) {
	server := newMockTelemetryServer()
	metrics := NewIteratorMetrics(server)

	// Create test iterators
	iter1 := newTestIterator(map[string]string{
		"key1": "value1",
		"key3": "value3",
		"key5": "value5",
	})
	iter2 := newTestIterator(map[string]string{
		"key2": "value2",
		"key4": "value4",
		"key6": "value6",
	})

	// Create hierarchical iterator with real business logic
	hierarchical := NewHierarchicalIterator([]iterator.Iterator{iter1, iter2})
	hierarchical.SetTelemetry(metrics)

	// Perform real seek operation
	found := hierarchical.Seek([]byte("key3"))
	if !found {
		t.Fatal("Expected to find key3")
	}

	// Verify telemetry was recorded from real operations
	if count := server.getHistogramCount("kevo.iterator.seek.duration"); count == 0 {
		t.Error("Expected seek duration metrics to be recorded")
	}

	if count := server.getCounterCount("kevo.iterator.operations.total"); count == 0 {
		t.Error("Expected operation count metrics to be recorded")
	}

	// Check that iterator type was recorded
	if count := server.getCounterCount("kevo.iterator.type.instantiated"); count == 0 {
		t.Error("Expected iterator type metrics to be recorded")
	}
}

// Integration test with real HierarchicalIterator Next operations
func TestHierarchicalIterator_NextTelemetry(t *testing.T) {
	server := newMockTelemetryServer()
	metrics := NewIteratorMetrics(server)

	// Create test iterator with multiple keys
	iter := newTestIterator(map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	})

	// Create hierarchical iterator with real business logic
	hierarchical := NewHierarchicalIterator([]iterator.Iterator{iter})
	hierarchical.SetTelemetry(metrics)

	// Perform real iterator operations
	hierarchical.SeekToFirst()
	var keyCount int
	for hierarchical.Valid() {
		keyCount++
		if !hierarchical.Next() {
			break
		}
	}

	if keyCount != 3 {
		t.Errorf("Expected to iterate over 3 keys, got %d", keyCount)
	}

	// Verify telemetry was recorded from real operations
	if count := server.getHistogramCount("kevo.iterator.next.duration"); count == 0 {
		t.Error("Expected next duration metrics to be recorded")
	}

	// Should have multiple operation counts (next calls)
	if count := server.getCounterCount("kevo.iterator.operations.total"); count == 0 {
		t.Error("Expected operation count metrics to be recorded")
	}
}

// Test hierarchical merge operation telemetry
func TestHierarchicalIterator_MergeTelemetry(t *testing.T) {
	server := newMockTelemetryServer()
	metrics := NewIteratorMetrics(server)

	// Create multiple test iterators with overlapping keys
	iter1 := newTestIterator(map[string]string{
		"key1": "value1a", // Will be overridden by newer source
		"key3": "value3a",
	})
	iter2 := newTestIterator(map[string]string{
		"key1": "value1b", // Newer value
		"key2": "value2b",
	})

	// Create hierarchical iterator with real business logic (iter2 is newer)
	hierarchical := NewHierarchicalIterator([]iterator.Iterator{iter2, iter1})
	hierarchical.SetTelemetry(metrics)

	// Perform operations that trigger merge logic
	hierarchical.SeekToFirst()

	// Should get key1 with value1b (from newer iterator)
	if !hierarchical.Valid() {
		t.Fatal("Expected valid iterator")
	}
	if key := string(hierarchical.Key()); key != "key1" {
		t.Errorf("Expected key1, got %s", key)
	}
	if value := string(hierarchical.Value()); value != "value1b" {
		t.Errorf("Expected value1b, got %s", value)
	}

	// Verify hierarchical merge telemetry was recorded
	if count := server.getHistogramCount("kevo.iterator.hierarchical.merge.duration"); count == 0 {
		t.Error("Expected hierarchical merge duration metrics to be recorded")
	}
}

// Test iterator without telemetry works correctly (backward compatibility)
func TestHierarchicalIterator_WithoutTelemetry(t *testing.T) {
	// Create test iterator
	iter := newTestIterator(map[string]string{
		"key1": "value1",
		"key2": "value2",
	})

	// Create hierarchical iterator without telemetry
	hierarchical := NewHierarchicalIterator([]iterator.Iterator{iter})
	// Don't call SetTelemetry - should use no-op metrics

	// Should work correctly without telemetry
	hierarchical.SeekToFirst()
	if !hierarchical.Valid() {
		t.Fatal("Expected valid iterator")
	}

	if key := string(hierarchical.Key()); key != "key1" {
		t.Errorf("Expected key1, got %s", key)
	}

	// Next should work
	if !hierarchical.Next() {
		t.Fatal("Expected successful Next()")
	}
	if key := string(hierarchical.Key()); key != "key2" {
		t.Errorf("Expected key2, got %s", key)
	}
}

// Test SetTelemetry type assertion safety
func TestHierarchicalIterator_SetTelemetryTypeSafety(t *testing.T) {
	iter := newTestIterator(map[string]string{"key1": "value1"})
	hierarchical := NewHierarchicalIterator([]iterator.Iterator{iter})

	// Should handle invalid type gracefully
	hierarchical.SetTelemetry("invalid_type")
	hierarchical.SetTelemetry(42)
	hierarchical.SetTelemetry(nil)

	// Should still work correctly
	hierarchical.SeekToFirst()
	if !hierarchical.Valid() {
		t.Error("Iterator should still work after invalid SetTelemetry calls")
	}
}

// Test concurrent access for race conditions
func TestHierarchicalIterator_ConcurrentAccess(t *testing.T) {
	server := newMockTelemetryServer()
	metrics := NewIteratorMetrics(server)

	// Create test iterator
	iter := newTestIterator(map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	})

	hierarchical := NewHierarchicalIterator([]iterator.Iterator{iter})
	hierarchical.SetTelemetry(metrics)

	// Test concurrent access
	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine performs iterator operations
			hierarchical.SeekToFirst()
			for j := 0; j < 5 && hierarchical.Valid(); j++ {
				_ = hierarchical.Key()
				_ = hierarchical.Value()
				hierarchical.Next()
			}
		}()
	}

	wg.Wait()

	// Should have recorded metrics from concurrent operations
	if count := server.getCounterCount("kevo.iterator.operations.total"); count == 0 {
		t.Error("Expected operation metrics from concurrent access")
	}
}

// Benchmark iterator telemetry overhead using real operations
func BenchmarkIteratorTelemetry_SeekOverhead(b *testing.B) {
	server := newMockTelemetryServer()
	metrics := NewIteratorMetrics(server)

	// Create test iterator with many keys
	keyValues := make(map[string]string)
	for i := 0; i < 1000; i++ {
		keyValues[fmt.Sprintf("key%04d", i)] = fmt.Sprintf("value%d", i)
	}
	iter := newTestIterator(keyValues)

	hierarchical := NewHierarchicalIterator([]iterator.Iterator{iter})
	hierarchical.SetTelemetry(metrics)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%04d", i%1000)
		hierarchical.Seek([]byte(key))
	}
}

// Benchmark iterator without telemetry for comparison
func BenchmarkIterator_WithoutTelemetry(b *testing.B) {
	// Create test iterator with many keys
	keyValues := make(map[string]string)
	for i := 0; i < 1000; i++ {
		keyValues[fmt.Sprintf("key%04d", i)] = fmt.Sprintf("value%d", i)
	}
	iter := newTestIterator(keyValues)

	hierarchical := NewHierarchicalIterator([]iterator.Iterator{iter})
	// No telemetry injection

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%04d", i%1000)
		hierarchical.Seek([]byte(key))
	}
}
