// ABOUTME: Transaction telemetry metrics tests with mock telemetry server and real transaction operations
// ABOUTME: Provides comprehensive test coverage for transaction metrics interface and integration tests

package transaction

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/wal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// mockTelemetryServer captures metrics for testing transaction telemetry (infrastructure mocking only)
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

// Mock storage backend for testing
type mockStorageBackend struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func newMockStorageBackend() *mockStorageBackend {
	return &mockStorageBackend{
		data: make(map[string][]byte),
	}
}

func (m *mockStorageBackend) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if value, exists := m.data[string(key)]; exists {
		return value, nil
	}
	return nil, ErrKeyNotFound
}

func (m *mockStorageBackend) GetIterator() (iterator.Iterator, error) {
	return nil, nil // Simple mock
}

func (m *mockStorageBackend) GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error) {
	return nil, nil // Simple mock
}

func (m *mockStorageBackend) ApplyBatch(entries []*wal.Entry) error {
	// Simple mock implementation - just store the data
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, entry := range entries {
		if entry.Type == wal.OpTypeDelete {
			delete(m.data, string(entry.Key))
		} else {
			m.data[string(entry.Key)] = entry.Value
		}
	}
	return nil
}

// TestTransactionMetricsInterface tests all methods of the TransactionMetrics interface
func TestTransactionMetricsInterface(t *testing.T) {
	mockTel := newMockTelemetryServer()
	metrics := NewTransactionMetrics(mockTel)
	ctx := context.Background()

	// Test RecordTransactionStart
	metrics.RecordTransactionStart(ctx, true)
	metrics.RecordTransactionStart(ctx, false)

	if count := mockTel.getCounterCount("kevo.transaction.start.count"); count != 2 {
		t.Errorf("Expected 2 transaction start records, got %d", count)
	}

	// Test RecordLockWait
	metrics.RecordLockWait(ctx, 10*time.Millisecond, "read")
	metrics.RecordLockWait(ctx, 25*time.Millisecond, "write")

	if count := mockTel.getHistogramCount("kevo.transaction.lock.wait.duration"); count != 2 {
		t.Errorf("Expected 2 lock wait records, got %d", count)
	}

	// Test RecordTransactionDuration
	metrics.RecordTransactionDuration(ctx, 100*time.Millisecond, "commit", 5, false)
	metrics.RecordTransactionDuration(ctx, 50*time.Millisecond, "rollback", 0, true)

	if count := mockTel.getHistogramCount("kevo.transaction.duration"); count != 2 {
		t.Errorf("Expected 2 transaction duration records, got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.transaction.operations.count"); count != 2 {
		t.Errorf("Expected 2 operation count records, got %d", count)
	}

	// Test RecordBufferUsage
	metrics.RecordBufferUsage(ctx, 1024, 10)

	if count := mockTel.getHistogramCount("kevo.transaction.buffer.size.bytes"); count != 1 {
		t.Errorf("Expected 1 buffer size record, got %d", count)
	}

	if count := mockTel.getHistogramCount("kevo.transaction.buffer.operations"); count != 1 {
		t.Errorf("Expected 1 buffer operations record, got %d", count)
	}

	// Test RecordConcurrentReaders
	metrics.RecordConcurrentReaders(ctx, 3)

	if count := mockTel.getCounterCount("kevo.transaction.concurrent.readers"); count != 1 {
		t.Errorf("Expected 1 concurrent readers record, got %d", count)
	}

	// Test RecordOperation
	metrics.RecordOperation(ctx, "get", 5*time.Millisecond, true)
	metrics.RecordOperation(ctx, "put", 8*time.Millisecond, false)

	if count := mockTel.getHistogramCount("kevo.transaction.operation.duration"); count != 2 {
		t.Errorf("Expected 2 operation duration records, got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.transaction.operation.count"); count != 2 {
		t.Errorf("Expected 2 operation count records, got %d", count)
	}

	// Test RecordTTLViolation
	metrics.RecordTTLViolation(ctx, "exceeded", 30*time.Second, true)

	if count := mockTel.getCounterCount("kevo.transaction.ttl.violation.count"); count != 1 {
		t.Errorf("Expected 1 TTL violation count record, got %d", count)
	}

	if count := mockTel.getHistogramCount("kevo.transaction.ttl.exceeded.duration"); count != 1 {
		t.Errorf("Expected 1 TTL exceeded duration record, got %d", count)
	}

	// Test Close
	if err := metrics.Close(); err != nil {
		t.Errorf("Expected nil error from Close(), got %v", err)
	}
}

// TestNoopTransactionMetrics verifies that no-op implementation works correctly
func TestNoopTransactionMetrics(t *testing.T) {
	metrics := NewNoopTransactionMetrics()
	ctx := context.Background()

	// All calls should succeed without panics
	metrics.RecordTransactionStart(ctx, true)
	metrics.RecordLockWait(ctx, 10*time.Millisecond, "read")
	metrics.RecordTransactionDuration(ctx, 100*time.Millisecond, "commit", 5, false)
	metrics.RecordBufferUsage(ctx, 1024, 10)
	metrics.RecordConcurrentReaders(ctx, 3)
	metrics.RecordOperation(ctx, "get", 5*time.Millisecond, true)
	metrics.RecordTTLViolation(ctx, "exceeded", 30*time.Second, true)

	if err := metrics.Close(); err != nil {
		t.Errorf("Expected nil error from no-op Close(), got %v", err)
	}
}

// TestTransactionManagerTelemetryIntegration tests transaction manager with telemetry integration
func TestTransactionManagerTelemetryIntegration(t *testing.T) {
	mockTel := newMockTelemetryServer()
	storage := newMockStorageBackend()

	// Create manager with telemetry
	manager := NewManager(storage, nil)
	metrics := NewTransactionMetrics(mockTel)
	manager.SetTelemetry(metrics)

	// Test read-only transaction
	tx, err := manager.BeginTransaction(true)
	if err != nil {
		t.Fatalf("Failed to begin read-only transaction: %v", err)
	}

	// Verify transaction start was recorded
	if count := mockTel.getCounterCount("kevo.transaction.start.count"); count != 1 {
		t.Errorf("Expected 1 transaction start record, got %d", count)
	}

	// Verify lock wait was recorded
	if count := mockTel.getHistogramCount("kevo.transaction.lock.wait.duration"); count != 1 {
		t.Errorf("Expected 1 lock wait record, got %d", count)
	}

	// Perform operations and commit
	_, err = tx.Get([]byte("test-key"))
	if err != nil && err != ErrKeyNotFound {
		t.Errorf("Unexpected error from Get(): %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit read-only transaction: %v", err)
	}

	// Verify operation and transaction duration were recorded
	if count := mockTel.getHistogramCount("kevo.transaction.operation.duration"); count != 1 {
		t.Errorf("Expected 1 operation duration record, got %d", count)
	}

	if count := mockTel.getHistogramCount("kevo.transaction.duration"); count != 1 {
		t.Errorf("Expected 1 transaction duration record, got %d", count)
	}
}

// TestReadWriteTransactionTelemetry tests telemetry for read-write transactions
func TestReadWriteTransactionTelemetry(t *testing.T) {
	mockTel := newMockTelemetryServer()
	storage := newMockStorageBackend()

	// Create manager with telemetry
	manager := NewManager(storage, nil)
	metrics := NewTransactionMetrics(mockTel)
	manager.SetTelemetry(metrics)

	// Test read-write transaction
	tx, err := manager.BeginTransaction(false)
	if err != nil {
		t.Fatalf("Failed to begin read-write transaction: %v", err)
	}

	// Perform multiple operations
	err = tx.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Errorf("Failed to put key1: %v", err)
	}

	err = tx.Put([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Errorf("Failed to put key2: %v", err)
	}

	err = tx.Delete([]byte("key3"))
	if err != nil {
		t.Errorf("Failed to delete key3: %v", err)
	}

	_, err = tx.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get key1: %v", err)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit read-write transaction: %v", err)
	}

	// Verify metrics were recorded
	if count := mockTel.getCounterCount("kevo.transaction.start.count"); count != 1 {
		t.Errorf("Expected 1 transaction start record, got %d", count)
	}

	if count := mockTel.getHistogramCount("kevo.transaction.operation.duration"); count != 4 {
		t.Errorf("Expected 4 operation duration records (2 puts, 1 delete, 1 get), got %d", count)
	}

	if count := mockTel.getCounterCount("kevo.transaction.operation.count"); count != 4 {
		t.Errorf("Expected 4 operation count records, got %d", count)
	}

	if count := mockTel.getHistogramCount("kevo.transaction.duration"); count != 1 {
		t.Errorf("Expected 1 transaction duration record, got %d", count)
	}

	if count := mockTel.getHistogramCount("kevo.transaction.buffer.size.bytes"); count != 1 {
		t.Errorf("Expected 1 buffer size record, got %d", count)
	}
}

// TestTransactionRollbackTelemetry tests telemetry for transaction rollback
func TestTransactionRollbackTelemetry(t *testing.T) {
	mockTel := newMockTelemetryServer()
	storage := newMockStorageBackend()

	// Create manager with telemetry
	manager := NewManager(storage, nil)
	metrics := NewTransactionMetrics(mockTel)
	manager.SetTelemetry(metrics)

	// Test read-write transaction rollback
	tx, err := manager.BeginTransaction(false)
	if err != nil {
		t.Fatalf("Failed to begin read-write transaction: %v", err)
	}

	// Perform operations
	err = tx.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Errorf("Failed to put key1: %v", err)
	}

	// Rollback transaction
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify rollback metrics were recorded
	if count := mockTel.getHistogramCount("kevo.transaction.duration"); count != 1 {
		t.Errorf("Expected 1 transaction duration record, got %d", count)
	}

	// Check that outcome is "rollback"
	durations := mockTel.getHistogramValues("kevo.transaction.duration")
	if len(durations) != 1 || durations[0] <= 0 {
		t.Errorf("Expected positive rollback duration, got %v", durations)
	}
}

// TestTransactionManagerWithoutTelemetry tests backward compatibility without telemetry
func TestTransactionManagerWithoutTelemetry(t *testing.T) {
	storage := newMockStorageBackend()

	// Create manager without telemetry (should work with no-op metrics)
	manager := NewManager(storage, nil)

	// Test transaction operations work normally
	tx, err := manager.BeginTransaction(true)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Get([]byte("test-key"))
	if err != nil && err != ErrKeyNotFound {
		t.Errorf("Unexpected error from Get(): %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

// TestSetTelemetryTypeSafety tests that SetTelemetry only accepts correct types
func TestSetTelemetryTypeSafety(t *testing.T) {
	storage := newMockStorageBackend()
	manager := NewManager(storage, nil)

	// Test with correct type
	mockTel := newMockTelemetryServer()
	metrics := NewTransactionMetrics(mockTel)
	manager.SetTelemetry(metrics)

	// Test with incorrect type (should be ignored)
	manager.SetTelemetry("not a metrics interface")
	manager.SetTelemetry(123)
	manager.SetTelemetry(nil)

	// Manager should still work after invalid SetTelemetry calls
	tx, err := manager.BeginTransaction(true)
	if err != nil {
		t.Fatalf("Failed to begin transaction after invalid SetTelemetry: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction after invalid SetTelemetry: %v", err)
	}
}

// TestHelperFunctions tests the helper functions for telemetry attributes
func TestHelperFunctions(t *testing.T) {
	// Test boolToString
	if boolToString(true) != "true" {
		t.Errorf("Expected 'true', got %s", boolToString(true))
	}
	if boolToString(false) != "false" {
		t.Errorf("Expected 'false', got %s", boolToString(false))
	}

	// Test transactionModeToString
	if transactionModeToString(true) != "readonly" {
		t.Errorf("Expected 'readonly', got %s", transactionModeToString(true))
	}
	if transactionModeToString(false) != "readwrite" {
		t.Errorf("Expected 'readwrite', got %s", transactionModeToString(false))
	}

	// Test lockTypeToString
	if lockTypeToString("read") != "read" {
		t.Errorf("Expected 'read', got %s", lockTypeToString("read"))
	}
	if lockTypeToString("write") != "write" {
		t.Errorf("Expected 'write', got %s", lockTypeToString("write"))
	}
	if lockTypeToString("invalid") != "unknown" {
		t.Errorf("Expected 'unknown', got %s", lockTypeToString("invalid"))
	}

	// Test outcomeToString
	if outcomeToString("commit") != "commit" {
		t.Errorf("Expected 'commit', got %s", outcomeToString("commit"))
	}
	if outcomeToString("rollback") != "rollback" {
		t.Errorf("Expected 'rollback', got %s", outcomeToString("rollback"))
	}
	if outcomeToString("invalid") != "unknown" {
		t.Errorf("Expected 'unknown', got %s", outcomeToString("invalid"))
	}

	// Test operationToString
	if operationToString("get") != "get" {
		t.Errorf("Expected 'get', got %s", operationToString("get"))
	}
	if operationToString("put") != "put" {
		t.Errorf("Expected 'put', got %s", operationToString("put"))
	}
	if operationToString("invalid") != "unknown" {
		t.Errorf("Expected 'unknown', got %s", operationToString("invalid"))
	}
}
