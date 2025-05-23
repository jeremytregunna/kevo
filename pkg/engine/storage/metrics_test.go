// ABOUTME: Comprehensive test suite for storage telemetry metrics implementation
// ABOUTME: Tests all StorageMetrics interface methods with real storage operations and mock telemetry server

package storage

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/stats"
	"github.com/KevoDB/kevo/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// mockTelemetryServer provides a test implementation of telemetry.Telemetry for capturing metrics.
// This is infrastructure mocking only - all storage operations use real business logic.
type mockTelemetryServer struct {
	mu       sync.RWMutex
	counters map[string][]mockCounterCall
	histograms map[string][]mockHistogramCall
}

type mockCounterCall struct {
	value int64
	attrs []attribute.KeyValue
}

type mockHistogramCall struct {
	value float64
	attrs []attribute.KeyValue
}

func newMockTelemetryServer() *mockTelemetryServer {
	return &mockTelemetryServer{
		counters:   make(map[string][]mockCounterCall),
		histograms: make(map[string][]mockHistogramCall),
	}
}

func (m *mockTelemetryServer) RecordHistogram(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.histograms[name] = append(m.histograms[name], mockHistogramCall{
		value: value,
		attrs: attrs,
	})
}

func (m *mockTelemetryServer) RecordCounter(ctx context.Context, name string, value int64, attrs ...attribute.KeyValue) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name] = append(m.counters[name], mockCounterCall{
		value: value,
		attrs: attrs,
	})
}

func (m *mockTelemetryServer) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return ctx, nil
}

func (m *mockTelemetryServer) Shutdown(ctx context.Context) error {
	return nil
}

func (m *mockTelemetryServer) getCounterCalls(name string) []mockCounterCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	calls, exists := m.counters[name]
	if !exists {
		return nil
	}
	// Return a copy to avoid race conditions
	result := make([]mockCounterCall, len(calls))
	copy(result, calls)
	return result
}

func (m *mockTelemetryServer) getHistogramCalls(name string) []mockHistogramCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	calls, exists := m.histograms[name]
	if !exists {
		return nil
	}
	// Return a copy to avoid race conditions
	result := make([]mockHistogramCall, len(calls))
	copy(result, calls)
	return result
}

func (m *mockTelemetryServer) getCounterTotal(name string) int64 {
	calls := m.getCounterCalls(name)
	var total int64
	for _, call := range calls {
		total += call.value
	}
	return total
}

func (m *mockTelemetryServer) hasAttribute(attrs []attribute.KeyValue, key, value string) bool {
	for _, attr := range attrs {
		if attr.Key == attribute.Key(key) {
			// Handle both string and bool attribute values
			if attr.Value.AsString() == value {
				return true
			}
			// For boolean attributes
			if value == "true" && attr.Value.AsBool() {
				return true
			}
			if value == "false" && !attr.Value.AsBool() {
				return true
			}
		}
	}
	return false
}

// Test StorageMetrics interface methods with mock telemetry
func TestStorageMetrics_InterfaceMethods(t *testing.T) {
	mockTel := newMockTelemetryServer()
	metrics := NewStorageMetrics(mockTel)
	ctx := context.Background()
	
	// Test RecordGet
	metrics.RecordGet(ctx, 100*time.Millisecond, "memtable", true)
	
	// Verify histogram recording
	histCalls := mockTel.getHistogramCalls("kevo.storage.get.duration")
	if len(histCalls) != 1 {
		t.Fatalf("expected 1 histogram call, got %d", len(histCalls))
	}
	if histCalls[0].value != 0.1 { // 100ms = 0.1 seconds
		t.Errorf("expected histogram value 0.1, got %f", histCalls[0].value)
	}
	if !mockTel.hasAttribute(histCalls[0].attrs, telemetry.AttrComponent, telemetry.ComponentStorage) {
		t.Error("expected component attribute to be storage")
	}
	if !mockTel.hasAttribute(histCalls[0].attrs, telemetry.AttrLayer, "memtable") {
		t.Error("expected layer attribute to be memtable")
	}
	
	// Verify counter recording for operation count
	counterCalls := mockTel.getCounterCalls("kevo.storage.operations.total")
	if len(counterCalls) != 1 {
		t.Fatalf("expected 1 counter call, got %d", len(counterCalls))
	}
	if counterCalls[0].value != 1 {
		t.Errorf("expected counter value 1, got %d", counterCalls[0].value)
	}
	
	// Test RecordPut
	metrics.RecordPut(ctx, 50*time.Millisecond, 1024)
	
	putHistCalls := mockTel.getHistogramCalls("kevo.storage.put.duration")
	if len(putHistCalls) != 1 {
		t.Fatalf("expected 1 put histogram call, got %d", len(putHistCalls))
	}
	if putHistCalls[0].value != 0.05 { // 50ms = 0.05 seconds
		t.Errorf("expected put histogram value 0.05, got %f", putHistCalls[0].value)
	}
	
	putBytesCalls := mockTel.getCounterCalls("kevo.storage.put.bytes")
	if len(putBytesCalls) != 1 {
		t.Fatalf("expected 1 put bytes call, got %d", len(putBytesCalls))
	}
	if putBytesCalls[0].value != 1024 {
		t.Errorf("expected put bytes value 1024, got %d", putBytesCalls[0].value)
	}
	
	// Test RecordDelete
	metrics.RecordDelete(ctx, 25*time.Millisecond)
	
	deleteHistCalls := mockTel.getHistogramCalls("kevo.storage.delete.duration")
	if len(deleteHistCalls) != 1 {
		t.Fatalf("expected 1 delete histogram call, got %d", len(deleteHistCalls))
	}
	if deleteHistCalls[0].value != 0.025 { // 25ms = 0.025 seconds
		t.Errorf("expected delete histogram value 0.025, got %f", deleteHistCalls[0].value)
	}
	
	// Test RecordFlush
	metrics.RecordFlush(ctx, 500*time.Millisecond, 2048, 1536)
	
	flushHistCalls := mockTel.getHistogramCalls("kevo.storage.flush.duration")
	if len(flushHistCalls) != 1 {
		t.Fatalf("expected 1 flush histogram call, got %d", len(flushHistCalls))
	}
	if flushHistCalls[0].value != 0.5 { // 500ms = 0.5 seconds
		t.Errorf("expected flush histogram value 0.5, got %f", flushHistCalls[0].value)
	}
	
	memTableSizeCalls := mockTel.getHistogramCalls("kevo.storage.flush.memtable_size")
	if len(memTableSizeCalls) != 1 {
		t.Fatalf("expected 1 memtable size call, got %d", len(memTableSizeCalls))
	}
	if memTableSizeCalls[0].value != 2048.0 {
		t.Errorf("expected memtable size value 2048.0, got %f", memTableSizeCalls[0].value)
	}
	
	sstableSizeCalls := mockTel.getHistogramCalls("kevo.storage.flush.sstable_size")
	if len(sstableSizeCalls) != 1 {
		t.Fatalf("expected 1 sstable size call, got %d", len(sstableSizeCalls))
	}
	if sstableSizeCalls[0].value != 1536.0 {
		t.Errorf("expected sstable size value 1536.0, got %f", sstableSizeCalls[0].value)
	}
	
	// Test RecordLayerAccess
	metrics.RecordLayerAccess(ctx, "sstable_l0", "get", false)
	
	layerAccessCalls := mockTel.getCounterCalls("kevo.storage.layer.access")
	if len(layerAccessCalls) != 1 {
		t.Fatalf("expected 1 layer access call, got %d", len(layerAccessCalls))
	}
	if layerAccessCalls[0].value != 1 {
		t.Errorf("expected layer access value 1, got %d", layerAccessCalls[0].value)
	}
	if !mockTel.hasAttribute(layerAccessCalls[0].attrs, telemetry.AttrLayer, "sstable_l0") {
		t.Error("expected layer attribute to be sstable_l0")
	}
	
	// Test Close
	err := metrics.Close()
	if err != nil {
		t.Errorf("expected no error from Close(), got %v", err)
	}
}

// Test NoopStorageMetrics implementation
func TestNoopStorageMetrics(t *testing.T) {
	metrics := NewNoopStorageMetrics()
	ctx := context.Background()
	
	// All operations should be no-op and not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("noop metrics should not panic, but got: %v", r)
		}
	}()
	
	metrics.RecordGet(ctx, time.Millisecond, "memtable", true)
	metrics.RecordPut(ctx, time.Millisecond, 100)
	metrics.RecordDelete(ctx, time.Millisecond)
	metrics.RecordFlush(ctx, time.Millisecond, 100, 50)
	metrics.RecordLayerAccess(ctx, "memtable", "get", true)
	err := metrics.Close()
	if err != nil {
		t.Errorf("expected no error from noop Close(), got %v", err)
	}
}

// Test NewStorageMetrics with nil telemetry returns noop
func TestNewStorageMetrics_NilTelemetry(t *testing.T) {
	metrics := NewStorageMetrics(nil)
	if metrics == nil {
		t.Fatal("expected non-nil metrics")
	}
	
	// Should be no-op implementation
	ctx := context.Background()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("noop metrics should not panic, but got: %v", r)
		}
	}()
	
	metrics.RecordGet(ctx, time.Millisecond, "memtable", true)
}

// Test real storage operations with telemetry integration
func TestStorageManager_RealOperationsWithTelemetry(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "storage_telemetry_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	// Create test configuration
	cfg := &config.Config{
		SSTDir:         filepath.Join(tempDir, "sst"),
		WALDir:         filepath.Join(tempDir, "wal"),
		MemTableSize:   1024 * 1024, // 1MB
		MaxMemTables:   5,
		MaxMemTableAge: 300, // 5 minutes
		WALMaxSize:     1024 * 1024, // 1MB
		WALSyncMode:    config.SyncImmediate,
	}
	
	// Create stats collector
	statsCollector := stats.NewAtomicCollector()
	
	// Create storage manager with real implementation
	manager, err := NewManager(cfg, statsCollector)
	if err != nil {
		t.Fatalf("failed to create storage manager: %v", err)
	}
	defer manager.Close()
	
	// Set up telemetry monitoring
	mockTel := newMockTelemetryServer()
	storageMetrics := NewStorageMetrics(mockTel)
	manager.SetTelemetry(storageMetrics)
	
	// Test Put operation with real storage
	testKey := []byte("test-key")
	testValue := []byte("test-value")
	
	err = manager.Put(testKey, testValue)
	if err != nil {
		t.Fatalf("failed to put key-value: %v", err)
	}
	
	// Verify Put metrics were recorded
	putHistCalls := mockTel.getHistogramCalls("kevo.storage.put.duration")
	if len(putHistCalls) < 1 {
		t.Error("Put duration should be recorded")
	} else {
		if putHistCalls[0].value <= 0.0 {
			t.Error("Put duration should be positive")
		}
		if !mockTel.hasAttribute(putHistCalls[0].attrs, telemetry.AttrComponent, telemetry.ComponentStorage) {
			t.Error("Put should have storage component attribute")
		}
	}
	
	putBytesCalls := mockTel.getCounterCalls("kevo.storage.put.bytes")
	if len(putBytesCalls) < 1 {
		t.Error("Put bytes should be recorded")
	} else {
		expectedBytes := int64(len(testKey) + len(testValue))
		if putBytesCalls[0].value != expectedBytes {
			t.Errorf("expected Put bytes %d, got %d", expectedBytes, putBytesCalls[0].value)
		}
	}
	
	// Test Get operation with real storage
	retrievedValue, err := manager.Get(testKey)
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}
	if string(retrievedValue) != string(testValue) {
		t.Errorf("expected value %s, got %s", testValue, retrievedValue)
	}
	
	// Verify Get metrics were recorded
	getHistCalls := mockTel.getHistogramCalls("kevo.storage.get.duration")
	if len(getHistCalls) < 1 {
		t.Error("Get duration should be recorded")
	} else {
		if getHistCalls[0].value <= 0.0 {
			t.Error("Get duration should be positive")
		}
		if !mockTel.hasAttribute(getHistCalls[0].attrs, telemetry.AttrLayer, "memtable") {
			t.Error("Get should have memtable layer attribute")
		}
		if !mockTel.hasAttribute(getHistCalls[0].attrs, "found", "true") {
			t.Error("Get should have found=true attribute")
		}
	}
	
	// Verify layer access was recorded
	layerAccessCalls := mockTel.getCounterCalls("kevo.storage.layer.access")
	if len(layerAccessCalls) < 1 {
		t.Error("Layer access should be recorded")
	}
	
	// Test Delete operation with real storage
	err = manager.Delete(testKey)
	if err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}
	
	// Verify Delete metrics were recorded
	deleteHistCalls := mockTel.getHistogramCalls("kevo.storage.delete.duration")
	if len(deleteHistCalls) < 1 {
		t.Error("Delete duration should be recorded")
	} else {
		if deleteHistCalls[0].value <= 0.0 {
			t.Error("Delete duration should be positive")
		}
	}
	
	// Test Get operation after delete (should not find key)
	_, err = manager.Get(testKey)
	if err == nil {
		t.Error("expected error when getting deleted key")
	}
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

// Test storage manager works without telemetry (backward compatibility)
func TestStorageManager_WithoutTelemetry(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "storage_no_telemetry_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	// Create test configuration
	cfg := &config.Config{
		SSTDir:         filepath.Join(tempDir, "sst"),
		WALDir:         filepath.Join(tempDir, "wal"),
		MemTableSize:   1024 * 1024,
		MaxMemTables:   5,
		MaxMemTableAge: 300,
		WALMaxSize:     1024 * 1024,
		WALSyncMode:    config.SyncImmediate,
	}
	
	// Create stats collector
	statsCollector := stats.NewAtomicCollector()
	
	// Create storage manager without telemetry
	manager, err := NewManager(cfg, statsCollector)
	if err != nil {
		t.Fatalf("failed to create storage manager: %v", err)
	}
	defer manager.Close()
	
	// Test that storage operations work normally without telemetry
	testKey := []byte("test-key")
	testValue := []byte("test-value")
	
	err = manager.Put(testKey, testValue)
	if err != nil {
		t.Fatalf("failed to put key-value: %v", err)
	}
	
	retrievedValue, err := manager.Get(testKey)
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}
	if string(retrievedValue) != string(testValue) {
		t.Errorf("expected value %s, got %s", testValue, retrievedValue)
	}
	
	err = manager.Delete(testKey)
	if err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}
	
	_, err = manager.Get(testKey)
	if err == nil {
		t.Error("expected error when getting deleted key")
	}
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

// Test SetTelemetry type assertion safety
func TestStorageManager_SetTelemetry_TypeAssertion(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "storage_telemetry_type_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	// Create test configuration
	cfg := &config.Config{
		SSTDir:         filepath.Join(tempDir, "sst"),
		WALDir:         filepath.Join(tempDir, "wal"),
		MemTableSize:   1024 * 1024,
		MaxMemTables:   5,
		MaxMemTableAge: 300,
		WALMaxSize:     1024 * 1024,
		WALSyncMode:    config.SyncImmediate,
	}
	
	// Create stats collector
	statsCollector := stats.NewAtomicCollector()
	
	// Create storage manager
	manager, err := NewManager(cfg, statsCollector)
	if err != nil {
		t.Fatalf("failed to create storage manager: %v", err)
	}
	defer manager.Close()
	
	// Test SetTelemetry with wrong type - should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("SetTelemetry with wrong type should not panic, but got: %v", r)
		}
	}()
	manager.SetTelemetry("invalid-type")
	
	// Test SetTelemetry with nil - should not panic
	manager.SetTelemetry(nil)
	
	// Test SetTelemetry with correct type
	mockTel := newMockTelemetryServer()
	storageMetrics := NewStorageMetrics(mockTel)
	manager.SetTelemetry(storageMetrics)
	
	// Verify telemetry is working after proper setup
	testKey := []byte("test-key")
	testValue := []byte("test-value")
	
	err = manager.Put(testKey, testValue)
	if err != nil {
		t.Fatalf("failed to put key-value: %v", err)
	}
	
	// Should have recorded metrics
	putHistCalls := mockTel.getHistogramCalls("kevo.storage.put.duration")
	if len(putHistCalls) < 1 {
		t.Error("Put duration should be recorded after proper telemetry setup")
	}
}

// Test helper functions for telemetry attributes
func TestStorageMetrics_HelperFunctions(t *testing.T) {
	// Test getStatusFromFound
	if getStatusFromFound(true) != telemetry.StatusSuccess {
		t.Errorf("expected %s for found=true, got %s", telemetry.StatusSuccess, getStatusFromFound(true))
	}
	if getStatusFromFound(false) != "not_found" {
		t.Errorf("expected 'not_found' for found=false, got %s", getStatusFromFound(false))
	}
	
	// Test getLayerName
	if getLayerName(true, 0) != "memtable" {
		t.Errorf("expected 'memtable' for memtable layer, got %s", getLayerName(true, 0))
	}
	if getLayerName(false, 0) != "sstable_l0" {
		t.Errorf("expected 'sstable_l0' for sstable index 0, got %s", getLayerName(false, 0))
	}
	if getLayerName(false, 1) != "sstable" {
		t.Errorf("expected 'sstable' for sstable index 1, got %s", getLayerName(false, 1))
	}
	if getLayerName(false, 5) != "sstable" {
		t.Errorf("expected 'sstable' for sstable index 5, got %s", getLayerName(false, 5))
	}
}

// Test concurrent telemetry access (race condition safety)
func TestStorageMetrics_ConcurrentAccess(t *testing.T) {
	mockTel := newMockTelemetryServer()
	metrics := NewStorageMetrics(mockTel)
	ctx := context.Background()
	
	// Test concurrent metric recording
	var wg sync.WaitGroup
	concurrency := 10
	operations := 100
	
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				metrics.RecordGet(ctx, time.Microsecond, "memtable", true)
				metrics.RecordPut(ctx, time.Microsecond, 100)
				metrics.RecordDelete(ctx, time.Microsecond)
				metrics.RecordLayerAccess(ctx, "sstable", "get", false)
			}
		}()
	}
	
	wg.Wait()
	
	// Verify all metrics were recorded
	totalExpected := concurrency * operations
	
	getHistCalls := mockTel.getHistogramCalls("kevo.storage.get.duration")
	if len(getHistCalls) != totalExpected {
		t.Errorf("expected %d get histogram calls, got %d", totalExpected, len(getHistCalls))
	}
	
	putHistCalls := mockTel.getHistogramCalls("kevo.storage.put.duration")
	if len(putHistCalls) != totalExpected {
		t.Errorf("expected %d put histogram calls, got %d", totalExpected, len(putHistCalls))
	}
	
	deleteHistCalls := mockTel.getHistogramCalls("kevo.storage.delete.duration")
	if len(deleteHistCalls) != totalExpected {
		t.Errorf("expected %d delete histogram calls, got %d", totalExpected, len(deleteHistCalls))
	}
	
	layerAccessCalls := mockTel.getCounterCalls("kevo.storage.layer.access")
	if len(layerAccessCalls) != totalExpected {
		t.Errorf("expected %d layer access calls, got %d", totalExpected, len(layerAccessCalls))
	}
}