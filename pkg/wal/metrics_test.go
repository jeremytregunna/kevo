// ABOUTME: Unit tests for WAL telemetry metrics interface and implementation with mock telemetry server
// ABOUTME: Tests real WAL operations with mock telemetry server to verify metric recording accuracy

package wal

import (
	"context"
	"os"
	"path/filepath"
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

func TestWALMetricsInterface(t *testing.T) {
	mockServer := newMockTelemetryServer()
	metrics := NewWALMetrics(mockServer)

	ctx := context.Background()

	t.Run("RecordAppend", func(t *testing.T) {
		mockServer.reset()

		metrics.RecordAppend(ctx, 100*time.Millisecond, 1024, "put", false, "immediate")

		// Check histogram was recorded
		durHist := mockServer.findHistogram("kevo.wal.append.duration")
		if durHist == nil {
			t.Fatal("Expected append duration histogram to be recorded")
		}
		if durHist.value != 0.1 {
			t.Errorf("Expected duration 0.1s, got %f", durHist.value)
		}

		// Check bytes counter
		bytesCounter := mockServer.findCounter("kevo.wal.append.bytes")
		if bytesCounter == nil {
			t.Fatal("Expected append bytes counter to be recorded")
		}
		if bytesCounter.value != 1024 {
			t.Errorf("Expected bytes 1024, got %d", bytesCounter.value)
		}

		// Check operation counter
		opsCounter := mockServer.findCounter("kevo.wal.operations.total")
		if opsCounter == nil {
			t.Fatal("Expected operations counter to be recorded")
		}
		if opsCounter.value != 1 {
			t.Errorf("Expected operations count 1, got %d", opsCounter.value)
		}
	})

	t.Run("RecordSync", func(t *testing.T) {
		mockServer.reset()

		metrics.RecordSync(ctx, 50*time.Millisecond, "batch", true)

		// Check sync duration histogram
		durHist := mockServer.findHistogram("kevo.wal.sync.duration")
		if durHist == nil {
			t.Fatal("Expected sync duration histogram to be recorded")
		}
		if durHist.value != 0.05 {
			t.Errorf("Expected duration 0.05s, got %f", durHist.value)
		}

		// Check sync counter
		syncCounter := mockServer.findCounter("kevo.wal.sync.total")
		if syncCounter == nil {
			t.Fatal("Expected sync counter to be recorded")
		}
		if syncCounter.value != 1 {
			t.Errorf("Expected sync count 1, got %d", syncCounter.value)
		}
	})

	t.Run("RecordCorruption", func(t *testing.T) {
		mockServer.reset()

		metrics.RecordCorruption(ctx, "crc_mismatch", "file123")

		// Check corruption counter
		corruptCounter := mockServer.findCounter("kevo.wal.corruption.count")
		if corruptCounter == nil {
			t.Fatal("Expected corruption counter to be recorded")
		}
		if corruptCounter.value != 1 {
			t.Errorf("Expected corruption count 1, got %d", corruptCounter.value)
		}
	})

	t.Run("RecordRotation", func(t *testing.T) {
		mockServer.reset()

		metrics.RecordRotation(ctx, 67108864, "file456") // 64MB

		// Check rotation counter
		rotCounter := mockServer.findCounter("kevo.wal.rotation.count")
		if rotCounter == nil {
			t.Fatal("Expected rotation counter to be recorded")
		}
		if rotCounter.value != 1 {
			t.Errorf("Expected rotation count 1, got %d", rotCounter.value)
		}

		// Check file size histogram
		sizeHist := mockServer.findHistogram("kevo.wal.rotation.file_size")
		if sizeHist == nil {
			t.Fatal("Expected rotation file size histogram to be recorded")
		}
		if sizeHist.value != 67108864 {
			t.Errorf("Expected file size 67108864, got %f", sizeHist.value)
		}
	})

	t.Run("RecordBatch", func(t *testing.T) {
		mockServer.reset()

		metrics.RecordBatch(ctx, 200*time.Millisecond, 10, 5120)

		// Check batch duration histogram
		durHist := mockServer.findHistogram("kevo.wal.batch.duration")
		if durHist == nil {
			t.Fatal("Expected batch duration histogram to be recorded")
		}
		if durHist.value != 0.2 {
			t.Errorf("Expected duration 0.2s, got %f", durHist.value)
		}

		// Check batch entries counter
		entriesCounter := mockServer.findCounter("kevo.wal.batch.entries")
		if entriesCounter == nil {
			t.Fatal("Expected batch entries counter to be recorded")
		}
		if entriesCounter.value != 10 {
			t.Errorf("Expected entries count 10, got %d", entriesCounter.value)
		}

		// Check batch bytes counter
		bytesCounter := mockServer.findCounter("kevo.wal.batch.bytes")
		if bytesCounter == nil {
			t.Fatal("Expected batch bytes counter to be recorded")
		}
		if bytesCounter.value != 5120 {
			t.Errorf("Expected bytes 5120, got %d", bytesCounter.value)
		}

		// Check batch total counter
		totalCounter := mockServer.findCounter("kevo.wal.batch.total")
		if totalCounter == nil {
			t.Fatal("Expected batch total counter to be recorded")
		}
		if totalCounter.value != 1 {
			t.Errorf("Expected batch total 1, got %d", totalCounter.value)
		}
	})
}

func TestNoopWALMetrics(t *testing.T) {
	metrics := NewNoopWALMetrics()
	ctx := context.Background()

	// All methods should not panic and should be safe to call
	t.Run("SafeNoopCalls", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Noop metrics should not panic, got: %v", r)
			}
		}()

		metrics.RecordAppend(ctx, time.Millisecond, 100, "put", false, "immediate")
		metrics.RecordSync(ctx, time.Millisecond, "batch", false)
		metrics.RecordCorruption(ctx, "test", "file")
		metrics.RecordRotation(ctx, 1000, "file")
		metrics.RecordBatch(ctx, time.Millisecond, 5, 500)
		err := metrics.Close()
		if err != nil {
			t.Errorf("Noop metrics Close() should not return error, got: %v", err)
		}
	})
}

func TestWALWithTelemetry(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "wal_telemetry_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create config with immediate sync mode for predictable telemetry
	cfg := &config.Config{
		WALSyncMode:  config.SyncImmediate,
		WALSyncBytes: 1024,
		WALMaxSize:   64 * 1024 * 1024,
	}

	mockServer := newMockTelemetryServer()

	t.Run("AppendOperationWithTelemetry", func(t *testing.T) {
		DisableRecoveryLogs = true
		defer func() { DisableRecoveryLogs = false }()

		mockServer.reset()

		// Create real WAL
		wal, err := NewWAL(cfg, tempDir)
		if err != nil {
			t.Fatalf("Failed to create WAL: %v", err)
		}
		defer wal.Close()

		// Set telemetry with mock server (mocking infrastructure, not business logic)
		walMetrics := NewWALMetrics(mockServer)
		wal.SetTelemetry(walMetrics)

		// Perform real append operation
		key := []byte("test_key")
		value := []byte("test_value")
		seqNum, err := wal.Append(OpTypePut, key, value)
		if err != nil {
			t.Fatalf("Failed to append to WAL: %v", err)
		}
		if seqNum == 0 {
			t.Error("Expected non-zero sequence number")
		}

		// Verify telemetry was recorded by mock server
		if len(mockServer.histograms) == 0 {
			t.Error("Should have recorded histograms")
		}
		if len(mockServer.counters) == 0 {
			t.Error("Should have recorded counters")
		}

		// Check for append duration metric
		durHist := mockServer.findHistogram("kevo.wal.append.duration")
		if durHist == nil {
			t.Error("Should record append duration")
		} else if durHist.value <= 0 {
			t.Error("Duration should be positive")
		}

		// Check for append bytes metric
		bytesCounter := mockServer.findCounter("kevo.wal.append.bytes")
		if bytesCounter == nil {
			t.Error("Should record append bytes")
		} else if bytesCounter.value <= 0 {
			t.Error("Bytes should be positive")
		}

		// Check for operation count
		opsCounter := mockServer.findCounter("kevo.wal.operations.total")
		if opsCounter == nil {
			t.Error("Should record operation count")
		} else if opsCounter.value != 1 {
			t.Errorf("Expected 1 operation, got %d", opsCounter.value)
		}

		// Check for sync metrics (due to immediate sync mode)
		syncHist := mockServer.findHistogram("kevo.wal.sync.duration")
		if syncHist == nil {
			t.Error("Should record sync duration")
		} else if syncHist.value <= 0 {
			t.Error("Sync duration should be positive")
		}

		// Verify real WAL functionality is intact
		nextSeq := wal.GetNextSequence()
		if nextSeq != seqNum+1 {
			t.Errorf("Expected next sequence %d, got %d", seqNum+1, nextSeq)
		}
	})

	t.Run("BatchOperationWithTelemetry", func(t *testing.T) {
		DisableRecoveryLogs = true
		defer func() { DisableRecoveryLogs = false }()

		mockServer.reset()

		// Create new real WAL for clean test
		batchDir := filepath.Join(tempDir, "batch_test")
		wal, err := NewWAL(cfg, batchDir)
		if err != nil {
			t.Fatalf("Failed to create WAL: %v", err)
		}
		defer wal.Close()

		// Set telemetry with mock server
		walMetrics := NewWALMetrics(mockServer)
		wal.SetTelemetry(walMetrics)

		// Create real batch
		entries := []*Entry{
			{Type: OpTypePut, Key: []byte("key1"), Value: []byte("value1")},
			{Type: OpTypePut, Key: []byte("key2"), Value: []byte("value2")},
			{Type: OpTypeDelete, Key: []byte("key3")},
		}

		// Perform real batch operation
		startSeq, err := wal.AppendBatch(entries)
		if err != nil {
			t.Fatalf("Failed to append batch: %v", err)
		}
		if startSeq == 0 {
			t.Error("Expected non-zero start sequence")
		}

		// Verify batch telemetry was recorded by mock server
		batchHist := mockServer.findHistogram("kevo.wal.batch.duration")
		if batchHist == nil {
			t.Error("Should record batch duration")
		} else if batchHist.value <= 0 {
			t.Error("Batch duration should be positive")
		}

		batchEntriesCounter := mockServer.findCounter("kevo.wal.batch.entries")
		if batchEntriesCounter == nil {
			t.Error("Should record batch entries count")
		} else if batchEntriesCounter.value != 3 {
			t.Errorf("Expected 3 entries, got %d", batchEntriesCounter.value)
		}

		batchBytesCounter := mockServer.findCounter("kevo.wal.batch.bytes")
		if batchBytesCounter == nil {
			t.Error("Should record batch bytes")
		} else if batchBytesCounter.value <= 0 {
			t.Error("Batch bytes should be positive")
		}

		// Verify real WAL functionality is intact
		nextSeq := wal.GetNextSequence()
		expectedNext := startSeq + uint64(len(entries))
		if nextSeq != expectedNext {
			t.Errorf("Expected next sequence %d, got %d", expectedNext, nextSeq)
		}
	})

	t.Run("SyncOperationWithTelemetry", func(t *testing.T) {
		DisableRecoveryLogs = true
		defer func() { DisableRecoveryLogs = false }()

		mockServer.reset()

		// Create real WAL with no immediate sync
		syncCfg := &config.Config{
			WALSyncMode:  config.SyncNone,
			WALSyncBytes: 1024,
			WALMaxSize:   64 * 1024 * 1024,
		}

		syncDir := filepath.Join(tempDir, "sync_test")
		wal, err := NewWAL(syncCfg, syncDir)
		if err != nil {
			t.Fatalf("Failed to create WAL: %v", err)
		}
		defer wal.Close()

		// Set telemetry with mock server
		walMetrics := NewWALMetrics(mockServer)
		wal.SetTelemetry(walMetrics)

		// Write real data
		seqNum, err := wal.Append(OpTypePut, []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to append: %v", err)
		}

		// Manually sync (real operation)
		err = wal.Sync()
		if err != nil {
			t.Fatalf("Failed to sync: %v", err)
		}

		// Verify sync telemetry was recorded by mock server
		syncCounters := mockServer.countCounters("kevo.wal.sync.total")
		if syncCounters == 0 {
			t.Error("Should have recorded sync operations")
		}

		syncHist := mockServer.findHistogram("kevo.wal.sync.duration")
		if syncHist == nil {
			t.Error("Should record sync duration")
		} else if syncHist.value <= 0 {
			t.Error("Sync duration should be positive")
		}

		// Verify real WAL functionality is intact
		if seqNum == 0 {
			t.Error("Expected non-zero sequence number after sync")
		}
	})

	t.Run("TelemetryWithoutMetrics", func(t *testing.T) {
		DisableRecoveryLogs = true
		defer func() { DisableRecoveryLogs = false }()

		// Create real WAL without setting telemetry
		noTelDir := filepath.Join(tempDir, "no_tel_test")
		wal, err := NewWAL(cfg, noTelDir)
		if err != nil {
			t.Fatalf("Failed to create WAL: %v", err)
		}
		defer wal.Close()

		// Should work normally when metrics is nil (real business logic)
		seqNum, err := wal.Append(OpTypePut, []byte("key"), []byte("value"))
		if err != nil {
			t.Errorf("Append should work without telemetry: %v", err)
		}
		if seqNum == 0 {
			t.Error("Expected non-zero sequence number without telemetry")
		}

		err = wal.Sync()
		if err != nil {
			t.Errorf("Sync should work without telemetry: %v", err)
		}

		// Verify real WAL functionality is completely intact
		nextSeq := wal.GetNextSequence()
		if nextSeq != seqNum+1 {
			t.Errorf("Expected next sequence %d, got %d", seqNum+1, nextSeq)
		}
	})
}

func TestHelperFunctions(t *testing.T) {
	t.Run("getOpTypeName", func(t *testing.T) {
		tests := []struct {
			input    uint8
			expected string
		}{
			{OpTypePut, "put"},
			{OpTypeDelete, "delete"},
			{OpTypeMerge, "merge"},
			{OpTypeBatch, "batch"},
			{99, "unknown"},
		}

		for _, test := range tests {
			result := getOpTypeName(test.input)
			if result != test.expected {
				t.Errorf("getOpTypeName(%d) = %s, want %s", test.input, result, test.expected)
			}
		}
	})

	t.Run("getSyncModeName", func(t *testing.T) {
		tests := []struct {
			input    config.SyncMode
			expected string
		}{
			{config.SyncNone, "none"},
			{config.SyncImmediate, "immediate"},
			{config.SyncBatch, "batch"},
			{config.SyncMode(99), "unknown"},
		}

		for _, test := range tests {
			result := getSyncModeName(test.input)
			if result != test.expected {
				t.Errorf("getSyncModeName(%d) = %s, want %s", test.input, result, test.expected)
			}
		}
	})
}

func TestWALMetricsNilTelemetry(t *testing.T) {
	// Test that NewWALMetrics with nil telemetry returns noop implementation
	metrics := NewWALMetrics(nil)
	if metrics == nil {
		t.Fatal("NewWALMetrics with nil should return noop implementation")
	}

	// Should not panic
	ctx := context.Background()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("WAL metrics with nil telemetry should not panic, got: %v", r)
		}
	}()

	metrics.RecordAppend(ctx, time.Millisecond, 100, "put", false, "immediate")
	metrics.RecordSync(ctx, time.Millisecond, "batch", false)
	metrics.RecordCorruption(ctx, "test", "file")
	metrics.RecordRotation(ctx, 1000, "file")
	metrics.RecordBatch(ctx, time.Millisecond, 5, 500)
	err := metrics.Close()
	if err != nil {
		t.Errorf("Noop metrics Close() should not return error, got: %v", err)
	}
}
