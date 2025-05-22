// ABOUTME: WAL telemetry metrics interface and implementation for tracking write-ahead log operations
// ABOUTME: Provides instrumentation for append, sync, corruption, rotation, and batch operations

package wal

import (
	"context"
	"time"

	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// WALMetrics defines the interface for WAL telemetry operations.
// All metrics are optional - implementations can safely be no-op.
type WALMetrics interface {
	telemetry.ComponentMetrics

	// RecordAppend records metrics for a WAL append operation.
	RecordAppend(ctx context.Context, duration time.Duration, bytes int64, opType string, fragmented bool, syncMode string)

	// RecordSync records metrics for a WAL sync operation.
	RecordSync(ctx context.Context, duration time.Duration, mode string, forced bool)

	// RecordCorruption records when WAL corruption is detected.
	RecordCorruption(ctx context.Context, reason string, fileID string)

	// RecordRotation records when WAL file rotation occurs.
	RecordRotation(ctx context.Context, oldSize int64, newFileID string)

	// RecordBatch records metrics for batch operations.
	RecordBatch(ctx context.Context, duration time.Duration, entryCount int, totalBytes int64)
}

// walMetrics implements WALMetrics using the telemetry interface.
type walMetrics struct {
	tel telemetry.Telemetry
}

// NewWALMetrics creates a new WAL metrics implementation.
// If tel is nil, returns a no-op implementation.
func NewWALMetrics(tel telemetry.Telemetry) WALMetrics {
	if tel == nil {
		return &noopWALMetrics{}
	}
	return &walMetrics{tel: tel}
}

// NewNoopWALMetrics creates a no-op WAL metrics implementation for testing.
func NewNoopWALMetrics() WALMetrics {
	return &noopWALMetrics{}
}

// RecordAppend records WAL append operation metrics.
func (m *walMetrics) RecordAppend(ctx context.Context, duration time.Duration, bytes int64, opType string, fragmented bool, syncMode string) {
	// Record append duration
	m.tel.RecordHistogram(ctx, "kevo.wal.append.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
		attribute.String(telemetry.AttrOperationType, opType),
		attribute.Bool("fragmented", fragmented),
		attribute.String("sync_mode", syncMode),
	)

	// Record bytes written
	m.tel.RecordCounter(ctx, "kevo.wal.append.bytes", bytes,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
		attribute.String(telemetry.AttrOperationType, opType),
		attribute.Bool("fragmented", fragmented),
	)

	// Record operation count
	m.tel.RecordCounter(ctx, "kevo.wal.operations.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
		attribute.String(telemetry.AttrOperationType, opType),
		attribute.String(telemetry.AttrStatus, telemetry.StatusSuccess),
	)
}

// RecordSync records WAL sync operation metrics.
func (m *walMetrics) RecordSync(ctx context.Context, duration time.Duration, mode string, forced bool) {
	m.tel.RecordHistogram(ctx, "kevo.wal.sync.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
		attribute.String("sync_mode", mode),
		attribute.Bool("forced", forced),
	)

	m.tel.RecordCounter(ctx, "kevo.wal.sync.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
		attribute.String("sync_mode", mode),
		attribute.Bool("forced", forced),
	)
}

// RecordCorruption records WAL corruption detection.
func (m *walMetrics) RecordCorruption(ctx context.Context, reason string, fileID string) {
	m.tel.RecordCounter(ctx, "kevo.wal.corruption.count", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
		attribute.String(telemetry.AttrReason, reason),
		attribute.String(telemetry.AttrFileID, fileID),
	)
}

// RecordRotation records WAL file rotation.
func (m *walMetrics) RecordRotation(ctx context.Context, oldSize int64, newFileID string) {
	m.tel.RecordCounter(ctx, "kevo.wal.rotation.count", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
		attribute.String(telemetry.AttrFileID, newFileID),
	)

	m.tel.RecordHistogram(ctx, "kevo.wal.rotation.file_size", float64(oldSize),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
		attribute.String(telemetry.AttrFileID, newFileID),
	)
}

// RecordBatch records batch operation metrics.
func (m *walMetrics) RecordBatch(ctx context.Context, duration time.Duration, entryCount int, totalBytes int64) {
	m.tel.RecordHistogram(ctx, "kevo.wal.batch.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
		attribute.String(telemetry.AttrOperationType, "batch"),
	)

	m.tel.RecordCounter(ctx, "kevo.wal.batch.entries", int64(entryCount),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
	)

	m.tel.RecordCounter(ctx, "kevo.wal.batch.bytes", totalBytes,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
	)

	m.tel.RecordCounter(ctx, "kevo.wal.batch.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentWAL),
		attribute.String(telemetry.AttrStatus, telemetry.StatusSuccess),
	)
}

// Close releases any resources held by the metrics implementation.
func (m *walMetrics) Close() error {
	// No resources to clean up for this implementation
	return nil
}

// noopWALMetrics provides a no-operation implementation for testing or disabled telemetry.
type noopWALMetrics struct{}

// RecordAppend is a no-op.
func (n *noopWALMetrics) RecordAppend(ctx context.Context, duration time.Duration, bytes int64, opType string, fragmented bool, syncMode string) {
	// No-op
}

// RecordSync is a no-op.
func (n *noopWALMetrics) RecordSync(ctx context.Context, duration time.Duration, mode string, forced bool) {
	// No-op
}

// RecordCorruption is a no-op.
func (n *noopWALMetrics) RecordCorruption(ctx context.Context, reason string, fileID string) {
	// No-op
}

// RecordRotation is a no-op.
func (n *noopWALMetrics) RecordRotation(ctx context.Context, oldSize int64, newFileID string) {
	// No-op
}

// RecordBatch is a no-op.
func (n *noopWALMetrics) RecordBatch(ctx context.Context, duration time.Duration, entryCount int, totalBytes int64) {
	// No-op
}

// Close is a no-op.
func (n *noopWALMetrics) Close() error {
	return nil
}

// Helper functions for converting operation types to telemetry strings
func getOpTypeName(opType uint8) string {
	switch opType {
	case OpTypePut:
		return telemetry.OpTypePut
	case OpTypeDelete:
		return telemetry.OpTypeDelete
	case OpTypeMerge:
		return "merge"
	case OpTypeBatch:
		return "batch"
	default:
		return "unknown"
	}
}

// getSyncModeName converts config sync mode to telemetry string
func getSyncModeName(mode config.SyncMode) string {
	switch mode {
	case config.SyncNone:
		return "none"
	case config.SyncImmediate:
		return "immediate"
	case config.SyncBatch:
		return "batch"
	default:
		return "unknown"
	}
}
