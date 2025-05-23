// ABOUTME: Storage telemetry metrics interface and implementation for tracking storage layer operations
// ABOUTME: Provides instrumentation for Get/Put/Delete operations, layer hits/misses, and flush performance

package storage

import (
	"context"
	"time"

	"github.com/KevoDB/kevo/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// StorageMetrics defines the interface for storage layer telemetry operations.
// All metrics are optional - implementations can safely be no-op.
type StorageMetrics interface {
	telemetry.ComponentMetrics

	// RecordGet records metrics for a Get operation with layer tracking.
	RecordGet(ctx context.Context, duration time.Duration, layer string, found bool)

	// RecordPut records metrics for a Put operation.
	RecordPut(ctx context.Context, duration time.Duration, bytes int64)

	// RecordDelete records metrics for a Delete operation.
	RecordDelete(ctx context.Context, duration time.Duration)

	// RecordFlush records metrics for MemTable flush operations.
	RecordFlush(ctx context.Context, duration time.Duration, memTableSize int64, sstableSize int64)

	// RecordLayerAccess records which storage layer was accessed during an operation.
	RecordLayerAccess(ctx context.Context, layer string, operation string, found bool)
}

// storageMetrics implements StorageMetrics using the telemetry interface.
type storageMetrics struct {
	tel telemetry.Telemetry
}

// NewStorageMetrics creates a new storage metrics implementation.
// If tel is nil, returns a no-op implementation.
func NewStorageMetrics(tel telemetry.Telemetry) StorageMetrics {
	if tel == nil {
		return &noopStorageMetrics{}
	}
	return &storageMetrics{tel: tel}
}

// NewNoopStorageMetrics creates a no-op storage metrics implementation for testing.
func NewNoopStorageMetrics() StorageMetrics {
	return &noopStorageMetrics{}
}

// RecordGet records Get operation metrics with layer tracking.
func (m *storageMetrics) RecordGet(ctx context.Context, duration time.Duration, layer string, found bool) {
	// Record get operation duration
	m.tel.RecordHistogram(ctx, "kevo.storage.get.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypeGet),
		attribute.String(telemetry.AttrLayer, layer),
		attribute.Bool("found", found),
	)

	// Record operation count
	m.tel.RecordCounter(ctx, "kevo.storage.operations.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypeGet),
		attribute.String(telemetry.AttrLayer, layer),
		attribute.String(telemetry.AttrStatus, getStatusFromFound(found)),
	)

	// Record layer hit/miss
	hitValue := int64(0)
	if found {
		hitValue = 1
	}
	m.tel.RecordCounter(ctx, "kevo.storage.layer.hits", hitValue,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrLayer, layer),
	)

	m.tel.RecordCounter(ctx, "kevo.storage.layer.accesses", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrLayer, layer),
	)
}

// RecordPut records Put operation metrics.
func (m *storageMetrics) RecordPut(ctx context.Context, duration time.Duration, bytes int64) {
	// Record put operation duration
	m.tel.RecordHistogram(ctx, "kevo.storage.put.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypePut),
	)

	// Record bytes written
	m.tel.RecordCounter(ctx, "kevo.storage.put.bytes", bytes,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypePut),
	)

	// Record operation count
	m.tel.RecordCounter(ctx, "kevo.storage.operations.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypePut),
		attribute.String(telemetry.AttrStatus, telemetry.StatusSuccess),
	)
}

// RecordDelete records Delete operation metrics.
func (m *storageMetrics) RecordDelete(ctx context.Context, duration time.Duration) {
	// Record delete operation duration
	m.tel.RecordHistogram(ctx, "kevo.storage.delete.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypeDelete),
	)

	// Record operation count
	m.tel.RecordCounter(ctx, "kevo.storage.operations.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypeDelete),
		attribute.String(telemetry.AttrStatus, telemetry.StatusSuccess),
	)
}

// RecordFlush records MemTable flush operation metrics.
func (m *storageMetrics) RecordFlush(ctx context.Context, duration time.Duration, memTableSize int64, sstableSize int64) {
	// Record flush duration
	m.tel.RecordHistogram(ctx, "kevo.storage.flush.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypeFlush),
	)

	// Record memtable size being flushed
	m.tel.RecordHistogram(ctx, "kevo.storage.flush.memtable_size", float64(memTableSize),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
	)

	// Record resulting sstable size
	m.tel.RecordHistogram(ctx, "kevo.storage.flush.sstable_size", float64(sstableSize),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
	)

	// Record flush count
	m.tel.RecordCounter(ctx, "kevo.storage.flush.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrStatus, telemetry.StatusSuccess),
	)
}

// RecordLayerAccess records storage layer access patterns.
func (m *storageMetrics) RecordLayerAccess(ctx context.Context, layer string, operation string, found bool) {
	// Record layer access
	m.tel.RecordCounter(ctx, "kevo.storage.layer.access", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentStorage),
		attribute.String(telemetry.AttrLayer, layer),
		attribute.String(telemetry.AttrOperationType, operation),
		attribute.Bool("found", found),
	)
}

// Close releases any resources held by the metrics implementation.
func (m *storageMetrics) Close() error {
	// No resources to clean up for this implementation
	return nil
}

// noopStorageMetrics provides a no-operation implementation for testing or disabled telemetry.
type noopStorageMetrics struct{}

// RecordGet is a no-op.
func (n *noopStorageMetrics) RecordGet(ctx context.Context, duration time.Duration, layer string, found bool) {
	// No-op
}

// RecordPut is a no-op.
func (n *noopStorageMetrics) RecordPut(ctx context.Context, duration time.Duration, bytes int64) {
	// No-op
}

// RecordDelete is a no-op.
func (n *noopStorageMetrics) RecordDelete(ctx context.Context, duration time.Duration) {
	// No-op
}

// RecordFlush is a no-op.
func (n *noopStorageMetrics) RecordFlush(ctx context.Context, duration time.Duration, memTableSize int64, sstableSize int64) {
	// No-op
}

// RecordLayerAccess is a no-op.
func (n *noopStorageMetrics) RecordLayerAccess(ctx context.Context, layer string, operation string, found bool) {
	// No-op
}

// Close is a no-op.
func (n *noopStorageMetrics) Close() error {
	return nil
}

// Helper functions for telemetry attributes

// getStatusFromFound converts found boolean to status string.
func getStatusFromFound(found bool) string {
	if found {
		return telemetry.StatusSuccess
	}
	return "not_found"
}

// getLayerName converts storage access to standardized layer names.
func getLayerName(isMemTable bool, sstableIndex int) string {
	if isMemTable {
		return "memtable"
	}
	if sstableIndex == 0 {
		return "sstable_l0"
	}
	return "sstable"
}