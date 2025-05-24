// ABOUTME: MemTable telemetry metrics interface and implementation for tracking in-memory table operations
// ABOUTME: Provides instrumentation for size tracking, flush triggers, operations, and pool management

package memtable

import (
	"context"
	"time"

	"github.com/KevoDB/kevo/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// MemTableMetrics defines the interface for MemTable telemetry operations.
// All metrics are optional - implementations can safely be no-op.
type MemTableMetrics interface {
	telemetry.ComponentMetrics

	// RecordOperation records metrics for individual MemTable operations (Put/Delete/Get).
	RecordOperation(ctx context.Context, opType string, duration time.Duration)

	// RecordFlushTrigger records when a flush is triggered and why.
	RecordFlushTrigger(ctx context.Context, reason string, memTableSize int64, memTableAge float64)

	// RecordFlushDuration records metrics for MemTable flush operations to SSTable.
	RecordFlushDuration(ctx context.Context, duration time.Duration, memTableSize int64, entryCount int64)

	// RecordSizeChange records changes in MemTable size for monitoring growth.
	RecordSizeChange(ctx context.Context, newSize int64, delta int64, memTableType string)

	// RecordPoolState records the state of the MemTablePool.
	RecordPoolState(ctx context.Context, activeSize int64, immutableCount int, totalSize int64)
}

// memTableMetrics implements MemTableMetrics using the telemetry interface.
type memTableMetrics struct {
	tel telemetry.Telemetry
}

// NewMemTableMetrics creates a new MemTable metrics implementation.
// If tel is nil, returns a no-op implementation.
func NewMemTableMetrics(tel telemetry.Telemetry) MemTableMetrics {
	if tel == nil {
		return &noopMemTableMetrics{}
	}
	return &memTableMetrics{tel: tel}
}

// NewNoopMemTableMetrics creates a no-op MemTable metrics implementation for testing.
func NewNoopMemTableMetrics() MemTableMetrics {
	return &noopMemTableMetrics{}
}

// RecordOperation records MemTable operation metrics.
func (m *memTableMetrics) RecordOperation(ctx context.Context, opType string, duration time.Duration) {
	// Record operation duration
	m.tel.RecordHistogram(ctx, "kevo.memtable.operation.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
		attribute.String(telemetry.AttrOperationType, opType),
	)

	// Record operation count
	m.tel.RecordCounter(ctx, "kevo.memtable.operations.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
		attribute.String(telemetry.AttrOperationType, opType),
		attribute.String(telemetry.AttrStatus, telemetry.StatusSuccess),
	)
}

// RecordFlushTrigger records flush trigger events.
func (m *memTableMetrics) RecordFlushTrigger(ctx context.Context, reason string, memTableSize int64, memTableAge float64) {
	m.tel.RecordCounter(ctx, "kevo.memtable.flush.trigger.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
		attribute.String(telemetry.AttrReason, reason),
	)

	// Record the size that triggered the flush
	m.tel.RecordHistogram(ctx, "kevo.memtable.flush.trigger.size", float64(memTableSize),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
		attribute.String(telemetry.AttrReason, reason),
	)

	// Record the age that triggered the flush
	m.tel.RecordHistogram(ctx, "kevo.memtable.flush.trigger.age", memTableAge,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
		attribute.String(telemetry.AttrReason, reason),
	)
}

// RecordFlushDuration records MemTable flush operation metrics.
func (m *memTableMetrics) RecordFlushDuration(ctx context.Context, duration time.Duration, memTableSize int64, entryCount int64) {
	m.tel.RecordHistogram(ctx, "kevo.memtable.flush.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypeFlush),
	)

	m.tel.RecordHistogram(ctx, "kevo.memtable.flush.size", float64(memTableSize),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
	)

	m.tel.RecordCounter(ctx, "kevo.memtable.flush.entries", entryCount,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
	)

	m.tel.RecordCounter(ctx, "kevo.memtable.flush.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
		attribute.String(telemetry.AttrStatus, telemetry.StatusSuccess),
	)
}

// RecordSizeChange records MemTable size changes.
func (m *memTableMetrics) RecordSizeChange(ctx context.Context, newSize int64, delta int64, memTableType string) {
	m.tel.RecordHistogram(ctx, "kevo.memtable.size.bytes", float64(newSize),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
		attribute.String("memtable.type", memTableType),
	)

	// Record size delta (positive for growth, negative for shrinkage)
	m.tel.RecordHistogram(ctx, "kevo.memtable.size.delta", float64(delta),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
		attribute.String("memtable.type", memTableType),
	)
}

// RecordPoolState records MemTablePool state metrics.
func (m *memTableMetrics) RecordPoolState(ctx context.Context, activeSize int64, immutableCount int, totalSize int64) {
	m.tel.RecordHistogram(ctx, "kevo.memtable.pool.active.size", float64(activeSize),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
	)

	m.tel.RecordHistogram(ctx, "kevo.memtable.pool.immutable.count", float64(immutableCount),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
	)

	m.tel.RecordHistogram(ctx, "kevo.memtable.pool.total.size", float64(totalSize),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentMemTable),
	)
}

// Close releases any resources held by the metrics implementation.
func (m *memTableMetrics) Close() error {
	// No resources to clean up for this implementation
	return nil
}

// noopMemTableMetrics provides a no-operation implementation for testing or disabled telemetry.
type noopMemTableMetrics struct{}

// RecordOperation is a no-op.
func (n *noopMemTableMetrics) RecordOperation(ctx context.Context, opType string, duration time.Duration) {
	// No-op
}

// RecordFlushTrigger is a no-op.
func (n *noopMemTableMetrics) RecordFlushTrigger(ctx context.Context, reason string, memTableSize int64, memTableAge float64) {
	// No-op
}

// RecordFlushDuration is a no-op.
func (n *noopMemTableMetrics) RecordFlushDuration(ctx context.Context, duration time.Duration, memTableSize int64, entryCount int64) {
	// No-op
}

// RecordSizeChange is a no-op.
func (n *noopMemTableMetrics) RecordSizeChange(ctx context.Context, newSize int64, delta int64, memTableType string) {
	// No-op
}

// RecordPoolState is a no-op.
func (n *noopMemTableMetrics) RecordPoolState(ctx context.Context, activeSize int64, immutableCount int, totalSize int64) {
	// No-op
}

// Close is a no-op.
func (n *noopMemTableMetrics) Close() error {
	return nil
}

// Helper functions for MemTable telemetry

// getFlushReasonName converts flush trigger reasons to telemetry strings
func getFlushReasonName(sizeTriggered bool, ageTriggered bool, manual bool) string {
	if manual {
		return "manual"
	}
	if sizeTriggered && ageTriggered {
		return "size_and_age"
	}
	if sizeTriggered {
		return "size"
	}
	if ageTriggered {
		return "age"
	}
	return "unknown"
}

// getMemTableTypeName converts MemTable type to telemetry string
func getMemTableTypeName(immutable bool) string {
	if immutable {
		return "immutable"
	}
	return "active"
}
