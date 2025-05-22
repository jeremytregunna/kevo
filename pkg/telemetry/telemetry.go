// ABOUTME: Core telemetry abstraction interface over OpenTelemetry for Kevo storage engine instrumentation
// ABOUTME: Provides metric creation, tracing, and lifecycle management with optional no-op implementations

package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Telemetry provides the core abstraction over OpenTelemetry for Kevo components.
// Components use this interface to record metrics and spans without depending directly on OpenTelemetry.
type Telemetry interface {
	// RecordHistogram records a histogram value with optional attributes.
	RecordHistogram(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue)

	// RecordCounter records a counter increment with optional attributes.
	RecordCounter(ctx context.Context, name string, value int64, attrs ...attribute.KeyValue)

	// StartSpan creates a new tracing span with the given name and attributes.
	StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span)

	// Shutdown gracefully shuts down all telemetry providers and exports remaining data.
	Shutdown(ctx context.Context) error
}

// ComponentMetrics is a marker interface for component-specific metrics interfaces.
// Each component (WAL, MemTable, etc.) defines its own metrics interface extending this.
type ComponentMetrics interface {
	// Close releases any resources held by the metrics implementation.
	Close() error
}

// NoopTelemetry provides a no-operation implementation of Telemetry for testing or disabled scenarios.
type NoopTelemetry struct{}

// NewNoop creates a new no-operation telemetry instance.
func NewNoop() Telemetry {
	return &NoopTelemetry{}
}

// RecordHistogram is a no-op.
func (n *NoopTelemetry) RecordHistogram(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
	// No-op
}

// RecordCounter is a no-op.
func (n *NoopTelemetry) RecordCounter(ctx context.Context, name string, value int64, attrs ...attribute.KeyValue) {
	// No-op
}

// StartSpan returns the original context and a no-op span.
func (n *NoopTelemetry) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return ctx, trace.SpanFromContext(ctx)
}

// Shutdown is a no-op.
func (n *NoopTelemetry) Shutdown(ctx context.Context) error {
	return nil
}

// Common telemetry utilities

// RecordDuration is a helper function to record operation duration in a histogram.
func RecordDuration(ctx context.Context, tel Telemetry, name string, start time.Time, attrs ...attribute.KeyValue) {
	duration := time.Since(start).Seconds()
	tel.RecordHistogram(ctx, name, duration, attrs...)
}

// RecordBytes is a helper function to record byte counts in a counter.
func RecordBytes(ctx context.Context, tel Telemetry, name string, bytes int64, attrs ...attribute.KeyValue) {
	tel.RecordCounter(ctx, name, bytes, attrs...)
}

// Common attribute keys for consistent naming across components
const (
	// Operation type attributes
	AttrOperationType = "operation.type"
	AttrOperationName = "operation.name"

	// Component attributes
	AttrComponent = "component"
	AttrLayer     = "layer"

	// Status attributes
	AttrStatus    = "status"
	AttrSuccess   = "success"
	AttrErrorType = "error.type"

	// Resource attributes
	AttrFileID  = "file.id"
	AttrTableID = "table.id"
	AttrLevel   = "level"
	AttrReason  = "reason"
)

// Common attribute values
const (
	// Operation types
	OpTypePut    = "put"
	OpTypeDelete = "delete"
	OpTypeGet    = "get"
	OpTypeScan   = "scan"
	OpTypeFlush  = "flush"
	OpTypeSync   = "sync"

	// Status values
	StatusSuccess = "success"
	StatusError   = "error"
	StatusTimeout = "timeout"

	// Component names
	ComponentWAL        = "wal"
	ComponentMemTable   = "memtable"
	ComponentSSTable    = "sstable"
	ComponentCompaction = "compaction"
	ComponentStorage    = "storage"
	ComponentEngine     = "engine"
)
