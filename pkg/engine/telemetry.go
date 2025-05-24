// ABOUTME: Engine-level telemetry coordination for resource monitoring and operation tracing
// ABOUTME: Provides comprehensive metrics for memory usage, file descriptors, disk usage, and engine operations

package engine

import (
	"context"
	"runtime"
	"time"

	"github.com/KevoDB/kevo/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// EngineMetrics defines the interface for engine-level telemetry
type EngineMetrics interface {
	// Resource monitoring
	RecordMemoryUsage(ctx context.Context, component string, bytes int64)
	RecordFileDescriptors(ctx context.Context, open, limit int64)
	RecordDiskUsage(ctx context.Context, component string, bytes int64)

	// Operation tracing
	RecordEngineOperation(ctx context.Context, operation string, duration time.Duration, success bool)
	RecordOperationLatency(ctx context.Context, operation string, duration time.Duration)
	RecordOperationThroughput(ctx context.Context, operation string, bytesPerSecond float64)

	// Component initialization
	RecordComponentInitialization(ctx context.Context, component string, duration time.Duration, success bool)
	RecordStartupMetrics(ctx context.Context, totalStartupTime time.Duration, componentCount int64)

	// Error tracking
	RecordError(ctx context.Context, errorType, component string, severity string)
	RecordSystemHealth(ctx context.Context, healthStatus string, uptime time.Duration)

	// Resource cleanup
	Close() error
}

// engineMetrics implements EngineMetrics using the telemetry interface
type engineMetrics struct {
	tel telemetry.Telemetry
}

// NewEngineMetrics creates a new EngineMetrics instance
func NewEngineMetrics(tel telemetry.Telemetry) EngineMetrics {
	return &engineMetrics{
		tel: tel,
	}
}

// NewNoopEngineMetrics creates a no-op EngineMetrics for testing or when telemetry is disabled
func NewNoopEngineMetrics() EngineMetrics {
	return &noopEngineMetrics{}
}

// RecordMemoryUsage records memory usage by component
func (m *engineMetrics) RecordMemoryUsage(ctx context.Context, component string, bytes int64) {
	if m.tel == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Silently handle telemetry panics
		}
	}()

	attrs := []attribute.KeyValue{
		{Key: telemetry.AttrComponent, Value: attribute.StringValue(component)},
		{Key: "memory.type", Value: attribute.StringValue("allocated")},
	}

	m.tel.RecordCounter(ctx, "kevo.engine.memory.usage.bytes", bytes, attrs...)
}

// RecordFileDescriptors records current file descriptor usage
func (m *engineMetrics) RecordFileDescriptors(ctx context.Context, open, limit int64) {
	if m.tel == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Silently handle telemetry panics
		}
	}()

	// Record open file descriptors
	openAttrs := []attribute.KeyValue{
		{Key: "descriptor.state", Value: attribute.StringValue("open")},
	}
	m.tel.RecordCounter(ctx, "kevo.engine.file.descriptors.count", open, openAttrs...)

	// Record file descriptor limit
	limitAttrs := []attribute.KeyValue{
		{Key: "descriptor.state", Value: attribute.StringValue("limit")},
	}
	m.tel.RecordCounter(ctx, "kevo.engine.file.descriptors.count", limit, limitAttrs...)

	// Calculate and record utilization percentage
	if limit > 0 {
		utilizationPct := float64(open) / float64(limit) * 100.0
		utilizationAttrs := []attribute.KeyValue{
			{Key: "metric.type", Value: attribute.StringValue("utilization_percentage")},
		}
		m.tel.RecordHistogram(ctx, "kevo.engine.file.descriptors.utilization", utilizationPct, utilizationAttrs...)
	}
}

// RecordDiskUsage records disk space usage by component
func (m *engineMetrics) RecordDiskUsage(ctx context.Context, component string, bytes int64) {
	if m.tel == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Silently handle telemetry panics
		}
	}()

	attrs := []attribute.KeyValue{
		{Key: telemetry.AttrComponent, Value: attribute.StringValue(component)},
		{Key: "disk.type", Value: attribute.StringValue("usage")},
	}

	m.tel.RecordCounter(ctx, "kevo.engine.disk.usage.bytes", bytes, attrs...)
}

// RecordEngineOperation records engine-level operation metrics with success tracking
func (m *engineMetrics) RecordEngineOperation(ctx context.Context, operation string, duration time.Duration, success bool) {
	if m.tel == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Silently handle telemetry panics
		}
	}()

	// Record operation duration
	durationAttrs := []attribute.KeyValue{
		{Key: telemetry.AttrOperation, Value: attribute.StringValue(operation)},
		{Key: telemetry.AttrSuccess, Value: attribute.StringValue(boolToString(success))},
		{Key: telemetry.AttrComponent, Value: attribute.StringValue(telemetry.ComponentEngine)},
	}
	m.tel.RecordHistogram(ctx, "kevo.engine.operation.duration", duration.Seconds(), durationAttrs...)

	// Record operation count
	countAttrs := []attribute.KeyValue{
		{Key: telemetry.AttrOperation, Value: attribute.StringValue(operation)},
		{Key: telemetry.AttrSuccess, Value: attribute.StringValue(boolToString(success))},
	}
	m.tel.RecordCounter(ctx, "kevo.engine.operation.count", 1.0, countAttrs...)
}

// RecordOperationLatency records operation latency percentiles
func (m *engineMetrics) RecordOperationLatency(ctx context.Context, operation string, duration time.Duration) {
	if m.tel == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Silently handle telemetry panics
		}
	}()

	attrs := []attribute.KeyValue{
		{Key: telemetry.AttrOperation, Value: attribute.StringValue(operation)},
		{Key: telemetry.AttrComponent, Value: attribute.StringValue(telemetry.ComponentEngine)},
		{Key: "metric.type", Value: attribute.StringValue("latency")},
	}

	m.tel.RecordHistogram(ctx, "kevo.engine.latency.percentiles", duration.Seconds(), attrs...)
}

// RecordOperationThroughput records operation throughput in bytes per second
func (m *engineMetrics) RecordOperationThroughput(ctx context.Context, operation string, bytesPerSecond float64) {
	if m.tel == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Silently handle telemetry panics
		}
	}()

	attrs := []attribute.KeyValue{
		{Key: telemetry.AttrOperation, Value: attribute.StringValue(operation)},
		{Key: telemetry.AttrComponent, Value: attribute.StringValue(telemetry.ComponentEngine)},
		{Key: "metric.type", Value: attribute.StringValue("throughput")},
	}

	m.tel.RecordHistogram(ctx, "kevo.engine.throughput.bytes_per_second", bytesPerSecond, attrs...)
}

// RecordComponentInitialization records component startup metrics
func (m *engineMetrics) RecordComponentInitialization(ctx context.Context, component string, duration time.Duration, success bool) {
	if m.tel == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Silently handle telemetry panics
		}
	}()

	attrs := []attribute.KeyValue{
		{Key: telemetry.AttrComponent, Value: attribute.StringValue(component)},
		{Key: telemetry.AttrSuccess, Value: attribute.StringValue(boolToString(success))},
		{Key: "initialization.type", Value: attribute.StringValue("startup")},
	}

	m.tel.RecordHistogram(ctx, "kevo.engine.component.initialization.duration", duration.Seconds(), attrs...)
}

// RecordStartupMetrics records overall engine startup metrics
func (m *engineMetrics) RecordStartupMetrics(ctx context.Context, totalStartupTime time.Duration, componentCount int64) {
	if m.tel == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Silently handle telemetry panics
		}
	}()

	// Record total startup time
	startupAttrs := []attribute.KeyValue{
		{Key: telemetry.AttrComponent, Value: attribute.StringValue(telemetry.ComponentEngine)},
		{Key: "startup.type", Value: attribute.StringValue("total")},
	}
	m.tel.RecordHistogram(ctx, "kevo.engine.startup.duration", totalStartupTime.Seconds(), startupAttrs...)

	// Record component count
	componentAttrs := []attribute.KeyValue{
		{Key: telemetry.AttrComponent, Value: attribute.StringValue(telemetry.ComponentEngine)},
		{Key: "startup.metric", Value: attribute.StringValue("component_count")},
	}
	m.tel.RecordCounter(ctx, "kevo.engine.startup.components", componentCount, componentAttrs...)
}

// RecordError records engine errors with categorization
func (m *engineMetrics) RecordError(ctx context.Context, errorType, component string, severity string) {
	if m.tel == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Silently handle telemetry panics
		}
	}()

	attrs := []attribute.KeyValue{
		{Key: "error.type", Value: attribute.StringValue(errorType)},
		{Key: telemetry.AttrComponent, Value: attribute.StringValue(component)},
		{Key: "error.severity", Value: attribute.StringValue(severity)},
	}

	m.tel.RecordCounter(ctx, "kevo.engine.errors.total", 1.0, attrs...)
}

// RecordSystemHealth records overall system health status
func (m *engineMetrics) RecordSystemHealth(ctx context.Context, healthStatus string, uptime time.Duration) {
	if m.tel == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			// Silently handle telemetry panics
		}
	}()

	// Record health status
	healthAttrs := []attribute.KeyValue{
		{Key: "health.status", Value: attribute.StringValue(healthStatus)},
		{Key: telemetry.AttrComponent, Value: attribute.StringValue(telemetry.ComponentEngine)},
	}
	m.tel.RecordCounter(ctx, "kevo.engine.health.status", 1.0, healthAttrs...)

	// Record uptime
	uptimeAttrs := []attribute.KeyValue{
		{Key: telemetry.AttrComponent, Value: attribute.StringValue(telemetry.ComponentEngine)},
		{Key: "metric.type", Value: attribute.StringValue("uptime")},
	}
	m.tel.RecordHistogram(ctx, "kevo.engine.uptime.seconds", uptime.Seconds(), uptimeAttrs...)
}

// Close closes the metrics and cleans up resources
func (m *engineMetrics) Close() error {
	// Engine metrics doesn't own the telemetry instance, so we don't close it
	return nil
}

// noopEngineMetrics provides a no-op implementation for testing or disabled telemetry
type noopEngineMetrics struct{}

func (n *noopEngineMetrics) RecordMemoryUsage(ctx context.Context, component string, bytes int64) {}
func (n *noopEngineMetrics) RecordFileDescriptors(ctx context.Context, open, limit int64)         {}
func (n *noopEngineMetrics) RecordDiskUsage(ctx context.Context, component string, bytes int64)   {}
func (n *noopEngineMetrics) RecordEngineOperation(ctx context.Context, operation string, duration time.Duration, success bool) {
}
func (n *noopEngineMetrics) RecordOperationLatency(ctx context.Context, operation string, duration time.Duration) {
}
func (n *noopEngineMetrics) RecordOperationThroughput(ctx context.Context, operation string, bytesPerSecond float64) {
}
func (n *noopEngineMetrics) RecordComponentInitialization(ctx context.Context, component string, duration time.Duration, success bool) {
}
func (n *noopEngineMetrics) RecordStartupMetrics(ctx context.Context, totalStartupTime time.Duration, componentCount int64) {
}
func (n *noopEngineMetrics) RecordError(ctx context.Context, errorType, component string, severity string) {
}
func (n *noopEngineMetrics) RecordSystemHealth(ctx context.Context, healthStatus string, uptime time.Duration) {
}
func (n *noopEngineMetrics) Close() error { return nil }

// Helper functions

// boolToString converts boolean to string for telemetry attributes
func boolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// GetMemoryStats retrieves current memory statistics using runtime
func GetMemoryStats() (heapAlloc, heapSys, stackInuse int64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return int64(m.HeapAlloc), int64(m.HeapSys), int64(m.StackInuse)
}
