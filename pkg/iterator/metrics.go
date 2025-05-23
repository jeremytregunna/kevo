// ABOUTME: Iterator telemetry metrics interface and implementation for tracking iterator operations
// ABOUTME: Provides instrumentation for seek operations, range scans, hierarchical merges, and efficiency tracking

package iterator

import (
	"context"
	"time"

	"github.com/KevoDB/kevo/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// IteratorMetrics defines the interface for iterator telemetry operations.
// All metrics are optional - implementations can safely be no-op.
type IteratorMetrics interface {
	telemetry.ComponentMetrics

	// RecordSeek records metrics for seek operations and their efficiency.
	RecordSeek(ctx context.Context, duration time.Duration, found bool, keysScanned int64)

	// RecordNext records metrics for Next() operation calls.
	RecordNext(ctx context.Context, duration time.Duration, valid bool)

	// RecordRangeScan records metrics for range scan operations.
	RecordRangeScan(ctx context.Context, duration time.Duration, keysReturned int64, startKey, endKey []byte)

	// RecordHierarchicalMerge records metrics for hierarchical iterator merging.
	RecordHierarchicalMerge(ctx context.Context, sourceCount int, mergeTime time.Duration)

	// RecordIteratorType records the type of iterator being used.
	RecordIteratorType(ctx context.Context, iteratorType string, sourceCount int)

	// RecordBoundedOperation records metrics for bounded iterator operations.
	RecordBoundedOperation(ctx context.Context, operation string, duration time.Duration, withinBounds bool)

	// RecordFilteredOperation records metrics for filtered iterator operations.
	RecordFilteredOperation(ctx context.Context, operation string, duration time.Duration, passed bool)
}

// iteratorMetrics implements IteratorMetrics using the telemetry interface.
type iteratorMetrics struct {
	tel telemetry.Telemetry
}

// NewIteratorMetrics creates a new iterator metrics implementation.
// If tel is nil, returns a no-op implementation.
func NewIteratorMetrics(tel telemetry.Telemetry) IteratorMetrics {
	if tel == nil {
		return &noopIteratorMetrics{}
	}
	return &iteratorMetrics{tel: tel}
}

// NewNoopIteratorMetrics creates a no-op iterator metrics implementation for testing.
func NewNoopIteratorMetrics() IteratorMetrics {
	return &noopIteratorMetrics{}
}

// RecordSeek records seek operation metrics.
func (m *iteratorMetrics) RecordSeek(ctx context.Context, duration time.Duration, found bool, keysScanned int64) {
	// Record seek operation duration
	m.tel.RecordHistogram(ctx, "kevo.iterator.seek.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypeSeek),
		attribute.Bool("found", found),
	)

	// Record operation count
	m.tel.RecordCounter(ctx, "kevo.iterator.operations.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypeSeek),
		attribute.String(telemetry.AttrStatus, getStatusFromFound(found)),
	)

	// Record keys scanned during seek for efficiency tracking
	if keysScanned > 0 {
		m.tel.RecordHistogram(ctx, "kevo.iterator.seek.keys_scanned", float64(keysScanned),
			attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
			attribute.Bool("found", found),
		)
	}
}

// RecordNext records Next() operation metrics.
func (m *iteratorMetrics) RecordNext(ctx context.Context, duration time.Duration, valid bool) {
	// Record next operation duration
	m.tel.RecordHistogram(ctx, "kevo.iterator.next.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypeNext),
		attribute.Bool("valid", valid),
	)

	// Record operation count
	m.tel.RecordCounter(ctx, "kevo.iterator.operations.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, telemetry.OpTypeNext),
		attribute.String(telemetry.AttrStatus, getStatusFromValid(valid)),
	)
}

// RecordRangeScan records range scan operation metrics.
func (m *iteratorMetrics) RecordRangeScan(ctx context.Context, duration time.Duration, keysReturned int64, startKey, endKey []byte) {
	// Record range scan duration
	m.tel.RecordHistogram(ctx, "kevo.iterator.range_scan.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, "range_scan"),
	)

	// Record keys returned in range scan
	m.tel.RecordHistogram(ctx, "kevo.iterator.range_scan.keys_returned", float64(keysReturned),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
	)

	// Record range scan operation count
	m.tel.RecordCounter(ctx, "kevo.iterator.operations.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, "range_scan"),
		attribute.String(telemetry.AttrStatus, telemetry.StatusSuccess),
	)

	// Record range characteristics (optional, for debugging range patterns)
	if startKey != nil && endKey != nil {
		m.tel.RecordCounter(ctx, "kevo.iterator.range_scan.with_bounds", 1,
			attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		)
	}
}

// RecordHierarchicalMerge records hierarchical iterator merge metrics.
func (m *iteratorMetrics) RecordHierarchicalMerge(ctx context.Context, sourceCount int, mergeTime time.Duration) {
	// Record merge operation duration
	m.tel.RecordHistogram(ctx, "kevo.iterator.hierarchical.merge.duration", mergeTime.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, "hierarchical_merge"),
		attribute.Int("source_count", sourceCount),
	)

	// Record number of sources being merged
	m.tel.RecordHistogram(ctx, "kevo.iterator.hierarchical.source_count", float64(sourceCount),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
	)

	// Record merge operation count
	m.tel.RecordCounter(ctx, "kevo.iterator.operations.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, "hierarchical_merge"),
		attribute.String(telemetry.AttrStatus, telemetry.StatusSuccess),
	)
}

// RecordIteratorType records the type of iterator being used.
func (m *iteratorMetrics) RecordIteratorType(ctx context.Context, iteratorType string, sourceCount int) {
	m.tel.RecordCounter(ctx, "kevo.iterator.type.instantiated", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String("iterator_type", iteratorType),
		attribute.Int("source_count", sourceCount),
	)
}

// RecordBoundedOperation records bounded iterator operation metrics.
func (m *iteratorMetrics) RecordBoundedOperation(ctx context.Context, operation string, duration time.Duration, withinBounds bool) {
	// Record bounded operation duration
	m.tel.RecordHistogram(ctx, "kevo.iterator.bounded.operation.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, operation),
		attribute.String("iterator_type", "bounded"),
		attribute.Bool("within_bounds", withinBounds),
	)

	// Record operation count
	m.tel.RecordCounter(ctx, "kevo.iterator.operations.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, operation),
		attribute.String("iterator_type", "bounded"),
		attribute.String(telemetry.AttrStatus, getStatusFromWithinBounds(withinBounds)),
	)

	// Track bounds compliance for bounded iterators
	boundsValue := int64(0)
	if withinBounds {
		boundsValue = 1
	}
	m.tel.RecordCounter(ctx, "kevo.iterator.bounded.bounds_compliance", boundsValue,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, operation),
	)
}

// RecordFilteredOperation records filtered iterator operation metrics.
func (m *iteratorMetrics) RecordFilteredOperation(ctx context.Context, operation string, duration time.Duration, passed bool) {
	// Record filtered operation duration
	m.tel.RecordHistogram(ctx, "kevo.iterator.filtered.operation.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, operation),
		attribute.String("iterator_type", "filtered"),
		attribute.Bool("passed_filter", passed),
	)

	// Record operation count
	m.tel.RecordCounter(ctx, "kevo.iterator.operations.total", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, operation),
		attribute.String("iterator_type", "filtered"),
		attribute.String(telemetry.AttrStatus, getStatusFromPassed(passed)),
	)

	// Track filter pass/fail rates
	passValue := int64(0)
	if passed {
		passValue = 1
	}
	m.tel.RecordCounter(ctx, "kevo.iterator.filtered.pass_rate", passValue,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentIterator),
		attribute.String(telemetry.AttrOperationType, operation),
	)
}

// Close implements ComponentMetrics interface.
func (m *iteratorMetrics) Close() error {
	// Iterator metrics don't hold any resources that need cleanup
	return nil
}

// noopIteratorMetrics provides a no-op implementation for testing and disabled telemetry.
type noopIteratorMetrics struct{}

func (n *noopIteratorMetrics) RecordSeek(ctx context.Context, duration time.Duration, found bool, keysScanned int64) {
}

func (n *noopIteratorMetrics) RecordNext(ctx context.Context, duration time.Duration, valid bool) {}

func (n *noopIteratorMetrics) RecordRangeScan(ctx context.Context, duration time.Duration, keysReturned int64, startKey, endKey []byte) {
}

func (n *noopIteratorMetrics) RecordHierarchicalMerge(ctx context.Context, sourceCount int, mergeTime time.Duration) {
}

func (n *noopIteratorMetrics) RecordIteratorType(ctx context.Context, iteratorType string, sourceCount int) {
}

func (n *noopIteratorMetrics) RecordBoundedOperation(ctx context.Context, operation string, duration time.Duration, withinBounds bool) {
}

func (n *noopIteratorMetrics) RecordFilteredOperation(ctx context.Context, operation string, duration time.Duration, passed bool) {
}

func (n *noopIteratorMetrics) Close() error { return nil }

// Helper functions for converting boolean values to status strings
func getStatusFromFound(found bool) string {
	if found {
		return telemetry.StatusSuccess
	}
	return "not_found"
}

func getStatusFromValid(valid bool) string {
	if valid {
		return telemetry.StatusSuccess
	}
	return "end_of_iterator"
}

func getStatusFromWithinBounds(withinBounds bool) string {
	if withinBounds {
		return telemetry.StatusSuccess
	}
	return "out_of_bounds"
}

func getStatusFromPassed(passed bool) string {
	if passed {
		return telemetry.StatusSuccess
	}
	return "filtered_out"
}
