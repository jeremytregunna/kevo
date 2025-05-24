// ABOUTME: Tests for core telemetry interface and no-op implementation functionality
// ABOUTME: Validates telemetry recording, span creation, and lifecycle management using real telemetry operations

package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func TestNoopTelemetry(t *testing.T) {
	tel := NewNoop()

	ctx := context.Background()

	// Test that no-op operations don't panic
	tel.RecordHistogram(ctx, "test.histogram", 1.5, attribute.String("key", "value"))
	tel.RecordCounter(ctx, "test.counter", 10, attribute.String("key", "value"))

	// Test span creation
	spanCtx, span := tel.StartSpan(ctx, "test.span", attribute.String("test", "value"))
	if spanCtx == nil {
		t.Error("StartSpan returned nil context")
	}
	if span == nil {
		t.Error("StartSpan returned nil span")
	}
	span.End()

	// Test shutdown
	if err := tel.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}
}

func TestNewForTesting(t *testing.T) {
	tel := NewForTesting()
	if tel == nil {
		t.Error("NewForTesting returned nil")
	}

	// Verify it behaves like no-op
	ctx := context.Background()
	tel.RecordHistogram(ctx, "test", 1.0)
	tel.RecordCounter(ctx, "test", 1)
}

func TestNewDisabled(t *testing.T) {
	tel := NewDisabled()
	if tel == nil {
		t.Error("NewDisabled returned nil")
	}

	// Verify it behaves like no-op
	ctx := context.Background()
	tel.RecordHistogram(ctx, "test", 1.0)
	tel.RecordCounter(ctx, "test", 1)
}

func TestRecordDuration(t *testing.T) {
	tel := NewNoop()
	ctx := context.Background()
	start := time.Now()

	// Sleep briefly to ensure duration > 0
	time.Sleep(time.Millisecond)

	// Test that RecordDuration doesn't panic with no-op telemetry
	RecordDuration(ctx, tel, "test.duration", start, attribute.String("op", "test"))
}

func TestRecordBytes(t *testing.T) {
	tel := NewNoop()
	ctx := context.Background()

	// Test that RecordBytes doesn't panic with no-op telemetry
	RecordBytes(ctx, tel, "test.bytes", 1024, attribute.String("op", "test"))
}

func TestAttributeConstants(t *testing.T) {
	// Verify that all attribute constants are defined
	attributes := []string{
		AttrOperationType,
		AttrOperationName,
		AttrComponent,
		AttrLayer,
		AttrStatus,
		AttrSuccess,
		AttrErrorType,
		AttrFileID,
		AttrTableID,
		AttrLevel,
		AttrReason,
	}

	for _, attr := range attributes {
		if attr == "" {
			t.Errorf("Attribute constant is empty: %s", attr)
		}
	}
}

func TestOperationTypeConstants(t *testing.T) {
	// Verify that all operation type constants are defined
	opTypes := []string{
		OpTypePut,
		OpTypeDelete,
		OpTypeGet,
		OpTypeScan,
		OpTypeFlush,
		OpTypeSync,
	}

	for _, opType := range opTypes {
		if opType == "" {
			t.Errorf("Operation type constant is empty: %s", opType)
		}
	}
}

func TestStatusConstants(t *testing.T) {
	// Verify that all status constants are defined
	statuses := []string{
		StatusSuccess,
		StatusError,
		StatusTimeout,
	}

	for _, status := range statuses {
		if status == "" {
			t.Errorf("Status constant is empty: %s", status)
		}
	}
}

func TestComponentConstants(t *testing.T) {
	// Verify that all component constants are defined
	components := []string{
		ComponentWAL,
		ComponentMemTable,
		ComponentSSTable,
		ComponentCompaction,
		ComponentStorage,
		ComponentEngine,
	}

	for _, component := range components {
		if component == "" {
			t.Errorf("Component constant is empty: %s", component)
		}
	}
}

func TestTelemetryInterfaceComplianceNoOp(t *testing.T) {
	// Verify that NoopTelemetry implements Telemetry interface
	var tel Telemetry = &NoopTelemetry{}

	ctx := context.Background()

	// Test all interface methods
	tel.RecordHistogram(ctx, "test", 1.0)
	tel.RecordCounter(ctx, "test", 1)

	spanCtx, span := tel.StartSpan(ctx, "test")
	if spanCtx == nil || span == nil {
		t.Error("StartSpan should return valid context and span")
	}
	span.End()

	if err := tel.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown should not return error for no-op: %v", err)
	}
}
