// ABOUTME: This file defines telemetry metrics interface for transaction operations
// ABOUTME: including lock contention, duration tracking, and transaction lifecycle monitoring

package transaction

import (
	"context"
	"time"

	"github.com/KevoDB/kevo/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// TransactionMetrics interface defines telemetry methods for transaction operations
type TransactionMetrics interface {
	// RecordTransactionStart records the start of a transaction
	RecordTransactionStart(ctx context.Context, readOnly bool)

	// RecordLockWait records time spent waiting for locks
	RecordLockWait(ctx context.Context, duration time.Duration, lockType string)

	// RecordTransactionDuration records transaction lifecycle duration
	RecordTransactionDuration(ctx context.Context, duration time.Duration, outcome string, operationCount int64, readOnly bool)

	// RecordBufferUsage records transaction buffer memory usage
	RecordBufferUsage(ctx context.Context, size int64, operationCount int64)

	// RecordConcurrentReaders records current number of concurrent read transactions
	RecordConcurrentReaders(ctx context.Context, count int64)

	// RecordOperation records individual transaction operations
	RecordOperation(ctx context.Context, operation string, duration time.Duration, success bool)

	// RecordTTLViolation records when transactions exceed their TTL
	RecordTTLViolation(ctx context.Context, violationType string, exceededBy time.Duration, readOnly bool)

	// Close cleans up any resources used by the metrics
	Close() error
}

// transactionMetrics implements TransactionMetrics using the telemetry package
type transactionMetrics struct {
	tel telemetry.Telemetry
}

// NewTransactionMetrics creates a new TransactionMetrics implementation
func NewTransactionMetrics(tel telemetry.Telemetry) TransactionMetrics {
	return &transactionMetrics{
		tel: tel,
	}
}

// NewNoopTransactionMetrics creates a no-op TransactionMetrics for testing/disabled scenarios
func NewNoopTransactionMetrics() TransactionMetrics {
	return &noopTransactionMetrics{}
}

// RecordTransactionStart records the start of a transaction
func (m *transactionMetrics) RecordTransactionStart(ctx context.Context, readOnly bool) {
	m.tel.RecordCounter(ctx, "kevo.transaction.start.count", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentTransaction),
		attribute.String("readonly", boolToString(readOnly)),
		attribute.String("transaction_mode", transactionModeToString(readOnly)),
	)
}

// RecordLockWait records time spent waiting for locks
func (m *transactionMetrics) RecordLockWait(ctx context.Context, duration time.Duration, lockType string) {
	m.tel.RecordHistogram(ctx, "kevo.transaction.lock.wait.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentTransaction),
		attribute.String("lock_type", lockType),
	)
}

// RecordTransactionDuration records transaction lifecycle duration
func (m *transactionMetrics) RecordTransactionDuration(ctx context.Context, duration time.Duration, outcome string, operationCount int64, readOnly bool) {
	m.tel.RecordHistogram(ctx, "kevo.transaction.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentTransaction),
		attribute.String("outcome", outcome),
		attribute.String("readonly", boolToString(readOnly)),
		attribute.String("transaction_mode", transactionModeToString(readOnly)),
	)

	m.tel.RecordCounter(ctx, "kevo.transaction.operations.count", operationCount,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentTransaction),
		attribute.String("outcome", outcome),
		attribute.String("readonly", boolToString(readOnly)),
	)
}

// RecordBufferUsage records transaction buffer memory usage
func (m *transactionMetrics) RecordBufferUsage(ctx context.Context, size int64, operationCount int64) {
	m.tel.RecordHistogram(ctx, "kevo.transaction.buffer.size.bytes", float64(size),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentTransaction),
	)

	m.tel.RecordHistogram(ctx, "kevo.transaction.buffer.operations", float64(operationCount),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentTransaction),
	)
}

// RecordConcurrentReaders records current number of concurrent read transactions
func (m *transactionMetrics) RecordConcurrentReaders(ctx context.Context, count int64) {
	m.tel.RecordCounter(ctx, "kevo.transaction.concurrent.readers", count,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentTransaction),
	)
}

// RecordOperation records individual transaction operations
func (m *transactionMetrics) RecordOperation(ctx context.Context, operation string, duration time.Duration, success bool) {
	m.tel.RecordHistogram(ctx, "kevo.transaction.operation.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentTransaction),
		attribute.String(telemetry.AttrOperationType, operation),
		attribute.String("success", boolToString(success)),
	)

	m.tel.RecordCounter(ctx, "kevo.transaction.operation.count", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentTransaction),
		attribute.String(telemetry.AttrOperationType, operation),
		attribute.String("success", boolToString(success)),
	)
}

// RecordTTLViolation records when transactions exceed their TTL
func (m *transactionMetrics) RecordTTLViolation(ctx context.Context, violationType string, exceededBy time.Duration, readOnly bool) {
	m.tel.RecordCounter(ctx, "kevo.transaction.ttl.violation.count", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentTransaction),
		attribute.String("violation_type", violationType),
		attribute.String("readonly", boolToString(readOnly)),
	)

	m.tel.RecordHistogram(ctx, "kevo.transaction.ttl.exceeded.duration", exceededBy.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentTransaction),
		attribute.String("violation_type", violationType),
		attribute.String("readonly", boolToString(readOnly)),
	)
}

// Close cleans up any resources used by the metrics
func (m *transactionMetrics) Close() error {
	// No resources to clean up for this implementation
	return nil
}

// noopTransactionMetrics provides a no-op implementation for testing/disabled scenarios
type noopTransactionMetrics struct{}

func (n *noopTransactionMetrics) RecordTransactionStart(ctx context.Context, readOnly bool) {}
func (n *noopTransactionMetrics) RecordLockWait(ctx context.Context, duration time.Duration, lockType string) {
}
func (n *noopTransactionMetrics) RecordTransactionDuration(ctx context.Context, duration time.Duration, outcome string, operationCount int64, readOnly bool) {
}
func (n *noopTransactionMetrics) RecordBufferUsage(ctx context.Context, size int64, operationCount int64) {
}
func (n *noopTransactionMetrics) RecordConcurrentReaders(ctx context.Context, count int64) {}
func (n *noopTransactionMetrics) RecordOperation(ctx context.Context, operation string, duration time.Duration, success bool) {
}
func (n *noopTransactionMetrics) RecordTTLViolation(ctx context.Context, violationType string, exceededBy time.Duration, readOnly bool) {
}
func (n *noopTransactionMetrics) Close() error { return nil }

// Helper functions for telemetry attribute conversion

// boolToString converts a boolean to a string representation
func boolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// transactionModeToString converts transaction mode to string representation
func transactionModeToString(readOnly bool) string {
	if readOnly {
		return "readonly"
	}
	return "readwrite"
}

// lockTypeToString converts lock operation types to string representation
func lockTypeToString(lockType string) string {
	switch lockType {
	case "read":
		return "read"
	case "write":
		return "write"
	default:
		return "unknown"
	}
}

// outcomeToString converts transaction outcomes to string representation
func outcomeToString(outcome string) string {
	switch outcome {
	case "commit":
		return "commit"
	case "rollback":
		return "rollback"
	case "abort":
		return "abort"
	default:
		return "unknown"
	}
}

// operationToString converts transaction operations to string representation
func operationToString(operation string) string {
	switch operation {
	case "get":
		return "get"
	case "put":
		return "put"
	case "delete":
		return "delete"
	case "iterator":
		return "iterator"
	case "range_iterator":
		return "range_iterator"
	default:
		return "unknown"
	}
}
