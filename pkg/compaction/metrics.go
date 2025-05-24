// ABOUTME: This file defines telemetry metrics interface for compaction operations
// ABOUTME: including strategy decisions, execution performance, and LSM level management monitoring

package compaction

import (
	"context"
	"time"

	"github.com/KevoDB/kevo/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// CompactionMetrics interface defines telemetry methods for compaction operations
type CompactionMetrics interface {
	// RecordCompactionStart records the start of a compaction operation
	RecordCompactionStart(ctx context.Context, level int, strategy string, inputFileCount int, inputSize int64)

	// RecordCompactionComplete records the completion of a compaction operation
	RecordCompactionComplete(ctx context.Context, duration time.Duration, inputSize int64, outputSize int64, tombstonesRemoved int64, success bool)

	// RecordLevelTransition records data movement between LSM levels
	RecordLevelTransition(ctx context.Context, fromLevel int, toLevel int, bytes int64)

	// RecordTombstoneCleanup records tombstone removal efficiency
	RecordTombstoneCleanup(ctx context.Context, tombstonesRemoved int64, level int)

	// RecordStrategyDecision records why compaction was triggered
	RecordStrategyDecision(ctx context.Context, strategy string, trigger string, urgency string, levelSizes map[int]int64)

	// RecordFileOperations records file creation/deletion during compaction
	RecordFileOperations(ctx context.Context, operation string, fileCount int, totalSize int64, level int)

	// RecordCompactionEfficiency records space reclamation metrics
	RecordCompactionEfficiency(ctx context.Context, compressionRatio float64, spaceReclaimed int64, duplicatesRemoved int64)

	// RecordLevelStats records current LSM level statistics
	RecordLevelStats(ctx context.Context, level int, fileCount int64, totalSize int64, keyCount int64)

	// RecordCompactionQueue records background compaction queue state
	RecordCompactionQueue(ctx context.Context, queueDepth int64, pendingBytes int64)

	// Close cleans up any resources used by the metrics
	Close() error
}

// compactionMetrics implements CompactionMetrics using the telemetry package
type compactionMetrics struct {
	tel telemetry.Telemetry
}

// NewCompactionMetrics creates a new CompactionMetrics implementation
func NewCompactionMetrics(tel telemetry.Telemetry) CompactionMetrics {
	return &compactionMetrics{
		tel: tel,
	}
}

// NewNoopCompactionMetrics creates a no-op CompactionMetrics for testing/disabled scenarios
func NewNoopCompactionMetrics() CompactionMetrics {
	return &noopCompactionMetrics{}
}

// RecordCompactionStart records the start of a compaction operation
func (m *compactionMetrics) RecordCompactionStart(ctx context.Context, level int, strategy string, inputFileCount int, inputSize int64) {
	m.tel.RecordCounter(ctx, "kevo.compaction.start.count", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.Int("level", level),
		attribute.String("strategy", strategy),
	)

	m.tel.RecordCounter(ctx, "kevo.compaction.input.files", int64(inputFileCount),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.Int("level", level),
		attribute.String("strategy", strategy),
	)

	m.tel.RecordCounter(ctx, "kevo.compaction.input.bytes", inputSize,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.Int("level", level),
		attribute.String("strategy", strategy),
	)
}

// RecordCompactionComplete records the completion of a compaction operation
func (m *compactionMetrics) RecordCompactionComplete(ctx context.Context, duration time.Duration, inputSize int64, outputSize int64, tombstonesRemoved int64, success bool) {
	m.tel.RecordHistogram(ctx, "kevo.compaction.execution.duration", duration.Seconds(),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.String(telemetry.AttrStatus, statusToString(success)),
	)

	m.tel.RecordCounter(ctx, "kevo.compaction.output.bytes", outputSize,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.String(telemetry.AttrStatus, statusToString(success)),
	)

	m.tel.RecordCounter(ctx, "kevo.compaction.tombstones.removed", tombstonesRemoved,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
	)

	// Calculate space reclaimed
	spaceReclaimed := inputSize - outputSize
	if spaceReclaimed > 0 {
		m.tel.RecordCounter(ctx, "kevo.compaction.space.reclaimed.bytes", spaceReclaimed,
			attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		)
	}

	// Calculate compression ratio
	if inputSize > 0 {
		compressionRatio := float64(outputSize) / float64(inputSize)
		m.tel.RecordHistogram(ctx, "kevo.compaction.compression.ratio", compressionRatio,
			attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		)
	}
}

// RecordLevelTransition records data movement between LSM levels
func (m *compactionMetrics) RecordLevelTransition(ctx context.Context, fromLevel int, toLevel int, bytes int64) {
	m.tel.RecordCounter(ctx, "kevo.compaction.level.transition.bytes", bytes,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.Int("from_level", fromLevel),
		attribute.Int("to_level", toLevel),
	)

	m.tel.RecordCounter(ctx, "kevo.compaction.level.transition.count", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.Int("from_level", fromLevel),
		attribute.Int("to_level", toLevel),
	)
}

// RecordTombstoneCleanup records tombstone removal efficiency
func (m *compactionMetrics) RecordTombstoneCleanup(ctx context.Context, tombstonesRemoved int64, level int) {
	m.tel.RecordCounter(ctx, "kevo.compaction.tombstones.cleaned", tombstonesRemoved,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.Int("level", level),
	)
}

// RecordStrategyDecision records why compaction was triggered
func (m *compactionMetrics) RecordStrategyDecision(ctx context.Context, strategy string, trigger string, urgency string, levelSizes map[int]int64) {
	m.tel.RecordCounter(ctx, "kevo.compaction.strategy.decision.count", 1,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.String("strategy", strategy),
		attribute.String("trigger", trigger),
		attribute.String("urgency", urgency),
	)

	// Record level sizes that influenced the decision
	for level, size := range levelSizes {
		m.tel.RecordHistogram(ctx, "kevo.compaction.level.size.bytes", float64(size),
			attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
			attribute.Int("level", level),
		)
	}
}

// RecordFileOperations records file creation/deletion during compaction
func (m *compactionMetrics) RecordFileOperations(ctx context.Context, operation string, fileCount int, totalSize int64, level int) {
	m.tel.RecordCounter(ctx, "kevo.compaction.file.operations.count", int64(fileCount),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.String(telemetry.AttrOperationType, operation),
		attribute.Int("level", level),
	)

	m.tel.RecordCounter(ctx, "kevo.compaction.file.operations.bytes", totalSize,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.String(telemetry.AttrOperationType, operation),
		attribute.Int("level", level),
	)
}

// RecordCompactionEfficiency records space reclamation metrics
func (m *compactionMetrics) RecordCompactionEfficiency(ctx context.Context, compressionRatio float64, spaceReclaimed int64, duplicatesRemoved int64) {
	m.tel.RecordHistogram(ctx, "kevo.compaction.efficiency.compression_ratio", compressionRatio,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
	)

	m.tel.RecordCounter(ctx, "kevo.compaction.efficiency.space_reclaimed", spaceReclaimed,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
	)

	m.tel.RecordCounter(ctx, "kevo.compaction.efficiency.duplicates_removed", duplicatesRemoved,
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
	)
}

// RecordLevelStats records current LSM level statistics
func (m *compactionMetrics) RecordLevelStats(ctx context.Context, level int, fileCount int64, totalSize int64, keyCount int64) {
	m.tel.RecordHistogram(ctx, "kevo.compaction.level.file_count", float64(fileCount),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.Int("level", level),
	)

	m.tel.RecordHistogram(ctx, "kevo.compaction.level.total_size", float64(totalSize),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.Int("level", level),
	)

	m.tel.RecordHistogram(ctx, "kevo.compaction.level.key_count", float64(keyCount),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
		attribute.Int("level", level),
	)
}

// RecordCompactionQueue records background compaction queue state
func (m *compactionMetrics) RecordCompactionQueue(ctx context.Context, queueDepth int64, pendingBytes int64) {
	m.tel.RecordHistogram(ctx, "kevo.compaction.queue.depth", float64(queueDepth),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
	)

	m.tel.RecordHistogram(ctx, "kevo.compaction.queue.pending_bytes", float64(pendingBytes),
		attribute.String(telemetry.AttrComponent, telemetry.ComponentCompaction),
	)
}

// Close cleans up any resources used by the metrics
func (m *compactionMetrics) Close() error {
	// No resources to clean up for this implementation
	return nil
}

// noopCompactionMetrics provides a no-op implementation for testing/disabled scenarios
type noopCompactionMetrics struct{}

func (n *noopCompactionMetrics) RecordCompactionStart(ctx context.Context, level int, strategy string, inputFileCount int, inputSize int64) {
}
func (n *noopCompactionMetrics) RecordCompactionComplete(ctx context.Context, duration time.Duration, inputSize int64, outputSize int64, tombstonesRemoved int64, success bool) {
}
func (n *noopCompactionMetrics) RecordLevelTransition(ctx context.Context, fromLevel int, toLevel int, bytes int64) {
}
func (n *noopCompactionMetrics) RecordTombstoneCleanup(ctx context.Context, tombstonesRemoved int64, level int) {
}
func (n *noopCompactionMetrics) RecordStrategyDecision(ctx context.Context, strategy string, trigger string, urgency string, levelSizes map[int]int64) {
}
func (n *noopCompactionMetrics) RecordFileOperations(ctx context.Context, operation string, fileCount int, totalSize int64, level int) {
}
func (n *noopCompactionMetrics) RecordCompactionEfficiency(ctx context.Context, compressionRatio float64, spaceReclaimed int64, duplicatesRemoved int64) {
}
func (n *noopCompactionMetrics) RecordLevelStats(ctx context.Context, level int, fileCount int64, totalSize int64, keyCount int64) {
}
func (n *noopCompactionMetrics) RecordCompactionQueue(ctx context.Context, queueDepth int64, pendingBytes int64) {
}
func (n *noopCompactionMetrics) Close() error { return nil }

// Helper functions for telemetry attribute conversion

// statusToString converts success/failure to string representation
func statusToString(success bool) string {
	if success {
		return telemetry.StatusSuccess
	}
	return telemetry.StatusError
}

// strategyToString converts compaction strategy to string representation
func strategyToString(strategy string) string {
	switch strategy {
	case "tiered":
		return "tiered"
	case "leveled":
		return "leveled"
	case "size":
		return "size"
	default:
		return "unknown"
	}
}

// triggerToString converts compaction trigger to string representation
func triggerToString(trigger string) string {
	switch trigger {
	case "size":
		return "size"
	case "file_count":
		return "file_count"
	case "manual":
		return "manual"
	case "scheduled":
		return "scheduled"
	default:
		return "unknown"
	}
}

// urgencyToString converts compaction urgency to string representation
func urgencyToString(urgency string) string {
	switch urgency {
	case "low":
		return "low"
	case "medium":
		return "medium"
	case "high":
		return "high"
	case "critical":
		return "critical"
	default:
		return "unknown"
	}
}
