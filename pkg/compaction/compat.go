package compaction

import (
	"time"

	"github.com/jer/kevo/pkg/config"
)

// NewCompactionManager creates a new compaction manager with the old API
// This is kept for backward compatibility with existing code
func NewCompactionManager(cfg *config.Config, sstableDir string) *DefaultCompactionCoordinator {
	// Create tombstone tracker with default 24-hour retention
	tombstones := NewTombstoneTracker(24 * time.Hour)

	// Create file tracker
	fileTracker := NewFileTracker()

	// Create compaction executor
	executor := NewCompactionExecutor(cfg, sstableDir, tombstones)

	// Create tiered compaction strategy
	strategy := NewTieredCompactionStrategy(cfg, sstableDir, executor)

	// Return the new coordinator
	return NewCompactionCoordinator(cfg, sstableDir, CompactionCoordinatorOptions{
		Strategy:           strategy,
		Executor:           executor,
		FileTracker:        fileTracker,
		TombstoneManager:   tombstones,
		CompactionInterval: cfg.CompactionInterval,
	})
}

// Temporary alias types for backward compatibility
type CompactionManager = DefaultCompactionCoordinator
type Compactor = BaseCompactionStrategy
type TieredCompactor = TieredCompactionStrategy

// NewCompactor creates a new compactor with the old API (backward compatibility)
func NewCompactor(cfg *config.Config, sstableDir string, tracker *TombstoneTracker) *BaseCompactionStrategy {
	return NewBaseCompactionStrategy(cfg, sstableDir)
}

// NewTieredCompactor creates a new tiered compactor with the old API (backward compatibility)
func NewTieredCompactor(cfg *config.Config, sstableDir string, tracker *TombstoneTracker) *TieredCompactionStrategy {
	executor := NewCompactionExecutor(cfg, sstableDir, tracker)
	return NewTieredCompactionStrategy(cfg, sstableDir, executor)
}
