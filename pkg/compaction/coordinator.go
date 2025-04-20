package compaction

import (
	"fmt"
	"sync"
	"time"

	"github.com/jer/kevo/pkg/config"
)

// CompactionCoordinatorOptions holds configuration options for the coordinator
type CompactionCoordinatorOptions struct {
	// Compaction strategy
	Strategy CompactionStrategy

	// Compaction executor
	Executor CompactionExecutor

	// File tracker
	FileTracker FileTracker

	// Tombstone manager
	TombstoneManager TombstoneManager

	// Compaction interval in seconds
	CompactionInterval int64
}

// DefaultCompactionCoordinator is the default implementation of CompactionCoordinator
type DefaultCompactionCoordinator struct {
	// Configuration
	cfg *config.Config

	// SSTable directory
	sstableDir string

	// Compaction strategy
	strategy CompactionStrategy

	// Compaction executor
	executor CompactionExecutor

	// File tracker
	fileTracker FileTracker

	// Tombstone manager
	tombstoneManager TombstoneManager

	// Next sequence number for SSTable files
	nextSeq uint64

	// Compaction state
	running      bool
	stopCh       chan struct{}
	compactingMu sync.Mutex

	// Last set of files produced by compaction
	lastCompactionOutputs []string
	resultsMu             sync.RWMutex

	// Compaction interval in seconds
	compactionInterval int64
}

// NewCompactionCoordinator creates a new compaction coordinator
func NewCompactionCoordinator(cfg *config.Config, sstableDir string, options CompactionCoordinatorOptions) *DefaultCompactionCoordinator {
	// Set defaults for any missing components
	if options.FileTracker == nil {
		options.FileTracker = NewFileTracker()
	}

	if options.TombstoneManager == nil {
		options.TombstoneManager = NewTombstoneTracker(24 * time.Hour)
	}

	if options.Executor == nil {
		options.Executor = NewCompactionExecutor(cfg, sstableDir, options.TombstoneManager)
	}

	if options.Strategy == nil {
		options.Strategy = NewTieredCompactionStrategy(cfg, sstableDir, options.Executor)
	}

	if options.CompactionInterval <= 0 {
		options.CompactionInterval = 1 // Default to 1 second
	}

	return &DefaultCompactionCoordinator{
		cfg:                   cfg,
		sstableDir:            sstableDir,
		strategy:              options.Strategy,
		executor:              options.Executor,
		fileTracker:           options.FileTracker,
		tombstoneManager:      options.TombstoneManager,
		nextSeq:               1,
		stopCh:                make(chan struct{}),
		lastCompactionOutputs: make([]string, 0),
		compactionInterval:    options.CompactionInterval,
	}
}

// Start begins background compaction
func (c *DefaultCompactionCoordinator) Start() error {
	c.compactingMu.Lock()
	defer c.compactingMu.Unlock()

	if c.running {
		return nil // Already running
	}

	// Load existing SSTables
	if err := c.strategy.LoadSSTables(); err != nil {
		return fmt.Errorf("failed to load SSTables: %w", err)
	}

	c.running = true
	c.stopCh = make(chan struct{})

	// Start background worker
	go c.compactionWorker()

	return nil
}

// Stop halts background compaction
func (c *DefaultCompactionCoordinator) Stop() error {
	c.compactingMu.Lock()
	defer c.compactingMu.Unlock()

	if !c.running {
		return nil // Already stopped
	}

	// Signal the worker to stop
	close(c.stopCh)
	c.running = false

	// Close strategy
	return c.strategy.Close()
}

// TrackTombstone adds a key to the tombstone tracker
func (c *DefaultCompactionCoordinator) TrackTombstone(key []byte) {
	// Track the tombstone in our tracker
	if c.tombstoneManager != nil {
		c.tombstoneManager.AddTombstone(key)
	}
}

// ForcePreserveTombstone marks a tombstone for special handling during compaction
// This is primarily for testing purposes, to ensure specific tombstones are preserved
func (c *DefaultCompactionCoordinator) ForcePreserveTombstone(key []byte) {
	if c.tombstoneManager != nil {
		c.tombstoneManager.ForcePreserveTombstone(key)
	}
}

// MarkFileObsolete marks a file as obsolete (can be deleted)
// For backward compatibility with tests
func (c *DefaultCompactionCoordinator) MarkFileObsolete(path string) {
	c.fileTracker.MarkFileObsolete(path)
}

// CleanupObsoleteFiles removes files that are no longer needed
// For backward compatibility with tests
func (c *DefaultCompactionCoordinator) CleanupObsoleteFiles() error {
	return c.fileTracker.CleanupObsoleteFiles()
}

// compactionWorker runs the compaction loop
func (c *DefaultCompactionCoordinator) compactionWorker() {
	// Ensure a minimum interval of 1 second
	interval := c.compactionInterval
	if interval <= 0 {
		interval = 1
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			// Only one compaction at a time
			c.compactingMu.Lock()

			// Run a compaction cycle
			err := c.runCompactionCycle()
			if err != nil {
				// In a real system, we'd log this error
				// fmt.Printf("Compaction error: %v\n", err)
			}

			// Try to clean up obsolete files
			err = c.fileTracker.CleanupObsoleteFiles()
			if err != nil {
				// In a real system, we'd log this error
				// fmt.Printf("Cleanup error: %v\n", err)
			}

			// Collect tombstone garbage periodically
			if manager, ok := c.tombstoneManager.(interface{ CollectGarbage() }); ok {
				manager.CollectGarbage()
			}

			c.compactingMu.Unlock()
		}
	}
}

// runCompactionCycle performs a single compaction cycle
func (c *DefaultCompactionCoordinator) runCompactionCycle() error {
	// Reload SSTables to get fresh information
	if err := c.strategy.LoadSSTables(); err != nil {
		return fmt.Errorf("failed to load SSTables: %w", err)
	}

	// Select files for compaction
	task, err := c.strategy.SelectCompaction()
	if err != nil {
		return fmt.Errorf("failed to select files for compaction: %w", err)
	}

	// If no compaction needed, return
	if task == nil {
		return nil
	}

	// Mark files as pending
	for _, files := range task.InputFiles {
		for _, file := range files {
			c.fileTracker.MarkFilePending(file.Path)
		}
	}

	// Perform compaction
	outputFiles, err := c.executor.CompactFiles(task)

	// Unmark files as pending
	for _, files := range task.InputFiles {
		for _, file := range files {
			c.fileTracker.UnmarkFilePending(file.Path)
		}
	}

	// Track the compaction outputs for statistics
	if err == nil && len(outputFiles) > 0 {
		// Record the compaction result
		c.resultsMu.Lock()
		c.lastCompactionOutputs = outputFiles
		c.resultsMu.Unlock()
	}

	// Handle compaction errors
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Mark input files as obsolete
	for _, files := range task.InputFiles {
		for _, file := range files {
			c.fileTracker.MarkFileObsolete(file.Path)
		}
	}

	// Try to clean up the files immediately
	return c.fileTracker.CleanupObsoleteFiles()
}

// TriggerCompaction forces a compaction cycle
func (c *DefaultCompactionCoordinator) TriggerCompaction() error {
	c.compactingMu.Lock()
	defer c.compactingMu.Unlock()

	return c.runCompactionCycle()
}

// CompactRange triggers compaction on a specific key range
func (c *DefaultCompactionCoordinator) CompactRange(minKey, maxKey []byte) error {
	c.compactingMu.Lock()
	defer c.compactingMu.Unlock()

	// Load current SSTable information
	if err := c.strategy.LoadSSTables(); err != nil {
		return fmt.Errorf("failed to load SSTables: %w", err)
	}

	// Delegate to the strategy for actual compaction
	return c.strategy.CompactRange(minKey, maxKey)
}

// GetCompactionStats returns statistics about the compaction state
func (c *DefaultCompactionCoordinator) GetCompactionStats() map[string]interface{} {
	c.resultsMu.RLock()
	defer c.resultsMu.RUnlock()

	stats := make(map[string]interface{})

	// Include info about last compaction
	stats["last_outputs_count"] = len(c.lastCompactionOutputs)

	// If there are recent compaction outputs, include information
	if len(c.lastCompactionOutputs) > 0 {
		stats["last_outputs"] = c.lastCompactionOutputs
	}

	return stats
}
