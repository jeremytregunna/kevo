package engine

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jer/kevo/pkg/compaction"
	"github.com/jer/kevo/pkg/sstable"
)

// setupCompaction initializes the compaction manager for the engine
func (e *Engine) setupCompaction() error {
	// Create the compaction manager
	e.compactionMgr = compaction.NewCompactionManager(e.cfg, e.sstableDir)

	// Start the compaction manager
	return e.compactionMgr.Start()
}

// shutdownCompaction stops the compaction manager
func (e *Engine) shutdownCompaction() error {
	if e.compactionMgr != nil {
		return e.compactionMgr.Stop()
	}
	return nil
}

// TriggerCompaction forces a compaction cycle
func (e *Engine) TriggerCompaction() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed.Load() {
		return ErrEngineClosed
	}

	if e.compactionMgr == nil {
		return fmt.Errorf("compaction manager not initialized")
	}

	return e.compactionMgr.TriggerCompaction()
}

// CompactRange forces compaction on a specific key range
func (e *Engine) CompactRange(startKey, endKey []byte) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed.Load() {
		return ErrEngineClosed
	}

	if e.compactionMgr == nil {
		return fmt.Errorf("compaction manager not initialized")
	}

	return e.compactionMgr.CompactRange(startKey, endKey)
}

// reloadSSTables reloads all SSTables from disk after compaction
func (e *Engine) reloadSSTables() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Close existing SSTable readers
	for _, reader := range e.sstables {
		if err := reader.Close(); err != nil {
			return fmt.Errorf("failed to close SSTable reader: %w", err)
		}
	}

	// Clear the list
	e.sstables = e.sstables[:0]

	// Find all SSTable files
	entries, err := os.ReadDir(e.sstableDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist yet
		}
		return fmt.Errorf("failed to read SSTable directory: %w", err)
	}

	// Open all SSTable files
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sst" {
			continue // Skip directories and non-SSTable files
		}

		path := filepath.Join(e.sstableDir, entry.Name())
		reader, err := sstable.OpenReader(path)
		if err != nil {
			return fmt.Errorf("failed to open SSTable %s: %w", path, err)
		}

		e.sstables = append(e.sstables, reader)
	}

	return nil
}

// GetCompactionStats returns statistics about the compaction state
func (e *Engine) GetCompactionStats() (map[string]interface{}, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	if e.compactionMgr == nil {
		return map[string]interface{}{
			"enabled": false,
		}, nil
	}

	stats := e.compactionMgr.GetCompactionStats()
	stats["enabled"] = true

	// Add memtable information
	stats["memtables"] = map[string]interface{}{
		"active":     len(e.memTablePool.GetMemTables()),
		"immutable":  len(e.immutableMTs),
		"total_size": e.memTablePool.TotalSize(),
	}

	return stats, nil
}

// maybeScheduleCompaction checks if compaction should be scheduled
func (e *Engine) maybeScheduleCompaction() {
	// No immediate action needed - the compaction manager handles it all
	// This is just a hook for future expansion

	// We could trigger a manual compaction in some cases
	if e.compactionMgr != nil && len(e.sstables) > e.cfg.MaxMemTables*2 {
		go func() {
			err := e.compactionMgr.TriggerCompaction()
			if err != nil {
				// In a real implementation, we would log this error
			}
		}()
	}
}
