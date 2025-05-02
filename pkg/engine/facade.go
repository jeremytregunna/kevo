package engine

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/engine/compaction"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	"github.com/KevoDB/kevo/pkg/engine/storage"
	"github.com/KevoDB/kevo/pkg/engine/transaction"
	"github.com/KevoDB/kevo/pkg/stats"
	"github.com/KevoDB/kevo/pkg/wal"
)

// Ensure EngineFacade implements the Engine interface
var _ interfaces.Engine = (*EngineFacade)(nil)

// Using existing errors defined in engine.go

// EngineFacade implements the Engine interface and delegates to appropriate components
type EngineFacade struct {
	// Configuration
	cfg     *config.Config
	dataDir string

	// Core components
	storage    interfaces.StorageManager
	txManager  interfaces.TransactionManager
	compaction interfaces.CompactionManager
	stats      stats.Collector

	// State
	closed   atomic.Bool
	readOnly atomic.Bool // Flag to indicate if the engine is in read-only mode (for replicas)
}

// We keep the Engine name used in legacy code, but redirect it to our new implementation
type Engine = EngineFacade

// NewEngine creates a new storage engine using the facade pattern
// This replaces the legacy implementation
func NewEngine(dataDir string) (*EngineFacade, error) {
	return NewEngineFacade(dataDir)
}

// NewEngineFacade creates a new storage engine using the facade pattern
// This will eventually replace NewEngine once the refactoring is complete
func NewEngineFacade(dataDir string) (*EngineFacade, error) {
	// Create data and component directories
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Load or create the configuration
	var cfg *config.Config
	cfg, err := config.LoadConfigFromManifest(dataDir)
	if err != nil {
		if !errors.Is(err, config.ErrManifestNotFound) {
			return nil, fmt.Errorf("failed to load configuration: %w", err)
		}
		// Create a new configuration
		cfg = config.NewDefaultConfig(dataDir)
		if err := cfg.SaveManifest(dataDir); err != nil {
			return nil, fmt.Errorf("failed to save configuration: %w", err)
		}
	}

	// Create the statistics collector
	statsCollector := stats.NewAtomicCollector()

	// Create the storage manager
	storageManager, err := storage.NewManager(cfg, statsCollector)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}

	// Create the transaction manager
	txManager := transaction.NewManager(storageManager, statsCollector)

	// Create the compaction manager
	compactionManager, err := compaction.NewManager(cfg, cfg.SSTDir, statsCollector)
	if err != nil {
		return nil, fmt.Errorf("failed to create compaction manager: %w", err)
	}

	// Create the facade
	facade := &EngineFacade{
		cfg:     cfg,
		dataDir: dataDir,

		// Initialize components
		storage:    storageManager,
		txManager:  txManager,
		compaction: compactionManager,
		stats:      statsCollector,
	}

	// Start the compaction manager
	if err := compactionManager.Start(); err != nil {
		// If compaction fails to start, continue but log the error
		statsCollector.TrackError("compaction_start_error")
	}

	// Return the fully implemented facade with no error
	return facade, nil
}

// Put adds a key-value pair to the database
func (e *EngineFacade) Put(key, value []byte) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	// Reject writes in read-only mode
	if e.readOnly.Load() {
		return ErrReadOnlyMode
	}

	// Track the operation start
	e.stats.TrackOperation(stats.OpPut)

	// Track operation latency
	start := time.Now()

	// Delegate to storage component
	err := e.storage.Put(key, value)

	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpPut, latencyNs)

	// Track bytes written
	if err == nil {
		e.stats.TrackBytes(true, uint64(len(key)+len(value)))
	} else {
		e.stats.TrackError("put_error")
	}

	return err
}

// PutInternal adds a key-value pair to the database, bypassing the read-only check
// This is used by replication to apply entries even when in read-only mode
func (e *EngineFacade) PutInternal(key, value []byte) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	// Track the operation start
	e.stats.TrackOperation(stats.OpPut)

	// Track operation latency
	start := time.Now()

	// Delegate to storage component
	err := e.storage.Put(key, value)

	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpPut, latencyNs)

	// Track bytes written
	if err == nil {
		e.stats.TrackBytes(true, uint64(len(key)+len(value)))
	} else {
		e.stats.TrackError("put_error")
	}

	return err
}

// Get retrieves the value for the given key
func (e *EngineFacade) Get(key []byte) ([]byte, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	// Track the operation start
	e.stats.TrackOperation(stats.OpGet)

	// Track operation latency
	start := time.Now()

	// Delegate to storage component
	value, err := e.storage.Get(key)

	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpGet, latencyNs)

	// Track bytes read
	if err == nil {
		e.stats.TrackBytes(false, uint64(len(key)+len(value)))
	} else if errors.Is(err, ErrKeyNotFound) {
		// Not really an error, just a miss
	} else {
		e.stats.TrackError("get_error")
	}

	return value, err
}

// Delete removes a key from the database
func (e *EngineFacade) Delete(key []byte) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	// Reject writes in read-only mode
	if e.readOnly.Load() {
		return ErrReadOnlyMode
	}

	// Track the operation start
	e.stats.TrackOperation(stats.OpDelete)

	// Track operation latency
	start := time.Now()

	// Delegate to storage component
	err := e.storage.Delete(key)

	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpDelete, latencyNs)

	// Track bytes written (just key for deletes)
	if err == nil {
		e.stats.TrackBytes(true, uint64(len(key)))

		// Track tombstone in compaction manager
		if e.compaction != nil {
			e.compaction.TrackTombstone(key)
		}
	} else {
		e.stats.TrackError("delete_error")
	}

	return err
}

// DeleteInternal removes a key from the database, bypassing the read-only check
// This is used by replication to apply delete operations even when in read-only mode
func (e *EngineFacade) DeleteInternal(key []byte) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	// Track the operation start
	e.stats.TrackOperation(stats.OpDelete)

	// Track operation latency
	start := time.Now()

	// Delegate to storage component
	err := e.storage.Delete(key)

	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpDelete, latencyNs)

	// Track bytes written (just key for deletes)
	if err == nil {
		e.stats.TrackBytes(true, uint64(len(key)))

		// Track tombstone in compaction manager
		if e.compaction != nil {
			e.compaction.TrackTombstone(key)
		}
	} else {
		e.stats.TrackError("delete_error")
	}

	return err
}

// IsDeleted returns true if the key exists and is marked as deleted
func (e *EngineFacade) IsDeleted(key []byte) (bool, error) {
	if e.closed.Load() {
		return false, ErrEngineClosed
	}

	// Track operation
	e.stats.TrackOperation(stats.OpGet) // Using OpGet since it's a read operation

	// Track operation latency
	start := time.Now()
	isDeleted, err := e.storage.IsDeleted(key)
	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpGet, latencyNs)

	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		e.stats.TrackError("is_deleted_error")
	}

	return isDeleted, err
}

// GetIterator returns an iterator over the entire keyspace
func (e *EngineFacade) GetIterator() (iterator.Iterator, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	// Track the operation start
	e.stats.TrackOperation(stats.OpScan)

	// Track operation latency
	start := time.Now()
	iter, err := e.storage.GetIterator()
	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpScan, latencyNs)

	return iter, err
}

// GetRangeIterator returns an iterator limited to a specific key range
func (e *EngineFacade) GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	// Track the operation start with the range-specific operation type
	e.stats.TrackOperation(stats.OpScanRange)

	// Track operation latency
	start := time.Now()
	iter, err := e.storage.GetRangeIterator(startKey, endKey)
	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpScanRange, latencyNs)

	return iter, err
}

// BeginTransaction starts a new transaction with the given read-only flag
func (e *EngineFacade) BeginTransaction(readOnly bool) (interfaces.Transaction, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	// Force read-only mode if engine is in read-only mode
	if e.readOnly.Load() {
		readOnly = true
	}

	// Track the operation start
	e.stats.TrackOperation(stats.OpTxBegin)

	// Check if we have a registered transaction creator for legacy compatibility
	creator := GetRegisteredTransactionCreator()
	if creator != nil {
		// For backward compatibility with existing code that might be using the legacy transaction system
		// Try to use the registered creator
		legacyTx, err := CreateTransactionWithCreator(e, readOnly)
		if err == nil {
			// Track that we successfully created a transaction
			e.stats.TrackOperation(stats.OpTxBegin)
			// We need to adapt between the legacy and new interfaces
			// Both have the same methods, so we can use type assertion safely if we're
			// sure the LegacyTransaction also implements interfaces.Transaction
			return legacyTx.(interfaces.Transaction), nil
		}
		// If legacy creator fails, fall back to the new implementation
	}

	// Track operation latency
	start := time.Now()
	tx, err := e.txManager.BeginTransaction(readOnly)
	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpTxBegin, latencyNs)

	return tx, err
}

// ApplyBatch atomically applies a batch of operations
func (e *EngineFacade) ApplyBatch(entries []*wal.Entry) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	// Reject writes in read-only mode
	if e.readOnly.Load() {
		return ErrReadOnlyMode
	}

	// Track the operation - using a custom operation type might be good in the future
	e.stats.TrackOperation(stats.OpPut) // Using OpPut since batch operations are primarily writes

	// Count bytes for statistics
	var totalBytes uint64
	for _, entry := range entries {
		totalBytes += uint64(len(entry.Key))
		if entry.Value != nil {
			totalBytes += uint64(len(entry.Value))
		}
	}

	// Track operation latency
	start := time.Now()
	err := e.storage.ApplyBatch(entries)
	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpPut, latencyNs)

	// Track bytes and errors
	if err == nil {
		e.stats.TrackBytes(true, totalBytes)

		// Track tombstones in compaction manager for delete operations
		if e.compaction != nil {
			for _, entry := range entries {
				if entry.Type == wal.OpTypeDelete {
					e.compaction.TrackTombstone(entry.Key)
				}
			}
		}
	} else {
		e.stats.TrackError("batch_error")
	}

	return err
}

// ApplyBatchInternal atomically applies a batch of operations, bypassing the read-only check
// This is used by replication to apply batch operations even when in read-only mode
func (e *EngineFacade) ApplyBatchInternal(entries []*wal.Entry) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	// Track the operation - using a custom operation type might be good in the future
	e.stats.TrackOperation(stats.OpPut) // Using OpPut since batch operations are primarily writes

	// Count bytes for statistics
	var totalBytes uint64
	for _, entry := range entries {
		totalBytes += uint64(len(entry.Key))
		if entry.Value != nil {
			totalBytes += uint64(len(entry.Value))
		}
	}

	// Track operation latency
	start := time.Now()
	err := e.storage.ApplyBatch(entries)
	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpPut, latencyNs)

	// Track bytes and errors
	if err == nil {
		e.stats.TrackBytes(true, totalBytes)

		// Track tombstones in compaction manager for delete operations
		if e.compaction != nil {
			for _, entry := range entries {
				if entry.Type == wal.OpTypeDelete {
					e.compaction.TrackTombstone(entry.Key)
				}
			}
		}
	} else {
		e.stats.TrackError("batch_error")
	}

	return err
}

// FlushImMemTables flushes all immutable MemTables to disk
func (e *EngineFacade) FlushImMemTables() error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	// Track the operation start
	e.stats.TrackOperation(stats.OpFlush)

	// Track operation latency
	start := time.Now()
	err := e.storage.FlushMemTables()
	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpFlush, latencyNs)

	return err
}

// TriggerCompaction forces a compaction cycle
func (e *EngineFacade) TriggerCompaction() error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	// Track the operation start
	e.stats.TrackOperation(stats.OpCompact)

	// Track operation latency
	start := time.Now()
	err := e.compaction.TriggerCompaction()
	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpCompact, latencyNs)

	if err != nil {
		e.stats.TrackError("compaction_trigger_error")
	} else {
		// Track a successful compaction
		e.stats.TrackCompaction()
	}

	return err
}

// CompactRange forces compaction on a specific key range
func (e *EngineFacade) CompactRange(startKey, endKey []byte) error {
	if e.closed.Load() {
		return ErrEngineClosed
	}

	// Track the operation start
	e.stats.TrackOperation(stats.OpCompact)

	// Track bytes processed
	keyBytes := uint64(len(startKey) + len(endKey))
	e.stats.TrackBytes(false, keyBytes)

	// Track operation latency
	start := time.Now()
	err := e.compaction.CompactRange(startKey, endKey)
	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpCompact, latencyNs)

	if err != nil {
		e.stats.TrackError("compaction_range_error")
	} else {
		// Track a successful compaction
		e.stats.TrackCompaction()
	}

	return err
}

// GetStats returns the current statistics for the engine
func (e *EngineFacade) GetStats() map[string]interface{} {
	// Combine stats from all components
	stats := e.stats.GetStats()

	// Add component-specific stats
	if e.storage != nil {
		for k, v := range e.storage.GetStorageStats() {
			stats["storage_"+k] = v
		}
	}

	if e.txManager != nil {
		for k, v := range e.txManager.GetTransactionStats() {
			stats["tx_"+k] = v
		}
	}

	// Add state information
	stats["closed"] = e.closed.Load()

	return stats
}

// GetTransactionManager returns the transaction manager
func (e *EngineFacade) GetTransactionManager() interfaces.TransactionManager {
	return e.txManager
}

// GetCompactionStats returns statistics about the compaction state
func (e *EngineFacade) GetCompactionStats() (map[string]interface{}, error) {
	if e.closed.Load() {
		return nil, ErrEngineClosed
	}

	if e.compaction != nil {
		// Get compaction stats from the manager
		compactionStats := e.compaction.GetCompactionStats()

		// Add additional information
		baseStats := map[string]interface{}{
			"enabled": true,
		}

		// Merge the stats
		for k, v := range compactionStats {
			baseStats[k] = v
		}

		return baseStats, nil
	}

	return map[string]interface{}{
		"enabled": false,
	}, nil
}

// Close closes the storage engine
func (e *EngineFacade) Close() error {
	// First set the closed flag to prevent new operations
	if e.closed.Swap(true) {
		return nil // Already closed
	}

	// Track operation latency
	start := time.Now()

	var err error

	// Close components in reverse order of dependency

	// 1. First close compaction manager (to stop background tasks)
	if e.compaction != nil {
		e.stats.TrackOperation(stats.OpCompact)

		if compErr := e.compaction.Stop(); compErr != nil {
			err = compErr
			e.stats.TrackError("close_compaction_error")
		}
	}

	// 2. Close storage (which will close sstables and WAL)
	if e.storage != nil {
		if storageErr := e.storage.Close(); storageErr != nil {
			if err == nil {
				err = storageErr
			}
			e.stats.TrackError("close_storage_error")
		}
	}

	// Even though we're closing, track the latency for monitoring purposes
	latencyNs := uint64(time.Since(start).Nanoseconds())
	e.stats.TrackOperationWithLatency(stats.OpFlush, latencyNs) // Using OpFlush as a proxy for engine operations

	return err
}
