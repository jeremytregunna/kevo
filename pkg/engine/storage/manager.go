package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	engineIterator "github.com/KevoDB/kevo/pkg/engine/iterator"
	"github.com/KevoDB/kevo/pkg/memtable"
	"github.com/KevoDB/kevo/pkg/sstable"
	"github.com/KevoDB/kevo/pkg/stats"
	"github.com/KevoDB/kevo/pkg/wal"
)

// Ensure Manager implements the interfaces.StorageManager interface
var _ interfaces.StorageManager = (*Manager)(nil)

const (
	// SSTable filename format: level_sequence_timestamp.sst
	sstableFilenameFormat = "%d_%06d_%020d.sst"
)

// Common errors
var (
	ErrStorageClosed = errors.New("storage is closed")
	ErrKeyNotFound   = errors.New("key not found")
)

// Manager implements the interfaces.StorageManager interface
type Manager struct {
	// Configuration and paths
	cfg        *config.Config
	dataDir    string
	sstableDir string
	walDir     string

	// Write-ahead log
	wal *wal.WAL

	// Memory tables
	memTablePool *memtable.MemTablePool
	immutableMTs []*memtable.MemTable

	// Storage layer
	sstables []*sstable.Reader

	// State management
	nextFileNum uint64
	lastSeqNum  uint64
	bgFlushCh   chan struct{}
	closed      atomic.Bool

	// Statistics
	stats stats.Collector

	// Concurrency control
	mu      sync.RWMutex // Main lock for engine state
	flushMu sync.Mutex   // Lock for flushing operations
}

// NewManager creates a new storage manager
func NewManager(cfg *config.Config, statsCollector stats.Collector) (*Manager, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}

	// Set up paths
	dataDir := filepath.Join(cfg.SSTDir, "..") // Go up one level from SSTDir
	sstableDir := cfg.SSTDir
	walDir := cfg.WALDir

	// Create required directories
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	if err := os.MkdirAll(sstableDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create sstable directory: %w", err)
	}

	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal directory: %w", err)
	}

	// Create or reuse a WAL
	var walLogger *wal.WAL
	var err error

	// First try to reuse an existing WAL file
	walLogger, err = wal.ReuseWAL(cfg, walDir, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to check for reusable WAL: %w", err)
	}

	// If no suitable WAL found, create a new one
	if walLogger == nil {
		walLogger, err = wal.NewWAL(cfg, walDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create WAL: %w", err)
		}
	}

	// Create the MemTable pool
	memTablePool := memtable.NewMemTablePool(cfg)

	m := &Manager{
		cfg:          cfg,
		dataDir:      dataDir,
		sstableDir:   sstableDir,
		walDir:       walDir,
		wal:          walLogger,
		memTablePool: memTablePool,
		immutableMTs: make([]*memtable.MemTable, 0),
		sstables:     make([]*sstable.Reader, 0),
		bgFlushCh:    make(chan struct{}, 1),
		nextFileNum:  1,
		stats:        statsCollector,
	}

	// Load existing SSTables
	if err := m.loadSSTables(); err != nil {
		return nil, fmt.Errorf("failed to load SSTables: %w", err)
	}

	// Recover from WAL if any exist
	if err := m.recoverFromWAL(); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	// Start background flush goroutine
	go m.backgroundFlush()

	return m, nil
}

// Put adds a key-value pair to the database
func (m *Manager) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed.Load() {
		return ErrStorageClosed
	}

	// Append to WAL
	seqNum, err := m.wal.Append(wal.OpTypePut, key, value)
	if err != nil {
		m.stats.TrackError("wal_append_error")
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	// Add to MemTable
	m.memTablePool.Put(key, value, seqNum)
	m.lastSeqNum = seqNum

	// Update memtable size estimate
	m.stats.TrackMemTableSize(uint64(m.memTablePool.TotalSize()))

	// Check if MemTable needs to be flushed
	if m.memTablePool.IsFlushNeeded() {
		if err := m.scheduleFlush(); err != nil {
			m.stats.TrackError("flush_schedule_error")
			return fmt.Errorf("failed to schedule flush: %w", err)
		}
	}

	return nil
}

// Get retrieves the value for the given key
func (m *Manager) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed.Load() {
		return nil, ErrStorageClosed
	}

	// Check the MemTablePool (active + immutables)
	if val, found := m.memTablePool.Get(key); found {
		// The key was found, but check if it's a deletion marker
		if val == nil {
			// This is a deletion marker - the key exists but was deleted
			return nil, ErrKeyNotFound
		}
		return val, nil
	}

	// Check the SSTables (searching from newest to oldest)
	for i := len(m.sstables) - 1; i >= 0; i-- {
		// Create a custom iterator to check for tombstones directly
		iter := m.sstables[i].NewIterator()

		// Position at the target key
		if !iter.Seek(key) {
			// Key not found in this SSTable, continue to the next one
			continue
		}

		// If the keys don't match exactly, continue to the next SSTable
		if !bytes.Equal(iter.Key(), key) {
			continue
		}

		// If we reach here, we found the key in this SSTable

		// Check if this is a tombstone
		if iter.IsTombstone() {
			// Found a tombstone, so this key is definitely deleted
			return nil, ErrKeyNotFound
		}

		// Found a non-tombstone value for this key
		return iter.Value(), nil
	}

	return nil, ErrKeyNotFound
}

// Delete removes a key from the database
func (m *Manager) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed.Load() {
		return ErrStorageClosed
	}

	// Append to WAL
	seqNum, err := m.wal.Append(wal.OpTypeDelete, key, nil)
	if err != nil {
		m.stats.TrackError("wal_append_error")
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	// Add deletion marker to MemTable
	m.memTablePool.Delete(key, seqNum)
	m.lastSeqNum = seqNum

	// Update memtable size estimate
	m.stats.TrackMemTableSize(uint64(m.memTablePool.TotalSize()))

	// Check if MemTable needs to be flushed
	if m.memTablePool.IsFlushNeeded() {
		if err := m.scheduleFlush(); err != nil {
			m.stats.TrackError("flush_schedule_error")
			return fmt.Errorf("failed to schedule flush: %w", err)
		}
	}

	return nil
}

// IsDeleted returns true if the key exists and is marked as deleted
func (m *Manager) IsDeleted(key []byte) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed.Load() {
		return false, ErrStorageClosed
	}

	// Check MemTablePool first
	if val, found := m.memTablePool.Get(key); found {
		// If value is nil, it's a deletion marker
		return val == nil, nil
	}

	// Check SSTables in order from newest to oldest
	for i := len(m.sstables) - 1; i >= 0; i-- {
		iter := m.sstables[i].NewIterator()

		// Look for the key
		if !iter.Seek(key) {
			continue
		}

		// Check if it's an exact match
		if !bytes.Equal(iter.Key(), key) {
			continue
		}

		// Found the key - check if it's a tombstone
		return iter.IsTombstone(), nil
	}

	// Key not found at all
	return false, ErrKeyNotFound
}

// GetIterator returns an iterator over the entire keyspace
func (m *Manager) GetIterator() (iterator.Iterator, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed.Load() {
		return nil, ErrStorageClosed
	}

	// Get all memtables from the pool
	memTables := m.memTablePool.GetMemTables()

	// Create iterator using the factory
	factory := engineIterator.NewFactory()
	return factory.CreateIterator(memTables, m.sstables), nil
}

// GetRangeIterator returns an iterator limited to a specific key range
func (m *Manager) GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed.Load() {
		return nil, ErrStorageClosed
	}

	// Get all memtables from the pool
	memTables := m.memTablePool.GetMemTables()

	// Create range-limited iterator using the factory
	factory := engineIterator.NewFactory()
	return factory.CreateRangeIterator(memTables, m.sstables, startKey, endKey), nil
}

// ApplyBatch atomically applies a batch of operations
func (m *Manager) ApplyBatch(entries []*wal.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed.Load() {
		return ErrStorageClosed
	}

	// Append batch to WAL
	startSeqNum, err := m.wal.AppendBatch(entries)
	if err != nil {
		m.stats.TrackError("wal_append_batch_error")
		return fmt.Errorf("failed to append batch to WAL: %w", err)
	}

	// Apply each entry to the MemTable
	for i, entry := range entries {
		seqNum := startSeqNum + uint64(i)

		switch entry.Type {
		case wal.OpTypePut:
			m.memTablePool.Put(entry.Key, entry.Value, seqNum)
		case wal.OpTypeDelete:
			m.memTablePool.Delete(entry.Key, seqNum)
		}

		m.lastSeqNum = seqNum
	}

	// Update memtable size
	m.stats.TrackMemTableSize(uint64(m.memTablePool.TotalSize()))

	// Check if MemTable needs to be flushed
	if m.memTablePool.IsFlushNeeded() {
		if err := m.scheduleFlush(); err != nil {
			m.stats.TrackError("flush_schedule_error")
			return fmt.Errorf("failed to schedule flush: %w", err)
		}
	}

	return nil
}

// FlushMemTables flushes all immutable MemTables to disk
func (m *Manager) FlushMemTables() error {
	m.flushMu.Lock()
	defer m.flushMu.Unlock()

	// Track operation
	m.stats.TrackOperation(stats.OpFlush)

	// If no immutable MemTables, flush the active one if needed
	if len(m.immutableMTs) == 0 {
		tables := m.memTablePool.GetMemTables()
		if len(tables) > 0 && tables[0].ApproximateSize() > 0 {
			// In testing, we might want to force flush the active table too
			// Create a new WAL file for future writes
			if err := m.rotateWAL(); err != nil {
				m.stats.TrackError("wal_rotate_error")
				return fmt.Errorf("failed to rotate WAL: %w", err)
			}

			if err := m.flushMemTable(tables[0]); err != nil {
				m.stats.TrackError("memtable_flush_error")
				return fmt.Errorf("failed to flush active MemTable: %w", err)
			}

			return nil
		}

		return nil
	}

	// Create a new WAL file for future writes
	if err := m.rotateWAL(); err != nil {
		m.stats.TrackError("wal_rotate_error")
		return fmt.Errorf("failed to rotate WAL: %w", err)
	}

	// Flush each immutable MemTable
	for i, imMem := range m.immutableMTs {
		if err := m.flushMemTable(imMem); err != nil {
			m.stats.TrackError("memtable_flush_error")
			return fmt.Errorf("failed to flush MemTable %d: %w", i, err)
		}
	}

	// Clear the immutable list - the MemTablePool manages reuse
	m.immutableMTs = m.immutableMTs[:0]

	// Track flush count
	m.stats.TrackFlush()

	return nil
}

// GetMemTableSize returns the current size of all memtables
func (m *Manager) GetMemTableSize() uint64 {
	return uint64(m.memTablePool.TotalSize())
}

// IsFlushNeeded returns true if a flush is needed
func (m *Manager) IsFlushNeeded() bool {
	return m.memTablePool.IsFlushNeeded()
}

// GetSSTables returns a list of SSTable filenames
func (m *Manager) GetSSTables() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sstables := make([]string, 0, len(m.sstables))
	for _, table := range m.sstables {
		sstables = append(sstables, table.FilePath())
	}
	return sstables
}

// ReloadSSTables reloads all SSTables from disk
func (m *Manager) ReloadSSTables() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close existing SSTable readers
	for _, reader := range m.sstables {
		if err := reader.Close(); err != nil {
			return fmt.Errorf("failed to close SSTable reader: %w", err)
		}
	}

	// Clear the list
	m.sstables = m.sstables[:0]

	// Find all SSTable files
	entries, err := os.ReadDir(m.sstableDir)
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

		path := filepath.Join(m.sstableDir, entry.Name())
		reader, err := sstable.OpenReader(path)
		if err != nil {
			return fmt.Errorf("failed to open SSTable %s: %w", path, err)
		}

		m.sstables = append(m.sstables, reader)
	}

	return nil
}

// RotateWAL creates a new WAL file and closes the old one
func (m *Manager) RotateWAL() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.rotateWAL()
}

// rotateWAL is the internal implementation of RotateWAL
func (m *Manager) rotateWAL() error {
	// Close the current WAL
	if err := m.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	// Create a new WAL
	wal, err := wal.NewWAL(m.cfg, m.walDir)
	if err != nil {
		return fmt.Errorf("failed to create new WAL: %w", err)
	}

	m.wal = wal
	return nil
}

// GetStorageStats returns storage-specific statistics
func (m *Manager) GetStorageStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]interface{})

	stats["memtable_size"] = m.memTablePool.TotalSize()
	stats["immutable_memtable_count"] = len(m.immutableMTs)
	stats["sstable_count"] = len(m.sstables)
	stats["last_sequence"] = m.lastSeqNum

	return stats
}

// Close closes the storage manager
func (m *Manager) Close() error {
	// First set the closed flag - use atomic operation to prevent race conditions
	if m.closed.Swap(true) {
		return nil // Already closed
	}

	// Close the WAL
	if err := m.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	// Close SSTables
	for _, table := range m.sstables {
		if err := table.Close(); err != nil {
			return fmt.Errorf("failed to close SSTable: %w", err)
		}
	}

	return nil
}

// scheduleFlush switches to a new MemTable and schedules flushing of the old one
func (m *Manager) scheduleFlush() error {
	// Get the MemTable that needs to be flushed
	immutable := m.memTablePool.SwitchToNewMemTable()

	// Add to our list of immutable tables to track
	m.immutableMTs = append(m.immutableMTs, immutable)

	// Signal background flush
	select {
	case m.bgFlushCh <- struct{}{}:
		// Signal sent successfully
	default:
		// A flush is already scheduled
	}

	return nil
}

// flushMemTable flushes a MemTable to disk as an SSTable
func (m *Manager) flushMemTable(mem *memtable.MemTable) error {
	// Verify the memtable has data to flush
	if mem.ApproximateSize() == 0 {
		return nil
	}

	// Ensure the SSTable directory exists
	err := os.MkdirAll(m.sstableDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create SSTable directory: %w", err)
	}

	// Generate the SSTable filename: level_sequence_timestamp.sst
	fileNum := atomic.AddUint64(&m.nextFileNum, 1) - 1
	timestamp := time.Now().UnixNano()
	filename := fmt.Sprintf(sstableFilenameFormat, 0, fileNum, timestamp)
	sstPath := filepath.Join(m.sstableDir, filename)

	// Create a new SSTable writer
	writer, err := sstable.NewWriter(sstPath)
	if err != nil {
		return fmt.Errorf("failed to create SSTable writer: %w", err)
	}

	// Get an iterator over the MemTable
	iter := mem.NewIterator()
	count := 0
	var bytesWritten uint64

	// Since memtable's skiplist returns keys in sorted order,
	// but possibly with duplicates (newer versions of same key first),
	// we need to track all processed keys (including tombstones)
	processedKeys := make(map[string]struct{})

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		keyStr := string(key) // Use as map key

		// Skip keys we've already processed (including tombstones)
		if _, seen := processedKeys[keyStr]; seen {
			continue
		}

		// Mark this key as processed regardless of whether it's a value or tombstone
		processedKeys[keyStr] = struct{}{}

		// Only write non-tombstone entries to the SSTable
		if value := iter.Value(); value != nil {
			bytesWritten += uint64(len(key) + len(value))
			if err := writer.Add(key, value); err != nil {
				writer.Abort()
				return fmt.Errorf("failed to add entry to SSTable: %w", err)
			}
			count++
		}
	}

	if count == 0 {
		writer.Abort()
		return nil
	}

	// Finish writing the SSTable
	if err := writer.Finish(); err != nil {
		return fmt.Errorf("failed to finish SSTable: %w", err)
	}

	// Track bytes written to SSTable
	m.stats.TrackBytes(true, bytesWritten)

	// Verify the file was created
	if _, err := os.Stat(sstPath); os.IsNotExist(err) {
		return fmt.Errorf("SSTable file was not created at %s", sstPath)
	}

	// Open the new SSTable for reading
	reader, err := sstable.OpenReader(sstPath)
	if err != nil {
		return fmt.Errorf("failed to open SSTable: %w", err)
	}

	// Add the SSTable to the list
	m.mu.Lock()
	m.sstables = append(m.sstables, reader)
	m.mu.Unlock()

	return nil
}

// backgroundFlush runs in a goroutine and periodically flushes immutable MemTables
func (m *Manager) backgroundFlush() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.bgFlushCh:
			// Received a flush signal
			if m.closed.Load() {
				return
			}

			m.FlushMemTables()
		case <-ticker.C:
			// Periodic check
			if m.closed.Load() {
				return
			}

			m.mu.RLock()
			hasWork := len(m.immutableMTs) > 0
			m.mu.RUnlock()

			if hasWork {
				m.FlushMemTables()
			}
		}
	}
}

// loadSSTables loads existing SSTable files from disk
func (m *Manager) loadSSTables() error {
	// Get all SSTable files in the directory
	entries, err := os.ReadDir(m.sstableDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist yet
		}
		return fmt.Errorf("failed to read SSTable directory: %w", err)
	}

	// Loop through all entries
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sst" {
			continue // Skip directories and non-SSTable files
		}

		// Open the SSTable
		path := filepath.Join(m.sstableDir, entry.Name())
		reader, err := sstable.OpenReader(path)
		if err != nil {
			return fmt.Errorf("failed to open SSTable %s: %w", path, err)
		}

		// Add to the list
		m.sstables = append(m.sstables, reader)
	}

	return nil
}

// recoverFromWAL recovers memtables from existing WAL files
func (m *Manager) recoverFromWAL() error {
	startTime := m.stats.StartRecovery()

	// Check if WAL directory exists
	if _, err := os.Stat(m.walDir); os.IsNotExist(err) {
		return nil // No WAL directory, nothing to recover
	}

	// List all WAL files
	walFiles, err := wal.FindWALFiles(m.walDir)
	if err != nil {
		m.stats.TrackError("wal_find_error")
		return fmt.Errorf("error listing WAL files: %w", err)
	}

	filesRecovered := uint64(len(walFiles))

	// Get recovery options
	recoveryOpts := memtable.DefaultRecoveryOptions(m.cfg)

	// Recover memtables from WAL
	memTables, maxSeqNum, err := memtable.RecoverFromWAL(m.cfg, recoveryOpts)
	if err != nil {
		// If recovery fails, let's try cleaning up WAL files
		m.stats.TrackError("wal_recovery_error")

		// Create a backup directory
		backupDir := filepath.Join(m.walDir, "backup_"+time.Now().Format("20060102_150405"))
		if err := os.MkdirAll(backupDir, 0755); err != nil {
			return fmt.Errorf("failed to recover from WAL: %w", err)
		}

		// Move problematic WAL files to backup
		for _, walFile := range walFiles {
			destFile := filepath.Join(backupDir, filepath.Base(walFile))
			if err := os.Rename(walFile, destFile); err != nil {
				m.stats.TrackError("wal_backup_error")
			}
		}

		// Create a fresh WAL
		newWal, err := wal.NewWAL(m.cfg, m.walDir)
		if err != nil {
			return fmt.Errorf("failed to create new WAL after recovery: %w", err)
		}
		m.wal = newWal

		// Record recovery with no entries
		m.stats.FinishRecovery(startTime, filesRecovered, 0, 0)
		return nil
	}

	// Update recovery statistics based on actual entries recovered
	var entriesRecovered, corruptedEntries uint64
	if len(walFiles) > 0 {
		// Use WALDir function directly to get stats
		recoveryStats, statErr := wal.ReplayWALDir(m.cfg.WALDir, func(entry *wal.Entry) error {
			return nil // Just counting, not processing
		})

		if statErr == nil && recoveryStats != nil {
			entriesRecovered = recoveryStats.EntriesProcessed
			corruptedEntries = recoveryStats.EntriesSkipped
		}
	}

	// No memtables recovered or empty WAL
	if len(memTables) == 0 {
		m.stats.FinishRecovery(startTime, filesRecovered, entriesRecovered, corruptedEntries)
		return nil
	}

	// Update sequence numbers
	m.lastSeqNum = maxSeqNum

	// Update WAL sequence number to continue from where we left off
	if maxSeqNum > 0 {
		m.wal.UpdateNextSequence(maxSeqNum + 1)
	}

	// Add recovered memtables to the pool
	for i, memTable := range memTables {
		if i == len(memTables)-1 {
			// The last memtable becomes the active one
			m.memTablePool.SetActiveMemTable(memTable)
		} else {
			// Previous memtables become immutable
			memTable.SetImmutable()
			m.immutableMTs = append(m.immutableMTs, memTable)
		}
	}

	// Record recovery stats
	m.stats.FinishRecovery(startTime, filesRecovered, entriesRecovered, corruptedEntries)

	return nil
}
