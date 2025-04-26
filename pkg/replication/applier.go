package replication

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/KevoDB/kevo/pkg/wal"
)

var (
	// ErrApplierClosed indicates the applier has been closed
	ErrApplierClosed = errors.New("applier is closed")

	// ErrOutOfOrderEntry indicates an entry was received out of order
	ErrOutOfOrderEntry = errors.New("out of order entry")

	// ErrInvalidEntryType indicates an unknown or invalid entry type
	ErrInvalidEntryType = errors.New("invalid entry type")

	// ErrStorageError indicates a storage-related error occurred
	ErrStorageError = errors.New("storage error")
)

// StorageInterface defines the minimum storage requirements for the WALApplier
type StorageInterface interface {
	Put(key, value []byte) error
	Delete(key []byte) error
}

// WALApplier applies WAL entries from a primary node to a replica's storage
type WALApplier struct {
	// Storage is the local storage where entries will be applied
	storage StorageInterface

	// highestApplied is the highest Lamport timestamp of entries that have been applied
	highestApplied uint64

	// pendingEntries holds entries that were received out of order and are waiting for earlier entries
	pendingEntries map[uint64]*wal.Entry

	// appliedCount tracks the number of entries successfully applied
	appliedCount uint64

	// skippedCount tracks the number of entries skipped (already applied)
	skippedCount uint64

	// errorCount tracks the number of application errors
	errorCount uint64

	// closed indicates whether the applier is closed
	closed int32

	// concurrency management
	mu sync.RWMutex
}

// NewWALApplier creates a new WAL applier
func NewWALApplier(storage StorageInterface) *WALApplier {
	return &WALApplier{
		storage:        storage,
		highestApplied: 0,
		pendingEntries: make(map[uint64]*wal.Entry),
	}
}

// Apply applies a single WAL entry to the local storage
// Returns whether the entry was applied and any error
func (a *WALApplier) Apply(entry *wal.Entry) (bool, error) {
	if atomic.LoadInt32(&a.closed) == 1 {
		return false, ErrApplierClosed
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if this entry has already been applied
	if entry.SequenceNumber <= a.highestApplied {
		atomic.AddUint64(&a.skippedCount, 1)
		return false, nil
	}

	// Check if this is the next entry we're expecting
	if entry.SequenceNumber != a.highestApplied+1 {
		// Entry is out of order, store it for later application
		a.pendingEntries[entry.SequenceNumber] = entry
		return false, nil
	}

	// Apply this entry and any subsequent pending entries
	return a.applyEntryAndPending(entry)
}

// ApplyBatch applies a batch of WAL entries to the local storage
// Returns the number of entries applied and any error
func (a *WALApplier) ApplyBatch(entries []*wal.Entry) (int, error) {
	if atomic.LoadInt32(&a.closed) == 1 {
		return 0, ErrApplierClosed
	}

	if len(entries) == 0 {
		return 0, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Sort entries by sequence number
	sortEntriesByTimestamp(entries)

	// Track how many entries we actually apply
	applied := 0

	// Process each entry
	for _, entry := range entries {
		// Skip already applied entries
		if entry.SequenceNumber <= a.highestApplied {
			atomic.AddUint64(&a.skippedCount, 1)
			continue
		}

		// Check if this is the next entry we're expecting
		if entry.SequenceNumber == a.highestApplied+1 {
			// Apply this entry and any subsequent pending entries
			wasApplied, err := a.applyEntryAndPending(entry)
			if err != nil {
				return applied, err
			}
			if wasApplied {
				applied++
			}
		} else {
			// Entry is out of order, store it for later application
			a.pendingEntries[entry.SequenceNumber] = entry
		}
	}

	return applied, nil
}

// GetHighestApplied returns the highest applied Lamport timestamp
func (a *WALApplier) GetHighestApplied() uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.highestApplied
}

// GetStats returns statistics about the applier
func (a *WALApplier) GetStats() map[string]uint64 {
	a.mu.RLock()
	pendingCount := len(a.pendingEntries)
	highestApplied := a.highestApplied
	a.mu.RUnlock()

	return map[string]uint64{
		"appliedCount":   atomic.LoadUint64(&a.appliedCount),
		"skippedCount":   atomic.LoadUint64(&a.skippedCount),
		"errorCount":     atomic.LoadUint64(&a.errorCount),
		"pendingCount":   uint64(pendingCount),
		"highestApplied": highestApplied,
	}
}

// Close closes the applier
func (a *WALApplier) Close() error {
	if !atomic.CompareAndSwapInt32(&a.closed, 0, 1) {
		return nil // Already closed
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Clear any pending entries
	a.pendingEntries = make(map[uint64]*wal.Entry)

	return nil
}

// applyEntryAndPending applies the given entry and any pending entries that become applicable
// Caller must hold the lock
func (a *WALApplier) applyEntryAndPending(entry *wal.Entry) (bool, error) {
	// Apply the current entry
	if err := a.applyEntryToStorage(entry); err != nil {
		atomic.AddUint64(&a.errorCount, 1)
		return false, err
	}

	// Update highest applied timestamp
	a.highestApplied = entry.SequenceNumber
	atomic.AddUint64(&a.appliedCount, 1)

	// Check for pending entries that can now be applied
	nextSeq := a.highestApplied + 1
	for {
		nextEntry, exists := a.pendingEntries[nextSeq]
		if !exists {
			break
		}

		// Apply this pending entry
		if err := a.applyEntryToStorage(nextEntry); err != nil {
			atomic.AddUint64(&a.errorCount, 1)
			return true, err
		}

		// Update highest applied and remove from pending
		a.highestApplied = nextSeq
		delete(a.pendingEntries, nextSeq)
		atomic.AddUint64(&a.appliedCount, 1)

		// Look for the next one
		nextSeq++
	}

	return true, nil
}

// applyEntryToStorage applies a single WAL entry to the storage engine
// Caller must hold the lock
func (a *WALApplier) applyEntryToStorage(entry *wal.Entry) error {
	switch entry.Type {
	case wal.OpTypePut:
		if err := a.storage.Put(entry.Key, entry.Value); err != nil {
			return fmt.Errorf("%w: %v", ErrStorageError, err)
		}
	case wal.OpTypeDelete:
		if err := a.storage.Delete(entry.Key); err != nil {
			return fmt.Errorf("%w: %v", ErrStorageError, err)
		}
	case wal.OpTypeBatch:
		// In the WAL the batch entry itself doesn't contain the operations
		// Actual batch operations should be received as separate entries
		// with sequential sequence numbers
		return nil
	default:
		return fmt.Errorf("%w: type %d", ErrInvalidEntryType, entry.Type)
	}
	return nil
}

// PendingEntryCount returns the number of pending entries
func (a *WALApplier) PendingEntryCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.pendingEntries)
}

// ResetHighestApplied allows manually setting a new highest applied value
// This should only be used during initialization or recovery
func (a *WALApplier) ResetHighestApplied(value uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.highestApplied = value
}

// HasEntry checks if a specific entry timestamp has been applied or is pending
func (a *WALApplier) HasEntry(timestamp uint64) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if timestamp <= a.highestApplied {
		return true
	}

	_, exists := a.pendingEntries[timestamp]
	return exists
}
