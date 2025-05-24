package transaction

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/stats"
)

// Manager implements the TransactionManager interface
type Manager struct {
	// Storage backend for transaction operations
	storage StorageBackend

	// Statistics collector
	stats stats.Collector

	// Telemetry metrics for transaction operations
	metrics TransactionMetrics

	// Transaction isolation lock
	txLock sync.RWMutex

	// Transaction counters
	txStarted   atomic.Uint64
	txCompleted atomic.Uint64
	txAborted   atomic.Uint64

	// TTL settings
	readOnlyTxTTL  time.Duration
	readWriteTxTTL time.Duration
	idleTxTimeout  time.Duration
}

// NewManager creates a new transaction manager with default TTL settings
func NewManager(storage StorageBackend, stats stats.Collector) *Manager {
	return &Manager{
		storage:        storage,
		stats:          stats,
		metrics:        NewNoopTransactionMetrics(), // Default to no-op, will be replaced by SetTelemetry
		readOnlyTxTTL:  3 * time.Minute,             // 3 minutes
		readWriteTxTTL: 1 * time.Minute,             // 1 minute
		idleTxTimeout:  30 * time.Second,            // 30 seconds
	}
}

// NewManagerWithTTL creates a new transaction manager with custom TTL settings
func NewManagerWithTTL(storage StorageBackend, stats stats.Collector, readOnlyTTL, readWriteTTL, idleTimeout time.Duration) *Manager {
	return &Manager{
		storage:        storage,
		stats:          stats,
		metrics:        NewNoopTransactionMetrics(), // Default to no-op, will be replaced by SetTelemetry
		readOnlyTxTTL:  readOnlyTTL,
		readWriteTxTTL: readWriteTTL,
		idleTxTimeout:  idleTimeout,
	}
}

// SetTelemetry allows post-creation telemetry injection from engine facade
func (m *Manager) SetTelemetry(tel interface{}) {
	if txMetrics, ok := tel.(TransactionMetrics); ok {
		m.metrics = txMetrics
	}
}

// BeginTransaction starts a new transaction
func (m *Manager) BeginTransaction(readOnly bool) (Transaction, error) {
	ctx := context.Background()
	lockWaitStart := time.Now()

	// Track transaction start
	if m.stats != nil {
		m.stats.TrackOperation(stats.OpTxBegin)
	}
	m.txStarted.Add(1)

	// Record transaction start telemetry
	if m.metrics != nil {
		m.recordTransactionStartMetrics(ctx, readOnly)
	}

	// Convert to transaction mode
	mode := ReadWrite
	if readOnly {
		mode = ReadOnly
	}

	// Create a new transaction
	now := time.Now()

	// Set TTL based on transaction mode
	var ttl time.Duration
	if mode == ReadOnly {
		ttl = m.readOnlyTxTTL
	} else {
		ttl = m.readWriteTxTTL
	}

	tx := &TransactionImpl{
		storage:        m.storage,
		mode:           mode,
		buffer:         NewBuffer(),
		rwLock:         &m.txLock,
		stats:          m,
		metrics:        m.metrics, // Pass telemetry to transaction
		creationTime:   now,
		lastActiveTime: now,
		ttl:            ttl,
	}

	// Set transaction as active
	tx.active.Store(true)

	// Acquire appropriate lock and record lock wait time
	if mode == ReadOnly {
		m.txLock.RLock()
		tx.hasReadLock.Store(true)
		if m.metrics != nil {
			m.recordLockWaitMetrics(ctx, lockWaitStart, "read")
		}
	} else {
		m.txLock.Lock()
		tx.hasWriteLock.Store(true)
		if m.metrics != nil {
			m.recordLockWaitMetrics(ctx, lockWaitStart, "write")
		}
	}

	return tx, nil
}

// GetRWLock returns the transaction isolation lock
func (m *Manager) GetRWLock() *sync.RWMutex {
	return &m.txLock
}

// IncrementTxCompleted increments the completed transaction counter
func (m *Manager) IncrementTxCompleted() {
	m.txCompleted.Add(1)

	// Track the commit operation
	if m.stats != nil {
		m.stats.TrackOperation(stats.OpTxCommit)
	}
}

// IncrementTxAborted increments the aborted transaction counter
func (m *Manager) IncrementTxAborted() {
	m.txAborted.Add(1)

	// Track the rollback operation
	if m.stats != nil {
		m.stats.TrackOperation(stats.OpTxRollback)
	}
}

// GetTransactionStats returns transaction statistics
func (m *Manager) GetTransactionStats() map[string]interface{} {
	stats := make(map[string]interface{})

	stats["tx_started"] = m.txStarted.Load()
	stats["tx_completed"] = m.txCompleted.Load()
	stats["tx_aborted"] = m.txAborted.Load()

	// Calculate active transactions
	active := m.txStarted.Load() - m.txCompleted.Load() - m.txAborted.Load()
	stats["tx_active"] = active

	return stats
}

// Helper methods for telemetry recording with panic protection

// recordTransactionStartMetrics records telemetry for transaction start
func (m *Manager) recordTransactionStartMetrics(ctx context.Context, readOnly bool) {
	defer func() {
		if r := recover(); r != nil {
			// Log telemetry panic but don't fail the operation
		}
	}()
	m.metrics.RecordTransactionStart(ctx, readOnly)
}

// recordLockWaitMetrics records telemetry for lock wait duration
func (m *Manager) recordLockWaitMetrics(ctx context.Context, start time.Time, lockType string) {
	defer func() {
		if r := recover(); r != nil {
			// Log telemetry panic but don't fail the operation
		}
	}()
	m.metrics.RecordLockWait(ctx, time.Since(start), lockType)
}
