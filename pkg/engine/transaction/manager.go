package transaction

import (
	"sync"
	"sync/atomic"

	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	"github.com/KevoDB/kevo/pkg/stats"
)

// Manager implements the interfaces.TransactionManager interface
type Manager struct {
	// Storage interface for transaction operations
	storage interfaces.StorageManager

	// Statistics collector
	stats stats.Collector

	// Transaction isolation lock
	txLock sync.RWMutex

	// Transaction counters
	txStarted   atomic.Uint64
	txCompleted atomic.Uint64
	txAborted   atomic.Uint64
}

// NewManager creates a new transaction manager
func NewManager(storage interfaces.StorageManager, stats stats.Collector) *Manager {
	return &Manager{
		storage: storage,
		stats:   stats,
	}
}

// BeginTransaction starts a new transaction
func (m *Manager) BeginTransaction(readOnly bool) (interfaces.Transaction, error) {
	// Track transaction start
	m.stats.TrackOperation(stats.OpTxBegin)
	m.txStarted.Add(1)
	
	// Create either a read-only or read-write transaction
	// This will acquire appropriate locks
	tx := NewTransaction(m, m.storage, readOnly)
	
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
	m.stats.TrackOperation(stats.OpTxCommit)
}

// IncrementTxAborted increments the aborted transaction counter
func (m *Manager) IncrementTxAborted() {
	m.txAborted.Add(1)
	
	// Track the rollback operation
	m.stats.TrackOperation(stats.OpTxRollback)
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