package storage

import (
	"github.com/KevoDB/kevo/pkg/wal"
)

// GetWAL returns the storage manager's WAL instance
// This is used by the replication manager to access the WAL
func (m *Manager) GetWAL() *wal.WAL {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return m.wal
}