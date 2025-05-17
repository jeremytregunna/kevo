package engine

import (
	"github.com/KevoDB/kevo/pkg/common/log"
	"github.com/KevoDB/kevo/pkg/wal"
)

// GetWAL exposes the WAL for replication purposes
func (e *EngineFacade) GetWAL() *wal.WAL {
	// This is an enhancement to the EngineFacade to support replication
	// It's used by the replication manager to access the WAL
	if e.storage == nil {
		return nil
	}

	// Get WAL from storage manager
	// For now, we'll use type assertion since the interface doesn't
	// have a GetWAL method
	type walProvider interface {
		GetWAL() *wal.WAL
	}

	if provider, ok := e.storage.(walProvider); ok {
		return provider.GetWAL()
	}

	return nil
}

// SetReadOnly sets the engine to read-only mode for replicas
func (e *EngineFacade) SetReadOnly(readOnly bool) {
	// This is an enhancement to the EngineFacade to support replication
	// Setting this will force the engine to reject write operations
	// Used by replicas to ensure they don't accept direct writes
	e.readOnly.Store(readOnly)
	log.Info("Engine read-only mode set to: %v", readOnly)
}

// IsReadOnly moved to facade.go
