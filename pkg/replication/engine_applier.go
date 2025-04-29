package replication

import (
	"fmt"

	"github.com/KevoDB/kevo/pkg/common/log"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	"github.com/KevoDB/kevo/pkg/wal"
)

// EngineApplier implements the WALEntryApplier interface for applying
// WAL entries to a database engine.
type EngineApplier struct {
	engine interfaces.Engine
}

// NewEngineApplier creates a new engine applier
func NewEngineApplier(engine interfaces.Engine) *EngineApplier {
	return &EngineApplier{
		engine: engine,
	}
}

// Apply applies a WAL entry to the engine through its API
// This bypasses the read-only check for replication purposes
func (e *EngineApplier) Apply(entry *wal.Entry) error {
	log.Info("Replica applying WAL entry through engine API: seq=%d, type=%d, key=%s",
		entry.SequenceNumber, entry.Type, string(entry.Key))

	// Check if engine is in read-only mode
	isReadOnly := false
	if checker, ok := e.engine.(interface{ IsReadOnly() bool }); ok {
		isReadOnly = checker.IsReadOnly()
	}

	// Handle application based on read-only status and operation type
	if isReadOnly {
		return e.applyInReadOnlyMode(entry)
	}

	return e.applyInNormalMode(entry)
}

// applyInReadOnlyMode applies a WAL entry in read-only mode
func (e *EngineApplier) applyInReadOnlyMode(entry *wal.Entry) error {
	log.Info("Applying entry in read-only mode: seq=%d", entry.SequenceNumber)

	switch entry.Type {
	case wal.OpTypePut:
		// Try internal interface first
		if putter, ok := e.engine.(interface{ PutInternal(key, value []byte) error }); ok {
			return putter.PutInternal(entry.Key, entry.Value)
		}

		// Try temporarily disabling read-only mode
		if setter, ok := e.engine.(interface{ SetReadOnly(bool) }); ok {
			setter.SetReadOnly(false)
			err := e.engine.Put(entry.Key, entry.Value)
			setter.SetReadOnly(true)
			return err
		}

		// Fall back to normal operation which may fail
		return e.engine.Put(entry.Key, entry.Value)

	case wal.OpTypeDelete:
		// Try internal interface first
		if deleter, ok := e.engine.(interface{ DeleteInternal(key []byte) error }); ok {
			return deleter.DeleteInternal(entry.Key)
		}

		// Try temporarily disabling read-only mode
		if setter, ok := e.engine.(interface{ SetReadOnly(bool) }); ok {
			setter.SetReadOnly(false)
			err := e.engine.Delete(entry.Key)
			setter.SetReadOnly(true)
			return err
		}

		// Fall back to normal operation which may fail
		return e.engine.Delete(entry.Key)

	case wal.OpTypeBatch:
		// Try internal interface first
		if batcher, ok := e.engine.(interface {
			ApplyBatchInternal(entries []*wal.Entry) error
		}); ok {
			return batcher.ApplyBatchInternal([]*wal.Entry{entry})
		}

		// Try temporarily disabling read-only mode
		if setter, ok := e.engine.(interface{ SetReadOnly(bool) }); ok {
			setter.SetReadOnly(false)
			err := e.engine.ApplyBatch([]*wal.Entry{entry})
			setter.SetReadOnly(true)
			return err
		}

		// Fall back to normal operation which may fail
		return e.engine.ApplyBatch([]*wal.Entry{entry})

	case wal.OpTypeMerge:
		// Handle merge as a put operation for compatibility
		if setter, ok := e.engine.(interface{ SetReadOnly(bool) }); ok {
			setter.SetReadOnly(false)
			err := e.engine.Put(entry.Key, entry.Value)
			setter.SetReadOnly(true)
			return err
		}
		return e.engine.Put(entry.Key, entry.Value)

	default:
		return fmt.Errorf("unsupported WAL entry type: %d", entry.Type)
	}
}

// applyInNormalMode applies a WAL entry in normal mode
func (e *EngineApplier) applyInNormalMode(entry *wal.Entry) error {
	log.Info("Applying entry in normal mode: seq=%d", entry.SequenceNumber)

	switch entry.Type {
	case wal.OpTypePut:
		return e.engine.Put(entry.Key, entry.Value)

	case wal.OpTypeDelete:
		return e.engine.Delete(entry.Key)

	case wal.OpTypeBatch:
		return e.engine.ApplyBatch([]*wal.Entry{entry})

	case wal.OpTypeMerge:
		// Handle merge as a put operation for compatibility
		return e.engine.Put(entry.Key, entry.Value)

	default:
		return fmt.Errorf("unsupported WAL entry type: %d", entry.Type)
	}
}

// Sync ensures all applied entries are persisted
func (e *EngineApplier) Sync() error {
	// Force a flush of in-memory tables to ensure durability
	return e.engine.FlushImMemTables()
}
