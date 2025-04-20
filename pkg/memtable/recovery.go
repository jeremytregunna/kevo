package memtable

import (
	"fmt"

	"github.com/jeremytregunna/kevo/pkg/config"
	"github.com/jeremytregunna/kevo/pkg/wal"
)

// RecoveryOptions contains options for MemTable recovery
type RecoveryOptions struct {
	// MaxSequenceNumber is the maximum sequence number to recover
	// Entries with sequence numbers greater than this will be ignored
	MaxSequenceNumber uint64

	// MaxMemTables is the maximum number of MemTables to create during recovery
	// If more MemTables would be needed, an error is returned
	MaxMemTables int

	// MemTableSize is the maximum size of each MemTable
	MemTableSize int64
}

// DefaultRecoveryOptions returns the default recovery options
func DefaultRecoveryOptions(cfg *config.Config) *RecoveryOptions {
	return &RecoveryOptions{
		MaxSequenceNumber: ^uint64(0), // Max uint64
		MaxMemTables:      cfg.MaxMemTables,
		MemTableSize:      cfg.MemTableSize,
	}
}

// RecoverFromWAL rebuilds MemTables from the write-ahead log
// Returns a list of recovered MemTables and the maximum sequence number seen
func RecoverFromWAL(cfg *config.Config, opts *RecoveryOptions) ([]*MemTable, uint64, error) {
	if opts == nil {
		opts = DefaultRecoveryOptions(cfg)
	}

	// Create the first MemTable
	memTables := []*MemTable{NewMemTable()}
	var maxSeqNum uint64

	// Function to process each WAL entry
	entryHandler := func(entry *wal.Entry) error {
		// Skip entries with sequence numbers beyond our max
		if entry.SequenceNumber > opts.MaxSequenceNumber {
			return nil
		}

		// Update the max sequence number
		if entry.SequenceNumber > maxSeqNum {
			maxSeqNum = entry.SequenceNumber
		}

		// Get the current memtable
		current := memTables[len(memTables)-1]

		// Check if we should create a new memtable based on size
		if current.ApproximateSize() >= opts.MemTableSize {
			// Make sure we don't exceed the max number of memtables
			if len(memTables) >= opts.MaxMemTables {
				return fmt.Errorf("maximum number of memtables (%d) exceeded during recovery", opts.MaxMemTables)
			}

			// Mark the current memtable as immutable
			current.SetImmutable()

			// Create a new memtable
			current = NewMemTable()
			memTables = append(memTables, current)
		}

		// Process the entry
		return current.ProcessWALEntry(entry)
	}

	// Replay the WAL directory
	if err := wal.ReplayWALDir(cfg.WALDir, entryHandler); err != nil {
		return nil, 0, fmt.Errorf("failed to replay WAL: %w", err)
	}

	// For batch operations, we need to adjust maxSeqNum
	finalTable := memTables[len(memTables)-1]
	nextSeq := finalTable.GetNextSequenceNumber()
	if nextSeq > maxSeqNum+1 {
		maxSeqNum = nextSeq - 1
	}

	return memTables, maxSeqNum, nil
}
