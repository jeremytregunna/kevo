package wal

// WALEntryObserver defines the interface for observing WAL operations.
// Components that need to be notified of WAL events (such as replication systems)
// can implement this interface and register with the WAL.
type WALEntryObserver interface {
	// OnWALEntryWritten is called when a single entry is written to the WAL.
	// This method is called after the entry has been written to the WAL buffer
	// but before it may have been synced to disk.
	OnWALEntryWritten(entry *Entry)

	// OnWALBatchWritten is called when a batch of entries is written to the WAL.
	// The startSeq parameter is the sequence number of the first entry in the batch.
	// This method is called after all entries in the batch have been written to
	// the WAL buffer but before they may have been synced to disk.
	OnWALBatchWritten(startSeq uint64, entries []*Entry)

	// OnWALSync is called when the WAL is synced to disk.
	// The upToSeq parameter is the highest sequence number that has been synced.
	// This method is called after the fsync operation has completed successfully.
	OnWALSync(upToSeq uint64)
}
