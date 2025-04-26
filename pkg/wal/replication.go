package wal

// LamportClock is an interface for a logical clock based on Lamport timestamps
// for maintaining causal ordering of events in a distributed system.
type LamportClock interface {
	// Tick increments the clock and returns the new timestamp value
	Tick() uint64
	
	// Update updates the clock based on a received timestamp,
	// ensuring the local clock is at least as large as the received timestamp,
	// then increments and returns the new value
	Update(received uint64) uint64
	
	// Current returns the current timestamp without incrementing the clock
	Current() uint64
}

// ReplicationHook is an interface for capturing WAL entries for replication purposes.
// It provides hook points for the WAL to notify when entries are written.
type ReplicationHook interface {
	// OnEntryWritten is called when a single WAL entry is written
	OnEntryWritten(entry *Entry)

	// OnBatchWritten is called when a batch of WAL entries is written
	OnBatchWritten(entries []*Entry)
}