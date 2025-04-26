package interfaces

// CompactionManager handles the compaction of SSTables
type CompactionManager interface {
	// Core operations
	TriggerCompaction() error
	CompactRange(startKey, endKey []byte) error

	// Tombstone management
	TrackTombstone(key []byte)
	ForcePreserveTombstone(key []byte)

	// Lifecycle management
	Start() error
	Stop() error

	// Statistics
	GetCompactionStats() map[string]interface{}
}

// CompactionCoordinator handles scheduling and coordination of compaction
type CompactionCoordinator interface {
	CompactionManager

	// Coordination methods
	ScheduleCompaction() error
	IsCompactionRunning() bool
	WaitForCompaction() error
}
