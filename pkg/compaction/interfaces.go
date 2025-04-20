package compaction

// CompactionStrategy defines the interface for selecting files for compaction
type CompactionStrategy interface {
	// SelectCompaction selects files for compaction and returns a CompactionTask
	SelectCompaction() (*CompactionTask, error)

	// CompactRange selects files within a key range for compaction
	CompactRange(minKey, maxKey []byte) error

	// LoadSSTables reloads SSTable information from disk
	LoadSSTables() error

	// Close closes any resources held by the strategy
	Close() error
}

// CompactionExecutor defines the interface for executing compaction tasks
type CompactionExecutor interface {
	// CompactFiles performs the actual compaction of the input files
	CompactFiles(task *CompactionTask) ([]string, error)

	// DeleteCompactedFiles removes the input files that were successfully compacted
	DeleteCompactedFiles(filePaths []string) error
}

// FileTracker defines the interface for tracking file states during compaction
type FileTracker interface {
	// MarkFileObsolete marks a file as obsolete (can be deleted)
	MarkFileObsolete(path string)

	// MarkFilePending marks a file as being used in a compaction
	MarkFilePending(path string)

	// UnmarkFilePending removes the pending mark from a file
	UnmarkFilePending(path string)

	// IsFileObsolete checks if a file is marked as obsolete
	IsFileObsolete(path string) bool

	// IsFilePending checks if a file is marked as pending compaction
	IsFilePending(path string) bool

	// CleanupObsoleteFiles removes files that are no longer needed
	CleanupObsoleteFiles() error
}

// TombstoneManager defines the interface for tracking and managing tombstones
type TombstoneManager interface {
	// AddTombstone records a key deletion
	AddTombstone(key []byte)

	// ForcePreserveTombstone marks a tombstone to be preserved indefinitely
	ForcePreserveTombstone(key []byte)

	// ShouldKeepTombstone checks if a tombstone should be preserved during compaction
	ShouldKeepTombstone(key []byte) bool

	// CollectGarbage removes expired tombstone records
	CollectGarbage()
}

// CompactionCoordinator defines the interface for coordinating compaction processes
type CompactionCoordinator interface {
	// Start begins background compaction
	Start() error

	// Stop halts background compaction
	Stop() error

	// TriggerCompaction forces a compaction cycle
	TriggerCompaction() error

	// CompactRange triggers compaction on a specific key range
	CompactRange(minKey, maxKey []byte) error

	// TrackTombstone adds a key to the tombstone tracker
	TrackTombstone(key []byte)

	// GetCompactionStats returns statistics about the compaction state
	GetCompactionStats() map[string]interface{}
}
