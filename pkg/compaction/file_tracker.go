package compaction

import (
	"fmt"
	"os"
	"sync"
)

// DefaultFileTracker is the default implementation of FileTracker
type DefaultFileTracker struct {
	// Map of file path -> true for files that have been obsoleted by compaction
	obsoleteFiles map[string]bool

	// Map of file path -> true for files that are currently being compacted
	pendingFiles map[string]bool

	// Mutex for file tracking maps
	filesMu sync.RWMutex
}

// NewFileTracker creates a new file tracker
func NewFileTracker() *DefaultFileTracker {
	return &DefaultFileTracker{
		obsoleteFiles: make(map[string]bool),
		pendingFiles:  make(map[string]bool),
	}
}

// MarkFileObsolete marks a file as obsolete (can be deleted)
func (f *DefaultFileTracker) MarkFileObsolete(path string) {
	f.filesMu.Lock()
	defer f.filesMu.Unlock()

	f.obsoleteFiles[path] = true
}

// MarkFilePending marks a file as being used in a compaction
func (f *DefaultFileTracker) MarkFilePending(path string) {
	f.filesMu.Lock()
	defer f.filesMu.Unlock()

	f.pendingFiles[path] = true
}

// UnmarkFilePending removes the pending mark from a file
func (f *DefaultFileTracker) UnmarkFilePending(path string) {
	f.filesMu.Lock()
	defer f.filesMu.Unlock()

	delete(f.pendingFiles, path)
}

// IsFileObsolete checks if a file is marked as obsolete
func (f *DefaultFileTracker) IsFileObsolete(path string) bool {
	f.filesMu.RLock()
	defer f.filesMu.RUnlock()

	return f.obsoleteFiles[path]
}

// IsFilePending checks if a file is marked as pending compaction
func (f *DefaultFileTracker) IsFilePending(path string) bool {
	f.filesMu.RLock()
	defer f.filesMu.RUnlock()

	return f.pendingFiles[path]
}

// CleanupObsoleteFiles removes files that are no longer needed
func (f *DefaultFileTracker) CleanupObsoleteFiles() error {
	f.filesMu.Lock()
	defer f.filesMu.Unlock()

	// Safely remove obsolete files that aren't pending
	for path := range f.obsoleteFiles {
		// Skip files that are still being used in a compaction
		if f.pendingFiles[path] {
			continue
		}

		// Try to delete the file
		if err := os.Remove(path); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to delete obsolete file %s: %w", path, err)
			}
			// If the file doesn't exist, remove it from our tracking
			delete(f.obsoleteFiles, path)
		} else {
			// Successfully deleted, remove from tracking
			delete(f.obsoleteFiles, path)
		}
	}

	return nil
}
