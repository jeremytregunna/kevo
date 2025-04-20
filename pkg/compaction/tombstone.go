package compaction

import (
	"bytes"
	"time"
)

// TombstoneTracker implements the TombstoneManager interface
type TombstoneTracker struct {
	// Map of deleted keys with deletion timestamp
	deletions map[string]time.Time

	// Map of keys that should always be preserved (for testing)
	preserveForever map[string]bool

	// Retention period for tombstones (after this time, they can be discarded)
	retention time.Duration
}

// NewTombstoneTracker creates a new tombstone tracker
func NewTombstoneTracker(retentionPeriod time.Duration) *TombstoneTracker {
	return &TombstoneTracker{
		deletions:       make(map[string]time.Time),
		preserveForever: make(map[string]bool),
		retention:       retentionPeriod,
	}
}

// AddTombstone records a key deletion
func (t *TombstoneTracker) AddTombstone(key []byte) {
	t.deletions[string(key)] = time.Now()
}

// ForcePreserveTombstone marks a tombstone to be preserved indefinitely
// This is primarily used for testing purposes
func (t *TombstoneTracker) ForcePreserveTombstone(key []byte) {
	t.preserveForever[string(key)] = true
}

// ShouldKeepTombstone checks if a tombstone should be preserved during compaction
func (t *TombstoneTracker) ShouldKeepTombstone(key []byte) bool {
	strKey := string(key)

	// First check if this key is in the preserveForever map
	if t.preserveForever[strKey] {
		return true // Always preserve this tombstone
	}

	// Otherwise check normal retention
	timestamp, exists := t.deletions[strKey]
	if !exists {
		return false // Not a tracked tombstone
	}

	// Keep the tombstone if it's still within the retention period
	return time.Since(timestamp) < t.retention
}

// CollectGarbage removes expired tombstone records
func (t *TombstoneTracker) CollectGarbage() {
	now := time.Now()
	for key, timestamp := range t.deletions {
		if now.Sub(timestamp) > t.retention {
			delete(t.deletions, key)
		}
	}
}

// TombstoneFilter is an interface for filtering tombstones during compaction
type TombstoneFilter interface {
	// ShouldKeep determines if a key-value pair should be kept during compaction
	// If value is nil, it's a tombstone marker
	ShouldKeep(key, value []byte) bool
}

// BasicTombstoneFilter implements a simple filter that keeps all non-tombstone entries
// and keeps tombstones during certain (lower) levels of compaction
type BasicTombstoneFilter struct {
	// The level of compaction (higher levels discard more tombstones)
	level int

	// The maximum level to retain tombstones
	maxTombstoneLevel int

	// The tombstone tracker (if any)
	tracker TombstoneManager
}

// NewBasicTombstoneFilter creates a new tombstone filter
func NewBasicTombstoneFilter(level, maxTombstoneLevel int, tracker TombstoneManager) *BasicTombstoneFilter {
	return &BasicTombstoneFilter{
		level:             level,
		maxTombstoneLevel: maxTombstoneLevel,
		tracker:           tracker,
	}
}

// ShouldKeep determines if a key-value pair should be kept
func (f *BasicTombstoneFilter) ShouldKeep(key, value []byte) bool {
	// Always keep normal entries (non-tombstones)
	if value != nil {
		return true
	}

	// For tombstones (value == nil):

	// If we have a tracker, use it to determine if the tombstone is still needed
	if f.tracker != nil {
		return f.tracker.ShouldKeepTombstone(key)
	}

	// Otherwise use level-based heuristic
	// Keep tombstones in lower levels, discard in higher levels
	return f.level <= f.maxTombstoneLevel
}

// TimeBasedTombstoneFilter implements a filter that keeps tombstones based on age
type TimeBasedTombstoneFilter struct {
	// Map of key to deletion time
	deletionTimes map[string]time.Time

	// Current time (for testing)
	now time.Time

	// Retention period
	retention time.Duration
}

// NewTimeBasedTombstoneFilter creates a new time-based tombstone filter
func NewTimeBasedTombstoneFilter(deletionTimes map[string]time.Time, retention time.Duration) *TimeBasedTombstoneFilter {
	return &TimeBasedTombstoneFilter{
		deletionTimes: deletionTimes,
		now:           time.Now(),
		retention:     retention,
	}
}

// ShouldKeep determines if a key-value pair should be kept
func (f *TimeBasedTombstoneFilter) ShouldKeep(key, value []byte) bool {
	// Always keep normal entries
	if value != nil {
		return true
	}

	// For tombstones, check if we know when this key was deleted
	strKey := string(key)
	deleteTime, found := f.deletionTimes[strKey]
	if !found {
		// If we don't know when it was deleted, keep it to be safe
		return true
	}

	// If the tombstone is older than our retention period, we can discard it
	return f.now.Sub(deleteTime) <= f.retention
}

// KeyRangeTombstoneFilter filters tombstones by key range
type KeyRangeTombstoneFilter struct {
	// Minimum key in the range (inclusive)
	minKey []byte

	// Maximum key in the range (exclusive)
	maxKey []byte

	// Delegate filter
	delegate TombstoneFilter
}

// NewKeyRangeTombstoneFilter creates a new key range tombstone filter
func NewKeyRangeTombstoneFilter(minKey, maxKey []byte, delegate TombstoneFilter) *KeyRangeTombstoneFilter {
	return &KeyRangeTombstoneFilter{
		minKey:   minKey,
		maxKey:   maxKey,
		delegate: delegate,
	}
}

// ShouldKeep determines if a key-value pair should be kept
func (f *KeyRangeTombstoneFilter) ShouldKeep(key, value []byte) bool {
	// Always keep normal entries
	if value != nil {
		return true
	}

	// Check if the key is in our targeted range
	inRange := true
	if f.minKey != nil && bytes.Compare(key, f.minKey) < 0 {
		inRange = false
	}
	if f.maxKey != nil && bytes.Compare(key, f.maxKey) >= 0 {
		inRange = false
	}

	// If not in range, keep the tombstone
	if !inRange {
		return true
	}

	// Otherwise, delegate to the wrapped filter
	return f.delegate.ShouldKeep(key, value)
}
