package iterator

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/common/iterator"
)

// HierarchicalIterator implements an iterator that follows the LSM-tree hierarchy
// where newer sources (earlier in the sources slice) take precedence over older sources
type HierarchicalIterator struct {
	// Iterators in order from newest to oldest
	iterators []iterator.Iterator

	// Current key and value
	key   []byte
	value []byte

	// Current valid state
	valid bool

	// Mutex for thread safety
	mu sync.Mutex

	// Telemetry metrics for iterator operations (optional)
	metrics IteratorMetrics
}

// NewHierarchicalIterator creates a new hierarchical iterator
// Sources must be provided in newest-to-oldest order
func NewHierarchicalIterator(iterators []iterator.Iterator) *HierarchicalIterator {
	h := &HierarchicalIterator{
		iterators: iterators,
		metrics:   NewNoopIteratorMetrics(), // Default to no-op, will be replaced by SetTelemetry
	}

	// Record iterator creation with telemetry
	if h.metrics != nil {
		h.metrics.RecordIteratorType(context.Background(), "hierarchical", len(iterators))
	}

	return h
}

// SetTelemetry allows post-creation telemetry injection from engine facade
func (h *HierarchicalIterator) SetTelemetry(tel interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if iterMetrics, ok := tel.(IteratorMetrics); ok {
		h.metrics = iterMetrics
		// Re-record the iterator type with the real telemetry now
		h.metrics.RecordIteratorType(context.Background(), "hierarchical", len(h.iterators))
	}
}

// SeekToFirst positions the iterator at the first key
func (h *HierarchicalIterator) SeekToFirst() {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Position all iterators at their first key
	for _, iter := range h.iterators {
		iter.SeekToFirst()
	}

	// Find the first key across all iterators
	h.findNextUniqueKey(nil)
}

// SeekToLast positions the iterator at the last key
func (h *HierarchicalIterator) SeekToLast() {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Position all iterators at their last key
	for _, iter := range h.iterators {
		iter.SeekToLast()
	}

	// Find the last key by taking the maximum key
	var maxKey []byte
	var maxValue []byte
	var maxSource int = -1

	for i, iter := range h.iterators {
		if !iter.Valid() {
			continue
		}

		key := iter.Key()
		if maxKey == nil || bytes.Compare(key, maxKey) > 0 {
			maxKey = key
			maxValue = iter.Value()
			maxSource = i
		}
	}

	if maxSource >= 0 {
		h.key = maxKey
		h.value = maxValue
		h.valid = true
	} else {
		h.valid = false
	}
}

// Seek positions the iterator at the first key >= target
func (h *HierarchicalIterator) Seek(target []byte) bool {
	start := time.Now()
	ctx := context.Background()

	h.mu.Lock()
	defer h.mu.Unlock()

	// Seek all iterators to the target
	for _, iter := range h.iterators {
		iter.Seek(target)
	}

	// For seek, we need to treat it differently than findNextUniqueKey since we want
	// keys >= target, not strictly > target
	var minKey []byte
	var minValue []byte
	var seenKeys = make(map[string]bool)
	h.valid = false

	// Find the smallest key >= target from all iterators
	for _, iter := range h.iterators {
		if !iter.Valid() {
			continue
		}

		key := iter.Key()
		value := iter.Value()

		// Skip keys < target (Seek should return keys >= target)
		if bytes.Compare(key, target) < 0 {
			continue
		}

		// Convert key to string for map lookup
		keyStr := string(key)

		// Only use this key if we haven't seen it from a newer iterator
		if !seenKeys[keyStr] {
			// Mark as seen
			seenKeys[keyStr] = true

			// Update min key if needed
			if minKey == nil || bytes.Compare(key, minKey) < 0 {
				minKey = key
				minValue = value
				h.valid = true
			}
		}
	}

	// Set the found key/value
	if h.valid {
		h.key = minKey
		h.value = minValue
	}

	// Record seek operation telemetry
	if h.metrics != nil {
		h.recordSeekMetrics(ctx, start, h.valid, int64(len(h.iterators)))
	}

	return h.valid
}

// Next advances the iterator to the next key
func (h *HierarchicalIterator) Next() bool {
	start := time.Now()
	ctx := context.Background()

	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.valid {
		// Record the failed next operation
		if h.metrics != nil {
			h.recordNextMetrics(ctx, start, false)
		}
		return false
	}

	// Remember current key to skip duplicates
	currentKey := h.key

	// Find the next unique key after the current key
	found := h.findNextUniqueKey(currentKey)

	// Record next operation telemetry
	if h.metrics != nil {
		h.recordNextMetrics(ctx, start, found)
	}

	return found
}

// Key returns the current key
func (h *HierarchicalIterator) Key() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.valid {
		return nil
	}
	return h.key
}

// Value returns the current value
func (h *HierarchicalIterator) Value() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.valid {
		return nil
	}
	return h.value
}

// Valid returns true if the iterator is positioned at a valid entry
func (h *HierarchicalIterator) Valid() bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.valid
}

// IsTombstone returns true if the current entry is a deletion marker
func (h *HierarchicalIterator) IsTombstone() bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	// If not valid, it can't be a tombstone
	if !h.valid {
		return false
	}

	// For hierarchical iterator, we infer tombstones from the value being nil
	// This is used during compaction to distinguish between regular nil values and tombstones
	return h.value == nil
}

// findNextUniqueKey finds the next key after the given key
// If prevKey is nil, finds the first key
// Returns true if a valid key was found
func (h *HierarchicalIterator) findNextUniqueKey(prevKey []byte) bool {
	start := time.Now()
	ctx := context.Background()

	// Find the smallest key among all iterators that is > prevKey
	var minKey []byte
	var minValue []byte
	var seenKeys = make(map[string]bool)
	h.valid = false

	// First pass: collect all valid keys and find min key > prevKey
	for _, iter := range h.iterators {
		// Skip invalid iterators
		if !iter.Valid() {
			continue
		}

		key := iter.Key()
		value := iter.Value()

		// Skip keys <= prevKey if we're looking for the next key
		if prevKey != nil && bytes.Compare(key, prevKey) <= 0 {
			// Advance to find a key > prevKey
			for iter.Valid() && bytes.Compare(iter.Key(), prevKey) <= 0 {
				if !iter.Next() {
					break
				}
			}

			// If we couldn't find a key > prevKey or the iterator is no longer valid, skip it
			if !iter.Valid() {
				continue
			}

			// Get the new key after advancing
			key = iter.Key()
			value = iter.Value()

			// If key is still <= prevKey after advancing, skip this iterator
			if bytes.Compare(key, prevKey) <= 0 {
				continue
			}
		}

		// Convert key to string for map lookup
		keyStr := string(key)

		// If this key hasn't been seen before, or this is a newer source for the same key
		if !seenKeys[keyStr] {
			// Mark this key as seen - it's from the newest source
			seenKeys[keyStr] = true

			// Check if this is a new minimum key
			if minKey == nil || bytes.Compare(key, minKey) < 0 {
				minKey = key
				minValue = value
				h.valid = true
			}
		}
	}

	// Set the key/value if we found a valid one
	if h.valid {
		h.key = minKey
		h.value = minValue
	}

	// Record hierarchical merge operation telemetry
	if h.metrics != nil {
		h.recordHierarchicalMerge(ctx, start)
	}

	return h.valid
}

// recordSeekMetrics records telemetry for seek operations with panic protection
func (h *HierarchicalIterator) recordSeekMetrics(ctx context.Context, start time.Time, found bool, keysScanned int64) {
	defer func() {
		if r := recover(); r != nil {
			// Log telemetry panic but don't fail the operation
			// This follows the same pattern as other components
		}
	}()

	h.metrics.RecordSeek(ctx, time.Since(start), found, keysScanned)
}

// recordNextMetrics records telemetry for next operations with panic protection
func (h *HierarchicalIterator) recordNextMetrics(ctx context.Context, start time.Time, valid bool) {
	defer func() {
		if r := recover(); r != nil {
			// Log telemetry panic but don't fail the operation
		}
	}()

	h.metrics.RecordNext(ctx, time.Since(start), valid)
}

// recordHierarchicalMerge records telemetry for hierarchical merge operations
func (h *HierarchicalIterator) recordHierarchicalMerge(ctx context.Context, start time.Time) {
	defer func() {
		if r := recover(); r != nil {
			// Log telemetry panic but don't fail the operation
		}
	}()

	h.metrics.RecordHierarchicalMerge(ctx, len(h.iterators), time.Since(start))
}
