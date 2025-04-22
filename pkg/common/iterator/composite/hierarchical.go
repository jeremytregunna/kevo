package composite

import (
	"bytes"
	"sync"

	"github.com/KevoDB/kevo/pkg/common/iterator"
)

// HierarchicalIterator implements an iterator that follows the LSM-tree hierarchy
// where newer sources (earlier in the sources slice) take precedence over older sources.
// When multiple sources contain the same key, the value from the newest source is used.
type HierarchicalIterator struct {
	// Iterators in order from newest to oldest
	iterators []iterator.Iterator

	// Current key and value
	key   []byte
	value []byte

	// Current valid state
	valid bool

	// Mutex for thread safety
	mu sync.RWMutex
}

// NewHierarchicalIterator creates a new hierarchical iterator
// Sources must be provided in newest-to-oldest order
func NewHierarchicalIterator(iterators []iterator.Iterator) *HierarchicalIterator {
	return &HierarchicalIterator{
		iterators: iterators,
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
	h.mu.Lock()
	defer h.mu.Unlock()

	// Seek all iterators to the target
	for _, iter := range h.iterators {
		iter.Seek(target)
	}

	// For seek, we need to find the smallest key >= target
	var bestKey []byte
	var bestValue []byte
	var bestIterIdx int = -1
	h.valid = false

	// First pass: find the smallest key >= target
	for i, iter := range h.iterators {
		if !iter.Valid() {
			continue
		}

		key := iter.Key()

		// Skip keys < target (Seek should return keys >= target)
		if bytes.Compare(key, target) < 0 {
			continue
		}

		// If we haven't found a valid key yet, or this key is smaller than the current best key
		if bestIterIdx == -1 || bytes.Compare(key, bestKey) < 0 {
			// This becomes our best candidate so far
			bestKey = key
			bestValue = iter.Value()
			bestIterIdx = i
		}
	}

	// Now we need to check if any newer iterators have the same key
	if bestIterIdx != -1 {
		// Check all newer iterators (earlier in the slice) for the same key
		for i := 0; i < bestIterIdx; i++ {
			iter := h.iterators[i]
			if !iter.Valid() {
				continue
			}

			// If a newer iterator has the same key, use its value
			if bytes.Equal(iter.Key(), bestKey) {
				bestValue = iter.Value()
				break // Since iterators are in newest-to-oldest order, we can stop at the first match
			}
		}

		// Set the found key/value
		h.key = bestKey
		h.value = bestValue
		h.valid = true
		return true
	}

	return false
}

// Next advances the iterator to the next key
func (h *HierarchicalIterator) Next() bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.valid {
		return false
	}

	// Remember current key to skip duplicates
	currentKey := h.key

	// Find the next unique key after the current key
	return h.findNextUniqueKey(currentKey)
}

// Key returns the current key
func (h *HierarchicalIterator) Key() []byte {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if !h.valid {
		return nil
	}
	return h.key
}

// Value returns the current value
func (h *HierarchicalIterator) Value() []byte {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if !h.valid {
		return nil
	}
	return h.value
}

// Valid returns true if the iterator is positioned at a valid entry
func (h *HierarchicalIterator) Valid() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.valid
}

// IsTombstone returns true if the current entry is a deletion marker
func (h *HierarchicalIterator) IsTombstone() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// If not valid, it can't be a tombstone
	if !h.valid {
		return false
	}

	// For hierarchical iterator, we infer tombstones from the value being nil
	// This is used during compaction to distinguish between regular nil values and tombstones
	return h.value == nil
}

// NumSources returns the number of source iterators
func (h *HierarchicalIterator) NumSources() int {
	return len(h.iterators)
}

// GetSourceIterators returns the underlying source iterators
func (h *HierarchicalIterator) GetSourceIterators() []iterator.Iterator {
	return h.iterators
}

// findNextUniqueKey finds the next key after the given key
// If prevKey is nil, finds the first key
// Returns true if a valid key was found
func (h *HierarchicalIterator) findNextUniqueKey(prevKey []byte) bool {
	// Find the smallest key among all iterators that is > prevKey
	var bestKey []byte
	var bestValue []byte
	var bestIterIdx int = -1
	h.valid = false

	// First pass: advance all iterators past prevKey and find the smallest next key
	for i, iter := range h.iterators {
		// Skip invalid iterators
		if !iter.Valid() {
			continue
		}

		// Skip keys <= prevKey if we're looking for the next key
		if prevKey != nil && bytes.Compare(iter.Key(), prevKey) <= 0 {
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
		}

		// Get the current key
		key := iter.Key()

		// If we haven't found a valid key yet, or this key is smaller than the current best key
		if bestIterIdx == -1 || bytes.Compare(key, bestKey) < 0 {
			// This becomes our best candidate so far
			bestKey = key
			bestValue = iter.Value()
			bestIterIdx = i
		}
	}

	// Now we need to check if any newer iterators have the same key
	if bestIterIdx != -1 {
		// Check all newer iterators (earlier in the slice) for the same key
		for i := 0; i < bestIterIdx; i++ {
			iter := h.iterators[i]
			if !iter.Valid() {
				continue
			}

			// If a newer iterator has the same key, use its value
			if bytes.Equal(iter.Key(), bestKey) {
				bestValue = iter.Value()
				break // Since iterators are in newest-to-oldest order, we can stop at the first match
			}
		}

		// Set the found key/value
		h.key = bestKey
		h.value = bestValue
		h.valid = true
		return true
	}

	return false
}
