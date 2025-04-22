package bounded

import (
	"bytes"

	"github.com/KevoDB/kevo/pkg/common/iterator"
)

// BoundedIterator wraps an iterator and limits it to a specific key range
type BoundedIterator struct {
	iterator.Iterator
	start []byte
	end   []byte
}

// NewBoundedIterator creates a new bounded iterator
func NewBoundedIterator(iter iterator.Iterator, startKey, endKey []byte) *BoundedIterator {
	bi := &BoundedIterator{
		Iterator: iter,
	}

	// Make copies of the bounds to avoid external modification
	if startKey != nil {
		bi.start = make([]byte, len(startKey))
		copy(bi.start, startKey)
	}

	if endKey != nil {
		bi.end = make([]byte, len(endKey))
		copy(bi.end, endKey)
	}

	return bi
}

// SetBounds sets the start and end bounds for the iterator
func (b *BoundedIterator) SetBounds(start, end []byte) {
	// Make copies of the bounds to avoid external modification
	if start != nil {
		b.start = make([]byte, len(start))
		copy(b.start, start)
	} else {
		b.start = nil
	}

	if end != nil {
		b.end = make([]byte, len(end))
		copy(b.end, end)
	} else {
		b.end = nil
	}

	// If we already have a valid position, check if it's still in bounds
	if b.Iterator.Valid() {
		b.checkBounds()
	}
}

// SeekToFirst positions at the first key in the bounded range
func (b *BoundedIterator) SeekToFirst() {
	if b.start != nil {
		// If we have a start bound, seek to it
		b.Iterator.Seek(b.start)
	} else {
		// Otherwise seek to the first key
		b.Iterator.SeekToFirst()
	}
	b.checkBounds()
}

// SeekToLast positions at the last key in the bounded range
func (b *BoundedIterator) SeekToLast() {
	if b.end != nil {
		// If we have an end bound, seek to it
		// The current implementation might not be efficient for finding the last
		// key before the end bound, but it works for now
		b.Iterator.Seek(b.end)

		// If we landed exactly at the end bound, back up one
		if b.Iterator.Valid() && bytes.Equal(b.Iterator.Key(), b.end) {
			// We need to back up because end is exclusive
			// This is inefficient but correct
			b.Iterator.SeekToFirst()

			// Scan to find the last key before the end bound
			var lastKey []byte
			for b.Iterator.Valid() && bytes.Compare(b.Iterator.Key(), b.end) < 0 {
				lastKey = b.Iterator.Key()
				b.Iterator.Next()
			}

			if lastKey != nil {
				b.Iterator.Seek(lastKey)
			} else {
				// No keys before the end bound
				b.Iterator.SeekToFirst()
				// This will be marked invalid by checkBounds
			}
		}
	} else {
		// No end bound, seek to the last key
		b.Iterator.SeekToLast()
	}

	// Verify we're within bounds
	b.checkBounds()
}

// Seek positions at the first key >= target within bounds
func (b *BoundedIterator) Seek(target []byte) bool {
	// If target is before start bound, use start bound instead
	if b.start != nil && bytes.Compare(target, b.start) < 0 {
		target = b.start
	}

	// If target is at or after end bound, the seek will fail
	if b.end != nil && bytes.Compare(target, b.end) >= 0 {
		return false
	}

	if b.Iterator.Seek(target) {
		return b.checkBounds()
	}
	return false
}

// Next advances to the next key within bounds
func (b *BoundedIterator) Next() bool {
	// First check if we're already at or beyond the end boundary
	if !b.checkBounds() {
		return false
	}

	// Then try to advance
	if !b.Iterator.Next() {
		return false
	}

	// Check if the new position is within bounds
	return b.checkBounds()
}

// Valid returns true if the iterator is positioned at a valid entry within bounds
func (b *BoundedIterator) Valid() bool {
	return b.Iterator.Valid() && b.checkBounds()
}

// Key returns the current key if within bounds
func (b *BoundedIterator) Key() []byte {
	if !b.Valid() {
		return nil
	}
	return b.Iterator.Key()
}

// Value returns the current value if within bounds
func (b *BoundedIterator) Value() []byte {
	if !b.Valid() {
		return nil
	}
	return b.Iterator.Value()
}

// IsTombstone returns true if the current entry is a deletion marker
func (b *BoundedIterator) IsTombstone() bool {
	if !b.Valid() {
		return false
	}
	return b.Iterator.IsTombstone()
}

// checkBounds verifies that the current position is within the bounds
// Returns true if the position is valid and within bounds
func (b *BoundedIterator) checkBounds() bool {
	if !b.Iterator.Valid() {
		return false
	}

	// Check if the current key is before the start bound
	if b.start != nil && bytes.Compare(b.Iterator.Key(), b.start) < 0 {
		return false
	}

	// Check if the current key is beyond the end bound
	if b.end != nil && bytes.Compare(b.Iterator.Key(), b.end) >= 0 {
		return false
	}

	return true
}
