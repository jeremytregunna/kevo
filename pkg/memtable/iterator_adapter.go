package memtable

// No imports needed

// IteratorAdapter adapts a memtable.Iterator to the common Iterator interface
type IteratorAdapter struct {
	iter *Iterator
}

// NewIteratorAdapter creates a new adapter for a memtable iterator
func NewIteratorAdapter(iter *Iterator) *IteratorAdapter {
	return &IteratorAdapter{iter: iter}
}

// SeekToFirst positions the iterator at the first key
func (a *IteratorAdapter) SeekToFirst() {
	a.iter.SeekToFirst()
}

// SeekToLast positions the iterator at the last key
func (a *IteratorAdapter) SeekToLast() {
	a.iter.SeekToFirst()

	// If no items, return early
	if !a.iter.Valid() {
		return
	}

	// Store the last key we've seen
	var lastKey []byte

	// Scan to find the last element
	for a.iter.Valid() {
		lastKey = a.iter.Key()
		a.iter.Next()
	}

	// Re-position at the last key we found
	if lastKey != nil {
		a.iter.Seek(lastKey)
	}
}

// Seek positions the iterator at the first key >= target
func (a *IteratorAdapter) Seek(target []byte) bool {
	a.iter.Seek(target)
	return a.iter.Valid()
}

// Next advances the iterator to the next key
func (a *IteratorAdapter) Next() bool {
	if !a.Valid() {
		return false
	}
	a.iter.Next()
	return a.iter.Valid()
}

// Key returns the current key
func (a *IteratorAdapter) Key() []byte {
	if !a.Valid() {
		return nil
	}
	return a.iter.Key()
}

// Value returns the current value
func (a *IteratorAdapter) Value() []byte {
	if !a.Valid() {
		return nil
	}

	// Check if this is a tombstone (deletion marker)
	if a.iter.IsTombstone() {
		// This ensures that during compaction, we know this is a deletion marker
		return nil
	}

	return a.iter.Value()
}

// Valid returns true if the iterator is positioned at a valid entry
func (a *IteratorAdapter) Valid() bool {
	return a.iter != nil && a.iter.Valid()
}

// IsTombstone returns true if the current entry is a deletion marker
func (a *IteratorAdapter) IsTombstone() bool {
	return a.iter != nil && a.iter.IsTombstone()
}

// SequenceNumber returns the sequence number of the current entry
func (a *IteratorAdapter) SequenceNumber() uint64 {
	if !a.Valid() || a.iter.Entry() == nil {
		return 0
	}
	return a.iter.Entry().seqNum
}
