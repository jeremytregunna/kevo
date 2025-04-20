package sstable

// No imports needed

// IteratorAdapter adapts an sstable.Iterator to the common Iterator interface
type IteratorAdapter struct {
	iter *Iterator
}

// NewIteratorAdapter creates a new adapter for an sstable iterator
func NewIteratorAdapter(iter *Iterator) *IteratorAdapter {
	return &IteratorAdapter{iter: iter}
}

// SeekToFirst positions the iterator at the first key
func (a *IteratorAdapter) SeekToFirst() {
	a.iter.SeekToFirst()
}

// SeekToLast positions the iterator at the last key
func (a *IteratorAdapter) SeekToLast() {
	a.iter.SeekToLast()
}

// Seek positions the iterator at the first key >= target
func (a *IteratorAdapter) Seek(target []byte) bool {
	return a.iter.Seek(target)
}

// Next advances the iterator to the next key
func (a *IteratorAdapter) Next() bool {
	return a.iter.Next()
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
	return a.iter.Value()
}

// Valid returns true if the iterator is positioned at a valid entry
func (a *IteratorAdapter) Valid() bool {
	return a.iter != nil && a.iter.Valid()
}

// IsTombstone returns true if the current entry is a deletion marker
func (a *IteratorAdapter) IsTombstone() bool {
	return a.Valid() && a.iter.IsTombstone()
}
