// Package filtered provides iterators that filter keys based on different criteria
package filtered

import (
	"bytes"

	"github.com/KevoDB/kevo/pkg/common/iterator"
)

// KeyFilterFunc is a function type for filtering keys
type KeyFilterFunc func(key []byte) bool

// FilteredIterator wraps an iterator and applies a key filter
type FilteredIterator struct {
	iter      iterator.Iterator
	keyFilter KeyFilterFunc
}

// NewFilteredIterator creates a new iterator with a key filter
func NewFilteredIterator(iter iterator.Iterator, filter KeyFilterFunc) *FilteredIterator {
	return &FilteredIterator{
		iter:      iter,
		keyFilter: filter,
	}
}

// Next advances to the next key that passes the filter
func (fi *FilteredIterator) Next() bool {
	for fi.iter.Next() {
		if fi.keyFilter(fi.iter.Key()) {
			return true
		}
	}
	return false
}

// Key returns the current key
func (fi *FilteredIterator) Key() []byte {
	return fi.iter.Key()
}

// Value returns the current value
func (fi *FilteredIterator) Value() []byte {
	return fi.iter.Value()
}

// Valid returns true if the iterator is at a valid position
func (fi *FilteredIterator) Valid() bool {
	return fi.iter.Valid() && fi.keyFilter(fi.iter.Key())
}

// IsTombstone returns true if the current entry is a deletion marker
func (fi *FilteredIterator) IsTombstone() bool {
	return fi.iter.IsTombstone()
}

// SeekToFirst positions at the first key that passes the filter
func (fi *FilteredIterator) SeekToFirst() {
	fi.iter.SeekToFirst()

	// Advance to the first key that passes the filter
	if fi.iter.Valid() && !fi.keyFilter(fi.iter.Key()) {
		fi.Next()
	}
}

// SeekToLast positions at the last key that passes the filter
func (fi *FilteredIterator) SeekToLast() {
	// This is a simplistic implementation that may not be efficient
	// For a production-quality implementation, we might want a more
	// sophisticated approach
	fi.iter.SeekToLast()

	// If we're at a valid position but it doesn't pass the filter,
	// we need to find the last key that does
	if fi.iter.Valid() && !fi.keyFilter(fi.iter.Key()) {
		// Inefficient but correct - scan from beginning to find last valid key
		var lastValidKey []byte
		fi.iter.SeekToFirst()

		for fi.iter.Valid() {
			if fi.keyFilter(fi.iter.Key()) {
				lastValidKey = make([]byte, len(fi.iter.Key()))
				copy(lastValidKey, fi.iter.Key())
			}
			fi.iter.Next()
		}

		// If we found a valid key, seek to it
		if lastValidKey != nil {
			fi.iter.Seek(lastValidKey)
		} else {
			// No valid keys found
			fi.iter.SeekToFirst()
			// This will be invalid after the filter is applied
		}
	}
}

// Seek positions at the first key >= target that passes the filter
func (fi *FilteredIterator) Seek(target []byte) bool {
	if !fi.iter.Seek(target) {
		return false
	}

	// If the current position doesn't pass the filter, find the next one that does
	if !fi.keyFilter(fi.iter.Key()) {
		return fi.Next()
	}

	return true
}

// PrefixFilterFunc creates a filter function for keys with a specific prefix
func PrefixFilterFunc(prefix []byte) KeyFilterFunc {
	return func(key []byte) bool {
		return bytes.HasPrefix(key, prefix)
	}
}

// SuffixFilterFunc creates a filter function for keys with a specific suffix
func SuffixFilterFunc(suffix []byte) KeyFilterFunc {
	return func(key []byte) bool {
		return bytes.HasSuffix(key, suffix)
	}
}

// PrefixIterator returns an iterator that filters keys by prefix
func NewPrefixIterator(iter iterator.Iterator, prefix []byte) *FilteredIterator {
	return NewFilteredIterator(iter, PrefixFilterFunc(prefix))
}

// SuffixIterator returns an iterator that filters keys by suffix
func NewSuffixIterator(iter iterator.Iterator, suffix []byte) *FilteredIterator {
	return NewFilteredIterator(iter, SuffixFilterFunc(suffix))
}
