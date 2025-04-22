package composite

import (
	"github.com/KevoDB/kevo/pkg/common/iterator"
)

// CompositeIterator is an interface for iterators that combine multiple source iterators
// into a single logical view.
type CompositeIterator interface {
	// Embeds the basic Iterator interface
	iterator.Iterator

	// NumSources returns the number of source iterators
	NumSources() int

	// GetSourceIterators returns the underlying source iterators
	GetSourceIterators() []iterator.Iterator
}
