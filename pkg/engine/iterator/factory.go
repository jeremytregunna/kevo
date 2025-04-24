package iterator

import (
	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/common/iterator/bounded"
	"github.com/KevoDB/kevo/pkg/common/iterator/composite"
	"github.com/KevoDB/kevo/pkg/memtable"
	"github.com/KevoDB/kevo/pkg/sstable"
)

// Factory provides methods to create iterators for the storage engine
type Factory struct{}

// NewFactory creates a new iterator factory
func NewFactory() *Factory {
	return &Factory{}
}

// CreateIterator creates a hierarchical iterator that combines
// memtables and sstables in the correct priority order
func (f *Factory) CreateIterator(
	memTables []*memtable.MemTable,
	ssTables []*sstable.Reader,
) iterator.Iterator {
	return f.createBaseIterator(memTables, ssTables)
}

// CreateRangeIterator creates an iterator limited to a specific key range
func (f *Factory) CreateRangeIterator(
	memTables []*memtable.MemTable,
	ssTables []*sstable.Reader,
	startKey, endKey []byte,
) iterator.Iterator {
	baseIter := f.createBaseIterator(memTables, ssTables)
	return bounded.NewBoundedIterator(baseIter, startKey, endKey)
}

// createBaseIterator creates the base hierarchical iterator
func (f *Factory) createBaseIterator(
	memTables []*memtable.MemTable,
	ssTables []*sstable.Reader,
) iterator.Iterator {
	// If there are no sources, return an empty iterator
	if len(memTables) == 0 && len(ssTables) == 0 {
		return newEmptyIterator()
	}

	// Create individual iterators in newest-to-oldest order
	iterators := make([]iterator.Iterator, 0, len(memTables)+len(ssTables))

	// Add memtable iterators (newest to oldest)
	for _, mt := range memTables {
		iterators = append(iterators, memtable.NewIteratorAdapter(mt.NewIterator()))
	}

	// Add sstable iterators (newest to oldest)
	for i := len(ssTables) - 1; i >= 0; i-- {
		iterators = append(iterators, sstable.NewIteratorAdapter(ssTables[i].NewIterator()))
	}

	// Create hierarchical iterator
	return composite.NewHierarchicalIterator(iterators)
}

// newEmptyIterator creates an iterator that contains no entries
func newEmptyIterator() iterator.Iterator {
	return &emptyIterator{}
}

// Simple empty iterator implementation
type emptyIterator struct{}

func (e *emptyIterator) SeekToFirst()            {}
func (e *emptyIterator) SeekToLast()             {}
func (e *emptyIterator) Seek(target []byte) bool { return false }
func (e *emptyIterator) Next() bool              { return false }
func (e *emptyIterator) Key() []byte             { return nil }
func (e *emptyIterator) Value() []byte           { return nil }
func (e *emptyIterator) Valid() bool             { return false }
func (e *emptyIterator) IsTombstone() bool       { return false }