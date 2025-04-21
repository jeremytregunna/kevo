package engine

import (
	"bytes"
	"container/heap"
	"sync"

	"github.com/jeremytregunna/kevo/pkg/common/iterator"
	"github.com/jeremytregunna/kevo/pkg/memtable"
	"github.com/jeremytregunna/kevo/pkg/sstable"
)

// iterHeapItem represents an item in the priority queue of iterators
type iterHeapItem struct {
	// The original source iterator
	source IterSource

	// The current key and value
	key   []byte
	value []byte

	// Internal heap index
	index int
}

// iterHeap is a min-heap of iterators, ordered by their current key
type iterHeap []*iterHeapItem

// Implement heap.Interface
func (h iterHeap) Len() int { return len(h) }

func (h iterHeap) Less(i, j int) bool {
	// Sort by key (primary) in ascending order
	return bytes.Compare(h[i].key, h[j].key) < 0
}

func (h iterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *iterHeap) Push(x interface{}) {
	item := x.(*iterHeapItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *iterHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// IterSource is an interface for any source that can provide key-value pairs
type IterSource interface {
	// GetIterator returns an iterator for this source
	GetIterator() iterator.Iterator

	// GetLevel returns the level of this source (lower is newer)
	GetLevel() int
}

// MemTableSource is an iterator source backed by a MemTable
type MemTableSource struct {
	mem   *memtable.MemTable
	level int
}

func (m *MemTableSource) GetIterator() iterator.Iterator {
	return memtable.NewIteratorAdapter(m.mem.NewIterator())
}

func (m *MemTableSource) GetLevel() int {
	return m.level
}

// SSTableSource is an iterator source backed by an SSTable
type SSTableSource struct {
	sst   *sstable.Reader
	level int
}

func (s *SSTableSource) GetIterator() iterator.Iterator {
	return sstable.NewIteratorAdapter(s.sst.NewIterator())
}

func (s *SSTableSource) GetLevel() int {
	return s.level
}

// The adapter implementations have been moved to their respective packages:
// - memtable.IteratorAdapter in pkg/memtable/iterator_adapter.go
// - sstable.IteratorAdapter in pkg/sstable/iterator_adapter.go

// MergedIterator merges multiple iterators into a single sorted view
// It uses a heap to efficiently merge the iterators
type MergedIterator struct {
	sources []IterSource
	iters   []iterator.Iterator
	heap    iterHeap
	current *iterHeapItem
	mu      sync.Mutex
}

// NewMergedIterator creates a new merged iterator from the given sources
// The sources should be provided in newest-to-oldest order
func NewMergedIterator(sources []IterSource) *MergedIterator {
	return &MergedIterator{
		sources: sources,
		iters:   make([]iterator.Iterator, len(sources)),
		heap:    make(iterHeap, 0, len(sources)),
	}
}

// SeekToFirst positions the iterator at the first key
func (m *MergedIterator) SeekToFirst() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize iterators if needed
	if len(m.iters) != len(m.sources) {
		m.initIterators()
	}

	// Position all iterators at their first key
	m.heap = m.heap[:0] // Clear heap
	for i, iter := range m.iters {
		iter.SeekToFirst()
		if iter.Valid() {
			heap.Push(&m.heap, &iterHeapItem{
				source: m.sources[i],
				key:    iter.Key(),
				value:  iter.Value(),
			})
		}
	}

	m.advanceHeap()
}

// Seek positions the iterator at the first key >= target
func (m *MergedIterator) Seek(target []byte) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize iterators if needed
	if len(m.iters) != len(m.sources) {
		m.initIterators()
	}

	// Position all iterators at or after the target key
	m.heap = m.heap[:0] // Clear heap
	for i, iter := range m.iters {
		if iter.Seek(target) {
			heap.Push(&m.heap, &iterHeapItem{
				source: m.sources[i],
				key:    iter.Key(),
				value:  iter.Value(),
			})
		}
	}

	m.advanceHeap()
	return m.current != nil
}

// SeekToLast positions the iterator at the last key
func (m *MergedIterator) SeekToLast() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize iterators if needed
	if len(m.iters) != len(m.sources) {
		m.initIterators()
	}

	// Position all iterators at their last key
	var lastKey []byte
	var lastValue []byte
	var lastSource IterSource
	var lastLevel int = -1

	for i, iter := range m.iters {
		iter.SeekToLast()
		if !iter.Valid() {
			continue
		}

		key := iter.Key()
		// If this is a new maximum key, or the same key but from a newer level
		if lastKey == nil ||
			bytes.Compare(key, lastKey) > 0 ||
			(bytes.Equal(key, lastKey) && m.sources[i].GetLevel() < lastLevel) {
			lastKey = key
			lastValue = iter.Value()
			lastSource = m.sources[i]
			lastLevel = m.sources[i].GetLevel()
		}
	}

	if lastKey != nil {
		m.current = &iterHeapItem{
			source: lastSource,
			key:    lastKey,
			value:  lastValue,
		}
	} else {
		m.current = nil
	}
}

// Next advances the iterator to the next key
func (m *MergedIterator) Next() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.current == nil {
		return false
	}

	// Get the current key to skip duplicates
	currentKey := m.current.key

	// Add back the iterator for the current source if it has more keys
	sourceIndex := -1
	for i, s := range m.sources {
		if s == m.current.source {
			sourceIndex = i
			break
		}
	}

	if sourceIndex >= 0 {
		iter := m.iters[sourceIndex]
		if iter.Next() && !bytes.Equal(iter.Key(), currentKey) {
			heap.Push(&m.heap, &iterHeapItem{
				source: m.sources[sourceIndex],
				key:    iter.Key(),
				value:  iter.Value(),
			})
		}
	}

	// Skip any entries with the same key (we've already returned the value from the newest source)
	for len(m.heap) > 0 && bytes.Equal(m.heap[0].key, currentKey) {
		item := heap.Pop(&m.heap).(*iterHeapItem)
		sourceIndex = -1
		for i, s := range m.sources {
			if s == item.source {
				sourceIndex = i
				break
			}
		}
		if sourceIndex >= 0 {
			iter := m.iters[sourceIndex]
			if iter.Next() && !bytes.Equal(iter.Key(), currentKey) {
				heap.Push(&m.heap, &iterHeapItem{
					source: m.sources[sourceIndex],
					key:    iter.Key(),
					value:  iter.Value(),
				})
			}
		}
	}

	m.advanceHeap()
	return m.current != nil
}

// Key returns the current key
func (m *MergedIterator) Key() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.current == nil {
		return nil
	}
	return m.current.key
}

// Value returns the current value
func (m *MergedIterator) Value() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.current == nil {
		return nil
	}
	return m.current.value
}

// Valid returns true if the iterator is positioned at a valid entry
func (m *MergedIterator) Valid() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.current != nil
}

// IsTombstone returns true if the current entry is a deletion marker
func (m *MergedIterator) IsTombstone() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.current == nil {
		return false
	}

	// In a MergedIterator, we need to check if the source iterator marks this as a tombstone
	for _, source := range m.sources {
		if source == m.current.source {
			iter := source.GetIterator()
			return iter.IsTombstone()
		}
	}

	return false
}

// initIterators initializes all iterators from sources
func (m *MergedIterator) initIterators() {
	for i, source := range m.sources {
		m.iters[i] = source.GetIterator()
	}
}

// advanceHeap advances the heap and updates the current item
func (m *MergedIterator) advanceHeap() {
	if len(m.heap) == 0 {
		m.current = nil
		return
	}

	// Get the smallest key
	m.current = heap.Pop(&m.heap).(*iterHeapItem)

	// Skip any entries with duplicate keys (keeping the one from the newest source)
	// Sources are already provided in newest-to-oldest order, and we've popped
	// the smallest key, so any item in the heap with the same key is from an older source
	currentKey := m.current.key
	for len(m.heap) > 0 && bytes.Equal(m.heap[0].key, currentKey) {
		item := heap.Pop(&m.heap).(*iterHeapItem)
		sourceIndex := -1
		for i, s := range m.sources {
			if s == item.source {
				sourceIndex = i
				break
			}
		}
		if sourceIndex >= 0 {
			iter := m.iters[sourceIndex]
			if iter.Next() && !bytes.Equal(iter.Key(), currentKey) {
				heap.Push(&m.heap, &iterHeapItem{
					source: m.sources[sourceIndex],
					key:    iter.Key(),
					value:  iter.Value(),
				})
			}
		}
	}
}

// newHierarchicalIterator creates a new hierarchical iterator for the engine
func newHierarchicalIterator(e *Engine) *boundedIterator {
	// Get all MemTables from the pool
	memTables := e.memTablePool.GetMemTables()

	// Create a list of all iterators in newest-to-oldest order
	iters := make([]iterator.Iterator, 0, len(memTables)+len(e.sstables))

	// Add MemTables (active first, then immutables)
	for _, table := range memTables {
		iters = append(iters, memtable.NewIteratorAdapter(table.NewIterator()))
	}

	// Add SSTables (from newest to oldest)
	for i := len(e.sstables) - 1; i >= 0; i-- {
		iters = append(iters, sstable.NewIteratorAdapter(e.sstables[i].NewIterator()))
	}

	// Create sources list for all iterators
	sources := make([]IterSource, 0, len(memTables)+len(e.sstables))

	// Add sources for memtables
	for i, table := range memTables {
		sources = append(sources, &MemTableSource{
			mem:   table,
			level: i, // Assign level numbers starting from 0 (active memtable is newest)
		})
	}

	// Add sources for SSTables
	for i := len(e.sstables) - 1; i >= 0; i-- {
		sources = append(sources, &SSTableSource{
			sst:   e.sstables[i],
			level: len(memTables) + (len(e.sstables) - 1 - i), // Continue level numbering after memtables
		})
	}

	// Wrap in a bounded iterator (unbounded by default)
	// If we have no iterators, use an empty one
	var baseIter iterator.Iterator
	if len(iters) == 0 {
		baseIter = &emptyIterator{}
	} else if len(iters) == 1 {
		baseIter = iters[0]
	} else {
		// Create a chained iterator that checks each source in order and handles duplicates
		baseIter = &chainedIterator{
			iterators: iters,
			sources:   sources,
		}
	}

	return &boundedIterator{
		Iterator: baseIter,
		end:      nil, // No end bound by default
	}
}

// chainedIterator is a simple iterator that checks multiple sources in order
type chainedIterator struct {
	iterators []iterator.Iterator
	sources   []IterSource // Corresponding sources for each iterator
	current   int
}

func (c *chainedIterator) SeekToFirst() {
	if len(c.iterators) == 0 {
		return
	}

	// Position all iterators at their first key
	for _, iter := range c.iterators {
		iter.SeekToFirst()
	}

	// Find the iterator with the smallest key from the newest source
	c.current = -1
	
	// Find the smallest valid key
	for i, iter := range c.iterators {
		if !iter.Valid() {
			continue
		}
		
		// If we haven't found a key yet, or this key is smaller than the current smallest
		if c.current == -1 || bytes.Compare(iter.Key(), c.iterators[c.current].Key()) < 0 {
			c.current = i
		} else if bytes.Equal(iter.Key(), c.iterators[c.current].Key()) {
			// If keys are equal, prefer the newer source (lower level)
			if c.sources[i].GetLevel() < c.sources[c.current].GetLevel() {
				c.current = i
			}
		}
	}
}

func (c *chainedIterator) SeekToLast() {
	if len(c.iterators) == 0 {
		return
	}

	// Position all iterators at their last key
	for _, iter := range c.iterators {
		iter.SeekToLast()
	}

	// Find the first valid iterator with the largest key
	c.current = -1
	var largestKey []byte

	for i, iter := range c.iterators {
		if !iter.Valid() {
			continue
		}

		if c.current == -1 || bytes.Compare(iter.Key(), largestKey) > 0 {
			c.current = i
			largestKey = iter.Key()
		}
	}
}

func (c *chainedIterator) Seek(target []byte) bool {
	if len(c.iterators) == 0 {
		return false
	}

	// Position all iterators at or after the target key
	for _, iter := range c.iterators {
		iter.Seek(target)
	}

	// Find the iterator with the smallest key from the newest source
	c.current = -1
	
	// Find the smallest valid key
	for i, iter := range c.iterators {
		if !iter.Valid() {
			continue
		}
		
		// If we haven't found a key yet, or this key is smaller than the current smallest
		if c.current == -1 || bytes.Compare(iter.Key(), c.iterators[c.current].Key()) < 0 {
			c.current = i
		} else if bytes.Equal(iter.Key(), c.iterators[c.current].Key()) {
			// If keys are equal, prefer the newer source (lower level)
			if c.sources[i].GetLevel() < c.sources[c.current].GetLevel() {
				c.current = i
			}
		}
	}

	return c.current != -1
}

func (c *chainedIterator) Next() bool {
	if !c.Valid() {
		return false
	}

	// Get the current key
	currentKey := c.iterators[c.current].Key()

	// Advance all iterators that are at the current key
	for _, iter := range c.iterators {
		if iter.Valid() && bytes.Equal(iter.Key(), currentKey) {
			iter.Next()
		}
	}

	// Find the iterator with the smallest key from the newest source
	c.current = -1
	
	// Find the smallest valid key that is greater than the current key
	for i, iter := range c.iterators {
		if !iter.Valid() {
			continue
		}
		
		// Skip if the key is the same as the current key (we've already advanced past it)
		if bytes.Equal(iter.Key(), currentKey) {
			continue
		}
		
		// If we haven't found a key yet, or this key is smaller than the current smallest
		if c.current == -1 || bytes.Compare(iter.Key(), c.iterators[c.current].Key()) < 0 {
			c.current = i
		} else if bytes.Equal(iter.Key(), c.iterators[c.current].Key()) {
			// If keys are equal, prefer the newer source (lower level)
			if c.sources[i].GetLevel() < c.sources[c.current].GetLevel() {
				c.current = i
			}
		}
	}

	return c.current != -1
}

func (c *chainedIterator) Key() []byte {
	if !c.Valid() {
		return nil
	}
	return c.iterators[c.current].Key()
}

func (c *chainedIterator) Value() []byte {
	if !c.Valid() {
		return nil
	}
	return c.iterators[c.current].Value()
}

func (c *chainedIterator) Valid() bool {
	return c.current != -1 && c.current < len(c.iterators) && c.iterators[c.current].Valid()
}

func (c *chainedIterator) IsTombstone() bool {
	if !c.Valid() {
		return false
	}
	return c.iterators[c.current].IsTombstone()
}

// emptyIterator is an iterator that contains no entries
type emptyIterator struct{}

func (e *emptyIterator) SeekToFirst()            {}
func (e *emptyIterator) SeekToLast()             {}
func (e *emptyIterator) Seek(target []byte) bool { return false }
func (e *emptyIterator) Next() bool              { return false }
func (e *emptyIterator) Key() []byte             { return nil }
func (e *emptyIterator) Value() []byte           { return nil }
func (e *emptyIterator) Valid() bool             { return false }
func (e *emptyIterator) IsTombstone() bool       { return false }

// Note: This is now replaced by the more comprehensive implementation in engine.go
// The hierarchical iterator code remains here to avoid impacting other code references

// boundedIterator wraps an iterator and limits it to a specific range
type boundedIterator struct {
	iterator.Iterator
	start []byte
	end   []byte
}

// SetBounds sets the start and end bounds for the iterator
func (b *boundedIterator) SetBounds(start, end []byte) {
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

func (b *boundedIterator) SeekToFirst() {
	if b.start != nil {
		// If we have a start bound, seek to it
		b.Iterator.Seek(b.start)
	} else {
		// Otherwise seek to the first key
		b.Iterator.SeekToFirst()
	}
	b.checkBounds()
}

func (b *boundedIterator) SeekToLast() {
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

func (b *boundedIterator) Seek(target []byte) bool {
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

func (b *boundedIterator) Next() bool {
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

func (b *boundedIterator) Valid() bool {
	return b.Iterator.Valid() && b.checkBounds()
}

func (b *boundedIterator) Key() []byte {
	if !b.Valid() {
		return nil
	}
	return b.Iterator.Key()
}

func (b *boundedIterator) Value() []byte {
	if !b.Valid() {
		return nil
	}
	return b.Iterator.Value()
}

// IsTombstone returns true if the current entry is a deletion marker
func (b *boundedIterator) IsTombstone() bool {
	if !b.Valid() {
		return false
	}
	return b.Iterator.IsTombstone()
}

func (b *boundedIterator) checkBounds() bool {
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
