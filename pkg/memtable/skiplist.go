package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// MaxHeight is the maximum height of the skip list
	MaxHeight = 12

	// BranchingFactor determines the probability of increasing the height
	BranchingFactor = 4

	// DefaultCacheLineSize aligns nodes to cache lines for better performance
	DefaultCacheLineSize = 64
)

// ValueType represents the type of a key-value entry
type ValueType uint8

const (
	// TypeValue indicates the entry contains a value
	TypeValue ValueType = iota + 1

	// TypeDeletion indicates the entry is a tombstone (deletion marker)
	TypeDeletion
)

// entry represents a key-value pair with additional metadata
type entry struct {
	key       []byte
	value     []byte
	valueType ValueType
	seqNum    uint64
}

// newEntry creates a new entry
func newEntry(key, value []byte, valueType ValueType, seqNum uint64) *entry {
	return &entry{
		key:       key,
		value:     value,
		valueType: valueType,
		seqNum:    seqNum,
	}
}

// size returns the approximate size of the entry in memory
func (e *entry) size() int {
	return len(e.key) + len(e.value) + 16 // adding overhead for metadata
}

// compare compares this entry with another key
// Returns: negative if e.key < key, 0 if equal, positive if e.key > key
func (e *entry) compare(key []byte) int {
	return bytes.Compare(e.key, key)
}

// compareWithEntry compares this entry with another entry
// First by key, then by sequence number (in reverse order to prioritize newer entries)
func (e *entry) compareWithEntry(other *entry) int {
	cmp := bytes.Compare(e.key, other.key)
	if cmp == 0 {
		// If keys are equal, compare sequence numbers in reverse order (newer first)
		if e.seqNum > other.seqNum {
			return -1
		} else if e.seqNum < other.seqNum {
			return 1
		}
		return 0
	}
	return cmp
}

// node represents a node in the skip list
type node struct {
	entry  *entry
	height int32
	// next contains pointers to the next nodes at each level
	// This is allocated as a single block for cache efficiency
	next [MaxHeight]unsafe.Pointer
}

// newNode creates a new node with a random height
func newNode(e *entry, height int) *node {
	return &node{
		entry:  e,
		height: int32(height),
	}
}

// getNext returns the next node at the given level
func (n *node) getNext(level int) *node {
	return (*node)(atomic.LoadPointer(&n.next[level]))
}

// setNext sets the next node at the given level
func (n *node) setNext(level int, next *node) {
	atomic.StorePointer(&n.next[level], unsafe.Pointer(next))
}

// SkipList is a concurrent skip list implementation for the MemTable
type SkipList struct {
	head      *node
	maxHeight int32
	rnd       *rand.Rand
	rndMtx    sync.Mutex
	size      int64
}

// NewSkipList creates a new skip list
func NewSkipList() *SkipList {
	seed := time.Now().UnixNano()
	list := &SkipList{
		head:      newNode(nil, MaxHeight),
		maxHeight: 1,
		rnd:       rand.New(rand.NewSource(seed)),
	}
	return list
}

// randomHeight generates a random height for a new node
func (s *SkipList) randomHeight() int {
	s.rndMtx.Lock()
	defer s.rndMtx.Unlock()

	height := 1
	for height < MaxHeight && s.rnd.Intn(BranchingFactor) == 0 {
		height++
	}
	return height
}

// getCurrentHeight returns the current maximum height of the skip list
func (s *SkipList) getCurrentHeight() int {
	return int(atomic.LoadInt32(&s.maxHeight))
}

// Insert adds a new entry to the skip list
func (s *SkipList) Insert(e *entry) {
	height := s.randomHeight()
	prev := [MaxHeight]*node{}
	node := newNode(e, height)

	// Try to increase the height of the list
	currHeight := s.getCurrentHeight()
	if height > currHeight {
		// Attempt to increase the height
		if atomic.CompareAndSwapInt32(&s.maxHeight, int32(currHeight), int32(height)) {
			currHeight = height
		}
	}

	// Find where to insert at each level
	current := s.head
	for level := currHeight - 1; level >= 0; level-- {
		// Find the insertion point at this level
		for next := current.getNext(level); next != nil; next = current.getNext(level) {
			if next.entry.compareWithEntry(e) >= 0 {
				break
			}
			current = next
		}
		prev[level] = current
	}

	// Insert the node at each level
	for level := 0; level < height; level++ {
		node.setNext(level, prev[level].getNext(level))
		prev[level].setNext(level, node)
	}

	// Update approximate size
	atomic.AddInt64(&s.size, int64(e.size()))
}

// Find looks for an entry with the specified key
// If multiple entries have the same key, the most recent one is returned
func (s *SkipList) Find(key []byte) *entry {
	var result *entry
	current := s.head
	height := s.getCurrentHeight()

	// Start from the highest level for efficient search
	for level := height - 1; level >= 0; level-- {
		// Scan forward until we find a key greater than or equal to the target
		for next := current.getNext(level); next != nil; next = current.getNext(level) {
			cmp := next.entry.compare(key)
			if cmp > 0 {
				// Key at next is greater than target, go down a level
				break
			} else if cmp == 0 {
				// Found a match, check if it's newer than our current result
				if result == nil || next.entry.seqNum > result.seqNum {
					result = next.entry
				}
				// Continue at this level to see if there are more entries with same key
				current = next
			} else {
				// Key at next is less than target, move forward
				current = next
			}
		}
	}

	// For level 0, do one more sweep to ensure we get the newest entry
	current = s.head
	for next := current.getNext(0); next != nil; next = next.getNext(0) {
		cmp := next.entry.compare(key)
		if cmp > 0 {
			// Past the key
			break
		} else if cmp == 0 {
			// Found a match, update result if it's newer
			if result == nil || next.entry.seqNum > result.seqNum {
				result = next.entry
			}
		}
		current = next
	}

	return result
}

// ApproximateSize returns the approximate size of the skip list in bytes
func (s *SkipList) ApproximateSize() int64 {
	return atomic.LoadInt64(&s.size)
}

// Iterator provides sequential access to the skip list entries
type Iterator struct {
	list    *SkipList
	current *node
}

// NewIterator creates a new Iterator for the skip list
func (s *SkipList) NewIterator() *Iterator {
	return &Iterator{
		list:    s,
		current: s.head,
	}
}

// Valid returns true if the iterator is positioned at a valid entry
func (it *Iterator) Valid() bool {
	return it.current != nil && it.current != it.list.head
}

// Next advances the iterator to the next entry
func (it *Iterator) Next() {
	if it.current == nil {
		return
	}
	it.current = it.current.getNext(0)
}

// SeekToFirst positions the iterator at the first entry
func (it *Iterator) SeekToFirst() {
	it.current = it.list.head.getNext(0)
}

// Seek positions the iterator at the first entry with a key >= target
func (it *Iterator) Seek(key []byte) {
	// Start from head
	current := it.list.head
	height := it.list.getCurrentHeight()

	// Search algorithm similar to Find
	for level := height - 1; level >= 0; level-- {
		for next := current.getNext(level); next != nil; next = current.getNext(level) {
			if next.entry.compare(key) >= 0 {
				break
			}
			current = next
		}
	}

	// Move to the next node, which should be >= target
	it.current = current.getNext(0)
}

// Key returns the key of the current entry
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.current.entry.key
}

// Value returns the value of the current entry
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}

	// For tombstones (deletion markers), we still return nil
	// but we preserve them during iteration so compaction can see them
	return it.current.entry.value
}

// ValueType returns the type of the current entry (TypeValue or TypeDeletion)
func (it *Iterator) ValueType() ValueType {
	if !it.Valid() {
		return 0 // Invalid type
	}
	return it.current.entry.valueType
}

// IsTombstone returns true if the current entry is a deletion marker
func (it *Iterator) IsTombstone() bool {
	return it.Valid() && it.current.entry.valueType == TypeDeletion
}

// Entry returns the current entry
func (it *Iterator) Entry() *entry {
	if !it.Valid() {
		return nil
	}
	return it.current.entry
}
