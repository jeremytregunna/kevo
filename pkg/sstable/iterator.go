package sstable

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/KevoDB/kevo/pkg/sstable/block"
)

// Iterator iterates over key-value pairs in an SSTable
type Iterator struct {
	reader        *Reader
	indexIterator *block.Iterator
	dataBlockIter *block.Iterator
	currentBlock  *block.Reader
	err           error
	initialized   bool
	mu            sync.Mutex
}

// SeekToFirst positions the iterator at the first key
func (it *Iterator) SeekToFirst() {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Reset error state
	it.err = nil

	// Position index iterator at the first entry
	it.indexIterator.SeekToFirst()

	// Load the first valid data block
	if it.indexIterator.Valid() {
		// Skip invalid entries
		if len(it.indexIterator.Value()) < 8 {
			it.skipInvalidIndexEntries()
		}

		if it.indexIterator.Valid() {
			// Load the data block
			it.loadCurrentDataBlock()

			// Position the data block iterator at the first key
			if it.dataBlockIter != nil {
				it.dataBlockIter.SeekToFirst()
			}
		}
	}

	if !it.indexIterator.Valid() || it.dataBlockIter == nil {
		// No valid index entries
		it.resetBlockIterator()
	}

	it.initialized = true
}

// SeekToLast positions the iterator at the last key
func (it *Iterator) SeekToLast() {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Reset error state
	it.err = nil

	// Find the last unique block by tracking all seen blocks
	lastBlockOffset, lastBlockValid := it.findLastUniqueBlockOffset()

	// Position index at an entry pointing to the last block
	if lastBlockValid {
		it.indexIterator.SeekToFirst()
		for it.indexIterator.Valid() {
			if len(it.indexIterator.Value()) >= 8 {
				blockOffset := binary.LittleEndian.Uint64(it.indexIterator.Value()[:8])
				if blockOffset == lastBlockOffset {
					break
				}
			}
			it.indexIterator.Next()
		}

		// Load the last data block
		it.loadCurrentDataBlock()

		// Position the data block iterator at the last key
		if it.dataBlockIter != nil {
			it.dataBlockIter.SeekToLast()
		}
	} else {
		// No valid index entries
		it.resetBlockIterator()
	}

	it.initialized = true
}

// Seek positions the iterator at the first key >= target
func (it *Iterator) Seek(target []byte) bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Reset error state
	it.err = nil
	it.initialized = true

	// Find the block that might contain the key
	// The index contains the first key of each block
	if !it.indexIterator.Seek(target) {
		// If seeking in the index fails, try the last block
		it.indexIterator.SeekToLast()
		if !it.indexIterator.Valid() {
			// No blocks in the SSTable
			it.resetBlockIterator()
			return false
		}
	}

	// Load the data block at the current index position
	it.loadCurrentDataBlock()
	if it.dataBlockIter == nil {
		return false
	}

	// Try to find the target key in this block
	if it.dataBlockIter.Seek(target) {
		// Found a key >= target in this block
		return true
	}

	// If we didn't find the key in this block, it might be in a later block
	return it.seekInNextBlocks()
}

// Next advances the iterator to the next key
func (it *Iterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if !it.initialized {
		it.SeekToFirst()
		return it.Valid()
	}

	if it.dataBlockIter == nil {
		// If we don't have a current block, attempt to load the one at the current index position
		if it.indexIterator.Valid() {
			it.loadCurrentDataBlock()
			if it.dataBlockIter != nil {
				it.dataBlockIter.SeekToFirst()
				return it.dataBlockIter.Valid()
			}
		}
		return false
	}

	// Try to advance within current block
	if it.dataBlockIter.Next() {
		// Successfully moved to the next entry in the current block
		return true
	}

	// We've reached the end of the current block, so try to move to the next block
	return it.advanceToNextBlock()
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()

	if !it.initialized || it.dataBlockIter == nil || !it.dataBlockIter.Valid() {
		return nil
	}
	return it.dataBlockIter.Key()
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()

	if !it.initialized || it.dataBlockIter == nil || !it.dataBlockIter.Valid() {
		return nil
	}
	return it.dataBlockIter.Value()
}

// Valid returns true if the iterator is positioned at a valid entry
func (it *Iterator) Valid() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	return it.initialized && it.dataBlockIter != nil && it.dataBlockIter.Valid()
}

// IsTombstone returns true if the current entry is a deletion marker
func (it *Iterator) IsTombstone() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Not valid means not a tombstone
	if !it.initialized || it.dataBlockIter == nil || !it.dataBlockIter.Valid() {
		return false
	}

	// For SSTable iterators, a nil value always represents a tombstone
	// The block iterator's Value method will return nil for tombstones
	return it.dataBlockIter.Value() == nil
}

// SequenceNumber returns the sequence number of the current entry
func (it *Iterator) SequenceNumber() uint64 {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Not valid means sequence number 0
	if !it.initialized || it.dataBlockIter == nil || !it.dataBlockIter.Valid() {
		return 0
	}

	return it.dataBlockIter.SequenceNumber()
}

// Error returns any error encountered during iteration
func (it *Iterator) Error() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	return it.err
}

// Helper methods for common operations

// resetBlockIterator resets current block and iterator
func (it *Iterator) resetBlockIterator() {
	it.currentBlock = nil
	it.dataBlockIter = nil
}

// skipInvalidIndexEntries advances the index iterator past any invalid entries
func (it *Iterator) skipInvalidIndexEntries() {
	for it.indexIterator.Next() {
		if len(it.indexIterator.Value()) >= 8 {
			break
		}
	}
}

// findLastUniqueBlockOffset scans the index to find the offset of the last unique block
func (it *Iterator) findLastUniqueBlockOffset() (uint64, bool) {
	seenBlocks := make(map[uint64]bool)
	var lastBlockOffset uint64
	var lastBlockValid bool

	// Position index iterator at the first entry
	it.indexIterator.SeekToFirst()

	// Scan through all blocks to find the last unique one
	for it.indexIterator.Valid() {
		if len(it.indexIterator.Value()) >= 8 {
			blockOffset := binary.LittleEndian.Uint64(it.indexIterator.Value()[:8])
			if !seenBlocks[blockOffset] {
				seenBlocks[blockOffset] = true
				lastBlockOffset = blockOffset
				lastBlockValid = true
			}
		}
		it.indexIterator.Next()
	}

	return lastBlockOffset, lastBlockValid
}

// seekInNextBlocks attempts to find the target key in subsequent blocks
func (it *Iterator) seekInNextBlocks() bool {
	var foundValidKey bool

	// Store current block offset to skip duplicates
	var currentBlockOffset uint64
	if len(it.indexIterator.Value()) >= 8 {
		currentBlockOffset = binary.LittleEndian.Uint64(it.indexIterator.Value()[:8])
	}

	// Try subsequent blocks, skipping duplicates
	for it.indexIterator.Next() {
		// Skip invalid entries or duplicates of the current block
		if !it.indexIterator.Valid() || len(it.indexIterator.Value()) < 8 {
			continue
		}

		nextBlockOffset := binary.LittleEndian.Uint64(it.indexIterator.Value()[:8])
		if nextBlockOffset == currentBlockOffset {
			// This is a duplicate index entry pointing to the same block, skip it
			continue
		}

		// Found a new block, update current offset
		currentBlockOffset = nextBlockOffset

		it.loadCurrentDataBlock()
		if it.dataBlockIter == nil {
			return false
		}

		// Position at the first key in the next block
		it.dataBlockIter.SeekToFirst()
		if it.dataBlockIter.Valid() {
			foundValidKey = true
			break
		}
	}

	return foundValidKey
}

// advanceToNextBlock moves to the next unique block
func (it *Iterator) advanceToNextBlock() bool {
	// Store the current block's offset to find the next unique block
	var currentBlockOffset uint64
	if len(it.indexIterator.Value()) >= 8 {
		currentBlockOffset = binary.LittleEndian.Uint64(it.indexIterator.Value()[:8])
	}

	// Find next block with a different offset
	nextBlockFound := it.findNextUniqueBlock(currentBlockOffset)

	if !nextBlockFound || !it.indexIterator.Valid() {
		// No more unique blocks in the index
		it.resetBlockIterator()
		return false
	}

	// Load the next block
	it.loadCurrentDataBlock()
	if it.dataBlockIter == nil {
		return false
	}

	// Start at the beginning of the new block
	it.dataBlockIter.SeekToFirst()
	return it.dataBlockIter.Valid()
}

// findNextUniqueBlock advances the index iterator to find a block with a different offset
func (it *Iterator) findNextUniqueBlock(currentBlockOffset uint64) bool {
	for it.indexIterator.Next() {
		// Skip invalid entries or entries pointing to the same block
		if !it.indexIterator.Valid() || len(it.indexIterator.Value()) < 8 {
			continue
		}

		nextBlockOffset := binary.LittleEndian.Uint64(it.indexIterator.Value()[:8])
		if nextBlockOffset != currentBlockOffset {
			// Found a new block
			return true
		}
	}
	return false
}

// loadCurrentDataBlock loads the data block at the current index iterator position
func (it *Iterator) loadCurrentDataBlock() {
	// Check if index iterator is valid
	if !it.indexIterator.Valid() {
		it.resetBlockIterator()
		it.err = fmt.Errorf("index iterator not valid")
		return
	}

	// Parse block location from index value
	locator, err := ParseBlockLocator(it.indexIterator.Key(), it.indexIterator.Value())
	if err != nil {
		it.err = fmt.Errorf("failed to parse block locator: %w", err)
		it.resetBlockIterator()
		return
	}

	// Fetch the block using the reader's block fetcher
	blockReader, err := it.reader.blockFetcher.FetchBlock(locator.Offset, locator.Size)
	if err != nil {
		it.err = fmt.Errorf("failed to fetch block: %w", err)
		it.resetBlockIterator()
		return
	}

	it.currentBlock = blockReader
	it.dataBlockIter = blockReader.Iterator()
}
