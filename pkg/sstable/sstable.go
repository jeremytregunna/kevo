package sstable

import (
	"errors"

	"github.com/KevoDB/kevo/pkg/sstable/block"
)

const (
	// IndexBlockEntrySize is the approximate size of an index entry
	IndexBlockEntrySize = 20
	// DefaultBlockSize is the target size for data blocks
	DefaultBlockSize = block.BlockSize
	// IndexKeyInterval controls how frequently we add keys to the index
	IndexKeyInterval = 64 * 1024 // Add index entry every ~64KB
)

var (
	// ErrNotFound indicates a key was not found in the SSTable
	ErrNotFound = errors.New("key not found in sstable")
	// ErrCorruption indicates data corruption was detected
	ErrCorruption = errors.New("sstable corruption detected")
)

// IndexEntry represents a block index entry
type IndexEntry struct {
	// BlockOffset is the offset of the block in the file
	BlockOffset uint64
	// BlockSize is the size of the block in bytes
	BlockSize uint32
	// FirstKey is the first key in the block
	FirstKey []byte
}
