package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"
)

// Builder constructs a sorted, serialized block
type Builder struct {
	entries       []Entry
	restartPoints []uint32
	restartCount  uint32
	currentSize   uint32
	lastKey       []byte
	restartIdx    int
}

// NewBuilder creates a new block builder
func NewBuilder() *Builder {
	return &Builder{
		entries:       make([]Entry, 0, MaxBlockEntries),
		restartPoints: make([]uint32, 0, MaxBlockEntries/RestartInterval+1),
		restartCount:  0,
		currentSize:   0,
	}
}

// Add adds a key-value pair to the block
// Keys must be added in sorted order
func (b *Builder) Add(key, value []byte) error {
	return b.AddWithSequence(key, value, 0) // Default sequence number to 0 for backward compatibility
}

// AddWithSequence adds a key-value pair to the block with a sequence number
// Keys must be added in sorted order
func (b *Builder) AddWithSequence(key, value []byte, seqNum uint64) error {
	// Ensure keys are added in sorted order
	if len(b.entries) > 0 && bytes.Compare(key, b.lastKey) <= 0 {
		return fmt.Errorf("keys must be added in strictly increasing order, got %s after %s",
			string(key), string(b.lastKey))
	}

	b.entries = append(b.entries, Entry{
		Key:         append([]byte(nil), key...),   // Make copies to avoid references
		Value:       append([]byte(nil), value...), // to external data
		SequenceNum: seqNum,
	})

	// Add restart point if needed
	if b.restartIdx == 0 || b.restartIdx >= RestartInterval {
		b.restartPoints = append(b.restartPoints, b.currentSize)
		b.restartIdx = 0
	}
	b.restartIdx++

	// Track the size
	b.currentSize += uint32(len(key) + len(value) + 16) // 16 bytes for metadata (including sequence number)
	b.lastKey = append([]byte(nil), key...)

	return nil
}

// GetEntries returns the entries in the block
func (b *Builder) GetEntries() []Entry {
	return b.entries
}

// Reset clears the builder state
func (b *Builder) Reset() {
	b.entries = b.entries[:0]
	b.restartPoints = b.restartPoints[:0]
	b.restartCount = 0
	b.currentSize = 0
	b.lastKey = nil
	b.restartIdx = 0
}

// EstimatedSize returns the approximate size of the block when serialized
func (b *Builder) EstimatedSize() uint32 {
	if len(b.entries) == 0 {
		return 0
	}
	// Data + restart points array + footer
	return b.currentSize + uint32(len(b.restartPoints)*4) + BlockFooterSize
}

// Entries returns the number of entries in the block
func (b *Builder) Entries() int {
	return len(b.entries)
}

// Finish serializes the block to a writer
func (b *Builder) Finish(w io.Writer) (uint64, error) {
	if len(b.entries) == 0 {
		return 0, fmt.Errorf("cannot finish empty block")
	}

	// Keys are already sorted by the Add method's requirement

	// Remove any duplicate keys (keeping the one with the highest sequence number)
	if len(b.entries) > 1 {
		// Group entries by key and find entry with highest sequence number
		keyMap := make(map[string]Entry)
		for _, entry := range b.entries {
			keyStr := string(entry.Key)
			if existing, exists := keyMap[keyStr]; !exists || entry.SequenceNum > existing.SequenceNum {
				keyMap[keyStr] = entry
			}
		}

		// Rebuild sorted entries from the map
		uniqueEntries := make([]Entry, 0, len(keyMap))
		for _, entry := range b.entries {
			keyStr := string(entry.Key)
			if best, exists := keyMap[keyStr]; exists {
				if bytes.Equal(entry.Key, best.Key) && entry.SequenceNum == best.SequenceNum {
					uniqueEntries = append(uniqueEntries, best)
					delete(keyMap, keyStr) // Delete to avoid duplicates
				}
			}
		}
		b.entries = uniqueEntries
	}

	// Reset restart points
	b.restartPoints = b.restartPoints[:0]
	b.restartPoints = append(b.restartPoints, 0) // First entry is always a restart point

	// Write all entries
	content := make([]byte, 0, b.EstimatedSize())
	buffer := bytes.NewBuffer(content)

	var prevKey []byte
	restartOffset := 0

	for i, entry := range b.entries {
		// Start a new restart point?
		isRestart := i == 0 || restartOffset >= RestartInterval
		if isRestart {
			restartOffset = 0
			if i > 0 {
				b.restartPoints = append(b.restartPoints, uint32(buffer.Len()))
			}
		}

		// Write entry
		if isRestart {
			// Full key for restart points
			keyLen := uint16(len(entry.Key))
			err := binary.Write(buffer, binary.LittleEndian, keyLen)
			if err != nil {
				return 0, fmt.Errorf("failed to write key length: %w", err)
			}
			n, err := buffer.Write(entry.Key)
			if err != nil {
				return 0, fmt.Errorf("failed to write key: %w", err)
			}
			if n != len(entry.Key) {
				return 0, fmt.Errorf("wrote incomplete key: %d of %d bytes", n, len(entry.Key))
			}
		} else {
			// For non-restart points, delta encode the key
			commonPrefix := 0
			for j := 0; j < len(prevKey) && j < len(entry.Key); j++ {
				if prevKey[j] != entry.Key[j] {
					break
				}
				commonPrefix++
			}

			// Format: [shared prefix length][unshared length][unshared bytes]
			err := binary.Write(buffer, binary.LittleEndian, uint16(commonPrefix))
			if err != nil {
				return 0, fmt.Errorf("failed to write common prefix length: %w", err)
			}

			unsharedLen := uint16(len(entry.Key) - commonPrefix)
			err = binary.Write(buffer, binary.LittleEndian, unsharedLen)
			if err != nil {
				return 0, fmt.Errorf("failed to write unshared length: %w", err)
			}

			n, err := buffer.Write(entry.Key[commonPrefix:])
			if err != nil {
				return 0, fmt.Errorf("failed to write unshared bytes: %w", err)
			}
			if n != int(unsharedLen) {
				return 0, fmt.Errorf("wrote incomplete unshared bytes: %d of %d bytes", n, unsharedLen)
			}
		}

		// Write sequence number
		err := binary.Write(buffer, binary.LittleEndian, entry.SequenceNum)
		if err != nil {
			return 0, fmt.Errorf("failed to write sequence number: %w", err)
		}

		// Write value
		valueLen := uint32(len(entry.Value))
		err = binary.Write(buffer, binary.LittleEndian, valueLen)
		if err != nil {
			return 0, fmt.Errorf("failed to write value length: %w", err)
		}

		n, err := buffer.Write(entry.Value)
		if err != nil {
			return 0, fmt.Errorf("failed to write value: %w", err)
		}
		if n != len(entry.Value) {
			return 0, fmt.Errorf("wrote incomplete value: %d of %d bytes", n, len(entry.Value))
		}

		prevKey = entry.Key
		restartOffset++
	}

	// Write restart points
	for _, point := range b.restartPoints {
		binary.Write(buffer, binary.LittleEndian, point)
	}

	// Write number of restart points
	binary.Write(buffer, binary.LittleEndian, uint32(len(b.restartPoints)))

	// Calculate checksum
	data := buffer.Bytes()
	checksum := xxhash.Sum64(data)

	// Write checksum
	binary.Write(buffer, binary.LittleEndian, checksum)

	// Write the entire buffer to the output writer
	n, err := w.Write(buffer.Bytes())
	if err != nil {
		return 0, fmt.Errorf("failed to write block: %w", err)
	}

	if n != buffer.Len() {
		return 0, fmt.Errorf("wrote incomplete block: %d of %d bytes", n, buffer.Len())
	}

	return checksum, nil
}
