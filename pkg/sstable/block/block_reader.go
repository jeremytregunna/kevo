package block

import (
	"encoding/binary"
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// Reader provides methods to read data from a serialized block
type Reader struct {
	data          []byte
	restartPoints []uint32
	numRestarts   uint32
	checksum      uint64
}

// NewReader creates a new block reader
func NewReader(data []byte) (*Reader, error) {
	if len(data) < BlockFooterSize {
		return nil, fmt.Errorf("block data too small: %d bytes", len(data))
	}

	// Read footer
	footerOffset := len(data) - BlockFooterSize
	numRestarts := binary.LittleEndian.Uint32(data[footerOffset : footerOffset+4])
	checksum := binary.LittleEndian.Uint64(data[footerOffset+4:])

	// Verify checksum - the checksum covers everything except the checksum itself
	computedChecksum := xxhash.Sum64(data[:len(data)-8])
	if computedChecksum != checksum {
		return nil, fmt.Errorf("block checksum mismatch: expected %d, got %d",
			checksum, computedChecksum)
	}

	// Read restart points
	restartOffset := footerOffset - int(numRestarts)*4
	if restartOffset < 0 {
		return nil, fmt.Errorf("invalid restart points offset")
	}

	restartPoints := make([]uint32, numRestarts)
	for i := uint32(0); i < numRestarts; i++ {
		restartPoints[i] = binary.LittleEndian.Uint32(
			data[restartOffset+int(i)*4:])
	}

	reader := &Reader{
		data:          data,
		restartPoints: restartPoints,
		numRestarts:   numRestarts,
		checksum:      checksum,
	}

	return reader, nil
}

// Iterator returns an iterator for the block
func (r *Reader) Iterator() *Iterator {
	// Calculate the data end position (everything before the restart points array)
	dataEnd := len(r.data) - BlockFooterSize - 4*len(r.restartPoints)

	return &Iterator{
		reader:      r,
		currentPos:  0,
		currentKey:  nil,
		currentVal:  nil,
		restartIdx:  0,
		initialized: false,
		dataEnd:     uint32(dataEnd),
	}
}
