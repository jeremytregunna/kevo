package footer

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	// FooterSize is the fixed size of the footer in bytes
	FooterSize = 52
	// FooterMagic is a magic number to verify we're reading a valid footer
	FooterMagic = uint64(0xFACEFEEDFACEFEED)
	// CurrentVersion is the current file format version
	CurrentVersion = uint32(1)
)

// Footer contains metadata for an SSTable file
type Footer struct {
	// Magic number for integrity checking
	Magic uint64
	// Version of the file format
	Version uint32
	// Timestamp of when the file was created
	Timestamp int64
	// Offset where the index block starts
	IndexOffset uint64
	// Size of the index block in bytes
	IndexSize uint32
	// Total number of key/value pairs
	NumEntries uint32
	// Smallest key in the file
	MinKeyOffset uint32
	// Largest key in the file
	MaxKeyOffset uint32
	// Checksum of all footer fields excluding the checksum itself
	Checksum uint64
}

// NewFooter creates a new footer with the given parameters
func NewFooter(indexOffset uint64, indexSize uint32, numEntries uint32,
	minKeyOffset, maxKeyOffset uint32) *Footer {

	return &Footer{
		Magic:        FooterMagic,
		Version:      CurrentVersion,
		Timestamp:    time.Now().UnixNano(),
		IndexOffset:  indexOffset,
		IndexSize:    indexSize,
		NumEntries:   numEntries,
		MinKeyOffset: minKeyOffset,
		MaxKeyOffset: maxKeyOffset,
		Checksum:     0, // Will be calculated during serialization
	}
}

// Encode serializes the footer to a byte slice
func (f *Footer) Encode() []byte {
	result := make([]byte, FooterSize)

	// Encode all fields directly into the buffer
	binary.LittleEndian.PutUint64(result[0:8], f.Magic)
	binary.LittleEndian.PutUint32(result[8:12], f.Version)
	binary.LittleEndian.PutUint64(result[12:20], uint64(f.Timestamp))
	binary.LittleEndian.PutUint64(result[20:28], f.IndexOffset)
	binary.LittleEndian.PutUint32(result[28:32], f.IndexSize)
	binary.LittleEndian.PutUint32(result[32:36], f.NumEntries)
	binary.LittleEndian.PutUint32(result[36:40], f.MinKeyOffset)
	binary.LittleEndian.PutUint32(result[40:44], f.MaxKeyOffset)

	// Calculate checksum of all fields excluding the checksum itself
	f.Checksum = xxhash.Sum64(result[:44])
	binary.LittleEndian.PutUint64(result[44:], f.Checksum)

	return result
}

// WriteTo writes the footer to an io.Writer
func (f *Footer) WriteTo(w io.Writer) (int64, error) {
	data := f.Encode()
	n, err := w.Write(data)
	return int64(n), err
}

// Decode parses a footer from a byte slice
func Decode(data []byte) (*Footer, error) {
	if len(data) < FooterSize {
		return nil, fmt.Errorf("footer data too small: %d bytes, expected %d",
			len(data), FooterSize)
	}

	footer := &Footer{
		Magic:        binary.LittleEndian.Uint64(data[0:8]),
		Version:      binary.LittleEndian.Uint32(data[8:12]),
		Timestamp:    int64(binary.LittleEndian.Uint64(data[12:20])),
		IndexOffset:  binary.LittleEndian.Uint64(data[20:28]),
		IndexSize:    binary.LittleEndian.Uint32(data[28:32]),
		NumEntries:   binary.LittleEndian.Uint32(data[32:36]),
		MinKeyOffset: binary.LittleEndian.Uint32(data[36:40]),
		MaxKeyOffset: binary.LittleEndian.Uint32(data[40:44]),
		Checksum:     binary.LittleEndian.Uint64(data[44:]),
	}

	// Verify magic number
	if footer.Magic != FooterMagic {
		return nil, fmt.Errorf("invalid footer magic: %x, expected %x",
			footer.Magic, FooterMagic)
	}

	// Verify checksum
	expectedChecksum := xxhash.Sum64(data[:44])
	if footer.Checksum != expectedChecksum {
		return nil, fmt.Errorf("footer checksum mismatch: file has %d, calculated %d",
			footer.Checksum, expectedChecksum)
	}

	return footer, nil
}
