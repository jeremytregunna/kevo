package replication

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/KevoDB/kevo/pkg/wal"
)

var (
	// ErrInvalidChecksum indicates a checksum validation failure during deserialization
	ErrInvalidChecksum = errors.New("invalid checksum")

	// ErrInvalidFormat indicates an invalid format of serialized data
	ErrInvalidFormat = errors.New("invalid entry format")

	// ErrBufferTooSmall indicates the provided buffer is too small for serialization
	ErrBufferTooSmall = errors.New("buffer too small")
)

const (
	// Entry serialization constants
	entryHeaderSize = 17 // checksum(4) + timestamp(8) + type(1) + keylen(4)
	// Additional 4 bytes for value length when not a delete operation
)

// EntrySerializer handles serialization and deserialization of WAL entries
type EntrySerializer struct {
	// ChecksumEnabled controls whether checksums are calculated/verified
	ChecksumEnabled bool
}

// NewEntrySerializer creates a new entry serializer
func NewEntrySerializer() *EntrySerializer {
	return &EntrySerializer{
		ChecksumEnabled: true,
	}
}

// SerializeEntry converts a WAL entry to a byte slice
func (s *EntrySerializer) SerializeEntry(entry *wal.Entry) []byte {
	// Calculate total size needed
	totalSize := entryHeaderSize + len(entry.Key)
	if entry.Value != nil {
		totalSize += 4 + len(entry.Value) // vallen(4) + value
	}

	// Allocate buffer
	data := make([]byte, totalSize)
	offset := 4 // Skip first 4 bytes for checksum

	// Write timestamp
	binary.LittleEndian.PutUint64(data[offset:offset+8], entry.SequenceNumber)
	offset += 8

	// Write entry type
	data[offset] = entry.Type
	offset++

	// Write key length and key
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(len(entry.Key)))
	offset += 4
	copy(data[offset:], entry.Key)
	offset += len(entry.Key)

	// Write value length and value (if present)
	if entry.Value != nil {
		binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(len(entry.Value)))
		offset += 4
		copy(data[offset:], entry.Value)
	}

	// Calculate and store checksum if enabled
	if s.ChecksumEnabled {
		checksum := crc32.ChecksumIEEE(data[4:])
		binary.LittleEndian.PutUint32(data[0:4], checksum)
	}

	return data
}

// SerializeEntryToBuffer serializes a WAL entry to an existing buffer
// Returns the number of bytes written or an error if the buffer is too small
func (s *EntrySerializer) SerializeEntryToBuffer(entry *wal.Entry, buffer []byte) (int, error) {
	// Calculate total size needed
	totalSize := entryHeaderSize + len(entry.Key)
	if entry.Value != nil {
		totalSize += 4 + len(entry.Value) // vallen(4) + value
	}

	// Check if buffer is large enough
	if len(buffer) < totalSize {
		return 0, ErrBufferTooSmall
	}

	// Write to buffer
	offset := 4 // Skip first 4 bytes for checksum

	// Write timestamp
	binary.LittleEndian.PutUint64(buffer[offset:offset+8], entry.SequenceNumber)
	offset += 8

	// Write entry type
	buffer[offset] = entry.Type
	offset++

	// Write key length and key
	binary.LittleEndian.PutUint32(buffer[offset:offset+4], uint32(len(entry.Key)))
	offset += 4
	copy(buffer[offset:], entry.Key)
	offset += len(entry.Key)

	// Write value length and value (if present)
	if entry.Value != nil {
		binary.LittleEndian.PutUint32(buffer[offset:offset+4], uint32(len(entry.Value)))
		offset += 4
		copy(buffer[offset:], entry.Value)
		offset += len(entry.Value)
	}

	// Calculate and store checksum if enabled
	if s.ChecksumEnabled {
		checksum := crc32.ChecksumIEEE(buffer[4:offset])
		binary.LittleEndian.PutUint32(buffer[0:4], checksum)
	}

	return offset, nil
}

// DeserializeEntry converts a byte slice back to a WAL entry
func (s *EntrySerializer) DeserializeEntry(data []byte) (*wal.Entry, error) {
	// Validate minimum size
	if len(data) < entryHeaderSize {
		return nil, ErrInvalidFormat
	}

	// Verify checksum if enabled
	if s.ChecksumEnabled {
		storedChecksum := binary.LittleEndian.Uint32(data[0:4])
		calculatedChecksum := crc32.ChecksumIEEE(data[4:])
		if storedChecksum != calculatedChecksum {
			return nil, ErrInvalidChecksum
		}
	}

	offset := 4 // Skip checksum

	// Read timestamp
	timestamp := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Read entry type
	entryType := data[offset]
	offset++

	// Read key length and key
	keyLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Validate key length
	if offset+int(keyLen) > len(data) {
		return nil, ErrInvalidFormat
	}

	key := make([]byte, keyLen)
	copy(key, data[offset:offset+int(keyLen)])
	offset += int(keyLen)

	// Read value length and value if present
	var value []byte
	if offset < len(data) {
		// Only read value if there's more data
		if offset+4 > len(data) {
			return nil, ErrInvalidFormat
		}

		valueLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		// Validate value length
		if offset+int(valueLen) > len(data) {
			return nil, ErrInvalidFormat
		}

		value = make([]byte, valueLen)
		copy(value, data[offset:offset+int(valueLen)])
	}

	// Create and return the entry
	return &wal.Entry{
		SequenceNumber: timestamp,
		Type:           entryType,
		Key:            key,
		Value:          value,
	}, nil
}

// BatchSerializer handles serialization of WAL entry batches
type BatchSerializer struct {
	entrySerializer *EntrySerializer
}

// NewBatchSerializer creates a new batch serializer
func NewBatchSerializer() *BatchSerializer {
	return &BatchSerializer{
		entrySerializer: NewEntrySerializer(),
	}
}

// SerializeBatch converts a batch of WAL entries to a byte slice
func (s *BatchSerializer) SerializeBatch(entries []*wal.Entry) []byte {
	if len(entries) == 0 {
		// Empty batch - just return header with count 0
		result := make([]byte, 12) // checksum(4) + count(4) + timestamp(4)
		binary.LittleEndian.PutUint32(result[4:8], 0)
		
		// Calculate and store checksum
		checksum := crc32.ChecksumIEEE(result[4:])
		binary.LittleEndian.PutUint32(result[0:4], checksum)
		
		return result
	}

	// First pass: calculate total size needed
	var totalSize int = 12 // header: checksum(4) + count(4) + base timestamp(4)
	
	for _, entry := range entries {
		// For each entry: size(4) + serialized entry data
		entrySize := entryHeaderSize + len(entry.Key)
		if entry.Value != nil {
			entrySize += 4 + len(entry.Value)
		}
		
		totalSize += 4 + entrySize
	}
	
	// Allocate buffer
	result := make([]byte, totalSize)
	offset := 4 // Skip checksum for now
	
	// Write entry count
	binary.LittleEndian.PutUint32(result[offset:offset+4], uint32(len(entries)))
	offset += 4
	
	// Write base timestamp (from first entry)
	binary.LittleEndian.PutUint32(result[offset:offset+4], uint32(entries[0].SequenceNumber))
	offset += 4
	
	// Write each entry
	for _, entry := range entries {
		// Reserve space for entry size
		sizeOffset := offset
		offset += 4
		
		// Serialize entry directly into the buffer
		entrySize, err := s.entrySerializer.SerializeEntryToBuffer(entry, result[offset:])
		if err != nil {
			// This shouldn't happen since we pre-calculated the size,
			// but handle it gracefully just in case
			panic("buffer too small for entry serialization")
		}
		
		offset += entrySize
		
		// Write the actual entry size
		binary.LittleEndian.PutUint32(result[sizeOffset:sizeOffset+4], uint32(entrySize))
	}
	
	// Calculate and store checksum
	checksum := crc32.ChecksumIEEE(result[4:offset])
	binary.LittleEndian.PutUint32(result[0:4], checksum)
	
	return result
}

// DeserializeBatch converts a byte slice back to a batch of WAL entries
func (s *BatchSerializer) DeserializeBatch(data []byte) ([]*wal.Entry, error) {
	// Validate minimum size for batch header
	if len(data) < 12 {
		return nil, ErrInvalidFormat
	}
	
	// Verify checksum
	storedChecksum := binary.LittleEndian.Uint32(data[0:4])
	calculatedChecksum := crc32.ChecksumIEEE(data[4:])
	if storedChecksum != calculatedChecksum {
		return nil, ErrInvalidChecksum
	}
	
	offset := 4 // Skip checksum
	
	// Read entry count
	count := binary.LittleEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// Read base timestamp (we don't use this currently, but read past it)
	offset += 4 // Skip base timestamp
	
	// Early return for empty batch
	if count == 0 {
		return []*wal.Entry{}, nil
	}
	
	// Deserialize each entry
	entries := make([]*wal.Entry, count)
	for i := uint32(0); i < count; i++ {
		// Validate we have enough data for entry size
		if offset+4 > len(data) {
			return nil, ErrInvalidFormat
		}
		
		// Read entry size
		entrySize := binary.LittleEndian.Uint32(data[offset:offset+4])
		offset += 4
		
		// Validate entry size
		if offset+int(entrySize) > len(data) {
			return nil, ErrInvalidFormat
		}
		
		// Deserialize entry
		entry, err := s.entrySerializer.DeserializeEntry(data[offset:offset+int(entrySize)])
		if err != nil {
			return nil, err
		}
		
		entries[i] = entry
		offset += int(entrySize)
	}
	
	return entries, nil
}

// EstimateEntrySize estimates the serialized size of a WAL entry without actually serializing it
func EstimateEntrySize(entry *wal.Entry) int {
	size := entryHeaderSize + len(entry.Key)
	if entry.Value != nil {
		size += 4 + len(entry.Value)
	}
	return size
}

// EstimateBatchSize estimates the serialized size of a batch of WAL entries
func EstimateBatchSize(entries []*wal.Entry) int {
	if len(entries) == 0 {
		return 12 // Empty batch header
	}
	
	size := 12 // Batch header: checksum(4) + count(4) + base timestamp(4)
	
	for _, entry := range entries {
		entrySize := EstimateEntrySize(entry)
		size += 4 + entrySize // size field(4) + entry data
	}
	
	return size
}