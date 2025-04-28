package replication

import (
	"fmt"
	"time"

	replication_proto "github.com/KevoDB/kevo/pkg/replication/proto"
	"github.com/KevoDB/kevo/pkg/wal"
)

// WALEntriesBuffer is a buffer for accumulating WAL entries to be sent in batches
type WALEntriesBuffer struct {
	entries     []*replication_proto.WALEntry
	sizeBytes   int
	maxSizeKB   int
	compression replication_proto.CompressionCodec
}

// NewWALEntriesBuffer creates a new buffer for WAL entries with the specified maximum size
func NewWALEntriesBuffer(maxSizeKB int, compression replication_proto.CompressionCodec) *WALEntriesBuffer {
	return &WALEntriesBuffer{
		entries:     make([]*replication_proto.WALEntry, 0),
		sizeBytes:   0,
		maxSizeKB:   maxSizeKB,
		compression: compression,
	}
}

// Add adds a new entry to the buffer
func (b *WALEntriesBuffer) Add(entry *replication_proto.WALEntry) bool {
	entrySize := len(entry.Payload)

	// Check if adding this entry would exceed the buffer size
	// If the buffer is empty, we always accept at least one entry
	// Otherwise, we check if adding this entry would exceed the limit
	if len(b.entries) > 0 && b.sizeBytes+entrySize > b.maxSizeKB*1024 {
		return false
	}

	b.entries = append(b.entries, entry)
	b.sizeBytes += entrySize
	return true
}

// Clear removes all entries from the buffer
func (b *WALEntriesBuffer) Clear() {
	b.entries = make([]*replication_proto.WALEntry, 0)
	b.sizeBytes = 0
}

// Entries returns the current entries in the buffer
func (b *WALEntriesBuffer) Entries() []*replication_proto.WALEntry {
	return b.entries
}

// Size returns the current size of the buffer in bytes
func (b *WALEntriesBuffer) Size() int {
	return b.sizeBytes
}

// Count returns the number of entries in the buffer
func (b *WALEntriesBuffer) Count() int {
	return len(b.entries)
}

// CreateResponse creates a WALStreamResponse from the current buffer
func (b *WALEntriesBuffer) CreateResponse() *replication_proto.WALStreamResponse {
	return &replication_proto.WALStreamResponse{
		Entries:    b.entries,
		Compressed: b.compression != replication_proto.CompressionCodec_NONE,
		Codec:      b.compression,
	}
}

// WALEntryToProto converts a WAL entry to a protocol buffer WAL entry
func WALEntryToProto(entry *wal.Entry, fragmentType replication_proto.FragmentType) (*replication_proto.WALEntry, error) {
	// Serialize the WAL entry
	payload, err := SerializeWALEntry(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize WAL entry: %w", err)
	}

	// Create the protocol buffer entry
	protoEntry := &replication_proto.WALEntry{
		SequenceNumber: entry.SequenceNumber,
		Payload:        payload,
		FragmentType:   fragmentType,
		// Calculate checksum (optional, could be done at a higher level)
		// Checksum:       crc32.ChecksumIEEE(payload),
	}

	return protoEntry, nil
}

// SerializeWALEntry converts a WAL entry to its binary representation
func SerializeWALEntry(entry *wal.Entry) ([]byte, error) {
	// This is a simple implementation that can be enhanced
	// with more efficient binary serialization if needed

	// Create a buffer with appropriate size
	entrySize := 1 + 8 + 4 + len(entry.Key) // type + seq + keylen + key
	if entry.Type != wal.OpTypeDelete {
		entrySize += 4 + len(entry.Value) // vallen + value
	}

	payload := make([]byte, entrySize)
	offset := 0

	// Write operation type
	payload[offset] = entry.Type
	offset++

	// Write sequence number (8 bytes)
	for i := 0; i < 8; i++ {
		payload[offset+i] = byte(entry.SequenceNumber >> (i * 8))
	}
	offset += 8

	// Write key length (4 bytes)
	keyLen := uint32(len(entry.Key))
	for i := 0; i < 4; i++ {
		payload[offset+i] = byte(keyLen >> (i * 8))
	}
	offset += 4

	// Write key
	copy(payload[offset:], entry.Key)
	offset += len(entry.Key)

	// Write value length and value (if not a delete)
	if entry.Type != wal.OpTypeDelete {
		// Write value length (4 bytes)
		valLen := uint32(len(entry.Value))
		for i := 0; i < 4; i++ {
			payload[offset+i] = byte(valLen >> (i * 8))
		}
		offset += 4

		// Write value
		copy(payload[offset:], entry.Value)
	}

	return payload, nil
}

// DeserializeWALEntry converts a binary payload back to a WAL entry
func DeserializeWALEntry(payload []byte) (*wal.Entry, error) {
	if len(payload) < 13 { // Minimum size: type(1) + seq(8) + keylen(4)
		return nil, fmt.Errorf("payload too small: %d bytes", len(payload))
	}

	offset := 0

	// Read operation type
	opType := payload[offset]
	offset++

	// Validate operation type
	if opType != wal.OpTypePut && opType != wal.OpTypeDelete && opType != wal.OpTypeMerge {
		return nil, fmt.Errorf("invalid operation type: %d", opType)
	}

	// Read sequence number (8 bytes)
	var seqNum uint64
	for i := 0; i < 8; i++ {
		seqNum |= uint64(payload[offset+i]) << (i * 8)
	}
	offset += 8

	// Read key length (4 bytes)
	var keyLen uint32
	for i := 0; i < 4; i++ {
		keyLen |= uint32(payload[offset+i]) << (i * 8)
	}
	offset += 4

	// Validate key length
	if offset+int(keyLen) > len(payload) {
		return nil, fmt.Errorf("invalid key length: %d", keyLen)
	}

	// Read key
	key := make([]byte, keyLen)
	copy(key, payload[offset:offset+int(keyLen)])
	offset += int(keyLen)

	// Create entry with default nil value
	entry := &wal.Entry{
		SequenceNumber: seqNum,
		Type:           opType,
		Key:            key,
		Value:          nil,
	}

	// Read value for non-delete operations
	if opType != wal.OpTypeDelete {
		// Make sure we have at least 4 bytes for value length
		if offset+4 > len(payload) {
			return nil, fmt.Errorf("payload too small for value length")
		}

		// Read value length (4 bytes)
		var valLen uint32
		for i := 0; i < 4; i++ {
			valLen |= uint32(payload[offset+i]) << (i * 8)
		}
		offset += 4

		// Validate value length
		if offset+int(valLen) > len(payload) {
			return nil, fmt.Errorf("invalid value length: %d", valLen)
		}

		// Read value
		value := make([]byte, valLen)
		copy(value, payload[offset:offset+int(valLen)])

		entry.Value = value
	}

	return entry, nil
}

// ReplicationError represents an error in the replication system
type ReplicationError struct {
	Code    ErrorCode
	Message string
	Time    time.Time
}

// ErrorCode defines the types of errors that can occur in replication
type ErrorCode int

const (
	// ErrorUnknown is used for unclassified errors
	ErrorUnknown ErrorCode = iota

	// ErrorConnection indicates a network connection issue
	ErrorConnection

	// ErrorProtocol indicates a protocol violation
	ErrorProtocol

	// ErrorSequenceGap indicates a gap in the WAL sequence
	ErrorSequenceGap

	// ErrorCompression indicates an error with compression/decompression
	ErrorCompression

	// ErrorAuthentication indicates an authentication failure
	ErrorAuthentication

	// ErrorRetention indicates a WAL retention issue (requested WAL no longer available)
	ErrorRetention
)

// Error implements the error interface
func (e *ReplicationError) Error() string {
	return fmt.Sprintf("%s: %s (at %s)", e.Code, e.Message, e.Time.Format(time.RFC3339))
}

// NewReplicationError creates a new replication error
func NewReplicationError(code ErrorCode, message string) *ReplicationError {
	return &ReplicationError{
		Code:    code,
		Message: message,
		Time:    time.Now(),
	}
}

// String returns a string representation of the error code
func (c ErrorCode) String() string {
	switch c {
	case ErrorUnknown:
		return "UNKNOWN"
	case ErrorConnection:
		return "CONNECTION"
	case ErrorProtocol:
		return "PROTOCOL"
	case ErrorSequenceGap:
		return "SEQUENCE_GAP"
	case ErrorCompression:
		return "COMPRESSION"
	case ErrorAuthentication:
		return "AUTHENTICATION"
	case ErrorRetention:
		return "RETENTION"
	default:
		return fmt.Sprintf("ERROR(%d)", c)
	}
}
