package replication

import (
	"fmt"
	"time"

	"github.com/KevoDB/kevo/pkg/wal"
	replication_proto "github.com/KevoDB/kevo/proto/kevo/replication"
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
	// Log the entry being serialized
	fmt.Printf("Serializing WAL entry: seq=%d, type=%d, key=%v\n",
		entry.SequenceNumber, entry.Type, string(entry.Key))

	// Create a buffer with appropriate size
	entrySize := 1 + 8 + 4 + len(entry.Key) // type + seq + keylen + key

	// Include value for Put, Merge, and Batch operations (but not Delete)
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

	// Write value length and value (for all types except delete)
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

	// Debug: show the first few bytes of the serialized entry
	hexBytes := ""
	for i, b := range payload {
		if i < 20 {
			hexBytes += fmt.Sprintf("%02x ", b)
		}
	}
	fmt.Printf("Serialized %d bytes, first 20: %s\n", len(payload), hexBytes)

	return payload, nil
}

// DeserializeWALEntry converts a binary payload back to a WAL entry
func DeserializeWALEntry(payload []byte) (*wal.Entry, error) {
	if len(payload) < 13 { // Minimum size: type(1) + seq(8) + keylen(4)
		return nil, fmt.Errorf("payload too small: %d bytes", len(payload))
	}

	fmt.Printf("Deserializing WAL entry with %d bytes\n", len(payload))

	// Debugging: show the first 32 bytes in hex for troubleshooting
	hexBytes := ""
	for i, b := range payload {
		if i < 32 {
			hexBytes += fmt.Sprintf("%02x ", b)
		}
	}
	fmt.Printf("Payload first 32 bytes: %s\n", hexBytes)

	offset := 0

	// Read operation type
	opType := payload[offset]
	fmt.Printf("Entry operation type: %d\n", opType)
	offset++

	// Check for supported batch operation
	if opType == wal.OpTypeBatch {
		fmt.Printf("Found batch operation (type 4), which is supported\n")
	}

	// Validate operation type
	// Fix: Add support for OpTypeBatch (4)
	if opType != wal.OpTypePut && opType != wal.OpTypeDelete &&
		opType != wal.OpTypeMerge && opType != wal.OpTypeBatch {
		return nil, fmt.Errorf("invalid operation type: %d", opType)
	}

	// Read sequence number (8 bytes)
	var seqNum uint64
	for i := 0; i < 8; i++ {
		seqNum |= uint64(payload[offset+i]) << (i * 8)
	}
	offset += 8
	fmt.Printf("Sequence number: %d\n", seqNum)

	// Read key length (4 bytes)
	var keyLen uint32
	for i := 0; i < 4; i++ {
		keyLen |= uint32(payload[offset+i]) << (i * 8)
	}
	offset += 4
	fmt.Printf("Key length: %d bytes\n", keyLen)

	// Validate key length
	if keyLen > 1024*1024 { // Sanity check - keys shouldn't be more than 1MB
		return nil, fmt.Errorf("key length too large: %d bytes", keyLen)
	}

	if offset+int(keyLen) > len(payload) {
		return nil, fmt.Errorf("invalid key length: %d, would exceed payload size", keyLen)
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

	// Show key as string if it's likely printable
	isPrintable := true
	for _, b := range key {
		if b < 32 || b > 126 {
			isPrintable = false
			break
		}
	}

	if isPrintable {
		fmt.Printf("Key as string: %s\n", string(key))
	} else {
		fmt.Printf("Key contains non-printable characters\n")
	}

	// Read value for non-delete operations
	if opType != wal.OpTypeDelete {
		// Make sure we have at least 4 bytes for value length
		if offset+4 > len(payload) {
			return nil, fmt.Errorf("payload too small for value length, offset=%d, remaining=%d",
				offset, len(payload)-offset)
		}

		// Read value length (4 bytes)
		var valLen uint32
		for i := 0; i < 4; i++ {
			valLen |= uint32(payload[offset+i]) << (i * 8)
		}
		offset += 4
		fmt.Printf("Value length: %d bytes\n", valLen)

		// Validate value length
		if valLen > 10*1024*1024 { // Sanity check - values shouldn't be more than 10MB
			return nil, fmt.Errorf("value length too large: %d bytes", valLen)
		}

		if offset+int(valLen) > len(payload) {
			return nil, fmt.Errorf("invalid value length: %d, would exceed payload size", valLen)
		}

		// Read value
		value := make([]byte, valLen)
		copy(value, payload[offset:offset+int(valLen)])
		offset += int(valLen)

		entry.Value = value

		// Check if we have unprocessed bytes
		if offset < len(payload) {
			fmt.Printf("Warning: %d unprocessed bytes in payload\n", len(payload)-offset)
		}
	}

	fmt.Printf("Successfully deserialized WAL entry with sequence %d\n", seqNum)
	return entry, nil
}

// ReplicationError represents an error in the replication system
type ReplicationError struct {
	Code     ErrorCode
	Message  string
	Time     time.Time
	Sequence uint64
	Cause    error
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

	// ErrorDeserialization represents an error deserializing WAL entries
	ErrorDeserialization

	// ErrorApplication represents an error applying WAL entries
	ErrorApplication
)

// Error implements the error interface
func (e *ReplicationError) Error() string {
	if e.Sequence > 0 {
		return fmt.Sprintf("%s: %s at sequence %d (at %s)",
			e.Code, e.Message, e.Sequence, e.Time.Format(time.RFC3339))
	}
	return fmt.Sprintf("%s: %s (at %s)", e.Code, e.Message, e.Time.Format(time.RFC3339))
}

// Unwrap returns the underlying cause
func (e *ReplicationError) Unwrap() error {
	return e.Cause
}

// NewReplicationError creates a new replication error
func NewReplicationError(code ErrorCode, message string) *ReplicationError {
	return &ReplicationError{
		Code:    code,
		Message: message,
		Time:    time.Now(),
	}
}

// WithCause adds a cause to the error
func (e *ReplicationError) WithCause(cause error) *ReplicationError {
	e.Cause = cause
	return e
}

// WithSequence adds a sequence number to the error
func (e *ReplicationError) WithSequence(seq uint64) *ReplicationError {
	e.Sequence = seq
	return e
}

// NewSequenceGapError creates a new sequence gap error
func NewSequenceGapError(expected, actual uint64) *ReplicationError {
	return &ReplicationError{
		Code:     ErrorSequenceGap,
		Message:  fmt.Sprintf("sequence gap: expected %d, got %d", expected, actual),
		Time:     time.Now(),
		Sequence: actual,
	}
}

// NewDeserializationError creates a new deserialization error
func NewDeserializationError(seq uint64, cause error) *ReplicationError {
	return &ReplicationError{
		Code:     ErrorDeserialization,
		Message:  "failed to deserialize entry",
		Time:     time.Now(),
		Sequence: seq,
		Cause:    cause,
	}
}

// NewApplicationError creates a new application error
func NewApplicationError(seq uint64, cause error) *ReplicationError {
	return &ReplicationError{
		Code:     ErrorApplication,
		Message:  "failed to apply entry",
		Time:     time.Now(),
		Sequence: seq,
		Cause:    cause,
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
	case ErrorDeserialization:
		return "DESERIALIZATION"
	case ErrorApplication:
		return "APPLICATION"
	default:
		return fmt.Sprintf("ERROR(%d)", c)
	}
}
