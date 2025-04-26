package replication

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/KevoDB/kevo/pkg/wal"
)

func TestEntrySerializer(t *testing.T) {
	// Create a serializer
	serializer := NewEntrySerializer()

	// Test different entry types
	testCases := []struct {
		name  string
		entry *wal.Entry
	}{
		{
			name: "Put operation",
			entry: &wal.Entry{
				SequenceNumber: 123,
				Type:           wal.OpTypePut,
				Key:            []byte("test-key"),
				Value:          []byte("test-value"),
			},
		},
		{
			name: "Delete operation",
			entry: &wal.Entry{
				SequenceNumber: 456,
				Type:           wal.OpTypeDelete,
				Key:            []byte("deleted-key"),
				Value:          nil,
			},
		},
		{
			name: "Large entry",
			entry: &wal.Entry{
				SequenceNumber: 789,
				Type:           wal.OpTypePut,
				Key:            bytes.Repeat([]byte("K"), 1000),
				Value:          bytes.Repeat([]byte("V"), 1000),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Serialize the entry
			data := serializer.SerializeEntry(tc.entry)

			// Deserialize back
			result, err := serializer.DeserializeEntry(data)
			if err != nil {
				t.Fatalf("Error deserializing entry: %v", err)
			}

			// Compare entries
			if result.SequenceNumber != tc.entry.SequenceNumber {
				t.Errorf("Expected sequence number %d, got %d",
					tc.entry.SequenceNumber, result.SequenceNumber)
			}

			if result.Type != tc.entry.Type {
				t.Errorf("Expected type %d, got %d", tc.entry.Type, result.Type)
			}

			if !bytes.Equal(result.Key, tc.entry.Key) {
				t.Errorf("Expected key %q, got %q", tc.entry.Key, result.Key)
			}

			if !bytes.Equal(result.Value, tc.entry.Value) {
				t.Errorf("Expected value %q, got %q", tc.entry.Value, result.Value)
			}
		})
	}
}

func TestEntrySerializerChecksum(t *testing.T) {
	// Create a serializer with checksums enabled
	serializer := NewEntrySerializer()
	serializer.ChecksumEnabled = true

	// Create a test entry
	entry := &wal.Entry{
		SequenceNumber: 123,
		Type:           wal.OpTypePut,
		Key:            []byte("test-key"),
		Value:          []byte("test-value"),
	}

	// Serialize the entry
	data := serializer.SerializeEntry(entry)

	// Corrupt the data
	data[10]++

	// Try to deserialize - should fail with checksum error
	_, err := serializer.DeserializeEntry(data)
	if err != ErrInvalidChecksum {
		t.Errorf("Expected checksum error, got %v", err)
	}

	// Now disable checksum verification and try again
	serializer.ChecksumEnabled = false
	result, err := serializer.DeserializeEntry(data)
	if err != nil {
		t.Errorf("Expected no error with checksums disabled, got %v", err)
	}
	if result == nil {
		t.Fatal("Expected entry to be returned with checksums disabled")
	}
}

func TestEntrySerializerInvalidFormat(t *testing.T) {
	serializer := NewEntrySerializer()

	// Test with empty data
	_, err := serializer.DeserializeEntry([]byte{})
	if err != ErrInvalidFormat {
		t.Errorf("Expected format error for empty data, got %v", err)
	}

	// Test with insufficient data
	_, err = serializer.DeserializeEntry(make([]byte, 10))
	if err != ErrInvalidFormat {
		t.Errorf("Expected format error for insufficient data, got %v", err)
	}

	// Test with invalid key length
	data := make([]byte, entryHeaderSize+4)
	offset := 4
	binary.LittleEndian.PutUint64(data[offset:offset+8], 123) // timestamp
	offset += 8
	data[offset] = wal.OpTypePut // type
	offset++
	binary.LittleEndian.PutUint32(data[offset:offset+4], 1000) // key length (too large)

	// Calculate a valid checksum for this data
	checksum := crc32.ChecksumIEEE(data[4:])
	binary.LittleEndian.PutUint32(data[0:4], checksum)

	_, err = serializer.DeserializeEntry(data)
	if err != ErrInvalidFormat {
		t.Errorf("Expected format error for invalid key length, got %v", err)
	}
}

func TestBatchSerializer(t *testing.T) {
	// Create batch serializer
	serializer := NewBatchSerializer()

	// Test batch with multiple entries
	entries := []*wal.Entry{
		{
			SequenceNumber: 101,
			Type:           wal.OpTypePut,
			Key:            []byte("key1"),
			Value:          []byte("value1"),
		},
		{
			SequenceNumber: 102,
			Type:           wal.OpTypeDelete,
			Key:            []byte("key2"),
			Value:          nil,
		},
		{
			SequenceNumber: 103,
			Type:           wal.OpTypePut,
			Key:            []byte("key3"),
			Value:          []byte("value3"),
		},
	}

	// Serialize batch
	data := serializer.SerializeBatch(entries)

	// Deserialize batch
	result, err := serializer.DeserializeBatch(data)
	if err != nil {
		t.Fatalf("Error deserializing batch: %v", err)
	}

	// Verify batch
	if len(result) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(result))
	}

	for i, entry := range entries {
		if result[i].SequenceNumber != entry.SequenceNumber {
			t.Errorf("Entry %d: Expected sequence number %d, got %d",
				i, entry.SequenceNumber, result[i].SequenceNumber)
		}
		if result[i].Type != entry.Type {
			t.Errorf("Entry %d: Expected type %d, got %d",
				i, entry.Type, result[i].Type)
		}
		if !bytes.Equal(result[i].Key, entry.Key) {
			t.Errorf("Entry %d: Expected key %q, got %q",
				i, entry.Key, result[i].Key)
		}
		if !bytes.Equal(result[i].Value, entry.Value) {
			t.Errorf("Entry %d: Expected value %q, got %q",
				i, entry.Value, result[i].Value)
		}
	}
}

func TestEmptyBatchSerialization(t *testing.T) {
	// Create batch serializer
	serializer := NewBatchSerializer()

	// Test empty batch
	entries := []*wal.Entry{}

	// Serialize batch
	data := serializer.SerializeBatch(entries)

	// Deserialize batch
	result, err := serializer.DeserializeBatch(data)
	if err != nil {
		t.Fatalf("Error deserializing empty batch: %v", err)
	}

	// Verify result is empty
	if len(result) != 0 {
		t.Errorf("Expected empty batch, got %d entries", len(result))
	}
}

func TestBatchSerializerChecksum(t *testing.T) {
	// Create batch serializer
	serializer := NewBatchSerializer()

	// Test batch with single entry
	entries := []*wal.Entry{
		{
			SequenceNumber: 101,
			Type:           wal.OpTypePut,
			Key:            []byte("key1"),
			Value:          []byte("value1"),
		},
	}

	// Serialize batch
	data := serializer.SerializeBatch(entries)

	// Corrupt data
	data[8]++

	// Attempt to deserialize - should fail
	_, err := serializer.DeserializeBatch(data)
	if err != ErrInvalidChecksum {
		t.Errorf("Expected checksum error for corrupted batch, got %v", err)
	}
}

func TestBatchSerializerInvalidFormat(t *testing.T) {
	serializer := NewBatchSerializer()

	// Test with empty data
	_, err := serializer.DeserializeBatch([]byte{})
	if err != ErrInvalidFormat {
		t.Errorf("Expected format error for empty data, got %v", err)
	}

	// Test with insufficient data
	_, err = serializer.DeserializeBatch(make([]byte, 10))
	if err != ErrInvalidFormat {
		t.Errorf("Expected format error for insufficient data, got %v", err)
	}
}

func TestEstimateEntrySize(t *testing.T) {
	// Test entries with different sizes
	testCases := []struct {
		name     string
		entry    *wal.Entry
		expected int
	}{
		{
			name: "Basic put entry",
			entry: &wal.Entry{
				SequenceNumber: 101,
				Type:           wal.OpTypePut,
				Key:            []byte("key"),
				Value:          []byte("value"),
			},
			expected: entryHeaderSize + 3 + 4 + 5, // header + key_len + value_len + value
		},
		{
			name: "Delete entry (no value)",
			entry: &wal.Entry{
				SequenceNumber: 102,
				Type:           wal.OpTypeDelete,
				Key:            []byte("delete-key"),
				Value:          nil,
			},
			expected: entryHeaderSize + 10, // header + key_len
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			size := EstimateEntrySize(tc.entry)
			if size != tc.expected {
				t.Errorf("Expected size %d, got %d", tc.expected, size)
			}

			// Verify estimate matches actual size
			serializer := NewEntrySerializer()
			data := serializer.SerializeEntry(tc.entry)
			if len(data) != size {
				t.Errorf("Estimated size %d doesn't match actual size %d",
					size, len(data))
			}
		})
	}
}

func TestEstimateBatchSize(t *testing.T) {
	// Test batches with different contents
	testCases := []struct {
		name     string
		entries  []*wal.Entry
		expected int
	}{
		{
			name:     "Empty batch",
			entries:  []*wal.Entry{},
			expected: 12, // Just batch header
		},
		{
			name: "Batch with one entry",
			entries: []*wal.Entry{
				{
					SequenceNumber: 101,
					Type:           wal.OpTypePut,
					Key:            []byte("key1"),
					Value:          []byte("value1"),
				},
			},
			expected: 12 + 4 + entryHeaderSize + 4 + 4 + 6, // batch header + entry size field + entry header + key + value size + value
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			size := EstimateBatchSize(tc.entries)
			if size != tc.expected {
				t.Errorf("Expected size %d, got %d", tc.expected, size)
			}

			// Verify estimate matches actual size
			serializer := NewBatchSerializer()
			data := serializer.SerializeBatch(tc.entries)
			if len(data) != size {
				t.Errorf("Estimated size %d doesn't match actual size %d",
					size, len(data))
			}
		})
	}
}

func TestSerializeToBuffer(t *testing.T) {
	serializer := NewEntrySerializer()

	// Create a test entry
	entry := &wal.Entry{
		SequenceNumber: 101,
		Type:           wal.OpTypePut,
		Key:            []byte("key1"),
		Value:          []byte("value1"),
	}

	// Estimate the size
	estimatedSize := EstimateEntrySize(entry)

	// Create a buffer of the estimated size
	buffer := make([]byte, estimatedSize)

	// Serialize to buffer
	n, err := serializer.SerializeEntryToBuffer(entry, buffer)
	if err != nil {
		t.Fatalf("Error serializing to buffer: %v", err)
	}

	// Check bytes written
	if n != estimatedSize {
		t.Errorf("Expected %d bytes written, got %d", estimatedSize, n)
	}

	// Verify by deserializing
	result, err := serializer.DeserializeEntry(buffer)
	if err != nil {
		t.Fatalf("Error deserializing from buffer: %v", err)
	}

	// Check result
	if result.SequenceNumber != entry.SequenceNumber {
		t.Errorf("Expected sequence number %d, got %d",
			entry.SequenceNumber, result.SequenceNumber)
	}
	if !bytes.Equal(result.Key, entry.Key) {
		t.Errorf("Expected key %q, got %q", entry.Key, result.Key)
	}
	if !bytes.Equal(result.Value, entry.Value) {
		t.Errorf("Expected value %q, got %q", entry.Value, result.Value)
	}

	// Test with too small buffer
	smallBuffer := make([]byte, estimatedSize-1)
	_, err = serializer.SerializeEntryToBuffer(entry, smallBuffer)
	if err != ErrBufferTooSmall {
		t.Errorf("Expected buffer too small error, got %v", err)
	}
}
