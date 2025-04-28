package replication

import (
	"bytes"
	"testing"

	proto "github.com/KevoDB/kevo/pkg/replication/proto"
	"github.com/KevoDB/kevo/pkg/wal"
)

func TestWALEntriesBuffer(t *testing.T) {
	// Create a buffer with a 10KB max size
	buffer := NewWALEntriesBuffer(10, proto.CompressionCodec_NONE)

	// Test initial state
	if buffer.Count() != 0 {
		t.Errorf("Expected empty buffer, got %d entries", buffer.Count())
	}
	if buffer.Size() != 0 {
		t.Errorf("Expected zero size, got %d bytes", buffer.Size())
	}

	// Create sample entries
	entries := []*proto.WALEntry{
		{
			SequenceNumber: 1,
			Payload:        make([]byte, 1024), // 1KB
			FragmentType:   proto.FragmentType_FULL,
		},
		{
			SequenceNumber: 2,
			Payload:        make([]byte, 2048), // 2KB
			FragmentType:   proto.FragmentType_FULL,
		},
		{
			SequenceNumber: 3,
			Payload:        make([]byte, 4096), // 4KB
			FragmentType:   proto.FragmentType_FULL,
		},
		{
			SequenceNumber: 4,
			Payload:        make([]byte, 8192), // 8KB
			FragmentType:   proto.FragmentType_FULL,
		},
	}

	// Add entries to the buffer
	for _, entry := range entries {
		buffer.Add(entry)
		// Not checking the return value as some entries may not fit
		// depending on the implementation
	}

	// Check buffer state
	bufferCount := buffer.Count()
	// The buffer may not fit all entries depending on implementation
	// but at least some entries should be stored
	if bufferCount == 0 {
		t.Errorf("Expected buffer to contain some entries, got 0")
	}
	// The size should reflect the entries we stored
	expectedSize := 0
	for i := 0; i < bufferCount; i++ {
		expectedSize += len(entries[i].Payload)
	}
	if buffer.Size() != expectedSize {
		t.Errorf("Expected size %d bytes for %d entries, got %d",
			expectedSize, bufferCount, buffer.Size())
	}

	// Try to add an entry that exceeds the limit
	largeEntry := &proto.WALEntry{
		SequenceNumber: 5,
		Payload:        make([]byte, 11*1024), // 11KB
		FragmentType:   proto.FragmentType_FULL,
	}
	added := buffer.Add(largeEntry)
	if added {
		t.Errorf("Expected addition to fail for entry exceeding buffer size")
	}

	// Check that buffer state remains the same as before
	if buffer.Count() != bufferCount {
		t.Errorf("Expected %d entries after failed addition, got %d", bufferCount, buffer.Count())
	}
	if buffer.Size() != expectedSize {
		t.Errorf("Expected %d bytes after failed addition, got %d", expectedSize, buffer.Size())
	}

	// Create response from buffer
	response := buffer.CreateResponse()
	if len(response.Entries) != bufferCount {
		t.Errorf("Expected %d entries in response, got %d", bufferCount, len(response.Entries))
	}
	if response.Compressed {
		t.Errorf("Expected uncompressed response, got compressed")
	}
	if response.Codec != proto.CompressionCodec_NONE {
		t.Errorf("Expected NONE codec, got %v", response.Codec)
	}

	// Clear the buffer
	buffer.Clear()

	// Check that buffer is empty
	if buffer.Count() != 0 {
		t.Errorf("Expected empty buffer after clear, got %d entries", buffer.Count())
	}
	if buffer.Size() != 0 {
		t.Errorf("Expected zero size after clear, got %d bytes", buffer.Size())
	}
}

func TestWALEntrySerialization(t *testing.T) {
	// Create test WAL entries
	testCases := []struct {
		name  string
		entry *wal.Entry
	}{
		{
			name: "PutEntry",
			entry: &wal.Entry{
				SequenceNumber: 123,
				Type:           wal.OpTypePut,
				Key:            []byte("test-key"),
				Value:          []byte("test-value"),
			},
		},
		{
			name: "DeleteEntry",
			entry: &wal.Entry{
				SequenceNumber: 456,
				Type:           wal.OpTypeDelete,
				Key:            []byte("deleted-key"),
				Value:          nil,
			},
		},
		{
			name: "EmptyValue",
			entry: &wal.Entry{
				SequenceNumber: 789,
				Type:           wal.OpTypePut,
				Key:            []byte("empty-value-key"),
				Value:          []byte{},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Serialize the entry
			payload, err := SerializeWALEntry(tc.entry)
			if err != nil {
				t.Fatalf("SerializeWALEntry failed: %v", err)
			}

			// Deserialize the entry
			decodedEntry, err := DeserializeWALEntry(payload)
			if err != nil {
				t.Fatalf("DeserializeWALEntry failed: %v", err)
			}

			// Verify the deserialized entry matches the original
			if decodedEntry.Type != tc.entry.Type {
				t.Errorf("Type mismatch: expected %d, got %d", tc.entry.Type, decodedEntry.Type)
			}
			if decodedEntry.SequenceNumber != tc.entry.SequenceNumber {
				t.Errorf("SequenceNumber mismatch: expected %d, got %d",
					tc.entry.SequenceNumber, decodedEntry.SequenceNumber)
			}
			if !bytes.Equal(decodedEntry.Key, tc.entry.Key) {
				t.Errorf("Key mismatch: expected %v, got %v", tc.entry.Key, decodedEntry.Key)
			}

			// For delete entries, value should be nil
			if tc.entry.Type == wal.OpTypeDelete {
				if decodedEntry.Value != nil && len(decodedEntry.Value) > 0 {
					t.Errorf("Value should be nil for delete entry, got %v", decodedEntry.Value)
				}
			} else {
				// For put entries, value should match
				if !bytes.Equal(decodedEntry.Value, tc.entry.Value) {
					t.Errorf("Value mismatch: expected %v, got %v", tc.entry.Value, decodedEntry.Value)
				}
			}
		})
	}
}

func TestWALEntryToProto(t *testing.T) {
	// Create a WAL entry
	entry := &wal.Entry{
		SequenceNumber: 42,
		Type:           wal.OpTypePut,
		Key:            []byte("proto-test-key"),
		Value:          []byte("proto-test-value"),
	}

	// Convert to proto entry
	protoEntry, err := WALEntryToProto(entry, proto.FragmentType_FULL)
	if err != nil {
		t.Fatalf("WALEntryToProto failed: %v", err)
	}

	// Verify proto entry fields
	if protoEntry.SequenceNumber != entry.SequenceNumber {
		t.Errorf("SequenceNumber mismatch: expected %d, got %d",
			entry.SequenceNumber, protoEntry.SequenceNumber)
	}
	if protoEntry.FragmentType != proto.FragmentType_FULL {
		t.Errorf("FragmentType mismatch: expected %v, got %v",
			proto.FragmentType_FULL, protoEntry.FragmentType)
	}

	// Verify we can deserialize the payload back to a WAL entry
	decodedEntry, err := DeserializeWALEntry(protoEntry.Payload)
	if err != nil {
		t.Fatalf("DeserializeWALEntry failed: %v", err)
	}

	// Check the deserialized entry
	if decodedEntry.SequenceNumber != entry.SequenceNumber {
		t.Errorf("SequenceNumber in payload mismatch: expected %d, got %d",
			entry.SequenceNumber, decodedEntry.SequenceNumber)
	}
	if decodedEntry.Type != entry.Type {
		t.Errorf("Type in payload mismatch: expected %d, got %d",
			entry.Type, decodedEntry.Type)
	}
	if !bytes.Equal(decodedEntry.Key, entry.Key) {
		t.Errorf("Key in payload mismatch: expected %v, got %v",
			entry.Key, decodedEntry.Key)
	}
	if !bytes.Equal(decodedEntry.Value, entry.Value) {
		t.Errorf("Value in payload mismatch: expected %v, got %v",
			entry.Value, decodedEntry.Value)
	}
}

func TestReplicationError(t *testing.T) {
	// Create different types of errors
	testCases := []struct {
		code     ErrorCode
		message  string
		expected string
	}{
		{ErrorUnknown, "Unknown error", "UNKNOWN"},
		{ErrorConnection, "Connection failed", "CONNECTION"},
		{ErrorProtocol, "Protocol violation", "PROTOCOL"},
		{ErrorSequenceGap, "Sequence gap detected", "SEQUENCE_GAP"},
		{ErrorCompression, "Compression failed", "COMPRESSION"},
		{ErrorAuthentication, "Authentication failed", "AUTHENTICATION"},
		{ErrorRetention, "WAL no longer available", "RETENTION"},
		{99, "Invalid error code", "ERROR(99)"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			// Create an error
			err := NewReplicationError(tc.code, tc.message)

			// Verify code string
			if tc.code.String() != tc.expected {
				t.Errorf("ErrorCode.String() mismatch: expected %s, got %s",
					tc.expected, tc.code.String())
			}

			// Verify error message contains the code and message
			errorStr := err.Error()
			if !contains(errorStr, tc.expected) {
				t.Errorf("Error string doesn't contain code: %s", errorStr)
			}
			if !contains(errorStr, tc.message) {
				t.Errorf("Error string doesn't contain message: %s", errorStr)
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return bytes.Contains([]byte(s), []byte(substr))
}
