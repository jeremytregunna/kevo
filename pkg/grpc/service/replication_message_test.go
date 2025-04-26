package service

import (
	"testing"

	"github.com/KevoDB/kevo/pkg/wal"
	"github.com/KevoDB/kevo/proto/kevo"
)

func TestWALEntryChecksum(t *testing.T) {
	// Create a sample WAL entry
	walEntry := &wal.Entry{
		SequenceNumber: 12345,
		Type:           1, // Put operation
		Key:            []byte("test-key"),
		Value:          []byte("test-value"),
	}

	// Convert to proto message with checksum
	pbEntry := convertWALEntryToProto(walEntry)

	// Verify checksum is not nil
	if pbEntry.Checksum == nil {
		t.Error("Expected checksum to be calculated, got nil")
	}

	// Modify the entry and verify checksum would be different
	pbEntryModified := &kevo.WALEntry{
		SequenceNumber: pbEntry.SequenceNumber,
		Type:           pbEntry.Type,
		Key:            pbEntry.Key,
		Value:          []byte("modified-value"), // Changed value
	}

	modifiedChecksum := calculateEntryChecksum(pbEntryModified)

	// The checksums should be different
	checksumMatches := true
	if len(pbEntry.Checksum) == len(modifiedChecksum) {
		checksumMatches = true
		for i := 0; i < len(pbEntry.Checksum); i++ {
			if pbEntry.Checksum[i] != modifiedChecksum[i] {
				checksumMatches = false
				break
			}
		}
	} else {
		checksumMatches = false
	}

	if checksumMatches {
		t.Error("Expected different checksums for modified entries")
	}
}

func TestBatchChecksum(t *testing.T) {
	// Create sample WAL entries
	walEntries := []*wal.Entry{
		{
			SequenceNumber: 12345,
			Type:           1, // Put operation
			Key:            []byte("key1"),
			Value:          []byte("value1"),
		},
		{
			SequenceNumber: 12346,
			Type:           1, // Put operation
			Key:            []byte("key2"),
			Value:          []byte("value2"),
		},
		{
			SequenceNumber: 12347,
			Type:           2, // Delete operation
			Key:            []byte("key3"),
			Value:          nil,
		},
	}

	// Convert to proto messages
	pbEntries := make([]*kevo.WALEntry, 0, len(walEntries))
	for _, entry := range walEntries {
		pbEntries = append(pbEntries, convertWALEntryToProto(entry))
	}

	// Create batch
	pbBatch := &kevo.WALEntryBatch{
		Entries:  pbEntries,
		FirstLsn: walEntries[0].SequenceNumber,
		LastLsn:  walEntries[len(walEntries)-1].SequenceNumber,
		Count:    uint32(len(walEntries)),
	}

	// Calculate checksum
	checksum := calculateBatchChecksum(pbBatch)

	// Verify checksum is not nil
	if checksum == nil {
		t.Error("Expected batch checksum to be calculated, got nil")
	}

	// Modify the batch and verify checksum would be different
	modifiedBatch := &kevo.WALEntryBatch{
		Entries:  pbEntries[:2], // Remove one entry
		FirstLsn: pbBatch.FirstLsn,
		LastLsn:  pbBatch.LastLsn,
		Count:    uint32(len(pbEntries) - 1),
	}

	modifiedChecksum := calculateBatchChecksum(modifiedBatch)

	// The checksums should be different
	checksumMatches := true
	if len(checksum) == len(modifiedChecksum) {
		checksumMatches = true
		for i := 0; i < len(checksum); i++ {
			if checksum[i] != modifiedChecksum[i] {
				checksumMatches = false
				break
			}
		}
	} else {
		checksumMatches = false
	}

	if checksumMatches {
		t.Error("Expected different checksums for modified batches")
	}
}

func TestWALEntryRoundTrip(t *testing.T) {
	// Test different entry types
	testCases := []struct {
		name  string
		entry *wal.Entry
	}{
		{
			name: "Put operation",
			entry: &wal.Entry{
				SequenceNumber: 12345,
				Type:           1, // Put
				Key:            []byte("test-key"),
				Value:          []byte("test-value"),
			},
		},
		{
			name: "Delete operation",
			entry: &wal.Entry{
				SequenceNumber: 12346,
				Type:           2, // Delete
				Key:            []byte("test-key"),
				Value:          nil,
			},
		},
		{
			name: "Empty value",
			entry: &wal.Entry{
				SequenceNumber: 12347,
				Type:           1, // Put with empty value
				Key:            []byte("test-key"),
				Value:          []byte{},
			},
		},
		{
			name: "Binary key and value",
			entry: &wal.Entry{
				SequenceNumber: 12348,
				Type:           1,
				Key:            []byte{0x00, 0x01, 0x02, 0x03},
				Value:          []byte{0xFF, 0xFE, 0xFD, 0xFC},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Convert to proto
			pbEntry := convertWALEntryToProto(tc.entry)

			// Verify fields were correctly converted
			if pbEntry.SequenceNumber != tc.entry.SequenceNumber {
				t.Errorf("SequenceNumber mismatch, expected: %d, got: %d",
					tc.entry.SequenceNumber, pbEntry.SequenceNumber)
			}

			if pbEntry.Type != uint32(tc.entry.Type) {
				t.Errorf("Type mismatch, expected: %d, got: %d",
					tc.entry.Type, pbEntry.Type)
			}

			if string(pbEntry.Key) != string(tc.entry.Key) {
				t.Errorf("Key mismatch, expected: %s, got: %s",
					string(tc.entry.Key), string(pbEntry.Key))
			}

			// For nil value, proto should have empty value (not nil)
			expectedValue := tc.entry.Value
			if expectedValue == nil {
				expectedValue = []byte{}
			}

			if string(pbEntry.Value) != string(expectedValue) {
				t.Errorf("Value mismatch, expected: %s, got: %s",
					string(expectedValue), string(pbEntry.Value))
			}

			// Verify checksum exists
			if pbEntry.Checksum == nil || len(pbEntry.Checksum) == 0 {
				t.Error("Checksum should not be empty")
			}
		})
	}
}
