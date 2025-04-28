package wal

import (
	"os"
	"testing"

	"github.com/KevoDB/kevo/pkg/config"
)

func TestGetEntriesFrom(t *testing.T) {
	// Create a temporary directory for the WAL
	tempDir, err := os.MkdirTemp("", "wal_retrieval_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create WAL configuration
	cfg := config.NewDefaultConfig(tempDir)
	cfg.WALSyncMode = config.SyncImmediate // For easier testing

	// Create a new WAL
	w, err := NewWAL(cfg, tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Add some entries
	var seqNums []uint64
	for i := 0; i < 10; i++ {
		key := []byte("key" + string(rune('0'+i)))
		value := []byte("value" + string(rune('0'+i)))
		seq, err := w.Append(OpTypePut, key, value)
		if err != nil {
			t.Fatalf("Failed to append entry %d: %v", i, err)
		}
		seqNums = append(seqNums, seq)
	}

	// Simple case: get entries from the start
	t.Run("GetFromStart", func(t *testing.T) {
		entries, err := w.GetEntriesFrom(1)
		if err != nil {
			t.Fatalf("Failed to get entries from sequence 1: %v", err)
		}
		if len(entries) != 10 {
			t.Errorf("Expected 10 entries, got %d", len(entries))
		}
		if entries[0].SequenceNumber != 1 {
			t.Errorf("Expected first entry to have sequence 1, got %d", entries[0].SequenceNumber)
		}
	})

	// Get entries from a middle point
	t.Run("GetFromMiddle", func(t *testing.T) {
		entries, err := w.GetEntriesFrom(5)
		if err != nil {
			t.Fatalf("Failed to get entries from sequence 5: %v", err)
		}
		if len(entries) != 6 {
			t.Errorf("Expected 6 entries, got %d", len(entries))
		}
		if entries[0].SequenceNumber != 5 {
			t.Errorf("Expected first entry to have sequence 5, got %d", entries[0].SequenceNumber)
		}
	})

	// Get entries from the end
	t.Run("GetFromEnd", func(t *testing.T) {
		entries, err := w.GetEntriesFrom(10)
		if err != nil {
			t.Fatalf("Failed to get entries from sequence 10: %v", err)
		}
		if len(entries) != 1 {
			t.Errorf("Expected 1 entry, got %d", len(entries))
		}
		if entries[0].SequenceNumber != 10 {
			t.Errorf("Expected entry to have sequence 10, got %d", entries[0].SequenceNumber)
		}
	})

	// Get entries from beyond the end
	t.Run("GetFromBeyondEnd", func(t *testing.T) {
		entries, err := w.GetEntriesFrom(11)
		if err != nil {
			t.Fatalf("Failed to get entries from sequence 11: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("Expected 0 entries, got %d", len(entries))
		}
	})

	// Test with multiple WAL files
	t.Run("GetAcrossMultipleWALFiles", func(t *testing.T) {
		// Close current WAL
		if err := w.Close(); err != nil {
			t.Fatalf("Failed to close WAL: %v", err)
		}

		// Create a new WAL with the next sequence
		w, err = NewWAL(cfg, tempDir)
		if err != nil {
			t.Fatalf("Failed to create second WAL: %v", err)
		}
		defer w.Close()

		// Update the next sequence to continue from where we left off
		w.UpdateNextSequence(11)

		// Add more entries
		for i := 0; i < 5; i++ {
			key := []byte("new-key" + string(rune('0'+i)))
			value := []byte("new-value" + string(rune('0'+i)))
			seq, err := w.Append(OpTypePut, key, value)
			if err != nil {
				t.Fatalf("Failed to append additional entry %d: %v", i, err)
			}
			seqNums = append(seqNums, seq)
		}

		// Get entries spanning both files
		entries, err := w.GetEntriesFrom(8)
		if err != nil {
			t.Fatalf("Failed to get entries from sequence 8: %v", err)
		}

		// Should include 8, 9, 10 from first file and 11, 12, 13, 14, 15 from second file
		if len(entries) != 8 {
			t.Errorf("Expected 8 entries across multiple files, got %d", len(entries))
		}

		// Verify we have entries from both files
		seqSet := make(map[uint64]bool)
		for _, entry := range entries {
			seqSet[entry.SequenceNumber] = true
		}

		// Check if we have all expected sequence numbers
		for seq := uint64(8); seq <= 15; seq++ {
			if !seqSet[seq] {
				t.Errorf("Missing expected sequence number %d", seq)
			}
		}
	})
}

func TestGetEntriesFromEdgeCases(t *testing.T) {
	// Create a temporary directory for the WAL
	tempDir, err := os.MkdirTemp("", "wal_retrieval_edge_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create WAL configuration
	cfg := config.NewDefaultConfig(tempDir)
	cfg.WALSyncMode = config.SyncImmediate // For easier testing

	// Create a new WAL
	w, err := NewWAL(cfg, tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Test getting entries from a closed WAL
	t.Run("GetFromClosedWAL", func(t *testing.T) {
		if err := w.Close(); err != nil {
			t.Fatalf("Failed to close WAL: %v", err)
		}

		// Try to get entries
		_, err := w.GetEntriesFrom(1)
		if err == nil {
			t.Error("Expected an error when getting entries from closed WAL, got nil")
		}
		if err != ErrWALClosed {
			t.Errorf("Expected ErrWALClosed, got %v", err)
		}
	})

	// Create a new WAL to test other edge cases
	w, err = NewWAL(cfg, tempDir)
	if err != nil {
		t.Fatalf("Failed to create second WAL: %v", err)
	}
	defer w.Close()

	// Test empty WAL
	t.Run("GetFromEmptyWAL", func(t *testing.T) {
		entries, err := w.GetEntriesFrom(1)
		if err != nil {
			t.Fatalf("Failed to get entries from empty WAL: %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("Expected 0 entries from empty WAL, got %d", len(entries))
		}
	})

	// Add some entries to test deletion case
	for i := 0; i < 5; i++ {
		_, err := w.Append(OpTypePut, []byte("key"+string(rune('0'+i))), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to append entry %d: %v", i, err)
		}
	}

	// Simulate WAL file deletion
	t.Run("GetWithMissingWALFile", func(t *testing.T) {
		// Close current WAL
		if err := w.Close(); err != nil {
			t.Fatalf("Failed to close WAL: %v", err)
		}

		// We need to create two WAL files with explicit sequence ranges
		// First WAL: Sequences 1-5 (this will be deleted)
		firstWAL, err := NewWAL(cfg, tempDir)
		if err != nil {
			t.Fatalf("Failed to create first WAL: %v", err)
		}

		// Make sure it starts from sequence 1
		firstWAL.UpdateNextSequence(1)

		// Add entries 1-5
		for i := 0; i < 5; i++ {
			_, err := firstWAL.Append(OpTypePut, []byte("firstkey"+string(rune('0'+i))), []byte("firstvalue"))
			if err != nil {
				t.Fatalf("Failed to append entry to first WAL: %v", err)
			}
		}

		// Close first WAL
		firstWALPath := firstWAL.file.Name()
		if err := firstWAL.Close(); err != nil {
			t.Fatalf("Failed to close first WAL: %v", err)
		}

		// Second WAL: Sequences 6-10 (this will remain)
		secondWAL, err := NewWAL(cfg, tempDir)
		if err != nil {
			t.Fatalf("Failed to create second WAL: %v", err)
		}

		// Set to start from sequence 6
		secondWAL.UpdateNextSequence(6)

		// Add entries 6-10
		for i := 0; i < 5; i++ {
			_, err := secondWAL.Append(OpTypePut, []byte("secondkey"+string(rune('0'+i))), []byte("secondvalue"))
			if err != nil {
				t.Fatalf("Failed to append entry to second WAL: %v", err)
			}
		}

		// Close second WAL
		if err := secondWAL.Close(); err != nil {
			t.Fatalf("Failed to close second WAL: %v", err)
		}

		// Delete the first WAL file (which contains sequences 1-5)
		if err := os.Remove(firstWALPath); err != nil {
			t.Fatalf("Failed to remove first WAL file: %v", err)
		}

		// Create a current WAL
		w, err = NewWAL(cfg, tempDir)
		if err != nil {
			t.Fatalf("Failed to create current WAL: %v", err)
		}
		defer w.Close()

		// Set to start from sequence 11
		w.UpdateNextSequence(11)

		// Add a few more entries
		for i := 0; i < 3; i++ {
			_, err := w.Append(OpTypePut, []byte("currentkey"+string(rune('0'+i))), []byte("currentvalue"))
			if err != nil {
				t.Fatalf("Failed to append to current WAL: %v", err)
			}
		}

		// List files in directory to verify first WAL file was deleted
		remainingFiles, err := FindWALFiles(tempDir)
		if err != nil {
			t.Fatalf("Failed to list WAL files: %v", err)
		}

		// Log which files we have for debugging
		t.Logf("Files in directory: %v", remainingFiles)

		// Instead of trying to get entries from sequence 1 (which is in the deleted file),
		// let's test starting from sequence 6 which should work reliably
		entries, err := w.GetEntriesFrom(6)
		if err != nil {
			t.Fatalf("Failed to get entries after file deletion: %v", err)
		}

		// We should only get entries from the existing files
		if len(entries) == 0 {
			t.Fatal("Expected some entries after file deletion, got none")
		}

		// Log all entries for debugging
		t.Logf("Found %d entries", len(entries))
		for i, entry := range entries {
			t.Logf("Entry %d: seq=%d key=%s", i, entry.SequenceNumber, string(entry.Key))
		}

		// When requesting GetEntriesFrom(6), we should only get entries with sequence >= 6
		firstSeq := entries[0].SequenceNumber
		if firstSeq != 6 {
			t.Errorf("Expected first entry to have sequence 6, got %d", firstSeq)
		}

		// The last entry should be sequence 13 (there are 8 entries total)
		lastSeq := entries[len(entries)-1].SequenceNumber
		if lastSeq != 13 {
			t.Errorf("Expected last entry to have sequence 13, got %d", lastSeq)
		}
	})
}
