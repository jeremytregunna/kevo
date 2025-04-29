package replication

import (
	"errors"
	"testing"

	"github.com/KevoDB/kevo/pkg/wal"
	proto "github.com/KevoDB/kevo/proto/kevo/replication"
)

func TestWALBatcher(t *testing.T) {
	// Create a new batcher with a small max batch size
	batcher := NewWALBatcher(10, proto.CompressionCodec_NONE, false)

	// Create test entries
	entries := []*wal.Entry{
		{
			SequenceNumber: 1,
			Type:           wal.OpTypePut,
			Key:            []byte("key1"),
			Value:          []byte("value1"),
		},
		{
			SequenceNumber: 2,
			Type:           wal.OpTypePut,
			Key:            []byte("key2"),
			Value:          []byte("value2"),
		},
		{
			SequenceNumber: 3,
			Type:           wal.OpTypeDelete,
			Key:            []byte("key3"),
		},
	}

	// Add entries and check batch status
	for i, entry := range entries {
		ready, err := batcher.AddEntry(entry)
		if err != nil {
			t.Fatalf("Failed to add entry %d: %v", i, err)
		}

		// The batch shouldn't be ready yet with these small entries
		if ready {
			t.Logf("Batch ready after entry %d (expected to fit more entries)", i)
		}
	}

	// Verify batch content
	if batcher.GetBatchCount() != 3 {
		t.Errorf("Expected batch to contain 3 entries, got %d", batcher.GetBatchCount())
	}

	// Get the batch and verify it's the correct format
	batch := batcher.GetBatch()
	if len(batch.Entries) != 3 {
		t.Errorf("Expected batch to contain 3 entries, got %d", len(batch.Entries))
	}
	if batch.Compressed {
		t.Errorf("Expected batch to be uncompressed")
	}
	if batch.Codec != proto.CompressionCodec_NONE {
		t.Errorf("Expected codec to be NONE, got %v", batch.Codec)
	}

	// Verify batch is now empty
	if batcher.GetBatchCount() != 0 {
		t.Errorf("Expected batch to be empty after GetBatch(), got %d entries", batcher.GetBatchCount())
	}
}

func TestWALBatcherSizeLimit(t *testing.T) {
	// Create a batcher with a very small limit (2KB)
	batcher := NewWALBatcher(2, proto.CompressionCodec_NONE, false)

	// Create a large entry (approximately 1.5KB)
	largeValue := make([]byte, 1500)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	entry1 := &wal.Entry{
		SequenceNumber: 1,
		Type:           wal.OpTypePut,
		Key:            []byte("large-key-1"),
		Value:          largeValue,
	}

	// Add the first large entry
	ready, err := batcher.AddEntry(entry1)
	if err != nil {
		t.Fatalf("Failed to add large entry 1: %v", err)
	}
	if ready {
		t.Errorf("Batch shouldn't be ready after first large entry")
	}

	// Create another large entry
	entry2 := &wal.Entry{
		SequenceNumber: 2,
		Type:           wal.OpTypePut,
		Key:            []byte("large-key-2"),
		Value:          largeValue,
	}

	// Add the second large entry, this should make the batch ready
	ready, err = batcher.AddEntry(entry2)
	if err != nil {
		t.Fatalf("Failed to add large entry 2: %v", err)
	}
	if !ready {
		t.Errorf("Batch should be ready after second large entry")
	}

	// Verify batch is not empty
	batchCount := batcher.GetBatchCount()
	if batchCount == 0 {
		t.Errorf("Expected batch to contain entries, got 0")
	}

	// Get the batch and verify
	batch := batcher.GetBatch()
	if len(batch.Entries) == 0 {
		t.Errorf("Expected batch to contain entries, got 0")
	}
}

func TestWALBatcherWithTransactionBoundaries(t *testing.T) {
	// Create a batcher that respects transaction boundaries
	batcher := NewWALBatcher(10, proto.CompressionCodec_NONE, true)

	// Create a batch entry (simulating a transaction start)
	batchEntry := &wal.Entry{
		SequenceNumber: 1,
		Type:           wal.OpTypeBatch,
		Key:            []byte{}, // Batch entries might have a special format
	}

	// Add the batch entry
	_, err := batcher.AddEntry(batchEntry)
	if err != nil {
		t.Fatalf("Failed to add batch entry: %v", err)
	}

	// Add a few more entries
	for i := 2; i <= 5; i++ {
		entry := &wal.Entry{
			SequenceNumber: uint64(i),
			Type:           wal.OpTypePut,
			Key:            []byte("key"),
			Value:          []byte("value"),
		}

		_, err = batcher.AddEntry(entry)
		if err != nil {
			t.Fatalf("Failed to add entry %d: %v", i, err)
		}
	}

	// Get the batch
	batch := batcher.GetBatch()
	if len(batch.Entries) != 5 {
		t.Errorf("Expected batch to contain 5 entries, got %d", len(batch.Entries))
	}
}

func TestWALBatcherReset(t *testing.T) {
	// Create a batcher
	batcher := NewWALBatcher(10, proto.CompressionCodec_NONE, false)

	// Add an entry
	entry := &wal.Entry{
		SequenceNumber: 1,
		Type:           wal.OpTypePut,
		Key:            []byte("key"),
		Value:          []byte("value"),
	}

	_, err := batcher.AddEntry(entry)
	if err != nil {
		t.Fatalf("Failed to add entry: %v", err)
	}

	// Verify the entry is in the buffer
	if batcher.GetBatchCount() != 1 {
		t.Errorf("Expected batch to contain 1 entry, got %d", batcher.GetBatchCount())
	}

	// Reset the batcher
	batcher.Reset()

	// Verify the buffer is empty
	if batcher.GetBatchCount() != 0 {
		t.Errorf("Expected batch to be empty after reset, got %d entries", batcher.GetBatchCount())
	}
}

func TestWALBatchApplier(t *testing.T) {
	// Create a batch applier starting at sequence 0
	applier := NewWALBatchApplier(0)

	// Create a set of proto entries with sequential sequence numbers
	protoEntries := createSequentialProtoEntries(1, 5)

	// Mock apply function that just counts calls
	applyCount := 0
	applyFn := func(entry *wal.Entry) error {
		applyCount++
		return nil
	}

	// Apply the entries
	maxApplied, hasGap, err := applier.ApplyEntries(protoEntries, applyFn)
	if err != nil {
		t.Fatalf("Failed to apply entries: %v", err)
	}
	if hasGap {
		t.Errorf("Unexpected gap reported")
	}
	if maxApplied != 5 {
		t.Errorf("Expected max applied sequence to be 5, got %d", maxApplied)
	}
	if applyCount != 5 {
		t.Errorf("Expected apply function to be called 5 times, got %d", applyCount)
	}

	// Verify tracking
	if applier.GetMaxApplied() != 5 {
		t.Errorf("Expected GetMaxApplied to return 5, got %d", applier.GetMaxApplied())
	}
	if applier.GetExpectedNext() != 6 {
		t.Errorf("Expected GetExpectedNext to return 6, got %d", applier.GetExpectedNext())
	}

	// Test acknowledgement
	applier.AcknowledgeUpTo(5)
	if applier.GetLastAcknowledged() != 5 {
		t.Errorf("Expected GetLastAcknowledged to return 5, got %d", applier.GetLastAcknowledged())
	}
}

func TestWALBatchApplierWithGap(t *testing.T) {
	// Create a batch applier starting at sequence 0
	applier := NewWALBatchApplier(0)

	// Create a set of proto entries with a gap
	protoEntries := createSequentialProtoEntries(2, 5) // Start at 2 instead of expected 1

	// Apply the entries
	_, hasGap, err := applier.ApplyEntries(protoEntries, func(entry *wal.Entry) error {
		return nil
	})

	// Should detect a gap
	if !hasGap {
		t.Errorf("Expected gap to be detected")
	}
	if err == nil {
		t.Errorf("Expected error for sequence gap")
	}
}

func TestWALBatchApplierWithApplyError(t *testing.T) {
	// Create a batch applier starting at sequence 0
	applier := NewWALBatchApplier(0)

	// Create a set of proto entries
	protoEntries := createSequentialProtoEntries(1, 5)

	// Mock apply function that returns an error
	applyErr := errors.New("apply error")
	applyFn := func(entry *wal.Entry) error {
		return applyErr
	}

	// Apply the entries
	_, _, err := applier.ApplyEntries(protoEntries, applyFn)
	if err == nil {
		t.Errorf("Expected error from apply function")
	}
}

func TestWALBatchApplierReset(t *testing.T) {
	// Create a batch applier and apply some entries
	applier := NewWALBatchApplier(0)

	// Apply entries up to sequence 5
	protoEntries := createSequentialProtoEntries(1, 5)
	applier.ApplyEntries(protoEntries, func(entry *wal.Entry) error {
		return nil
	})

	// Reset to sequence 10
	applier.Reset(10)

	// Verify state was reset
	if applier.GetMaxApplied() != 10 {
		t.Errorf("Expected max applied to be 10 after reset, got %d", applier.GetMaxApplied())
	}
	if applier.GetLastAcknowledged() != 10 {
		t.Errorf("Expected last acknowledged to be 10 after reset, got %d", applier.GetLastAcknowledged())
	}
	if applier.GetExpectedNext() != 11 {
		t.Errorf("Expected expected next to be 11 after reset, got %d", applier.GetExpectedNext())
	}

	// Apply entries starting from sequence 11
	protoEntries = createSequentialProtoEntries(11, 15)
	_, hasGap, err := applier.ApplyEntries(protoEntries, func(entry *wal.Entry) error {
		return nil
	})

	// Should not detect a gap
	if hasGap {
		t.Errorf("Unexpected gap detected after reset")
	}
	if err != nil {
		t.Errorf("Unexpected error after reset: %v", err)
	}
}

// Helper function to create a sequence of proto entries
func createSequentialProtoEntries(start, end uint64) []*proto.WALEntry {
	var entries []*proto.WALEntry

	for seq := start; seq <= end; seq++ {
		// Create a simple WAL entry
		walEntry := &wal.Entry{
			SequenceNumber: seq,
			Type:           wal.OpTypePut,
			Key:            []byte("key"),
			Value:          []byte("value"),
		}

		// Serialize it
		payload, _ := SerializeWALEntry(walEntry)

		// Create proto entry
		protoEntry := &proto.WALEntry{
			SequenceNumber: seq,
			Payload:        payload,
			FragmentType:   proto.FragmentType_FULL,
		}

		entries = append(entries, protoEntry)
	}

	return entries
}
