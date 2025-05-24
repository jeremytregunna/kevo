// ABOUTME: Tests sequence number functionality in storage manager
// ABOUTME: Verifies correct version selection during recovery with sequence numbers

package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/stats"
)

// TestSequenceNumberVersioning tests that sequence numbers are properly tracked
// and used for version selection during recovery.
func TestSequenceNumberVersioning(t *testing.T) {
	// Create temporary directories for the test
	tempDir, err := os.MkdirTemp("", "sequence-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create subdirectories for SSTables and WAL
	sstDir := filepath.Join(tempDir, "sst")
	walDir := filepath.Join(tempDir, "wal")

	// Create configuration
	cfg := &config.Config{
		Version:         config.CurrentManifestVersion,
		SSTDir:          sstDir,
		WALDir:          walDir,
		MemTableSize:    1024 * 1024, // 1MB
		MemTablePoolCap: 2,
		MaxMemTables:    2,
	}

	// Create a stats collector
	statsCollector := stats.NewAtomicCollector()

	// Create a new storage manager
	manager, err := NewManager(cfg, statsCollector)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer manager.Close()

	// Step 1: Add a key with initial value
	testKey := []byte("test-key")
	initialValue := []byte("initial-value")
	err = manager.Put(testKey, initialValue)
	if err != nil {
		t.Fatalf("Failed to put initial value: %v", err)
	}

	// Verify the key is readable
	value, err := manager.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if !bytes.Equal(initialValue, value) {
		t.Errorf("Expected initial value %s, got %s", initialValue, value)
	}

	// Step 2: Flush to create an SSTable
	err = manager.FlushMemTables()
	if err != nil {
		t.Fatalf("Failed to flush memtables: %v", err)
	}

	// Verify data is still accessible after flush
	value, err = manager.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get value after flush: %v", err)
	}
	if !bytes.Equal(initialValue, value) {
		t.Errorf("Expected initial value %s after flush, got %s", initialValue, value)
	}

	// Step 3: Update the key with a new value
	updatedValue := []byte("updated-value")
	err = manager.Put(testKey, updatedValue)
	if err != nil {
		t.Fatalf("Failed to put updated value: %v", err)
	}

	// Verify the updated value is readable
	value, err = manager.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get updated value: %v", err)
	}
	if !bytes.Equal(updatedValue, value) {
		t.Errorf("Expected updated value %s, got %s", updatedValue, value)
	}

	// Step 4: Flush again to create another SSTable with the updated value
	err = manager.FlushMemTables()
	if err != nil {
		t.Fatalf("Failed to flush memtables again: %v", err)
	}

	// Verify updated data is still accessible after second flush
	value, err = manager.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get value after second flush: %v", err)
	}
	if !bytes.Equal(updatedValue, value) {
		t.Errorf("Expected updated value %s after second flush, got %s", updatedValue, value)
	}

	// Get the last sequence number
	lastSeqNum := manager.lastSeqNum

	// Step 5: Close the manager and simulate a recovery scenario
	err = manager.Close()
	if err != nil {
		t.Fatalf("Failed to close manager: %v", err)
	}

	// Create a new manager to simulate recovery
	recoveredManager, err := NewManager(cfg, statsCollector)
	if err != nil {
		t.Fatalf("Failed to create recovered manager: %v", err)
	}
	defer recoveredManager.Close()

	// Verify the key still has the latest value after recovery
	recoveredValue, err := recoveredManager.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get value after recovery: %v", err)
	}
	if !bytes.Equal(updatedValue, recoveredValue) {
		t.Errorf("Expected updated value %s after recovery, got %s", updatedValue, recoveredValue)
	}

	// Verify the sequence number was properly recovered
	if recoveredManager.lastSeqNum < lastSeqNum {
		t.Errorf("Recovered sequence number %d is less than last known sequence number %d",
			recoveredManager.lastSeqNum, lastSeqNum)
	}
}
