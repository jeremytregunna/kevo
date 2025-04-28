package replication

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/wal"
	proto "github.com/KevoDB/kevo/pkg/replication/proto"
)

// TestPrimaryCreation tests that a primary can be created with a WAL
func TestPrimaryCreation(t *testing.T) {
	// Create a temporary directory for the WAL
	tempDir, err := os.MkdirTemp("", "primary_creation_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a WAL
	cfg := config.NewDefaultConfig(tempDir)
	w, err := wal.NewWAL(cfg, filepath.Join(tempDir, "wal"))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Create a primary
	primary, err := NewPrimary(w, DefaultPrimaryConfig())
	if err != nil {
		t.Fatalf("Failed to create primary: %v", err)
	}
	defer primary.Close()

	// Check that the primary was configured correctly
	if primary.wal != w {
		t.Errorf("Primary has incorrect WAL reference")
	}

	if primary.batcher == nil {
		t.Errorf("Primary has nil batcher")
	}

	if primary.compressor == nil {
		t.Errorf("Primary has nil compressor")
	}

	if primary.sessions == nil {
		t.Errorf("Primary has nil sessions map")
	}
}

// TestPrimaryWALObserver tests that the primary correctly observes WAL events
func TestPrimaryWALObserver(t *testing.T) {
	// Create a temporary directory for the WAL
	tempDir, err := os.MkdirTemp("", "primary_observer_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a WAL
	cfg := config.NewDefaultConfig(tempDir)
	w, err := wal.NewWAL(cfg, filepath.Join(tempDir, "wal"))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Create a primary
	primary, err := NewPrimary(w, DefaultPrimaryConfig())
	if err != nil {
		t.Fatalf("Failed to create primary: %v", err)
	}
	defer primary.Close()

	// Write a single entry to the WAL
	key := []byte("test-key")
	value := []byte("test-value")
	seq, err := w.Append(wal.OpTypePut, key, value)
	if err != nil {
		t.Fatalf("Failed to append to WAL: %v", err)
	}
	if seq != 1 {
		t.Errorf("Expected sequence 1, got %d", seq)
	}

	// Allow some time for notifications to be processed
	time.Sleep(50 * time.Millisecond)

	// Verify the batcher has entries
	if primary.batcher.GetBatchCount() <= 0 {
		t.Errorf("Primary batcher did not receive WAL entry")
	}

	// Sync the WAL and verify the primary observes it
	lastSyncedBefore := primary.lastSyncedSeq
	err = w.Sync()
	if err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Allow time for sync notification
	time.Sleep(50 * time.Millisecond)

	// Check that lastSyncedSeq was updated
	if primary.lastSyncedSeq <= lastSyncedBefore {
		t.Errorf("Primary did not update lastSyncedSeq after WAL sync")
	}
}

// TestPrimarySessionManagement tests session registration and management
func TestPrimarySessionManagement(t *testing.T) {
	// Create a temporary directory for the WAL
	tempDir, err := os.MkdirTemp("", "primary_session_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a WAL
	cfg := config.NewDefaultConfig(tempDir)
	w, err := wal.NewWAL(cfg, filepath.Join(tempDir, "wal"))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Create a primary
	primary, err := NewPrimary(w, DefaultPrimaryConfig())
	if err != nil {
		t.Fatalf("Failed to create primary: %v", err)
	}
	defer primary.Close()

	// Register a session
	session := &ReplicaSession{
		ID:              "test-session",
		StartSequence:   0,
		LastAckSequence: 0,
		Connected:       true,
		Active:          true,
		LastActivity:    time.Now(),
		SupportedCodecs: []proto.CompressionCodec{proto.CompressionCodec_NONE},
	}

	primary.registerReplicaSession(session)

	// Verify session was registered
	if len(primary.sessions) != 1 {
		t.Errorf("Expected 1 session, got %d", len(primary.sessions))
	}

	// Unregister session
	primary.unregisterReplicaSession("test-session")

	// Verify session was unregistered
	if len(primary.sessions) != 0 {
		t.Errorf("Expected 0 sessions after unregistering, got %d", len(primary.sessions))
	}
}
