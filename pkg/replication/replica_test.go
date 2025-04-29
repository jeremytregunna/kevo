package replication

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/wal"
	replication_proto "github.com/KevoDB/kevo/proto/kevo/replication"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// testWALEntryApplier implements WALEntryApplier for testing
type testWALEntryApplier struct {
	entries      []*wal.Entry
	appliedCount int
	syncCount    int
	mu           sync.Mutex
	shouldFail   bool
	wal          *wal.WAL
}

func newTestWALEntryApplier(walDir string) (*testWALEntryApplier, error) {
	// Create a WAL for the applier to write to
	cfg := &config.Config{
		WALDir:      walDir,
		WALSyncMode: config.SyncImmediate,
		WALMaxSize:  64 * 1024 * 1024, // 64MB
	}
	testWal, err := wal.NewWAL(cfg, walDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL for applier: %w", err)
	}

	return &testWALEntryApplier{
		entries: make([]*wal.Entry, 0),
		wal:     testWal,
	}, nil
}

func (a *testWALEntryApplier) Apply(entry *wal.Entry) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.shouldFail {
		return fmt.Errorf("simulated apply failure")
	}

	// Store the entry in our list
	a.entries = append(a.entries, entry)
	a.appliedCount++

	return nil
}

func (a *testWALEntryApplier) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.shouldFail {
		return fmt.Errorf("simulated sync failure")
	}

	// Sync the WAL
	if err := a.wal.Sync(); err != nil {
		return err
	}

	a.syncCount++
	return nil
}

func (a *testWALEntryApplier) Close() error {
	return a.wal.Close()
}

func (a *testWALEntryApplier) GetAppliedEntries() []*wal.Entry {
	a.mu.Lock()
	defer a.mu.Unlock()

	result := make([]*wal.Entry, len(a.entries))
	copy(result, a.entries)
	return result
}

func (a *testWALEntryApplier) GetAppliedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.appliedCount
}

func (a *testWALEntryApplier) GetSyncCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.syncCount
}

func (a *testWALEntryApplier) SetShouldFail(shouldFail bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.shouldFail = shouldFail
}

// bufConnServerConnector is a connector that uses bufconn for testing
type bufConnServerConnector struct {
	client replication_proto.WALReplicationServiceClient
}

func (c *bufConnServerConnector) Connect(r *Replica) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.client = c.client
	return nil
}

// setupTestEnvironment sets up a complete test environment with WAL, Primary, and gRPC server
func setupTestEnvironment(t *testing.T) (string, *wal.WAL, *Primary, replication_proto.WALReplicationServiceClient, func()) {
	// Create a temporary directory for the WAL files
	tempDir, err := ioutil.TempDir("", "wal_replication_test")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}

	// Create primary WAL directory
	primaryWalDir := filepath.Join(tempDir, "primary_wal")
	if err := os.MkdirAll(primaryWalDir, 0755); err != nil {
		t.Fatalf("Failed to create primary WAL directory: %v", err)
	}

	// Create replica WAL directory
	replicaWalDir := filepath.Join(tempDir, "replica_wal")
	if err := os.MkdirAll(replicaWalDir, 0755); err != nil {
		t.Fatalf("Failed to create replica WAL directory: %v", err)
	}

	// Create the primary WAL
	primaryCfg := &config.Config{
		WALDir:      primaryWalDir,
		WALSyncMode: config.SyncImmediate,
		WALMaxSize:  64 * 1024 * 1024, // 64MB
	}
	primaryWAL, err := wal.NewWAL(primaryCfg, primaryWalDir)
	if err != nil {
		t.Fatalf("Failed to create primary WAL: %v", err)
	}

	// Create a Primary with the WAL
	primary, err := NewPrimary(primaryWAL, &PrimaryConfig{
		MaxBatchSizeKB:    256, // 256 KB
		EnableCompression: false,
		CompressionCodec:  replication_proto.CompressionCodec_NONE,
		RetentionConfig: WALRetentionConfig{
			MaxAgeHours: 1, // 1 hour retention
		},
	})
	if err != nil {
		t.Fatalf("Failed to create primary: %v", err)
	}

	// Setup gRPC server over bufconn
	listener := bufconn.Listen(bufSize)
	server := grpc.NewServer()
	replication_proto.RegisterWALReplicationServiceServer(server, primary)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	// Create a client connection
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	client := replication_proto.NewWALReplicationServiceClient(conn)

	// Return a cleanup function
	cleanup := func() {
		conn.Close()
		server.Stop()
		listener.Close()
		primaryWAL.Close()
		os.RemoveAll(tempDir)
	}

	return replicaWalDir, primaryWAL, primary, client, cleanup
}

// Test creating a new replica
func TestNewReplica(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := ioutil.TempDir("", "replica_test")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create an applier
	applier, err := newTestWALEntryApplier(tempDir)
	if err != nil {
		t.Fatalf("Failed to create test applier: %v", err)
	}
	defer applier.Close()

	// Create a replica
	config := DefaultReplicaConfig()
	replica, err := NewReplica(0, applier, config)
	if err != nil {
		t.Fatalf("Failed to create replica: %v", err)
	}

	// Check initial state
	if got, want := replica.GetLastAppliedSequence(), uint64(0); got != want {
		t.Errorf("GetLastAppliedSequence() = %d, want %d", got, want)
	}
	if got, want := replica.GetCurrentState(), StateConnecting; got != want {
		t.Errorf("GetCurrentState() = %v, want %v", got, want)
	}

	// Clean up
	if err := replica.Stop(); err != nil {
		t.Errorf("Failed to stop replica: %v", err)
	}
}

// Test connection and streaming with real WAL entries
func TestReplicaStreamingWithRealWAL(t *testing.T) {
	// Setup test environment
	replicaWalDir, primaryWAL, _, client, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create test applier for the replica
	applier, err := newTestWALEntryApplier(replicaWalDir)
	if err != nil {
		t.Fatalf("Failed to create test applier: %v", err)
	}
	defer applier.Close()

	// Write some entries to the primary WAL
	numEntries := 10
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key%d", i+1))
		value := []byte(fmt.Sprintf("value%d", i+1))
		if _, err := primaryWAL.Append(wal.OpTypePut, key, value); err != nil {
			t.Fatalf("Failed to append to primary WAL: %v", err)
		}
	}

	// Sync the primary WAL to ensure entries are persisted
	if err := primaryWAL.Sync(); err != nil {
		t.Fatalf("Failed to sync primary WAL: %v", err)
	}

	// Create replica config
	config := DefaultReplicaConfig()
	config.Connection.PrimaryAddress = "bufnet" // This will be ignored with our custom connector

	// Create replica
	replica, err := NewReplica(0, applier, config)
	if err != nil {
		t.Fatalf("Failed to create replica: %v", err)
	}

	// Set custom connector for testing
	replica.SetConnector(&bufConnServerConnector{client: client})

	// Start the replica
	if err := replica.Start(); err != nil {
		t.Fatalf("Failed to start replica: %v", err)
	}

	// Wait for replication to complete
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		// Check if entries were applied
		appliedEntries := applier.GetAppliedEntries()
		t.Logf("Waiting for replication, current applied entries: %d/%d", len(appliedEntries), numEntries)

		// Log the state of the replica for debugging
		t.Logf("Replica state: %s", replica.GetStateString())

		// Also check sync count
		syncCount := applier.GetSyncCount()
		t.Logf("Current sync count: %d", syncCount)

		// Success condition: all entries applied and at least one sync
		if len(appliedEntries) == numEntries && syncCount > 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Verify entries were applied with more specific messages
	appliedEntries := applier.GetAppliedEntries()
	if len(appliedEntries) != numEntries {
		for i, entry := range appliedEntries {
			t.Logf("Applied entry %d: sequence=%d, key=%s, value=%s",
				i, entry.SequenceNumber, string(entry.Key), string(entry.Value))
		}
		t.Errorf("Expected %d entries to be applied, got %d", numEntries, len(appliedEntries))
	} else {
		t.Logf("All %d entries were successfully applied", numEntries)
	}

	// Verify sync was called
	syncCount := applier.GetSyncCount()
	if syncCount == 0 {
		t.Error("Sync was not called")
	} else {
		t.Logf("Sync was called %d times", syncCount)
	}

	// Verify last applied sequence matches the expected sequence
	lastSeq := replica.GetLastAppliedSequence()
	if lastSeq != uint64(numEntries) {
		t.Errorf("Expected last applied sequence to be %d, got %d", numEntries, lastSeq)
	} else {
		t.Logf("Last applied sequence is correct: %d", lastSeq)
	}

	// Stop the replica
	if err := replica.Stop(); err != nil {
		t.Errorf("Failed to stop replica: %v", err)
	}
}

// Test state transitions
func TestReplicaStateTransitions(t *testing.T) {
	// Setup test environment
	replicaWalDir, _, _, client, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create test applier for the replica
	applier, err := newTestWALEntryApplier(replicaWalDir)
	if err != nil {
		t.Fatalf("Failed to create test applier: %v", err)
	}
	defer applier.Close()

	// Create replica
	config := DefaultReplicaConfig()
	replica, err := NewReplica(0, applier, config)
	if err != nil {
		t.Fatalf("Failed to create replica: %v", err)
	}

	// Set custom connector for testing
	replica.SetConnector(&bufConnServerConnector{client: client})

	// Test initial state
	if got, want := replica.GetCurrentState(), StateConnecting; got != want {
		t.Errorf("Initial state = %v, want %v", got, want)
	}

	// Test connecting state transition
	err = replica.handleConnectingState()
	if err != nil {
		t.Errorf("handleConnectingState() error = %v", err)
	}
	if got, want := replica.GetCurrentState(), StateStreamingEntries; got != want {
		t.Errorf("State after connecting = %v, want %v", got, want)
	}

	// Test error state transition
	err = replica.stateTracker.SetError(fmt.Errorf("test error"))
	if err != nil {
		t.Errorf("SetError() error = %v", err)
	}
	if got, want := replica.GetCurrentState(), StateError; got != want {
		t.Errorf("State after error = %v, want %v", got, want)
	}

	// Clean up
	if err := replica.Stop(); err != nil {
		t.Errorf("Failed to stop replica: %v", err)
	}
}

// Test error handling and recovery
func TestReplicaErrorRecovery(t *testing.T) {
	// Setup test environment
	replicaWalDir, primaryWAL, _, client, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create test applier for the replica
	applier, err := newTestWALEntryApplier(replicaWalDir)
	if err != nil {
		t.Fatalf("Failed to create test applier: %v", err)
	}
	defer applier.Close()

	// Create replica with fast retry settings
	config := DefaultReplicaConfig()
	config.Connection.RetryBaseDelay = 50 * time.Millisecond
	config.Connection.RetryMaxDelay = 200 * time.Millisecond
	replica, err := NewReplica(0, applier, config)
	if err != nil {
		t.Fatalf("Failed to create replica: %v", err)
	}

	// Set custom connector for testing
	replica.SetConnector(&bufConnServerConnector{client: client})

	// Start the replica
	if err := replica.Start(); err != nil {
		t.Fatalf("Failed to start replica: %v", err)
	}

	// Write some initial entries to the primary WAL
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i+1))
		value := []byte(fmt.Sprintf("value%d", i+1))
		if _, err := primaryWAL.Append(wal.OpTypePut, key, value); err != nil {
			t.Fatalf("Failed to append to primary WAL: %v", err)
		}
	}
	if err := primaryWAL.Sync(); err != nil {
		t.Fatalf("Failed to sync primary WAL: %v", err)
	}

	// Wait for initial replication
	time.Sleep(500 * time.Millisecond)

	// Simulate an applier failure
	applier.SetShouldFail(true)

	// Write more entries that will cause errors
	for i := 5; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i+1))
		value := []byte(fmt.Sprintf("value%d", i+1))
		if _, err := primaryWAL.Append(wal.OpTypePut, key, value); err != nil {
			t.Fatalf("Failed to append to primary WAL: %v", err)
		}
	}
	if err := primaryWAL.Sync(); err != nil {
		t.Fatalf("Failed to sync primary WAL: %v", err)
	}

	// Wait for error to occur
	time.Sleep(200 * time.Millisecond)

	// Fix the applier and allow recovery
	applier.SetShouldFail(false)

	// Wait for recovery to complete
	time.Sleep(1 * time.Second)

	// Verify that at least some entries were applied
	appliedEntries := applier.GetAppliedEntries()
	if len(appliedEntries) == 0 {
		t.Error("No entries were applied")
	}

	// Stop the replica
	if err := replica.Stop(); err != nil {
		t.Errorf("Failed to stop replica: %v", err)
	}
}
