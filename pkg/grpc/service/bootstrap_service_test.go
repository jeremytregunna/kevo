package service

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transport"
	"github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc"
)

// MockBootstrapStorageSnapshot implements replication.StorageSnapshot for testing
type MockBootstrapStorageSnapshot struct {
	replication.StorageSnapshot
	pairs       []replication.KeyValuePair
	keyCount    int64
	nextErr     error
	position    int
	iterCreated bool
	snapshotLSN uint64
}

func NewMockBootstrapStorageSnapshot(pairs []replication.KeyValuePair) *MockBootstrapStorageSnapshot {
	return &MockBootstrapStorageSnapshot{
		pairs:       pairs,
		keyCount:    int64(len(pairs)),
		snapshotLSN: 12345, // Set default snapshot LSN for tests
	}
}

func (m *MockBootstrapStorageSnapshot) CreateSnapshotIterator() (replication.SnapshotIterator, error) {
	m.position = 0
	m.iterCreated = true
	return m, nil
}

func (m *MockBootstrapStorageSnapshot) KeyCount() int64 {
	return m.keyCount
}

func (m *MockBootstrapStorageSnapshot) Next() ([]byte, []byte, error) {
	if m.nextErr != nil {
		return nil, nil, m.nextErr
	}

	if m.position >= len(m.pairs) {
		return nil, nil, io.EOF
	}

	pair := m.pairs[m.position]
	m.position++

	return pair.Key, pair.Value, nil
}

func (m *MockBootstrapStorageSnapshot) Close() error {
	return nil
}

// MockBootstrapSnapshotProvider implements replication.StorageSnapshotProvider for testing
type MockBootstrapSnapshotProvider struct {
	snapshot  *MockBootstrapStorageSnapshot
	createErr error
}

func NewMockBootstrapSnapshotProvider(snapshot *MockBootstrapStorageSnapshot) *MockBootstrapSnapshotProvider {
	return &MockBootstrapSnapshotProvider{
		snapshot: snapshot,
	}
}

func (m *MockBootstrapSnapshotProvider) CreateSnapshot() (replication.StorageSnapshot, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}
	return m.snapshot, nil
}

// MockBootstrapWALReplicator implements a simple replicator for testing
type MockBootstrapWALReplicator struct {
	replication.WALReplicator
	highestTimestamp uint64
}

func (r *MockBootstrapWALReplicator) GetHighestTimestamp() uint64 {
	return r.highestTimestamp
}

// Mock ReplicationService_RequestBootstrapServer for testing
type mockBootstrapStream struct {
	grpc.ServerStream
	ctx       context.Context
	batches   []*kevo.BootstrapBatch
	sendError error
	mu        sync.Mutex
}

func newMockBootstrapStream() *mockBootstrapStream {
	return &mockBootstrapStream{
		ctx:     context.Background(),
		batches: make([]*kevo.BootstrapBatch, 0),
	}
}

func (m *mockBootstrapStream) Context() context.Context {
	return m.ctx
}

func (m *mockBootstrapStream) Send(batch *kevo.BootstrapBatch) error {
	if m.sendError != nil {
		return m.sendError
	}

	m.mu.Lock()
	m.batches = append(m.batches, batch)
	m.mu.Unlock()

	return nil
}

func (m *mockBootstrapStream) GetBatches() []*kevo.BootstrapBatch {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.batches
}

// Helper function to create a temporary directory for testing
func createTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "bootstrap-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

// Helper function to clean up temporary directory
func cleanupTempDir(t *testing.T, dir string) {
	os.RemoveAll(dir)
}

// TestBootstrapService tests the bootstrap service component
func TestBootstrapService(t *testing.T) {
	// Create test directory
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	// Create test data
	testData := []replication.KeyValuePair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
	}

	// Create mock storage snapshot
	mockSnapshot := NewMockBootstrapStorageSnapshot(testData)

	// Create mock replicator with timestamp
	replicator := &MockBootstrapWALReplicator{
		highestTimestamp: 12345,
	}

	// Create bootstrap service
	options := DefaultBootstrapServiceOptions()
	options.BootstrapBatchSize = 2 // Use small batch size for testing

	bootstrapSvc, err := newBootstrapService(
		options,
		NewMockBootstrapSnapshotProvider(mockSnapshot),
		replicator,
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to create bootstrap service: %v", err)
	}

	// Create mock stream
	stream := newMockBootstrapStream()

	// Create bootstrap request
	req := &kevo.BootstrapRequest{
		ReplicaId: "test-replica",
	}

	// Handle bootstrap request
	err = bootstrapSvc.handleBootstrapRequest(req, stream)
	if err != nil {
		t.Fatalf("Bootstrap request failed: %v", err)
	}

	// Verify batches
	batches := stream.GetBatches()

	// Expected: 3 batches (2 full, 1 final with last item)
	if len(batches) != 3 {
		t.Errorf("Expected 3 batches, got %d", len(batches))
	}

	// Verify first batch
	if len(batches) > 0 {
		batch := batches[0]
		if len(batch.Pairs) != 2 {
			t.Errorf("Expected 2 pairs in first batch, got %d", len(batch.Pairs))
		}
		if batch.IsLast {
			t.Errorf("First batch should not be marked as last")
		}
		if batch.SnapshotLsn != 12345 {
			t.Errorf("Expected snapshot LSN 12345, got %d", batch.SnapshotLsn)
		}
	}

	// Verify final batch
	if len(batches) > 2 {
		batch := batches[2]
		if !batch.IsLast {
			t.Errorf("Final batch should be marked as last")
		}
	}

	// Verify active bootstraps
	bootstrapStatus := bootstrapSvc.getBootstrapStatus()
	if bootstrapStatus["role"] != "primary" {
		t.Errorf("Expected role 'primary', got %s", bootstrapStatus["role"])
	}

	// Get active bootstraps
	activeBootstraps, ok := bootstrapStatus["active_bootstraps"].(map[string]map[string]interface{})
	if !ok {
		t.Fatalf("Expected active_bootstraps to be a map")
	}

	// Verify bootstrap for our test replica
	replicaInfo, exists := activeBootstraps["test-replica"]
	if !exists {
		t.Fatalf("Expected to find test-replica in active bootstraps")
	}

	// Check if bootstrap was completed
	completed, ok := replicaInfo["completed"].(bool)
	if !ok {
		t.Fatalf("Expected 'completed' to be a boolean")
	}

	if !completed {
		t.Errorf("Expected bootstrap to be marked as completed")
	}
}

// TestReplicationService_Bootstrap tests the bootstrap integration in the ReplicationService
func TestReplicationService_Bootstrap(t *testing.T) {
	// Create test directory
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	// Create test data
	testData := []replication.KeyValuePair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}

	// Create mock storage snapshot
	mockSnapshot := NewMockBootstrapStorageSnapshot(testData)

	// Create mock replicator
	replicator := &MockBootstrapWALReplicator{
		highestTimestamp: 12345,
	}

	// Create replication service options
	options := DefaultReplicationServiceOptions()
	options.DataDir = filepath.Join(tempDir, "replication-data")
	options.BootstrapOptions.BootstrapBatchSize = 2 // Small batch size for testing

	// Create replication service
	service, err := NewReplicationService(
		replicator,
		nil, // No WAL applier for this test
		replication.NewEntrySerializer(),
		mockSnapshot,
		options,
	)
	if err != nil {
		t.Fatalf("Failed to create replication service: %v", err)
	}

	// Register a test replica
	service.replicas["test-replica"] = &transport.ReplicaInfo{
		ID:       "test-replica",
		Address:  "localhost:12345",
		Role:     transport.RoleReplica,
		Status:   transport.StatusConnecting,
		LastSeen: time.Now(),
	}

	// Create mock stream
	stream := newMockBootstrapStream()

	// Create bootstrap request
	req := &kevo.BootstrapRequest{
		ReplicaId: "test-replica",
	}

	// Handle bootstrap request
	err = service.RequestBootstrap(req, stream)
	if err != nil {
		t.Fatalf("Bootstrap request failed: %v", err)
	}

	// Verify batches
	batches := stream.GetBatches()

	// Expected: 2 batches (1 full, 1 final)
	if len(batches) < 2 {
		t.Errorf("Expected at least 2 batches, got %d", len(batches))
	}

	// Verify final batch
	lastBatch := batches[len(batches)-1]
	if !lastBatch.IsLast {
		t.Errorf("Final batch should be marked as last")
	}

	// Verify replica status was updated
	service.replicasMutex.RLock()
	replica := service.replicas["test-replica"]
	service.replicasMutex.RUnlock()

	if replica.Status != transport.StatusSyncing {
		t.Errorf("Expected replica status to be StatusSyncing, got %s", replica.Status)
	}

	if replica.CurrentLSN != 12345 {
		t.Errorf("Expected replica LSN to be 12345, got %d", replica.CurrentLSN)
	}
}

// TestBootstrapManager_Integration tests the bootstrap manager component
func TestBootstrapManager_Integration(t *testing.T) {
	// Create test directory
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	// Mock storage applier for testing
	storageApplier := &MockStorageApplier{
		applied: make(map[string][]byte),
	}

	// Create bootstrap manager
	manager, err := replication.NewBootstrapManager(
		storageApplier,
		nil, // No WAL applier for this test
		tempDir,
		nil, // Use default logger
	)
	if err != nil {
		t.Fatalf("Failed to create bootstrap manager: %v", err)
	}

	// Create test bootstrap data
	testData := []replication.KeyValuePair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}

	// Create mock bootstrap iterator
	iterator := &MockBootstrapIterator{
		pairs:       testData,
		snapshotLSN: 12345,
	}
	
	// Set the snapshot LSN on the bootstrap manager
	manager.SetSnapshotLSN(iterator.snapshotLSN)

	// Start bootstrap
	err = manager.StartBootstrap("test-replica", iterator, 2)
	if err != nil {
		t.Fatalf("Failed to start bootstrap: %v", err)
	}

	// Wait for bootstrap to complete
	for i := 0; i < 50; i++ {
		if !manager.IsBootstrapInProgress() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify bootstrap completed
	if manager.IsBootstrapInProgress() {
		t.Fatalf("Bootstrap did not complete in time")
	}

	// Verify all keys were applied
	if storageApplier.appliedCount != len(testData) {
		t.Errorf("Expected %d applied keys, got %d", len(testData), storageApplier.appliedCount)
	}

	// Verify bootstrap state
	state := manager.GetBootstrapState()
	if state == nil {
		t.Fatalf("Bootstrap state is nil")
	}

	if !state.Completed {
		t.Errorf("Expected bootstrap to be marked as completed")
	}

	if state.Progress != 1.0 {
		t.Errorf("Expected progress to be 1.0, got %f", state.Progress)
	}
}

// MockStorageApplier implements replication.StorageApplier for testing
type MockStorageApplier struct {
	applied      map[string][]byte
	appliedCount int
	flushCount   int
}

func (m *MockStorageApplier) Apply(key, value []byte) error {
	m.applied[string(key)] = value
	m.appliedCount++
	return nil
}

func (m *MockStorageApplier) ApplyBatch(pairs []replication.KeyValuePair) error {
	for _, pair := range pairs {
		m.applied[string(pair.Key)] = pair.Value
	}
	m.appliedCount += len(pairs)
	return nil
}

func (m *MockStorageApplier) Flush() error {
	m.flushCount++
	return nil
}

// MockBootstrapIterator implements transport.BootstrapIterator for testing
type MockBootstrapIterator struct {
	pairs       []replication.KeyValuePair
	position    int
	snapshotLSN uint64
	progress    float64
}

func (m *MockBootstrapIterator) Next() ([]byte, []byte, error) {
	if m.position >= len(m.pairs) {
		return nil, nil, io.EOF
	}

	pair := m.pairs[m.position]
	m.position++

	if len(m.pairs) > 0 {
		m.progress = float64(m.position) / float64(len(m.pairs))
	} else {
		m.progress = 1.0
	}

	return pair.Key, pair.Value, nil
}

func (m *MockBootstrapIterator) Close() error {
	return nil
}

func (m *MockBootstrapIterator) Progress() float64 {
	return m.progress
}
