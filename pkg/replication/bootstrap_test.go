package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/common/log"
)

// MockStorageApplier implements StorageApplier for testing
type MockStorageApplier struct {
	applied      map[string][]byte
	appliedCount int
	appliedMu    sync.Mutex
	flushCount   int
	failApply    bool
	failFlush    bool
}

func NewMockStorageApplier() *MockStorageApplier {
	return &MockStorageApplier{
		applied: make(map[string][]byte),
	}
}

func (m *MockStorageApplier) Apply(key, value []byte) error {
	m.appliedMu.Lock()
	defer m.appliedMu.Unlock()

	if m.failApply {
		return ErrBootstrapFailed
	}

	m.applied[string(key)] = value
	m.appliedCount++
	return nil
}

func (m *MockStorageApplier) ApplyBatch(pairs []KeyValuePair) error {
	m.appliedMu.Lock()
	defer m.appliedMu.Unlock()

	if m.failApply {
		return ErrBootstrapFailed
	}

	for _, pair := range pairs {
		m.applied[string(pair.Key)] = pair.Value
	}

	m.appliedCount += len(pairs)
	return nil
}

func (m *MockStorageApplier) Flush() error {
	m.appliedMu.Lock()
	defer m.appliedMu.Unlock()

	if m.failFlush {
		return ErrBootstrapFailed
	}

	m.flushCount++
	return nil
}

func (m *MockStorageApplier) GetAppliedCount() int {
	m.appliedMu.Lock()
	defer m.appliedMu.Unlock()
	return m.appliedCount
}

func (m *MockStorageApplier) SetFailApply(fail bool) {
	m.appliedMu.Lock()
	defer m.appliedMu.Unlock()
	m.failApply = fail
}

func (m *MockStorageApplier) SetFailFlush(fail bool) {
	m.appliedMu.Lock()
	defer m.appliedMu.Unlock()
	m.failFlush = fail
}

// MockBootstrapIterator implements transport.BootstrapIterator for testing
type MockBootstrapIterator struct {
	pairs        []KeyValuePair
	position     int
	snapshotLSN  uint64
	progress     float64
	failAfter    int
	closeError   error
	progressFunc func(pos int) float64
}

func NewMockBootstrapIterator(pairs []KeyValuePair, snapshotLSN uint64) *MockBootstrapIterator {
	return &MockBootstrapIterator{
		pairs:       pairs,
		snapshotLSN: snapshotLSN,
		failAfter:   -1, // Don't fail by default
		progressFunc: func(pos int) float64 {
			if len(pairs) == 0 {
				return 1.0
			}
			return float64(pos) / float64(len(pairs))
		},
	}
}

func (m *MockBootstrapIterator) Next() ([]byte, []byte, error) {
	if m.position >= len(m.pairs) {
		return nil, nil, io.EOF
	}

	if m.failAfter > 0 && m.position >= m.failAfter {
		return nil, nil, ErrBootstrapFailed
	}

	pair := m.pairs[m.position]
	m.position++
	m.progress = m.progressFunc(m.position)

	return pair.Key, pair.Value, nil
}

func (m *MockBootstrapIterator) Close() error {
	return m.closeError
}

func (m *MockBootstrapIterator) Progress() float64 {
	return m.progress
}

func (m *MockBootstrapIterator) SetFailAfter(failAfter int) {
	m.failAfter = failAfter
}

func (m *MockBootstrapIterator) SetCloseError(err error) {
	m.closeError = err
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

// Define JSON helpers for tests
func testWriteJSONFile(file *os.File, v interface{}) error {
	encoder := json.NewEncoder(file)
	return encoder.Encode(v)
}

func testReadJSONFile(file *os.File, v interface{}) error {
	decoder := json.NewDecoder(file)
	return decoder.Decode(v)
}

// TestBootstrapManager_Basic tests basic bootstrap functionality
func TestBootstrapManager_Basic(t *testing.T) {
	// Create test directory
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	// Create test data
	testData := []KeyValuePair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
	}

	// Create mock components
	storageApplier := NewMockStorageApplier()
	logger := log.GetDefaultLogger()

	// Create bootstrap manager
	manager, err := NewBootstrapManager(storageApplier, nil, tempDir, logger)
	if err != nil {
		t.Fatalf("Failed to create bootstrap manager: %v", err)
	}

	// Create mock bootstrap iterator
	snapshotLSN := uint64(12345)
	iterator := NewMockBootstrapIterator(testData, snapshotLSN)

	// Start bootstrap process
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

	// We don't check the exact count here, as it may include previously applied items
	// and it's an implementation detail whether they get reapplied or skipped
	appliedCount := storageApplier.GetAppliedCount()
	if appliedCount < len(testData) {
		t.Errorf("Expected at least %d applied items, got %d", len(testData), appliedCount)
	}

	// Verify bootstrap state
	state := manager.GetBootstrapState()
	if state == nil {
		t.Fatalf("Bootstrap state is nil")
	}

	if !state.Completed {
		t.Errorf("Bootstrap state should be marked as completed")
	}

	if state.AppliedKeys != len(testData) {
		t.Errorf("Expected %d applied keys in state, got %d", len(testData), state.AppliedKeys)
	}

	if state.Progress != 1.0 {
		t.Errorf("Expected progress 1.0, got %f", state.Progress)
	}
}

// TestBootstrapManager_Resume tests bootstrap resumability
func TestBootstrapManager_Resume(t *testing.T) {
	// Create test directory
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	// Create test data
	testData := []KeyValuePair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
		{Key: []byte("key6"), Value: []byte("value6")},
		{Key: []byte("key7"), Value: []byte("value7")},
		{Key: []byte("key8"), Value: []byte("value8")},
		{Key: []byte("key9"), Value: []byte("value9")},
		{Key: []byte("key10"), Value: []byte("value10")},
	}

	// Create mock components
	storageApplier := NewMockStorageApplier()
	logger := log.GetDefaultLogger()

	// Create bootstrap manager
	manager, err := NewBootstrapManager(storageApplier, nil, tempDir, logger)
	if err != nil {
		t.Fatalf("Failed to create bootstrap manager: %v", err)
	}

	// Create initial bootstrap iterator that will fail after 2 items
	snapshotLSN := uint64(12345)
	iterator1 := NewMockBootstrapIterator(testData, snapshotLSN)
	iterator1.SetFailAfter(2)

	// Start first bootstrap attempt
	err = manager.StartBootstrap("test-replica", iterator1, 2)
	if err != nil {
		t.Fatalf("Failed to start bootstrap: %v", err)
	}

	// Wait for the bootstrap to fail
	for i := 0; i < 50; i++ {
		if !manager.IsBootstrapInProgress() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify bootstrap state shows failure
	state1 := manager.GetBootstrapState()
	if state1 == nil {
		t.Fatalf("Bootstrap state is nil after failed attempt")
	}

	if state1.Completed {
		t.Errorf("Bootstrap state should not be marked as completed after failure")
	}

	if state1.AppliedKeys != 2 {
		t.Errorf("Expected 2 applied keys in state after failure, got %d", state1.AppliedKeys)
	}

	// Create a new bootstrap manager that should load the existing state
	manager2, err := NewBootstrapManager(storageApplier, nil, tempDir, logger)
	if err != nil {
		t.Fatalf("Failed to create second bootstrap manager: %v", err)
	}

	// Create a new iterator for the resume
	iterator2 := NewMockBootstrapIterator(testData, snapshotLSN)

	// Start the resumed bootstrap
	err = manager2.StartBootstrap("test-replica", iterator2, 2)
	if err != nil {
		t.Fatalf("Failed to start resumed bootstrap: %v", err)
	}

	// Wait for bootstrap to complete
	for i := 0; i < 50; i++ {
		if !manager2.IsBootstrapInProgress() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify bootstrap completed
	if manager2.IsBootstrapInProgress() {
		t.Fatalf("Resumed bootstrap did not complete in time")
	}

	// We don't check the exact count here, as it may include previously applied items
	// and it's an implementation detail whether they get reapplied or skipped
	appliedCount := storageApplier.GetAppliedCount()
	if appliedCount < len(testData) {
		t.Errorf("Expected at least %d applied items, got %d", len(testData), appliedCount)
	}

	// Verify bootstrap state
	state2 := manager2.GetBootstrapState()
	if state2 == nil {
		t.Fatalf("Bootstrap state is nil after resume")
	}

	if !state2.Completed {
		t.Errorf("Bootstrap state should be marked as completed after resume")
	}

	if state2.AppliedKeys != len(testData) {
		t.Errorf("Expected %d applied keys in state after resume, got %d", len(testData), state2.AppliedKeys)
	}

	if state2.Progress != 1.0 {
		t.Errorf("Expected progress 1.0 after resume, got %f", state2.Progress)
	}
}

// TestBootstrapManager_WALTransition tests transition to WAL replication
func TestBootstrapManager_WALTransition(t *testing.T) {
	// Create test directory
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	// Create test data
	testData := []KeyValuePair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}

	// Create mock components
	storageApplier := NewMockStorageApplier()

	// Create mock WAL applier
	walApplier := &MockWALApplier{
		mu:             sync.RWMutex{},
		highestApplied: uint64(1000),
	}

	logger := log.GetDefaultLogger()

	// Create bootstrap manager
	manager, err := NewBootstrapManager(storageApplier, walApplier, tempDir, logger)
	if err != nil {
		t.Fatalf("Failed to create bootstrap manager: %v", err)
	}

	// Create mock bootstrap iterator
	snapshotLSN := uint64(12345)
	iterator := NewMockBootstrapIterator(testData, snapshotLSN)

	// Set the snapshot LSN
	manager.SetSnapshotLSN(snapshotLSN)

	// Start bootstrap process
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

	// Transition to WAL replication
	err = manager.TransitionToWALReplication()
	if err != nil {
		t.Fatalf("Failed to transition to WAL replication: %v", err)
	}

	// Verify WAL applier's highest applied LSN was updated
	walApplier.mu.RLock()
	highestApplied := walApplier.highestApplied
	walApplier.mu.RUnlock()

	if highestApplied != snapshotLSN {
		t.Errorf("Expected WAL applier highest applied LSN to be %d, got %d", snapshotLSN, highestApplied)
	}
}

// TestBootstrapGenerator_Basic tests basic bootstrap generator functionality
func TestBootstrapGenerator_Basic(t *testing.T) {
	// Create test data
	testData := []KeyValuePair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
	}

	// Create mock storage snapshot
	mockSnapshot := NewMemoryStorageSnapshot(testData)

	// Create mock snapshot provider
	snapshotProvider := &MockSnapshotProvider{
		snapshot: mockSnapshot,
	}

	// Create mock replicator
	replicator := &WALReplicator{
		highestTimestamp: 12345,
	}

	// Create bootstrap generator
	generator := NewBootstrapGenerator(snapshotProvider, replicator, nil)

	// Start bootstrap generation
	ctx := context.Background()
	iterator, snapshotLSN, err := generator.StartBootstrapGeneration(ctx, "test-replica")
	if err != nil {
		t.Fatalf("Failed to start bootstrap generation: %v", err)
	}

	// Verify snapshotLSN
	if snapshotLSN != 12345 {
		t.Errorf("Expected snapshot LSN 12345, got %d", snapshotLSN)
	}

	// Read all data
	var receivedData []KeyValuePair
	for {
		key, value, err := iterator.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error reading from iterator: %v", err)
		}

		receivedData = append(receivedData, KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	// Verify all data was received
	if len(receivedData) != len(testData) {
		t.Errorf("Expected %d items, got %d", len(testData), len(receivedData))
	}

	// Verify active bootstraps
	activeBootstraps := generator.GetActiveBootstraps()
	if len(activeBootstraps) != 1 {
		t.Errorf("Expected 1 active bootstrap, got %d", len(activeBootstraps))
	}

	replicaInfo, exists := activeBootstraps["test-replica"]
	if !exists {
		t.Fatalf("Expected to find test-replica in active bootstraps")
	}

	completed, ok := replicaInfo["completed"].(bool)
	if !ok {
		t.Fatalf("Expected 'completed' to be a boolean")
	}

	if !completed {
		t.Errorf("Expected bootstrap to be marked as completed")
	}
}

// TestBootstrapGenerator_Cancel tests cancellation of bootstrap generation
func TestBootstrapGenerator_Cancel(t *testing.T) {
	// Create test data
	var testData []KeyValuePair
	for i := 0; i < 1000; i++ {
		testData = append(testData, KeyValuePair{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: []byte(fmt.Sprintf("value%d", i)),
		})
	}

	// Create mock storage snapshot
	mockSnapshot := NewMemoryStorageSnapshot(testData)

	// Create mock snapshot provider
	snapshotProvider := &MockSnapshotProvider{
		snapshot: mockSnapshot,
	}

	// Create mock replicator
	replicator := &WALReplicator{
		highestTimestamp: 12345,
	}

	// Create bootstrap generator
	generator := NewBootstrapGenerator(snapshotProvider, replicator, nil)

	// Start bootstrap generation
	ctx := context.Background()
	iterator, _, err := generator.StartBootstrapGeneration(ctx, "test-replica")
	if err != nil {
		t.Fatalf("Failed to start bootstrap generation: %v", err)
	}

	// Read a few items
	for i := 0; i < 5; i++ {
		_, _, err := iterator.Next()
		if err != nil {
			t.Fatalf("Error reading from iterator: %v", err)
		}
	}

	// Cancel the bootstrap
	cancelled := generator.CancelBootstrapGeneration("test-replica")
	if !cancelled {
		t.Errorf("Expected to cancel bootstrap, but CancelBootstrapGeneration returned false")
	}

	// Try to read more items, should get cancelled error
	_, _, err = iterator.Next()
	if err != ErrBootstrapGenerationCancelled {
		t.Errorf("Expected ErrBootstrapGenerationCancelled, got %v", err)
	}

	// Verify active bootstraps
	activeBootstraps := generator.GetActiveBootstraps()
	replicaInfo, exists := activeBootstraps["test-replica"]
	if !exists {
		t.Fatalf("Expected to find test-replica in active bootstraps")
	}

	cancelled, ok := replicaInfo["cancelled"].(bool)
	if !ok {
		t.Fatalf("Expected 'cancelled' to be a boolean")
	}

	if !cancelled {
		t.Errorf("Expected bootstrap to be marked as cancelled")
	}
}

// MockSnapshotProvider implements StorageSnapshotProvider for testing
type MockSnapshotProvider struct {
	snapshot    StorageSnapshot
	createError error
}

func (m *MockSnapshotProvider) CreateSnapshot() (StorageSnapshot, error) {
	if m.createError != nil {
		return nil, m.createError
	}
	return m.snapshot, nil
}

// MockWALReplicator simulates WALReplicator for tests
type MockWALReplicator struct {
	highestTimestamp uint64
}

func (r *MockWALReplicator) GetHighestTimestamp() uint64 {
	return r.highestTimestamp
}

// MockWALApplier simulates WALApplier for tests
type MockWALApplier struct {
	mu             sync.RWMutex
	highestApplied uint64
}

func (a *MockWALApplier) ResetHighestApplied(lsn uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.highestApplied = lsn
}
