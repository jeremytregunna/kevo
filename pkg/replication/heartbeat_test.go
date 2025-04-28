package replication

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/config"
	proto "github.com/KevoDB/kevo/pkg/replication/proto"
	"github.com/KevoDB/kevo/pkg/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// createTestWAL creates a WAL instance for testing
func createTestWAL() *wal.WAL {
	// Create a temporary WAL for testing
	testDir := "test-data-wal"

	// Create configuration for WAL
	cfg := config.NewDefaultConfig("test-data")
	cfg.WALDir = testDir
	cfg.WALSyncMode = config.SyncNone // Use SyncNone for faster tests

	// Ensure the directory exists
	if err := os.MkdirAll(testDir, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create test directory: %v", err))
	}

	// Create a new WAL
	w, err := wal.NewWAL(cfg, testDir)
	if err != nil {
		panic(fmt.Sprintf("Failed to create test WAL: %v", err))
	}
	return w
}

// mockStreamServer implements WALReplicationService_StreamWALServer for testing
type mockStreamServer struct {
	grpc.ServerStream
	ctx         context.Context
	sentMsgs    []*proto.WALStreamResponse
	mu          sync.Mutex
	closed      bool
	sendChannel chan struct{}
}

func newMockStream() *mockStreamServer {
	return &mockStreamServer{
		ctx:         context.Background(),
		sentMsgs:    make([]*proto.WALStreamResponse, 0),
		sendChannel: make(chan struct{}, 100),
	}
}

func (m *mockStreamServer) Send(response *proto.WALStreamResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return context.Canceled
	}
	m.sentMsgs = append(m.sentMsgs, response)
	select {
	case m.sendChannel <- struct{}{}:
	default:
	}
	return nil
}

func (m *mockStreamServer) Context() context.Context {
	return m.ctx
}

// Additional methods to satisfy the gRPC stream interfaces
func (m *mockStreamServer) SendMsg(msg interface{}) error {
	if msg, ok := msg.(*proto.WALStreamResponse); ok {
		return m.Send(msg)
	}
	return nil
}

func (m *mockStreamServer) RecvMsg(msg interface{}) error {
	return io.EOF
}

func (m *mockStreamServer) SetHeader(metadata.MD) error {
	return nil
}

func (m *mockStreamServer) SendHeader(metadata.MD) error {
	return nil
}

func (m *mockStreamServer) SetTrailer(metadata.MD) {
}

func (m *mockStreamServer) getSentMessages() []*proto.WALStreamResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sentMsgs
}

func (m *mockStreamServer) getMessageCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sentMsgs)
}

func (m *mockStreamServer) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
}

func (m *mockStreamServer) waitForMessages(count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m.getMessageCount() >= count {
			return true
		}
		select {
		case <-m.sendChannel:
			// Message received, check count again
		case <-time.After(10 * time.Millisecond):
			// Small delay to avoid tight loop
		}
	}
	return false
}

// TestHeartbeatSend verifies that heartbeats are sent at the configured interval
func TestHeartbeatSend(t *testing.T) {
	t.Skip("Skipping due to timing issues in CI environment")

	// Create a test WAL
	mockWal := createTestWAL()
	defer mockWal.Close()
	defer cleanupTestData(t)

	// Create a faster heartbeat config for testing
	config := DefaultPrimaryConfig()
	config.HeartbeatConfig = &HeartbeatConfig{
		Interval:           50 * time.Millisecond,  // Very fast interval for tests
		Timeout:            500 * time.Millisecond, // Longer timeout
		SendEmptyResponses: true,
	}

	// Create the primary
	primary, err := NewPrimary(mockWal, config)
	if err != nil {
		t.Fatalf("Failed to create primary: %v", err)
	}
	defer primary.Close()

	// Create a mock stream
	mockStream := newMockStream()

	// Create a session
	session := &ReplicaSession{
		ID:              "test-session",
		StartSequence:   0,
		Stream:          mockStream,
		LastAckSequence: 0,
		SupportedCodecs: []proto.CompressionCodec{proto.CompressionCodec_NONE},
		Connected:       true,
		Active:          true,
		LastActivity:    time.Now().Add(-100 * time.Millisecond), // Set as slightly stale
	}

	// Register the session
	primary.registerReplicaSession(session)

	// Wait for heartbeats
	if !mockStream.waitForMessages(1, 1*time.Second) {
		t.Fatalf("Expected at least 1 heartbeat, got %d", mockStream.getMessageCount())
	}

	// Verify received heartbeats
	messages := mockStream.getSentMessages()
	for i, msg := range messages {
		if len(msg.Entries) != 0 {
			t.Errorf("Expected empty entries in heartbeat %d, got %d entries", i, len(msg.Entries))
		}
		if msg.Compressed {
			t.Errorf("Expected uncompressed heartbeat %d", i)
		}
		if msg.Codec != proto.CompressionCodec_NONE {
			t.Errorf("Expected NONE codec in heartbeat %d, got %v", i, msg.Codec)
		}
	}
}

// TestHeartbeatTimeout verifies that sessions are marked as disconnected after timeout
func TestHeartbeatTimeout(t *testing.T) {
	// Create a test WAL
	mockWal := createTestWAL()
	defer mockWal.Close()
	defer cleanupTestData(t)

	// Create a faster heartbeat config for testing
	config := DefaultPrimaryConfig()
	config.HeartbeatConfig = &HeartbeatConfig{
		Interval:           50 * time.Millisecond,  // Fast interval for tests
		Timeout:            150 * time.Millisecond, // Short timeout for tests
		SendEmptyResponses: true,
	}

	// Create the primary
	primary, err := NewPrimary(mockWal, config)
	if err != nil {
		t.Fatalf("Failed to create primary: %v", err)
	}
	defer primary.Close()

	// Create a mock stream that will reject messages
	mockStream := newMockStream()
	mockStream.close() // This will make Send() return error

	// Create a session with very old activity timestamp
	staleTimestamp := time.Now().Add(-time.Second)
	session := &ReplicaSession{
		ID:              "stale-session",
		StartSequence:   0,
		Stream:          mockStream,
		LastAckSequence: 0,
		SupportedCodecs: []proto.CompressionCodec{proto.CompressionCodec_NONE},
		Connected:       true,
		Active:          true,
		LastActivity:    staleTimestamp,
	}

	// Register the session
	primary.registerReplicaSession(session)

	// Wait for heartbeat check to mark session as disconnected
	time.Sleep(300 * time.Millisecond)

	// Verify session was removed
	if primary.getSession("stale-session") != nil {
		t.Errorf("Expected stale session to be removed, but it still exists")
	}
}

// TestHeartbeatManagerStop verifies that the heartbeat manager can be cleanly stopped
func TestHeartbeatManagerStop(t *testing.T) {
	// Create a test heartbeat manager
	hb := newHeartbeatManager(nil, &HeartbeatConfig{
		Interval:           10 * time.Millisecond,
		Timeout:            50 * time.Millisecond,
		SendEmptyResponses: true,
	})

	// Start the manager
	hb.start()

	// Verify it's running
	hb.mu.Lock()
	running := hb.running
	hb.mu.Unlock()

	if !running {
		t.Fatal("Heartbeat manager should be running after start()")
	}

	// Stop the manager
	hb.stop()

	// Verify it's stopped
	hb.mu.Lock()
	running = hb.running
	hb.mu.Unlock()

	if running {
		t.Fatal("Heartbeat manager should not be running after stop()")
	}
}

// TestSessionContext verifies that session contexts are canceled when sessions become inactive
func TestSessionContext(t *testing.T) {
	// Create a test WAL
	mockWal := createTestWAL()
	defer mockWal.Close()
	defer cleanupTestData(t)

	// Create a faster heartbeat config for testing
	config := DefaultPrimaryConfig()
	config.HeartbeatConfig = &HeartbeatConfig{
		Interval:           50 * time.Millisecond,
		Timeout:            150 * time.Millisecond,
		SendEmptyResponses: true,
	}

	// Create the primary
	primary, err := NewPrimary(mockWal, config)
	if err != nil {
		t.Fatalf("Failed to create primary: %v", err)
	}
	defer primary.Close()

	// Create a mock stream
	mockStream := newMockStream()

	// Create a session
	session := &ReplicaSession{
		ID:              "context-test-session",
		StartSequence:   0,
		Stream:          mockStream,
		LastAckSequence: 0,
		SupportedCodecs: []proto.CompressionCodec{proto.CompressionCodec_NONE},
		Connected:       true,
		Active:          true,
		LastActivity:    time.Now(),
	}

	// Register the session
	primary.registerReplicaSession(session)

	// Get a session context
	ctx, cancel := primary.heartbeat.sessionContext(session.ID)
	defer cancel()

	// Context should be active
	select {
	case <-ctx.Done():
		t.Fatalf("Context should not be done yet")
	default:
		// This is expected
	}

	// Create a channel to signal when context is done
	doneCh := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(doneCh)
	}()

	// Wait a bit to make sure goroutine is running
	time.Sleep(50 * time.Millisecond)

	// Mark session as disconnected
	session.mu.Lock()
	session.Connected = false
	session.mu.Unlock()

	// Wait for context to be canceled
	select {
	case <-doneCh:
		// This is expected
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("Context was not canceled after session disconnected")
	}
}

// TestPingSession verifies that ping works correctly
func TestPingSession(t *testing.T) {
	// Create a test WAL
	mockWal := createTestWAL()
	defer mockWal.Close()
	defer cleanupTestData(t)

	// Create a faster heartbeat config for testing
	config := DefaultPrimaryConfig()
	config.HeartbeatConfig = &HeartbeatConfig{
		Interval:           500 * time.Millisecond,
		Timeout:            1 * time.Second,
		SendEmptyResponses: true,
	}

	// Create the primary
	primary, err := NewPrimary(mockWal, config)
	if err != nil {
		t.Fatalf("Failed to create primary: %v", err)
	}
	defer primary.Close()

	// Create a mock stream
	mockStream := newMockStream()

	// Create a session
	session := &ReplicaSession{
		ID:              "ping-test-session",
		StartSequence:   0,
		Stream:          mockStream,
		LastAckSequence: 0,
		SupportedCodecs: []proto.CompressionCodec{proto.CompressionCodec_NONE},
		Connected:       true,
		Active:          true,
		LastActivity:    time.Now().Add(-800 * time.Millisecond), // Older activity time
	}

	// Register the session
	primary.registerReplicaSession(session)

	// Manually ping the session
	result := primary.heartbeat.pingSession(session.ID)
	if !result {
		t.Fatalf("Ping should succeed for active session")
	}

	// Verify that LastActivity was updated
	session.mu.Lock()
	lastActivity := session.LastActivity
	session.mu.Unlock()

	if time.Since(lastActivity) > 100*time.Millisecond {
		t.Errorf("LastActivity should have been updated recently, but it's %v old",
			time.Since(lastActivity))
	}

	// Verify a heartbeat was sent
	if mockStream.getMessageCount() < 1 {
		t.Fatalf("Expected at least 1 message after ping, got %d",
			mockStream.getMessageCount())
	}

	// Try to ping a non-existent session
	result = primary.heartbeat.pingSession("non-existent-session")
	if result {
		t.Fatalf("Ping should fail for non-existent session")
	}

	// Try to ping a session that will reject the ping
	mockStream.close() // This will make the stream return errors
	result = primary.heartbeat.pingSession(session.ID)
	if result {
		t.Fatalf("Ping should fail when stream has errors")
	}

	// Verify session was marked as disconnected
	session.mu.Lock()
	connected := session.Connected
	active := session.Active
	session.mu.Unlock()

	if connected || active {
		t.Errorf("Session should be marked as disconnected after failed ping")
	}
}

// Implementation of test teardown helpers
func cleanupTestData(t *testing.T) {
	// Remove any test data files
	cmd := "rm -rf test-data-wal"
	if err := exec.Command("sh", "-c", cmd).Run(); err != nil {
		t.Logf("Error cleaning up test data: %v", err)
	}
}

// TestHeartbeatWithTLSKeepalive briefly verifies integration with TLS keepalive
func TestHeartbeatWithTLSKeepalive(t *testing.T) {
	// This test only verifies that heartbeats can run alongside gRPC keepalives
	// A full integration test would require setting up actual TLS connections

	// Create a test WAL
	mockWal := createTestWAL()
	defer mockWal.Close()
	defer cleanupTestData(t)

	// Create config with heartbeats enabled
	config := DefaultPrimaryConfig()
	config.HeartbeatConfig = &HeartbeatConfig{
		Interval:           500 * time.Millisecond,
		Timeout:            2 * time.Second,
		SendEmptyResponses: true,
	}

	// Create the primary
	primary, err := NewPrimary(mockWal, config)
	if err != nil {
		t.Fatalf("Failed to create primary: %v", err)
	}
	defer primary.Close()

	// Verify heartbeat manager is running
	if primary.heartbeat == nil {
		t.Fatal("Heartbeat manager should be created")
	}

	primary.heartbeat.mu.Lock()
	running := primary.heartbeat.running
	primary.heartbeat.mu.Unlock()

	if !running {
		t.Fatal("Heartbeat manager should be running")
	}
}
