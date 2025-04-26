package transport

import (
	"context"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/wal"
)

// MockReplicationClient implements ReplicationClient for testing
type MockReplicationClient struct {
	connected          bool
	registeredAsReplica bool
	heartbeatSent       bool
	walEntriesRequested bool
	bootstrapRequested  bool
	replicaID           string
	walEntries          []*wal.Entry
	bootstrapIterator   BootstrapIterator
	status              TransportStatus
}

func NewMockReplicationClient() *MockReplicationClient {
	return &MockReplicationClient{
		connected:          false,
		registeredAsReplica: false,
		heartbeatSent:       false,
		walEntriesRequested: false,
		bootstrapRequested:  false,
		status: TransportStatus{
			Connected:     false,
			LastConnected: time.Time{},
			LastError:     nil,
			BytesSent:     0,
			BytesReceived: 0,
			RTT:           0,
		},
	}
}

func (c *MockReplicationClient) Connect(ctx context.Context) error {
	c.connected = true
	c.status.Connected = true
	c.status.LastConnected = time.Now()
	return nil
}

func (c *MockReplicationClient) Close() error {
	c.connected = false
	c.status.Connected = false
	return nil
}

func (c *MockReplicationClient) IsConnected() bool {
	return c.connected
}

func (c *MockReplicationClient) Status() TransportStatus {
	return c.status
}

func (c *MockReplicationClient) Send(ctx context.Context, request Request) (Response, error) {
	return nil, ErrInvalidRequest
}

func (c *MockReplicationClient) Stream(ctx context.Context) (Stream, error) {
	return nil, ErrInvalidRequest
}

func (c *MockReplicationClient) RegisterAsReplica(ctx context.Context, replicaID string) error {
	c.registeredAsReplica = true
	c.replicaID = replicaID
	return nil
}

func (c *MockReplicationClient) SendHeartbeat(ctx context.Context, status *ReplicaInfo) error {
	c.heartbeatSent = true
	return nil
}

func (c *MockReplicationClient) RequestWALEntries(ctx context.Context, fromLSN uint64) ([]*wal.Entry, error) {
	c.walEntriesRequested = true
	return c.walEntries, nil
}

func (c *MockReplicationClient) RequestBootstrap(ctx context.Context) (BootstrapIterator, error) {
	c.bootstrapRequested = true
	return c.bootstrapIterator, nil
}

// MockBootstrapIterator implements BootstrapIterator for testing
type MockBootstrapIterator struct {
	pairs    []struct{ key, value []byte }
	position int
	progress float64
	closed   bool
}

func NewMockBootstrapIterator() *MockBootstrapIterator {
	return &MockBootstrapIterator{
		pairs: []struct{ key, value []byte }{
			{[]byte("key1"), []byte("value1")},
			{[]byte("key2"), []byte("value2")},
			{[]byte("key3"), []byte("value3")},
		},
		position: 0,
		progress: 0.0,
		closed:   false,
	}
}

func (it *MockBootstrapIterator) Next() ([]byte, []byte, error) {
	if it.position >= len(it.pairs) {
		return nil, nil, nil
	}
	
	pair := it.pairs[it.position]
	it.position++
	it.progress = float64(it.position) / float64(len(it.pairs))
	
	return pair.key, pair.value, nil
}

func (it *MockBootstrapIterator) Close() error {
	it.closed = true
	return nil
}

func (it *MockBootstrapIterator) Progress() float64 {
	return it.progress
}

// Tests

func TestReplicationClientInterface(t *testing.T) {
	// Create a mock client
	client := NewMockReplicationClient()
	
	// Test Connect
	ctx := context.Background()
	err := client.Connect(ctx)
	if err != nil {
		t.Errorf("Connect failed: %v", err)
	}
	
	// Test IsConnected
	if !client.IsConnected() {
		t.Errorf("Expected client to be connected")
	}
	
	// Test Status
	status := client.Status()
	if !status.Connected {
		t.Errorf("Expected status.Connected to be true")
	}
	
	// Test RegisterAsReplica
	err = client.RegisterAsReplica(ctx, "replica1")
	if err != nil {
		t.Errorf("RegisterAsReplica failed: %v", err)
	}
	if !client.registeredAsReplica {
		t.Errorf("Expected client to be registered as replica")
	}
	if client.replicaID != "replica1" {
		t.Errorf("Expected replicaID to be 'replica1', got '%s'", client.replicaID)
	}
	
	// Test SendHeartbeat
	replicaInfo := &ReplicaInfo{
		ID:            "replica1",
		Address:       "localhost:50051",
		Role:          RoleReplica,
		Status:        StatusReady,
		LastSeen:      time.Now(),
		CurrentLSN:    100,
		ReplicationLag: 0,
	}
	err = client.SendHeartbeat(ctx, replicaInfo)
	if err != nil {
		t.Errorf("SendHeartbeat failed: %v", err)
	}
	if !client.heartbeatSent {
		t.Errorf("Expected heartbeat to be sent")
	}
	
	// Test RequestWALEntries
	client.walEntries = []*wal.Entry{
		{SequenceNumber: 101, Type: 1, Key: []byte("key1"), Value: []byte("value1")},
		{SequenceNumber: 102, Type: 1, Key: []byte("key2"), Value: []byte("value2")},
	}
	entries, err := client.RequestWALEntries(ctx, 100)
	if err != nil {
		t.Errorf("RequestWALEntries failed: %v", err)
	}
	if !client.walEntriesRequested {
		t.Errorf("Expected WAL entries to be requested")
	}
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}
	
	// Test RequestBootstrap
	client.bootstrapIterator = NewMockBootstrapIterator()
	iterator, err := client.RequestBootstrap(ctx)
	if err != nil {
		t.Errorf("RequestBootstrap failed: %v", err)
	}
	if !client.bootstrapRequested {
		t.Errorf("Expected bootstrap to be requested")
	}
	
	// Test iterator
	key, value, err := iterator.Next()
	if err != nil {
		t.Errorf("Iterator.Next failed: %v", err)
	}
	if string(key) != "key1" || string(value) != "value1" {
		t.Errorf("Expected key1/value1, got %s/%s", string(key), string(value))
	}
	
	progress := iterator.Progress()
	if progress != 1.0/3.0 {
		t.Errorf("Expected progress to be 1/3, got %f", progress)
	}
	
	// Test Close
	err = client.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
	if client.IsConnected() {
		t.Errorf("Expected client to be disconnected")
	}
	
	// Test iterator Close
	err = iterator.Close()
	if err != nil {
		t.Errorf("Iterator.Close failed: %v", err)
	}
	mockIter := iterator.(*MockBootstrapIterator)
	if !mockIter.closed {
		t.Errorf("Expected iterator to be closed")
	}
}

// MockReplicationServer implements ReplicationServer for testing
type MockReplicationServer struct {
	started           bool
	stopped           bool
	replicas          map[string]*ReplicaInfo
	streamingReplicas map[string]bool
}

func NewMockReplicationServer() *MockReplicationServer {
	return &MockReplicationServer{
		started:           false,
		stopped:           false,
		replicas:          make(map[string]*ReplicaInfo),
		streamingReplicas: make(map[string]bool),
	}
}

func (s *MockReplicationServer) Start() error {
	s.started = true
	return nil
}

func (s *MockReplicationServer) Serve() error {
	s.started = true
	return nil
}

func (s *MockReplicationServer) Stop(ctx context.Context) error {
	s.stopped = true
	return nil
}

func (s *MockReplicationServer) SetRequestHandler(handler RequestHandler) {
	// No-op for testing
}

func (s *MockReplicationServer) RegisterReplica(replicaInfo *ReplicaInfo) error {
	s.replicas[replicaInfo.ID] = replicaInfo
	return nil
}

func (s *MockReplicationServer) UpdateReplicaStatus(replicaID string, status ReplicaStatus, lsn uint64) error {
	replica, exists := s.replicas[replicaID]
	if !exists {
		return ErrInvalidRequest
	}
	
	replica.Status = status
	replica.CurrentLSN = lsn
	return nil
}

func (s *MockReplicationServer) GetReplicaInfo(replicaID string) (*ReplicaInfo, error) {
	replica, exists := s.replicas[replicaID]
	if !exists {
		return nil, ErrInvalidRequest
	}
	
	return replica, nil
}

func (s *MockReplicationServer) ListReplicas() ([]*ReplicaInfo, error) {
	result := make([]*ReplicaInfo, 0, len(s.replicas))
	for _, replica := range s.replicas {
		result = append(result, replica)
	}
	
	return result, nil
}

func (s *MockReplicationServer) StreamWALEntriesToReplica(ctx context.Context, replicaID string, fromLSN uint64) error {
	_, exists := s.replicas[replicaID]
	if !exists {
		return ErrInvalidRequest
	}
	
	s.streamingReplicas[replicaID] = true
	return nil
}

func TestReplicationServerInterface(t *testing.T) {
	// Create a mock server
	server := NewMockReplicationServer()
	
	// Test Start
	err := server.Start()
	if err != nil {
		t.Errorf("Start failed: %v", err)
	}
	if !server.started {
		t.Errorf("Expected server to be started")
	}
	
	// Test RegisterReplica
	replica1 := &ReplicaInfo{
		ID:            "replica1",
		Address:       "localhost:50051",
		Role:          RoleReplica,
		Status:        StatusConnecting,
		LastSeen:      time.Now(),
		CurrentLSN:    0,
		ReplicationLag: 0,
	}
	err = server.RegisterReplica(replica1)
	if err != nil {
		t.Errorf("RegisterReplica failed: %v", err)
	}
	
	// Test UpdateReplicaStatus
	err = server.UpdateReplicaStatus("replica1", StatusReady, 100)
	if err != nil {
		t.Errorf("UpdateReplicaStatus failed: %v", err)
	}
	
	// Test GetReplicaInfo
	replica, err := server.GetReplicaInfo("replica1")
	if err != nil {
		t.Errorf("GetReplicaInfo failed: %v", err)
	}
	if replica.Status != StatusReady {
		t.Errorf("Expected status to be StatusReady, got %v", replica.Status)
	}
	if replica.CurrentLSN != 100 {
		t.Errorf("Expected LSN to be 100, got %d", replica.CurrentLSN)
	}
	
	// Test ListReplicas
	replicas, err := server.ListReplicas()
	if err != nil {
		t.Errorf("ListReplicas failed: %v", err)
	}
	if len(replicas) != 1 {
		t.Errorf("Expected 1 replica, got %d", len(replicas))
	}
	
	// Test StreamWALEntriesToReplica
	ctx := context.Background()
	err = server.StreamWALEntriesToReplica(ctx, "replica1", 0)
	if err != nil {
		t.Errorf("StreamWALEntriesToReplica failed: %v", err)
	}
	if !server.streamingReplicas["replica1"] {
		t.Errorf("Expected replica1 to be streaming")
	}
	
	// Test Stop
	err = server.Stop(ctx)
	if err != nil {
		t.Errorf("Stop failed: %v", err)
	}
	if !server.stopped {
		t.Errorf("Expected server to be stopped")
	}
}