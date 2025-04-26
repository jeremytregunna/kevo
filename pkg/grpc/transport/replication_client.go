package transport

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transport"
	"github.com/KevoDB/kevo/pkg/wal"
	"github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// ReplicationGRPCClient implements the ReplicationClient interface using gRPC
type ReplicationGRPCClient struct {
	conn             *grpc.ClientConn
	client           kevo.ReplicationServiceClient
	endpoint         string
	options          transport.TransportOptions
	replicaID        string
	status           transport.TransportStatus
	applier          *replication.WALApplier
	serializer       *replication.EntrySerializer
	highestAppliedLSN uint64
	currentLSN       uint64
	mu               sync.RWMutex
}

// NewReplicationGRPCClient creates a new ReplicationGRPCClient
func NewReplicationGRPCClient(
	endpoint string,
	options transport.TransportOptions,
	replicaID string,
	applier *replication.WALApplier,
	serializer *replication.EntrySerializer,
) (*ReplicationGRPCClient, error) {
	return &ReplicationGRPCClient{
		endpoint:    endpoint,
		options:     options,
		replicaID:   replicaID,
		applier:     applier,
		serializer:  serializer,
		status: transport.TransportStatus{
			Connected:     false,
			LastConnected: time.Time{},
			LastError:     nil,
			BytesSent:     0,
			BytesReceived: 0,
			RTT:           0,
		},
	}, nil
}

// Connect establishes a connection to the server
func (c *ReplicationGRPCClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Set up connection options
	dialOptions := []grpc.DialOption{
		grpc.WithBlock(),
	}

	// Add TLS if configured - TODO: Add TLS support once TLS helpers are implemented
	if c.options.TLSEnabled {
		// We'll need to implement TLS credentials loading
		// For now, we'll skip TLS
		c.options.TLSEnabled = false
	} else {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}

	// Set timeout for connection
	dialCtx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()

	// Establish connection
	conn, err := grpc.DialContext(dialCtx, c.endpoint, dialOptions...)
	if err != nil {
		c.status.LastError = err
		return err
	}

	c.conn = conn
	c.client = kevo.NewReplicationServiceClient(conn)
	c.status.Connected = true
	c.status.LastConnected = time.Now()

	return nil
}

// Close closes the connection
func (c *ReplicationGRPCClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.client = nil
		c.status.Connected = false
		if err != nil {
			c.status.LastError = err
			return err
		}
	}

	return nil
}

// IsConnected returns whether the client is connected
func (c *ReplicationGRPCClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil {
		return false
	}

	state := c.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// Status returns the current status of the connection
func (c *ReplicationGRPCClient) Status() transport.TransportStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Send sends a request and waits for a response
func (c *ReplicationGRPCClient) Send(ctx context.Context, request transport.Request) (transport.Response, error) {
	// Implementation depends on specific replication messages
	// This is a placeholder that would be completed for each message type
	return nil, transport.ErrInvalidRequest
}

// Stream opens a bidirectional stream
func (c *ReplicationGRPCClient) Stream(ctx context.Context) (transport.Stream, error) {
	// Not implemented for replication client
	return nil, transport.ErrInvalidRequest
}

// RegisterAsReplica registers this client as a replica with the primary
func (c *ReplicationGRPCClient) RegisterAsReplica(ctx context.Context, replicaID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return transport.ErrNotConnected
	}

	// Create registration request
	req := &kevo.RegisterReplicaRequest{
		ReplicaId: replicaID,
		Address:   c.endpoint, // Use client endpoint as the replica address
		Role:      kevo.ReplicaRole_REPLICA,
	}

	// Call the service
	resp, err := c.client.RegisterReplica(ctx, req)
	if err != nil {
		c.status.LastError = err
		return err
	}

	// Update client info based on response
	c.replicaID = replicaID
	c.currentLSN = resp.CurrentLsn

	return nil
}

// SendHeartbeat sends a heartbeat to the primary
func (c *ReplicationGRPCClient) SendHeartbeat(ctx context.Context, info *transport.ReplicaInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return transport.ErrNotConnected
	}

	// Convert status to proto enum
	var pbStatus kevo.ReplicaStatus
	switch info.Status {
	case transport.StatusConnecting:
		pbStatus = kevo.ReplicaStatus_CONNECTING
	case transport.StatusSyncing:
		pbStatus = kevo.ReplicaStatus_SYNCING
	case transport.StatusBootstrapping:
		pbStatus = kevo.ReplicaStatus_BOOTSTRAPPING
	case transport.StatusReady:
		pbStatus = kevo.ReplicaStatus_READY
	case transport.StatusDisconnected:
		pbStatus = kevo.ReplicaStatus_DISCONNECTED
	case transport.StatusError:
		pbStatus = kevo.ReplicaStatus_ERROR
	default:
		pbStatus = kevo.ReplicaStatus_DISCONNECTED
	}

	// Create heartbeat request
	req := &kevo.ReplicaHeartbeatRequest{
		ReplicaId:    c.replicaID,
		Status:       pbStatus,
		CurrentLsn:   c.highestAppliedLSN,
		ErrorMessage: "",
	}

	// Add error message if any
	if info.Error != nil {
		req.ErrorMessage = info.Error.Error()
	}

	// Call the service
	resp, err := c.client.ReplicaHeartbeat(ctx, req)
	if err != nil {
		c.status.LastError = err
		return err
	}

	// Update client info based on response
	c.currentLSN = resp.PrimaryLsn

	return nil
}

// RequestWALEntries requests WAL entries from the primary starting from a specific LSN
func (c *ReplicationGRPCClient) RequestWALEntries(ctx context.Context, fromLSN uint64) ([]*wal.Entry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return nil, transport.ErrNotConnected
	}

	// Create request
	req := &kevo.GetWALEntriesRequest{
		ReplicaId:   c.replicaID,
		FromLsn:     fromLSN,
		MaxEntries:  1000, // Configurable
	}

	// Call the service
	resp, err := c.client.GetWALEntries(ctx, req)
	if err != nil {
		c.status.LastError = err
		return nil, err
	}

	// Convert proto entries to WAL entries
	entries := make([]*wal.Entry, 0, len(resp.Batch.Entries))
	for _, pbEntry := range resp.Batch.Entries {
		entry := &wal.Entry{
			SequenceNumber: pbEntry.SequenceNumber,
			Type:           uint8(pbEntry.Type),
			Key:            pbEntry.Key,
			Value:          pbEntry.Value,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// RequestBootstrap requests a snapshot for bootstrap purposes
func (c *ReplicationGRPCClient) RequestBootstrap(ctx context.Context) (transport.BootstrapIterator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return nil, transport.ErrNotConnected
	}

	// Create request
	req := &kevo.BootstrapRequest{
		ReplicaId: c.replicaID,
	}

	// Call the service
	stream, err := c.client.RequestBootstrap(ctx, req)
	if err != nil {
		c.status.LastError = err
		return nil, err
	}

	// Create and return bootstrap iterator
	return &GRPCBootstrapIterator{
		stream:     stream,
		totalPairs: 0,
		seenPairs:  0,
		progress:   0.0,
		mu:         &sync.Mutex{},
		applier:    c.applier,
	}, nil
}

// StartReplicationStream starts a stream for continuous replication
func (c *ReplicationGRPCClient) StartReplicationStream(ctx context.Context) error {
	if !c.IsConnected() {
		return transport.ErrNotConnected
	}

	// Get current highest applied LSN
	c.mu.Lock()
	fromLSN := c.highestAppliedLSN
	c.mu.Unlock()

	// Create stream request
	req := &kevo.StreamWALEntriesRequest{
		ReplicaId:  c.replicaID,
		FromLsn:    fromLSN,
		Continuous: true,
	}

	// Start streaming
	stream, err := c.client.StreamWALEntries(ctx, req)
	if err != nil {
		return err
	}

	// Process stream in a goroutine
	go c.processWALStream(ctx, stream)

	return nil
}

// processWALStream handles the incoming WAL entry stream
func (c *ReplicationGRPCClient) processWALStream(ctx context.Context, stream kevo.ReplicationService_StreamWALEntriesClient) {
	for {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return
		default:
			// Continue processing
		}

		// Receive next batch
		batch, err := stream.Recv()
		if err == io.EOF {
			// Stream completed normally
			return
		}
		if err != nil {
			// Stream error
			c.mu.Lock()
			c.status.LastError = err
			c.mu.Unlock()
			return
		}

		// Process entries in batch
		entries := make([]*wal.Entry, 0, len(batch.Entries))
		for _, pbEntry := range batch.Entries {
			entry := &wal.Entry{
				SequenceNumber: pbEntry.SequenceNumber,
				Type:           uint8(pbEntry.Type),
				Key:            pbEntry.Key,
				Value:          pbEntry.Value,
			}
			entries = append(entries, entry)
		}

		// Apply entries
		if len(entries) > 0 {
			_, err = c.applier.ApplyBatch(entries)
			if err != nil {
				c.mu.Lock()
				c.status.LastError = err
				c.mu.Unlock()
				return
			}

			// Update highest applied LSN
			c.mu.Lock()
			c.highestAppliedLSN = batch.LastLsn
			c.mu.Unlock()

			// Report applied entries
			go c.reportAppliedEntries(context.Background(), batch.LastLsn)
		}
	}
}

// reportAppliedEntries reports the highest applied LSN to the primary
func (c *ReplicationGRPCClient) reportAppliedEntries(ctx context.Context, appliedLSN uint64) {
	c.mu.RLock()
	client := c.client
	replicaID := c.replicaID
	c.mu.RUnlock()

	if client == nil {
		return
	}

	// Create request
	req := &kevo.ReportAppliedEntriesRequest{
		ReplicaId:   replicaID,
		AppliedLsn:  appliedLSN,
	}

	// Call the service
	_, err := client.ReportAppliedEntries(ctx, req)
	if err != nil {
		// Just log error, don't return it
		c.mu.Lock()
		c.status.LastError = err
		c.mu.Unlock()
	}
}

// GRPCBootstrapIterator implements the BootstrapIterator interface for gRPC
type GRPCBootstrapIterator struct {
	stream      kevo.ReplicationService_RequestBootstrapClient
	currentBatch *kevo.BootstrapBatch
	batchIndex   int
	totalPairs   int
	seenPairs    int
	progress     float64
	mu           *sync.Mutex
	applier      *replication.WALApplier
}

// Next returns the next key-value pair
func (it *GRPCBootstrapIterator) Next() ([]byte, []byte, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	// If we have a current batch and there are more pairs
	if it.currentBatch != nil && it.batchIndex < len(it.currentBatch.Pairs) {
		pair := it.currentBatch.Pairs[it.batchIndex]
		it.batchIndex++
		it.seenPairs++
		return pair.Key, pair.Value, nil
	}

	// Need to get a new batch
	batch, err := it.stream.Recv()
	if err == io.EOF {
		return nil, nil, io.EOF
	}
	if err != nil {
		return nil, nil, err
	}

	// Update progress
	it.currentBatch = batch
	it.batchIndex = 0
	it.progress = float64(batch.Progress)

	// If batch is empty and it's the last one
	if len(batch.Pairs) == 0 && batch.IsLast {
		// Store the snapshot LSN for later use
		if it.applier != nil {
			it.applier.ResetHighestApplied(batch.SnapshotLsn)
		}
		return nil, nil, io.EOF
	}

	// If batch is empty but not the last one, try again
	if len(batch.Pairs) == 0 {
		return it.Next()
	}

	// Return the first pair from the new batch
	pair := batch.Pairs[it.batchIndex]
	it.batchIndex++
	it.seenPairs++
	return pair.Key, pair.Value, nil
}

// Close closes the iterator
func (it *GRPCBootstrapIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Store the snapshot LSN if we have a current batch and it's the last one
	if it.currentBatch != nil && it.currentBatch.IsLast && it.applier != nil {
		it.applier.ResetHighestApplied(it.currentBatch.SnapshotLsn)
	}

	return nil
}

// Progress returns the progress of the bootstrap operation (0.0-1.0)
func (it *GRPCBootstrapIterator) Progress() float64 {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.progress
}