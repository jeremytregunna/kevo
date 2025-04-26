package transport

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/common/log"
	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transport"
	"github.com/KevoDB/kevo/pkg/wal"
	"github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

// ReplicationGRPCClient implements the ReplicationClient interface using gRPC
type ReplicationGRPCClient struct {
	conn              *grpc.ClientConn
	client            kevo.ReplicationServiceClient
	endpoint          string
	options           transport.TransportOptions
	replicaID         string
	status            transport.TransportStatus
	applier           *replication.WALApplier
	serializer        *replication.EntrySerializer
	highestAppliedLSN uint64
	currentLSN        uint64
	mu                sync.RWMutex

	// Reliability components
	circuitBreaker   *transport.CircuitBreaker
	reconnectAttempt int
	shuttingDown     bool
	logger           log.Logger
}

// NewReplicationGRPCClient creates a new ReplicationGRPCClient
func NewReplicationGRPCClient(
	endpoint string,
	options transport.TransportOptions,
	replicaID string,
	applier *replication.WALApplier,
	serializer *replication.EntrySerializer,
) (*ReplicationGRPCClient, error) {
	// Use default retry policy if not provided
	if options.RetryPolicy.MaxRetries == 0 {
		options.RetryPolicy = transport.RetryPolicy{
			MaxRetries:     3,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     5 * time.Second,
			BackoffFactor:  2.0,
			Jitter:         0.2,
		}
	}

	// Create logger with component and replica ID fields
	logger := log.GetDefaultLogger().WithFields(map[string]interface{}{
		"component": "replication_client",
		"replica":   replicaID,
		"endpoint":  endpoint,
	})

	// Create circuit breaker (3 failures within 5 seconds will open the circuit)
	cb := transport.NewCircuitBreaker(3, 5*time.Second)

	return &ReplicationGRPCClient{
		endpoint:   endpoint,
		options:    options,
		replicaID:  replicaID,
		applier:    applier,
		serializer: serializer,
		status: transport.TransportStatus{
			Connected:     false,
			LastConnected: time.Time{},
			LastError:     nil,
			BytesSent:     0,
			BytesReceived: 0,
			RTT:           0,
		},
		circuitBreaker:   cb,
		reconnectAttempt: 0,
		shuttingDown:     false,
		logger:           logger,
	}, nil
}

// Connect establishes a connection to the server
func (c *ReplicationGRPCClient) Connect(ctx context.Context) error {
	// Check if circuit breaker is open
	if c.circuitBreaker.IsOpen() {
		c.logger.Warn("Circuit breaker is open, connection attempt rejected")
		return transport.ErrCircuitOpen
	}

	// Function to attempt connection with circuit breaker protection
	connectFn := func(ctx context.Context) error {
		c.mu.Lock()
		defer c.mu.Unlock()

		// Check if we're already connected
		if c.IsConnected() {
			c.logger.Debug("Already connected to %s", c.endpoint)
			return nil
		}

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

		// Set timeout for dial context
		dialCtx, cancel := context.WithTimeout(ctx, c.options.Timeout)
		defer cancel()

		c.logger.Info("Connecting to %s...", c.endpoint)

		// Establish connection
		conn, err := grpc.DialContext(dialCtx, c.endpoint, dialOptions...)
		if err != nil {
			c.logger.Error("Failed to connect to %s: %v", c.endpoint, err)
			c.status.LastError = err

			// Classify error for retry logic
			if status.Code(err) == codes.Unavailable ||
				status.Code(err) == codes.DeadlineExceeded {
				return transport.NewTemporaryError(err, true)
			}
			return err
		}

		c.logger.Info("Successfully connected to %s", c.endpoint)
		c.conn = conn
		c.client = kevo.NewReplicationServiceClient(conn)
		c.status.Connected = true
		c.status.LastConnected = time.Now()
		c.reconnectAttempt = 0

		return nil
	}

	// Execute the connect function with retry logic
	return c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		return transport.WithRetry(ctx, c.options.RetryPolicy, connectFn)
	})
}

// Close closes the connection
func (c *ReplicationGRPCClient) Close() error {
	c.mu.Lock()

	// Mark as shutting down to prevent reconnection attempts
	c.shuttingDown = true

	// Check if already closed
	if c.conn == nil {
		c.mu.Unlock()
		return nil
	}

	c.logger.Info("Closing connection to %s", c.endpoint)

	// Close the connection
	err := c.conn.Close()
	c.conn = nil
	c.client = nil
	c.status.Connected = false

	if err != nil {
		c.status.LastError = err
		c.logger.Error("Error closing connection: %v", err)
		c.mu.Unlock()
		return err
	}

	c.mu.Unlock()
	c.logger.Info("Connection to %s closed successfully", c.endpoint)
	return nil
}

// IsConnected returns whether the client is connected
func (c *ReplicationGRPCClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil || c.shuttingDown {
		return false
	}

	// Check actual connection state
	state := c.conn.GetState()
	isConnected := state == connectivity.Ready || state == connectivity.Idle

	// If we think we're connected but the connection is not ready or idle,
	// update our status to reflect the actual state
	if c.status.Connected && !isConnected {
		// Using defer here won't work well with the existing defer
		c.mu.RUnlock()
		c.mu.Lock()
		c.status.Connected = false
		c.mu.Unlock()
		c.mu.RLock()

		// Start reconnection in a separate goroutine
		if !c.shuttingDown {
			go c.maybeReconnect()
		}
	}

	return isConnected
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
	// Check if we're connected first
	if !c.IsConnected() {
		c.logger.Warn("Not connected, attempting to connect before registration")
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	// Check if circuit breaker is open
	if c.circuitBreaker.IsOpen() {
		c.logger.Warn("Circuit breaker is open, registration request rejected")
		return transport.ErrCircuitOpen
	}

	// Define the registration function for retries
	registerFn := func(ctx context.Context) error {
		// Create registration request
		req := &kevo.RegisterReplicaRequest{
			ReplicaId: replicaID,
			Address:   c.endpoint, // Use client endpoint as the replica address
			Role:      kevo.ReplicaRole_REPLICA,
		}

		c.logger.Info("Registering as replica %s", replicaID)

		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		if client == nil {
			return transport.ErrNotConnected
		}

		// Call the service with timeout
		timeoutCtx, cancel := context.WithTimeout(ctx, c.options.Timeout)
		defer cancel()

		// Attempt registration
		resp, err := client.RegisterReplica(timeoutCtx, req)
		if err != nil {
			c.logger.Error("Failed to register replica: %v", err)
			c.mu.Lock()
			c.status.LastError = err
			c.mu.Unlock()

			// Classify error for retry logic
			if status.Code(err) == codes.Unavailable ||
				status.Code(err) == codes.DeadlineExceeded ||
				status.Code(err) == codes.ResourceExhausted {
				return transport.NewTemporaryError(err, true)
			}

			// Check for connection loss
			if !c.IsConnected() {
				c.handleConnectionError(err)
			}

			return err
		}

		// Update client info based on response
		c.mu.Lock()
		c.replicaID = replicaID
		c.currentLSN = resp.CurrentLsn
		c.mu.Unlock()

		c.logger.Info("Successfully registered as replica %s (current LSN: %d)",
			replicaID, resp.CurrentLsn)

		return nil
	}

	// Execute the register function with circuit breaker protection and retries
	return c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		return transport.WithRetry(ctx, c.options.RetryPolicy, registerFn)
	})
}

// SendHeartbeat sends a heartbeat to the primary
func (c *ReplicationGRPCClient) SendHeartbeat(ctx context.Context, info *transport.ReplicaInfo) error {
	// Check if we're connected first
	if !c.IsConnected() {
		// For heartbeats, just fail if we're not connected - don't try to reconnect
		// since this is a periodic operation and reconnection should be handled separately
		return transport.ErrNotConnected
	}

	// Check if circuit breaker is open
	if c.circuitBreaker.IsOpen() {
		c.logger.Debug("Circuit breaker is open, heartbeat request rejected")
		return transport.ErrCircuitOpen
	}

	// Define the heartbeat function for retries
	heartbeatFn := func(ctx context.Context) error {
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

		c.mu.RLock()
		replicaID := c.replicaID
		highestAppliedLSN := c.highestAppliedLSN
		client := c.client
		c.mu.RUnlock()

		if client == nil {
			return transport.ErrNotConnected
		}

		// Create heartbeat request
		req := &kevo.ReplicaHeartbeatRequest{
			ReplicaId:    replicaID,
			Status:       pbStatus,
			CurrentLsn:   highestAppliedLSN,
			ErrorMessage: "",
		}

		// Add error message if any
		if info.Error != nil {
			req.ErrorMessage = info.Error.Error()
		}

		c.logger.Debug("Sending heartbeat (LSN: %d, status: %s)",
			highestAppliedLSN, info.Status)

		// Call the service with timeout
		timeoutCtx, cancel := context.WithTimeout(ctx, c.options.Timeout)
		defer cancel()

		// Attempt heartbeat
		resp, err := client.ReplicaHeartbeat(timeoutCtx, req)
		if err != nil {
			c.logger.Error("Failed to send heartbeat: %v", err)

			c.mu.Lock()
			c.status.LastError = err
			c.mu.Unlock()

			// Classify error for retry logic
			if status.Code(err) == codes.Unavailable ||
				status.Code(err) == codes.DeadlineExceeded {
				return transport.NewTemporaryError(err, true)
			}

			// Check for connection loss
			if !c.IsConnected() {
				c.handleConnectionError(err)
			}

			return err
		}

		// Update client info based on response
		c.mu.Lock()
		c.currentLSN = resp.PrimaryLsn
		c.mu.Unlock()

		c.logger.Debug("Heartbeat successful (primary LSN: %d, lag: %dms)",
			resp.PrimaryLsn, resp.ReplicationLagMs)

		return nil
	}

	// Execute the heartbeat function with circuit breaker protection and retries
	// For heartbeats, we'll use a reduced retry policy to avoid stacking up heartbeats
	reducedRetryPolicy := transport.RetryPolicy{
		MaxRetries:     1, // Just one retry for heartbeats
		InitialBackoff: c.options.RetryPolicy.InitialBackoff,
		MaxBackoff:     c.options.RetryPolicy.MaxBackoff,
		BackoffFactor:  c.options.RetryPolicy.BackoffFactor,
		Jitter:         c.options.RetryPolicy.Jitter,
	}

	return c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		return transport.WithRetry(ctx, reducedRetryPolicy, heartbeatFn)
	})
}

// RequestWALEntries requests WAL entries from the primary starting from a specific LSN
func (c *ReplicationGRPCClient) RequestWALEntries(ctx context.Context, fromLSN uint64) ([]*wal.Entry, error) {
	// Check if we're connected first
	if !c.IsConnected() {
		c.logger.Warn("Not connected, attempting to connect before requesting WAL entries")
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

	// Check if circuit breaker is open
	if c.circuitBreaker.IsOpen() {
		c.logger.Warn("Circuit breaker is open, WAL entries request rejected")
		return nil, transport.ErrCircuitOpen
	}

	// Define the request function for retries
	var entries []*wal.Entry
	requestFn := func(ctx context.Context) error {
		c.mu.RLock()
		replicaID := c.replicaID
		client := c.client
		c.mu.RUnlock()

		if client == nil {
			return transport.ErrNotConnected
		}

		// Create request
		req := &kevo.GetWALEntriesRequest{
			ReplicaId:  replicaID,
			FromLsn:    fromLSN,
			MaxEntries: 1000, // Configurable
		}

		c.logger.Debug("Requesting WAL entries from LSN %d", fromLSN)

		// Call the service with timeout
		timeoutCtx, cancel := context.WithTimeout(ctx, c.options.Timeout)
		defer cancel()

		// Attempt to get WAL entries
		resp, err := client.GetWALEntries(timeoutCtx, req)
		if err != nil {
			c.logger.Error("Failed to request WAL entries: %v", err)

			c.mu.Lock()
			c.status.LastError = err
			c.mu.Unlock()

			// Classify error for retry logic
			if status.Code(err) == codes.Unavailable ||
				status.Code(err) == codes.DeadlineExceeded ||
				status.Code(err) == codes.ResourceExhausted {
				return transport.NewTemporaryError(err, true)
			}

			// Check for connection loss
			if !c.IsConnected() {
				c.handleConnectionError(err)
			}

			return err
		}

		// Convert proto entries to WAL entries
		entries = make([]*wal.Entry, 0, len(resp.Batch.Entries))
		for _, pbEntry := range resp.Batch.Entries {
			entry := &wal.Entry{
				SequenceNumber: pbEntry.SequenceNumber,
				Type:           uint8(pbEntry.Type),
				Key:            pbEntry.Key,
				Value:          pbEntry.Value,
			}
			entries = append(entries, entry)
		}

		c.logger.Debug("Received %d WAL entries from primary", len(entries))
		return nil
	}

	// Execute the request function with circuit breaker protection and retries
	err := c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		return transport.WithRetry(ctx, c.options.RetryPolicy, requestFn)
	})

	if err != nil {
		return nil, err
	}

	return entries, nil
}

// RequestBootstrap requests a snapshot for bootstrap purposes
func (c *ReplicationGRPCClient) RequestBootstrap(ctx context.Context) (transport.BootstrapIterator, error) {
	// Check if we're connected first
	if !c.IsConnected() {
		c.logger.Warn("Not connected, attempting to connect before bootstrap request")
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

	// Check if circuit breaker is open
	if c.circuitBreaker.IsOpen() {
		c.logger.Warn("Circuit breaker is open, bootstrap request rejected")
		return nil, transport.ErrCircuitOpen
	}

	var bootstrapStream kevo.ReplicationService_RequestBootstrapClient

	// Define the bootstrap function for retries
	bootstrapFn := func(ctx context.Context) error {
		c.mu.RLock()
		replicaID := c.replicaID
		client := c.client
		c.mu.RUnlock()

		if client == nil {
			return transport.ErrNotConnected
		}

		// Create request
		req := &kevo.BootstrapRequest{
			ReplicaId: replicaID,
		}

		c.logger.Info("Requesting bootstrap from primary")

		// Call the service with timeout
		// Note: For bootstrap we use a longer timeout since it's a potentially large operation
		timeoutCtx, cancel := context.WithTimeout(ctx, c.options.Timeout*2)
		defer cancel()

		// Attempt to get bootstrap stream
		stream, err := client.RequestBootstrap(timeoutCtx, req)
		if err != nil {
			c.logger.Error("Failed to request bootstrap: %v", err)

			c.mu.Lock()
			c.status.LastError = err
			c.mu.Unlock()

			// Classify error for retry logic
			if status.Code(err) == codes.Unavailable ||
				status.Code(err) == codes.DeadlineExceeded {
				return transport.NewTemporaryError(err, true)
			}

			// Check for connection loss
			if !c.IsConnected() {
				c.handleConnectionError(err)
			}

			return err
		}

		// Store the stream for later use
		bootstrapStream = stream
		c.logger.Info("Bootstrap stream established successfully")
		return nil
	}

	// Execute the bootstrap function with circuit breaker protection and retries
	err := c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		return transport.WithRetry(ctx, c.options.RetryPolicy, bootstrapFn)
	})

	if err != nil {
		return nil, err
	}

	// Create and return bootstrap iterator
	c.logger.Info("Creating bootstrap iterator")
	return &GRPCBootstrapIterator{
		stream:     bootstrapStream,
		totalPairs: 0,
		seenPairs:  0,
		progress:   0.0,
		mu:         &sync.Mutex{},
		applier:    c.applier,
		logger:     c.logger.WithField("component", "bootstrap_iterator"),
	}, nil
}

// StartReplicationStream starts a stream for continuous replication
func (c *ReplicationGRPCClient) StartReplicationStream(ctx context.Context) error {
	// Check if we're connected first
	if !c.IsConnected() {
		c.logger.Warn("Not connected, attempting to connect before starting replication stream")
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	// Check if circuit breaker is open
	if c.circuitBreaker.IsOpen() {
		c.logger.Warn("Circuit breaker is open, stream request rejected")
		return transport.ErrCircuitOpen
	}

	var replicationStream kevo.ReplicationService_StreamWALEntriesClient

	// Define the stream function for retries
	streamFn := func(ctx context.Context) error {
		// Get current highest applied LSN
		c.mu.RLock()
		fromLSN := c.highestAppliedLSN
		replicaID := c.replicaID
		client := c.client
		c.mu.RUnlock()

		if client == nil {
			return transport.ErrNotConnected
		}

		// Create stream request
		req := &kevo.StreamWALEntriesRequest{
			ReplicaId:  replicaID,
			FromLsn:    fromLSN,
			Continuous: true,
		}

		c.logger.Info("Starting WAL replication stream from LSN %d", fromLSN)

		// Start streaming with timeout context
		// We use a short timeout just for stream establishment
		timeoutCtx, cancel := context.WithTimeout(ctx, c.options.Timeout)
		defer cancel()

		// Attempt to start the stream
		stream, err := client.StreamWALEntries(timeoutCtx, req)
		if err != nil {
			c.logger.Error("Failed to start replication stream: %v", err)

			c.mu.Lock()
			c.status.LastError = err
			c.mu.Unlock()

			// Classify error for retry logic
			if status.Code(err) == codes.Unavailable ||
				status.Code(err) == codes.DeadlineExceeded {
				return transport.NewTemporaryError(err, true)
			}

			// Check for connection loss
			if !c.IsConnected() {
				c.handleConnectionError(err)
			}

			return err
		}

		// Store the stream for later use
		replicationStream = stream
		c.logger.Info("Replication stream established successfully")
		return nil
	}

	// Execute the stream function with circuit breaker protection and retries
	err := c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		return transport.WithRetry(ctx, c.options.RetryPolicy, streamFn)
	})

	if err != nil {
		return err
	}

	// Process stream in a goroutine with the parent context
	// This allows the stream to keep running even after this function returns
	c.logger.Info("Starting WAL entry stream processor")
	go c.processWALStream(ctx, replicationStream)

	return nil
}

// processWALStream handles the incoming WAL entry stream
func (c *ReplicationGRPCClient) processWALStream(ctx context.Context, stream kevo.ReplicationService_StreamWALEntriesClient) {
	c.logger.Info("Starting WAL stream processor")

	// Track consecutive errors for backoff
	consecutiveErrors := 0

	for {
		// Check if context is cancelled or client is shutting down
		select {
		case <-ctx.Done():
			c.logger.Info("WAL stream processor stopped: context cancelled")
			return
		default:
			// Continue processing
		}

		if c.shuttingDown {
			c.logger.Info("WAL stream processor stopped: client shutting down")
			return
		}

		// Receive next batch with timeout
		_, cancel := context.WithTimeout(ctx, c.options.Timeout)
		batch, err := stream.Recv()
		cancel()

		if err == io.EOF {
			// Stream completed normally
			c.logger.Info("WAL stream completed normally")
			return
		}

		if err != nil {
			// Stream error
			c.mu.Lock()
			c.status.LastError = err
			c.mu.Unlock()

			c.logger.Error("Error receiving from WAL stream: %v", err)

			// Check for connection loss
			if status.Code(err) == codes.Unavailable ||
				status.Code(err) == codes.DeadlineExceeded ||
				!c.IsConnected() {

				// Handle connection error
				c.handleConnectionError(err)

				// Try to restart the stream after a delay
				consecutiveErrors++
				backoff := calculateBackoff(consecutiveErrors, c.options.RetryPolicy)
				c.logger.Info("Will attempt to restart stream in %v", backoff)

				// Sleep with context awareness
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
					// Continue and try to restart stream
				}

				// Try to restart the stream
				if !c.shuttingDown {
					c.logger.Info("Attempting to restart replication stream")
					go func() {
						// Use a new background context for the restart
						newCtx := context.Background()
						if err := c.StartReplicationStream(newCtx); err != nil {
							c.logger.Error("Failed to restart replication stream: %v", err)
						}
					}()
				}

				return
			}

			// Other error, try to continue with a short delay
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Reset consecutive errors on successful receive
		consecutiveErrors = 0

		// No entries in batch, continue
		if len(batch.Entries) == 0 {
			continue
		}

		c.logger.Debug("Received WAL batch with %d entries (LSN range: %d-%d)",
			len(batch.Entries), batch.FirstLsn, batch.LastLsn)

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
				c.logger.Error("Error applying WAL batch: %v", err)
				c.mu.Lock()
				c.status.LastError = err
				c.mu.Unlock()

				// Short delay before continuing
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Update highest applied LSN
			c.mu.Lock()
			c.highestAppliedLSN = batch.LastLsn
			c.mu.Unlock()

			c.logger.Debug("Applied %d WAL entries, new highest LSN: %d",
				len(entries), batch.LastLsn)

			// Report applied entries asynchronously
			go c.reportAppliedEntries(context.Background(), batch.LastLsn)
		}
	}
}

// reportAppliedEntries reports the highest applied LSN to the primary
func (c *ReplicationGRPCClient) reportAppliedEntries(ctx context.Context, appliedLSN uint64) {
	// Check if we're connected
	if !c.IsConnected() {
		c.logger.Debug("Not connected, skipping report of applied entries")
		return
	}

	// Check if circuit breaker is open
	if c.circuitBreaker.IsOpen() {
		c.logger.Debug("Circuit breaker is open, skipping report of applied entries")
		return
	}

	// Define the report function for retries
	reportFn := func(ctx context.Context) error {
		c.mu.RLock()
		client := c.client
		replicaID := c.replicaID
		c.mu.RUnlock()

		if client == nil {
			return transport.ErrNotConnected
		}

		// Create request
		req := &kevo.ReportAppliedEntriesRequest{
			ReplicaId:  replicaID,
			AppliedLsn: appliedLSN,
		}

		c.logger.Debug("Reporting applied entries (LSN: %d)", appliedLSN)

		// Call the service with timeout
		timeoutCtx, cancel := context.WithTimeout(ctx, c.options.Timeout)
		defer cancel()

		// Attempt to report
		_, err := client.ReportAppliedEntries(timeoutCtx, req)
		if err != nil {
			c.logger.Debug("Failed to report applied entries: %v", err)

			c.mu.Lock()
			c.status.LastError = err
			c.mu.Unlock()

			// Classify error for retry logic
			if status.Code(err) == codes.Unavailable ||
				status.Code(err) == codes.DeadlineExceeded {
				return transport.NewTemporaryError(err, true)
			}

			// Check for connection loss
			if !c.IsConnected() {
				c.handleConnectionError(err)
			}

			return err
		}

		c.logger.Debug("Successfully reported applied entries (LSN: %d)", appliedLSN)
		return nil
	}

	// Execute the report function with circuit breaker protection and reduced retries
	// For reporting applied entries, we use a minimal retry policy since this is a background operation
	reducedRetryPolicy := transport.RetryPolicy{
		MaxRetries:     1, // Just one retry for applied entries reports
		InitialBackoff: c.options.RetryPolicy.InitialBackoff,
		MaxBackoff:     c.options.RetryPolicy.MaxBackoff,
		BackoffFactor:  c.options.RetryPolicy.BackoffFactor,
		Jitter:         c.options.RetryPolicy.Jitter,
	}

	// We ignore any errors here since this is a best-effort operation
	_ = c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		return transport.WithRetry(ctx, reducedRetryPolicy, reportFn)
	})
}

// GRPCBootstrapIterator implements the BootstrapIterator interface for gRPC
type GRPCBootstrapIterator struct {
	stream       kevo.ReplicationService_RequestBootstrapClient
	currentBatch *kevo.BootstrapBatch
	batchIndex   int
	totalPairs   int
	seenPairs    int
	progress     float64
	mu           *sync.Mutex
	applier      *replication.WALApplier
	logger       log.Logger
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
