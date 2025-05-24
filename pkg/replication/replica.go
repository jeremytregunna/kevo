package replication

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/wal"
	replication_proto "github.com/KevoDB/kevo/proto/kevo/replication"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ReplicaStats tracks statistics for a replica node
type ReplicaStats struct {
	entriesReceived atomic.Uint64 // Number of WAL entries received
	entriesApplied  atomic.Uint64 // Number of WAL entries successfully applied
	bytesReceived   atomic.Uint64 // Number of bytes received from the primary
	batchCount      atomic.Uint64 // Number of batches received
	errors          atomic.Uint64 // Number of errors during replication
	lastBatchTime   atomic.Int64  // Timestamp of the last batch received (Unix nanos)
}

// TrackEntriesReceived increments the count of received entries
func (s *ReplicaStats) TrackEntriesReceived(count uint64) {
	s.entriesReceived.Add(count)
}

// TrackEntriesApplied increments the count of applied entries
func (s *ReplicaStats) TrackEntriesApplied(count uint64) {
	s.entriesApplied.Add(count)
}

// TrackBytesReceived increments the count of bytes received
func (s *ReplicaStats) TrackBytesReceived(bytes uint64) {
	s.bytesReceived.Add(bytes)
}

// TrackBatchReceived increments the batch count and updates the last batch time
func (s *ReplicaStats) TrackBatchReceived() {
	s.batchCount.Add(1)
	s.lastBatchTime.Store(time.Now().UnixNano())
}

// TrackError increments the error count
func (s *ReplicaStats) TrackError() {
	s.errors.Add(1)
}

// GetEntriesReceived returns the number of entries received
func (s *ReplicaStats) GetEntriesReceived() uint64 {
	return s.entriesReceived.Load()
}

// GetEntriesApplied returns the number of entries applied
func (s *ReplicaStats) GetEntriesApplied() uint64 {
	return s.entriesApplied.Load()
}

// GetBytesReceived returns the number of bytes received
func (s *ReplicaStats) GetBytesReceived() uint64 {
	return s.bytesReceived.Load()
}

// GetBatchCount returns the number of batches received
func (s *ReplicaStats) GetBatchCount() uint64 {
	return s.batchCount.Load()
}

// GetErrorCount returns the number of errors encountered
func (s *ReplicaStats) GetErrorCount() uint64 {
	return s.errors.Load()
}

// GetLastBatchTime returns the timestamp of the last batch
func (s *ReplicaStats) GetLastBatchTime() int64 {
	return s.lastBatchTime.Load()
}

// GetLastBatchTimeDuration returns the duration since the last batch
func (s *ReplicaStats) GetLastBatchTimeDuration() time.Duration {
	lastBatch := s.lastBatchTime.Load()
	if lastBatch == 0 {
		return 0
	}
	return time.Since(time.Unix(0, lastBatch))
}

// WALEntryApplier interface is defined in interfaces.go

// ConnectionConfig contains configuration for connecting to the primary
type ConnectionConfig struct {
	// Primary server address in the format host:port
	PrimaryAddress string

	// Whether to use TLS for the connection
	UseTLS bool

	// TLS credentials for secure connections
	TLSCredentials credentials.TransportCredentials

	// Connection timeout
	DialTimeout time.Duration

	// Retry settings
	MaxRetries      int
	RetryBaseDelay  time.Duration
	RetryMaxDelay   time.Duration
	RetryMultiplier float64
}

// ReplicaConfig contains configuration for a replica node
type ReplicaConfig struct {
	// Connection configuration
	Connection ConnectionConfig

	// Replica's listener address that clients can connect to (from -replication-address)
	ReplicationListenerAddr string

	// Compression settings
	CompressionSupported bool
	PreferredCodec       replication_proto.CompressionCodec

	// Protocol version for compatibility
	ProtocolVersion uint32

	// Acknowledgment interval
	AckInterval time.Duration

	// Maximum batch size to process at once (in bytes)
	MaxBatchSize int

	// Whether to report detailed metrics
	ReportMetrics bool
}

// DefaultReplicaConfig returns a default configuration for replicas
func DefaultReplicaConfig() *ReplicaConfig {
	return &ReplicaConfig{
		Connection: ConnectionConfig{
			PrimaryAddress:  "localhost:50052",
			UseTLS:          false,
			DialTimeout:     time.Second * 10,
			MaxRetries:      5,
			RetryBaseDelay:  time.Second,
			RetryMaxDelay:   time.Minute,
			RetryMultiplier: 1.5,
		},
		ReplicationListenerAddr: "localhost:50053", // Default, should be overridden with CLI value
		CompressionSupported:    true,
		PreferredCodec:          replication_proto.CompressionCodec_ZSTD,
		ProtocolVersion:         1,
		AckInterval:             time.Second * 5,
		MaxBatchSize:            1024 * 1024, // 1MB
		ReportMetrics:           true,
	}
}

// Replica implements a replication replica node that connects to a primary,
// receives WAL entries, applies them locally, and acknowledges their application
type Replica struct {
	// The current state of the replica
	stateTracker *StateTracker

	// Configuration
	config *ReplicaConfig

	// Last applied sequence number
	lastAppliedSeq uint64

	// Applier for WAL entries
	applier WALEntryApplier

	// Client connection to the primary
	conn *grpc.ClientConn

	// Replication client
	client replication_proto.WALReplicationServiceClient

	// Stream client for receiving WAL entries
	streamClient replication_proto.WALReplicationService_StreamWALClient

	// Statistics for the replica
	stats *ReplicaStats

	// Session ID for communication with primary
	sessionID string

	// Compressor for handling compressed payloads
	compressor *CompressionManager

	// WAL batch applier
	batchApplier *WALBatchApplier

	// Context for controlling streaming and cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Flag to signal shutdown
	shutdown bool

	// Wait group for goroutines
	wg sync.WaitGroup

	// Mutex to protect state
	mu sync.RWMutex

	// Connector for connecting to primary (for testing)
	connector PrimaryConnector
}

// NewReplica creates a new replica instance
func NewReplica(lastAppliedSeq uint64, applier WALEntryApplier, config *ReplicaConfig) (*Replica, error) {
	if config == nil {
		config = DefaultReplicaConfig()
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Create compressor
	compressor, err := NewCompressionManager()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create compressor: %w", err)
	}

	// Create batch applier
	batchApplier := NewWALBatchApplier(lastAppliedSeq)

	// Create replica
	replica := &Replica{
		stateTracker:   NewStateTracker(),
		config:         config,
		lastAppliedSeq: lastAppliedSeq,
		applier:        applier,
		compressor:     compressor,
		batchApplier:   batchApplier,
		ctx:            ctx,
		cancel:         cancel,
		shutdown:       false,
		connector:      &DefaultPrimaryConnector{},
		stats:          &ReplicaStats{}, // Initialize statistics tracker
	}

	return replica, nil
}

// SetConnector sets a custom connector for testing purposes
func (r *Replica) SetConnector(connector PrimaryConnector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connector = connector
}

// Start initiates the replication process by connecting to the primary and
// beginning the state machine
func (r *Replica) Start() error {
	r.mu.Lock()
	if r.shutdown {
		r.mu.Unlock()
		return fmt.Errorf("replica is shut down")
	}
	r.mu.Unlock()

	// Launch the main replication loop
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.replicationLoop()
	}()

	return nil
}

// Stop gracefully stops the replication process
func (r *Replica) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.shutdown {
		return nil // Already shut down
	}

	// Signal shutdown
	r.shutdown = true
	r.cancel()

	// Wait for all goroutines to finish
	r.wg.Wait()

	// Close connection and reset clients
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
	r.client = nil
	r.streamClient = nil

	// Close compressor
	if r.compressor != nil {
		r.compressor.Close()
	}

	return nil
}

// GetLastAppliedSequence returns the last successfully applied sequence number
func (r *Replica) GetLastAppliedSequence() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastAppliedSeq
}

// GetCurrentState returns the current state of the replica
func (r *Replica) GetCurrentState() ReplicaState {
	return r.stateTracker.GetState()
}

// GetStateString returns the string representation of the current state
func (r *Replica) GetStateString() string {
	return r.stateTracker.GetStateString()
}

// replicationLoop runs the main replication state machine loop
func (r *Replica) replicationLoop() {
	backoff := r.createBackoff()

	for {
		select {
		case <-r.ctx.Done():
			// Context was cancelled, exit the loop
			fmt.Printf("Replication loop exiting due to context cancellation\n")
			return
		default:
			// Process based on current state
			var err error
			state := r.stateTracker.GetState()
			fmt.Printf("State machine tick: current state is %s\n", state.String())

			switch state {
			case StateConnecting:
				err = r.handleConnectingState()
			case StateStreamingEntries:
				err = r.handleStreamingState()
			case StateApplyingEntries:
				err = r.handleApplyingState()
			case StateFsyncPending:
				err = r.handleFsyncState()
			case StateAcknowledging:
				err = r.handleAcknowledgingState()
			case StateWaitingForData:
				err = r.handleWaitingForDataState()
			case StateError:
				err = r.handleErrorState(backoff)
			}

			if err != nil {
				fmt.Printf("Error in state %s: %v\n", state.String(), err)
				r.stateTracker.SetError(err)
			}

			// Add a small sleep to avoid busy-waiting and make logs more readable
			time.Sleep(time.Millisecond * 50)
		}
	}
}

// handleConnectingState handles the CONNECTING state
func (r *Replica) handleConnectingState() error {
	// Attempt to connect to the primary
	err := r.connectToPrimary()
	if err != nil {
		return fmt.Errorf("failed to connect to primary: %w", err)
	}

	// Transition to streaming state
	return r.stateTracker.SetState(StateStreamingEntries)
}

// handleStreamingState handles the STREAMING_ENTRIES state
func (r *Replica) handleStreamingState() error {
	// Check if we already have an active client and stream
	if r.client == nil {
		return fmt.Errorf("replication client is nil, reconnection required")
	}

	// Initialize streamClient if it doesn't exist
	if r.streamClient == nil {
		// Create a WAL stream request
		nextSeq := r.batchApplier.GetExpectedNext()
		fmt.Printf("Creating stream request, starting from sequence: %d\n", nextSeq)

		request := &replication_proto.WALStreamRequest{
			StartSequence:        nextSeq,
			ProtocolVersion:      r.config.ProtocolVersion,
			CompressionSupported: r.config.CompressionSupported,
			PreferredCodec:       r.config.PreferredCodec,
			ListenerAddress:      r.config.ReplicationListenerAddr, // Use the replica's actual replication listener address
		}

		// Start streaming from the primary
		var err error
		r.streamClient, err = r.client.StreamWAL(r.ctx, request)
		if err != nil {
			return fmt.Errorf("failed to start WAL stream: %w", err)
		}

		// Get the session ID from the response header metadata
		md, err := r.streamClient.Header()
		if err != nil {
			fmt.Printf("Failed to get header metadata: %v\n", err)
		} else {
			// Extract session ID
			sessionIDs := md.Get("session-id")
			if len(sessionIDs) > 0 {
				r.sessionID = sessionIDs[0]
				fmt.Printf("Received session ID from primary: %s\n", r.sessionID)
			} else {
				fmt.Printf("No session ID received from primary\n")
			}
		}

		fmt.Printf("Stream established, waiting for entries. Starting from sequence: %d\n", nextSeq)
	}

	// Process the stream - we'll use a non-blocking approach with a short timeout
	// to allow other state machine operations to happen
	select {
	case <-r.ctx.Done():
		fmt.Printf("Context done, exiting streaming state\n")
		return nil
	default:
		// Receive next batch with a timeout context to make this non-blocking
		// Increased timeout to 1 second to avoid missing entries due to timing
		receiveCtx, cancel := context.WithTimeout(r.ctx, 1000*time.Millisecond)
		defer cancel()

		fmt.Printf("Waiting to receive next batch...\n")

		// Make sure we have a valid stream client
		if r.streamClient == nil {
			return fmt.Errorf("stream client is nil")
		}

		// Set up a channel to receive the result
		type receiveResult struct {
			response *replication_proto.WALStreamResponse
			err      error
		}
		resultCh := make(chan receiveResult, 1)

		go func() {
			fmt.Printf("Starting Recv() call to wait for entries from primary\n")
			response, err := r.streamClient.Recv()
			if err != nil {
				fmt.Printf("Error in Recv() call: %v\n", err)
			} else if response != nil {
				numEntries := len(response.Entries)
				fmt.Printf("Successfully received a response with %d entries\n", numEntries)

				// IMPORTANT DEBUG: If we received entries but stay in WAITING_FOR_DATA,
				// this indicates a serious state machine issue
				if numEntries > 0 {
					fmt.Printf("CRITICAL: Received %d entries that need processing!\n", numEntries)
					for i, entry := range response.Entries {
						if i < 3 { // Only log a few entries
							fmt.Printf("Entry %d: seq=%d, fragment=%s, payload_size=%d\n",
								i, entry.SequenceNumber, entry.FragmentType, len(entry.Payload))
						}
					}
				}
			} else {
				fmt.Printf("Received nil response without error\n")
			}
			resultCh <- receiveResult{response, err}
		}()

		// Wait for either timeout or result
		var response *replication_proto.WALStreamResponse
		var err error

		select {
		case <-receiveCtx.Done():
			// Timeout occurred - this is normal if no data is available
			return r.stateTracker.SetState(StateWaitingForData)
		case result := <-resultCh:
			// Got a result
			response = result.response
			err = result.err
		}

		if err != nil {
			if err == io.EOF {
				// Stream ended normally
				fmt.Printf("Stream ended with EOF\n")
				return r.stateTracker.SetState(StateWaitingForData)
			}
			// Handle GRPC errors
			st, ok := status.FromError(err)
			if ok {
				switch st.Code() {
				case codes.Unavailable:
					// Connection issue, reconnect
					fmt.Printf("Connection unavailable: %s\n", st.Message())
					return NewReplicationError(ErrorConnection, st.Message())
				case codes.OutOfRange:
					// Requested sequence no longer available
					fmt.Printf("Sequence out of range: %s\n", st.Message())
					return NewReplicationError(ErrorRetention, st.Message())
				default:
					// Other gRPC error
					fmt.Printf("GRPC error: %s\n", st.Message())
					return fmt.Errorf("stream error: %w", err)
				}
			}
			fmt.Printf("Stream receive error: %v\n", err)
			return fmt.Errorf("stream receive error: %w", err)
		}

		// Check if we received entries
		entryCount := len(response.Entries)
		fmt.Printf("STREAM STATE: Received batch with %d entries\n", entryCount)

		if entryCount == 0 {
			// No entries received, wait for more
			fmt.Printf("Received empty batch, waiting for more data\n")
			return r.stateTracker.SetState(StateWaitingForData)
		}

		// Important fix: We have received entries and need to process them
		fmt.Printf("IMPORTANT: Processing %d entries DIRECTLY\n", entryCount)

		// Process the entries directly without going through state transitions
		fmt.Printf("DIRECT PROCESSING: Processing %d entries without state transitions\n", entryCount)
		receivedBatch := response

		if err := r.processEntriesWithoutStateTransitions(receivedBatch); err != nil {
			fmt.Printf("Error directly processing entries: %v\n", err)
			return err
		}

		fmt.Printf("Successfully processed entries directly\n")

		// Return to streaming state to continue receiving
		return r.stateTracker.SetState(StateStreamingEntries)
	}
}

// handleApplyingState handles the APPLYING_ENTRIES state
func (r *Replica) handleApplyingState() error {
	fmt.Printf("In APPLYING_ENTRIES state - processing received entries\n")

	// In practice, this state is directly handled in processEntries called from handleStreamingState
	// But we need to handle the case where we might end up in this state without active processing

	// Check if we have a valid stream client
	if r.streamClient == nil {
		fmt.Printf("Stream client is nil in APPLYING_ENTRIES state, transitioning to CONNECTING\n")
		return r.stateTracker.SetState(StateConnecting)
	}

	// If we're in this state without active processing, transition to STREAMING_ENTRIES
	// to try to receive more entries
	fmt.Printf("No active processing in APPLYING_ENTRIES state, transitioning back to STREAMING_ENTRIES\n")
	return r.stateTracker.SetState(StateStreamingEntries)
}

// handleFsyncState handles the FSYNC_PENDING state
func (r *Replica) handleFsyncState() error {
	fmt.Printf("Performing fsync for WAL entries\n")

	// Perform fsync to persist applied entries
	if err := r.applier.Sync(); err != nil {
		fmt.Printf("Failed to sync WAL entries: %v\n", err)
		return fmt.Errorf("failed to sync WAL entries: %w", err)
	}

	fmt.Printf("Sync completed successfully\n")

	// Move to acknowledging state
	fmt.Printf("Moving to ACKNOWLEDGING state\n")
	return r.stateTracker.SetState(StateAcknowledging)
}

// handleAcknowledgingState handles the ACKNOWLEDGING state
func (r *Replica) handleAcknowledgingState() error {
	// Get the last applied sequence
	maxApplied := r.batchApplier.GetMaxApplied()
	fmt.Printf("Acknowledging entries up to sequence: %d\n", maxApplied)

	// Check if the client is nil - can happen if connection was broken
	if r.client == nil {
		fmt.Printf("ERROR: Client is nil in ACKNOWLEDGING state, reconnecting\n")
		return r.stateTracker.SetState(StateConnecting)
	}

	// Send acknowledgment to the primary
	ack := &replication_proto.Ack{
		AcknowledgedUpTo: maxApplied,
	}

	// Update our tracking (even if ack fails, we've still applied the entries)
	r.mu.Lock()
	r.lastAppliedSeq = maxApplied
	r.mu.Unlock()

	// Create a context with the session ID in the metadata if we have one
	ctx := r.ctx
	if r.sessionID != "" {
		md := metadata.Pairs("session-id", r.sessionID)
		ctx = metadata.NewOutgoingContext(r.ctx, md)
		fmt.Printf("Adding session ID %s to acknowledgment metadata\n", r.sessionID)
	} else {
		fmt.Printf("WARNING: No session ID available for acknowledgment - this will likely fail\n")
		// Try to extract session ID from stream header if available and streamClient exists
		if r.streamClient != nil {
			md, err := r.streamClient.Header()
			if err == nil {
				sessionIDs := md.Get("session-id")
				if len(sessionIDs) > 0 {
					r.sessionID = sessionIDs[0]
					fmt.Printf("Retrieved session ID from stream header: %s\n", r.sessionID)
					md = metadata.Pairs("session-id", r.sessionID)
					ctx = metadata.NewOutgoingContext(r.ctx, md)
				}
			}
		}
	}

	// Log the actual request we're sending
	fmt.Printf("Sending acknowledgment request: {AcknowledgedUpTo: %d}\n", ack.AcknowledgedUpTo)

	// Send the acknowledgment with session ID in context
	fmt.Printf("Calling Acknowledge RPC method on primary...\n")
	resp, err := r.client.Acknowledge(ctx, ack)
	if err != nil {
		fmt.Printf("ERROR: Failed to send acknowledgment: %v\n", err)

		// Try to determine if it's a connection issue or session issue
		st, ok := status.FromError(err)
		if ok {
			switch st.Code() {
			case codes.Unavailable:
				fmt.Printf("Connection unavailable (code: %s): %s\n", st.Code(), st.Message())
				return r.stateTracker.SetState(StateConnecting)
			case codes.NotFound, codes.Unauthenticated, codes.PermissionDenied:
				fmt.Printf("Session issue (code: %s): %s\n", st.Code(), st.Message())
				// Try reconnecting to get a new session
				return r.stateTracker.SetState(StateConnecting)
			default:
				fmt.Printf("RPC error (code: %s): %s\n", st.Code(), st.Message())
			}
		}

		// Mark it as an error but don't update applied sequence since we did apply the entries
		return fmt.Errorf("failed to send acknowledgment: %w", err)
	}

	// Log the acknowledgment response
	if resp.Success {
		fmt.Printf("SUCCESS: Acknowledgment accepted by primary up to sequence %d\n", maxApplied)
	} else {
		fmt.Printf("ERROR: Acknowledgment rejected by primary: %s\n", resp.Message)

		// Try to recover from session errors by reconnecting
		if resp.Message == "Unknown session" {
			fmt.Printf("Session issue detected, reconnecting...\n")
			return r.stateTracker.SetState(StateConnecting)
		}
	}

	// Update the last acknowledged sequence only after successful acknowledgment
	r.batchApplier.AcknowledgeUpTo(maxApplied)
	fmt.Printf("Local state updated, acknowledged up to sequence %d\n", maxApplied)

	// Return to streaming state
	fmt.Printf("Moving back to STREAMING_ENTRIES state\n")

	// Reset the streamClient to ensure the next fetch starts from our last acknowledged position
	// This is important to fix the issue where the same entries were being fetched repeatedly
	r.mu.Lock()
	r.streamClient = nil
	fmt.Printf("Reset stream client after acknowledgment. Next expected sequence will be %d\n",
		r.batchApplier.GetExpectedNext())
	r.mu.Unlock()

	return r.stateTracker.SetState(StateStreamingEntries)
}

// handleWaitingForDataState handles the WAITING_FOR_DATA state
func (r *Replica) handleWaitingForDataState() error {
	// This is a critical transition point - we need to check if we have entries
	// that need to be processed

	// Check if we have any pending entries from our stream client
	if r.streamClient != nil {
		// Use a non-blocking check to see if data is available
		receiveCtx, cancel := context.WithTimeout(r.ctx, 50*time.Millisecond)
		defer cancel()

		// Use a separate goroutine to receive data to avoid blocking
		done := make(chan struct{})
		var response *replication_proto.WALStreamResponse
		var err error

		go func() {
			fmt.Printf("Quick check for available entries from primary\n")
			response, err = r.streamClient.Recv()
			close(done)
		}()

		// Wait for either the receive to complete or the timeout
		select {
		case <-receiveCtx.Done():
			// No data immediately available, continue waiting
			fmt.Printf("No data immediately available in WAITING_FOR_DATA state\n")
		case <-done:
			// We got some data!
			if err != nil {
				fmt.Printf("Error checking for entries in WAITING_FOR_DATA: %v\n", err)
			} else if response != nil && len(response.Entries) > 0 {
				fmt.Printf("Found %d entries in WAITING_FOR_DATA state - processing immediately\n",
					len(response.Entries))

				// Process these entries immediately
				fmt.Printf("Moving to APPLYING_ENTRIES state from WAITING_FOR_DATA\n")
				if err := r.stateTracker.SetState(StateApplyingEntries); err != nil {
					return err
				}

				// Process the entries
				fmt.Printf("Processing received entries from WAITING_FOR_DATA\n")
				if err := r.processEntries(response); err != nil {
					fmt.Printf("Error processing entries: %v\n", err)
					return err
				}
				fmt.Printf("Entries processed successfully from WAITING_FOR_DATA\n")

				// Return to streaming state
				return r.stateTracker.SetState(StateStreamingEntries)
			}
		}
	}

	// Default behavior - just wait for more data
	select {
	case <-r.ctx.Done():
		return nil
	case <-time.After(time.Second):
		// Simply continue in waiting state, we'll try to receive data again
		// This avoids closing and reopening connections

		// Try to transition back to STREAMING_ENTRIES occasionally
		// This helps recover if we're stuck in WAITING_FOR_DATA
		if rand.Intn(5) == 0 { // 20% chance to try streaming state again
			fmt.Printf("Periodic transition back to STREAMING_ENTRIES from WAITING_FOR_DATA\n")
			return r.stateTracker.SetState(StateStreamingEntries)
		}
		return nil
	}
}

// handleErrorState handles the ERROR state with exponential backoff
func (r *Replica) handleErrorState(backoff *time.Timer) error {
	// Reset backoff timer
	backoff.Reset(r.calculateBackoff())

	// Wait for backoff timer or cancellation
	select {
	case <-r.ctx.Done():
		return nil
	case <-backoff.C:
		// Reset the state machine
		r.mu.Lock()
		if r.conn != nil {
			r.conn.Close()
			r.conn = nil
		}
		r.client = nil
		r.streamClient = nil // Also reset the stream client
		r.mu.Unlock()

		// Transition back to connecting state
		return r.stateTracker.SetState(StateConnecting)
	}
}

// PrimaryConnector abstracts connection to the primary for testing
type PrimaryConnector interface {
	Connect(r *Replica) error
}

// DefaultPrimaryConnector is the default implementation that connects to a gRPC server
type DefaultPrimaryConnector struct{}

// Connect establishes a connection to the primary node
func (c *DefaultPrimaryConnector) Connect(r *Replica) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if already connected
	if r.conn != nil {
		return nil
	}

	fmt.Printf("Connecting to primary at %s\n", r.config.Connection.PrimaryAddress)

	// Set up connection options
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTimeout(r.config.Connection.DialTimeout),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second, // Send pings every 30 seconds if there is no activity
			Timeout:             10 * time.Second, // Wait 10 seconds for ping ack before assuming connection is dead
			PermitWithoutStream: true,             // Allow pings even when there are no active streams
		}),
	}

	// Set up transport security
	if r.config.Connection.UseTLS {
		if r.config.Connection.TLSCredentials != nil {
			opts = append(opts, grpc.WithTransportCredentials(r.config.Connection.TLSCredentials))
		} else {
			return fmt.Errorf("TLS enabled but no credentials provided")
		}
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Connect to the server
	fmt.Printf("Dialing primary server at %s with timeout %v\n",
		r.config.Connection.PrimaryAddress, r.config.Connection.DialTimeout)
	conn, err := grpc.Dial(r.config.Connection.PrimaryAddress, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to primary at %s: %w",
			r.config.Connection.PrimaryAddress, err)
	}
	fmt.Printf("Successfully connected to primary server\n")

	// Create client
	client := replication_proto.NewWALReplicationServiceClient(conn)

	// Store connection and client
	r.conn = conn
	r.client = client

	fmt.Printf("Connection established and client created\n")

	return nil
}

// connectToPrimary establishes a connection to the primary node
func (r *Replica) connectToPrimary() error {
	return r.connector.Connect(r)
}

// processEntriesWithoutStateTransitions processes a batch of WAL entries without attempting state transitions
// This function is called from handleStreamingState and skips the state transitions at the end
func (r *Replica) processEntriesWithoutStateTransitions(response *replication_proto.WALStreamResponse) error {
	entryCount := len(response.Entries)
	fmt.Printf("Processing %d entries (no state transitions)\n", entryCount)

	// Track statistics
	if r.stats != nil {
		r.stats.TrackBatchReceived()
		r.stats.TrackEntriesReceived(uint64(entryCount))

		// Calculate total bytes received
		var totalBytes uint64
		for _, entry := range response.Entries {
			totalBytes += uint64(len(entry.Payload))
		}
		r.stats.TrackBytesReceived(totalBytes)
	}

	// Check if entries are compressed
	entries := response.Entries
	if response.Compressed && len(entries) > 0 {
		fmt.Printf("Decompressing entries with codec: %v\n", response.Codec)
		// Decompress payload for each entry
		for i, entry := range entries {
			if len(entry.Payload) > 0 {
				decompressed, err := r.compressor.Decompress(entry.Payload, response.Codec)
				if err != nil {
					return NewReplicationError(ErrorCompression,
						fmt.Sprintf("failed to decompress entry %d: %v", i, err))
				}
				entries[i].Payload = decompressed
			}
		}
	}

	fmt.Printf("Starting to apply entries, expected next: %d\n", r.batchApplier.GetExpectedNext())

	// Log details of first few entries for debugging
	for i, entry := range entries {
		if i < 3 { // Only log a few
			fmt.Printf("Entry to apply %d: seq=%d, fragment=%v, payload=%d bytes\n",
				i, entry.SequenceNumber, entry.FragmentType, len(entry.Payload))

			// Add more detailed debug info for the first few entries
			if len(entry.Payload) > 0 {
				hexBytes := ""
				for j, b := range entry.Payload {
					if j < 16 {
						hexBytes += fmt.Sprintf("%02x ", b)
					}
				}
				fmt.Printf("  Payload first 16 bytes: %s\n", hexBytes)
			}
		}
	}

	// Apply the entries
	maxSeq, hasGap, err := r.batchApplier.ApplyEntries(entries, r.applyEntry)
	if err != nil {
		if hasGap {
			// Handle gap by requesting retransmission
			fmt.Printf("Sequence gap detected, requesting retransmission\n")
			return r.handleSequenceGap(entries[0].SequenceNumber)
		}
		fmt.Printf("Failed to apply entries: %v\n", err)
		return fmt.Errorf("failed to apply entries: %w", err)
	}

	fmt.Printf("Successfully applied entries up to sequence %d\n", maxSeq)

	// Update last applied sequence
	r.mu.Lock()
	r.lastAppliedSeq = maxSeq
	r.mu.Unlock()

	// Track applied entries in statistics
	if r.stats != nil {
		// Calculate the number of entries that were successfully applied
		appliedCount := uint64(0)
		for _, entry := range entries {
			if entry.SequenceNumber <= maxSeq {
				appliedCount++
			}
		}
		r.stats.TrackEntriesApplied(appliedCount)
	}

	// Perform fsync directly without transitioning state
	fmt.Printf("Performing direct fsync to ensure entries are persisted\n")
	if err := r.applier.Sync(); err != nil {
		fmt.Printf("Failed to sync WAL entries: %v\n", err)
		return fmt.Errorf("failed to sync WAL entries: %w", err)
	}
	fmt.Printf("Successfully synced WAL entries to disk\n")

	return nil
}

// processEntries processes a batch of WAL entries
func (r *Replica) processEntries(response *replication_proto.WALStreamResponse) error {
	entryCount := len(response.Entries)
	fmt.Printf("Processing %d entries\n", entryCount)

	// Track statistics
	if r.stats != nil {
		r.stats.TrackBatchReceived()
		r.stats.TrackEntriesReceived(uint64(entryCount))

		// Calculate total bytes received
		var totalBytes uint64
		for _, entry := range response.Entries {
			totalBytes += uint64(len(entry.Payload))
		}
		r.stats.TrackBytesReceived(totalBytes)
	}

	// Check if entries are compressed
	entries := response.Entries
	if response.Compressed && len(entries) > 0 {
		fmt.Printf("Decompressing entries with codec: %v\n", response.Codec)
		// Decompress payload for each entry
		for i, entry := range entries {
			if len(entry.Payload) > 0 {
				decompressed, err := r.compressor.Decompress(entry.Payload, response.Codec)
				if err != nil {
					return NewReplicationError(ErrorCompression,
						fmt.Sprintf("failed to decompress entry %d: %v", i, err))
				}
				entries[i].Payload = decompressed
			}
		}
	}

	fmt.Printf("Starting to apply entries, expected next: %d\n", r.batchApplier.GetExpectedNext())

	// Log details of first few entries for debugging
	for i, entry := range entries {
		if i < 3 { // Only log a few
			fmt.Printf("Entry to apply %d: seq=%d, fragment=%v, payload=%d bytes\n",
				i, entry.SequenceNumber, entry.FragmentType, len(entry.Payload))

			// Add more detailed debug info for the first few entries
			if len(entry.Payload) > 0 {
				hexBytes := ""
				for j, b := range entry.Payload {
					if j < 16 {
						hexBytes += fmt.Sprintf("%02x ", b)
					}
				}
				fmt.Printf("  Payload first 16 bytes: %s\n", hexBytes)
			}
		}
	}

	// Apply the entries
	maxSeq, hasGap, err := r.batchApplier.ApplyEntries(entries, r.applyEntry)
	if err != nil {
		if hasGap {
			// Handle gap by requesting retransmission
			fmt.Printf("Sequence gap detected, requesting retransmission\n")
			return r.handleSequenceGap(entries[0].SequenceNumber)
		}
		fmt.Printf("Failed to apply entries: %v\n", err)
		return fmt.Errorf("failed to apply entries: %w", err)
	}

	fmt.Printf("Successfully applied entries up to sequence %d\n", maxSeq)

	// Update last applied sequence
	r.mu.Lock()
	r.lastAppliedSeq = maxSeq
	r.mu.Unlock()

	// Track applied entries in statistics
	if r.stats != nil {
		// Calculate the number of entries that were successfully applied
		appliedCount := uint64(0)
		for _, entry := range entries {
			if entry.SequenceNumber <= maxSeq {
				appliedCount++
			}
		}
		r.stats.TrackEntriesApplied(appliedCount)
	}

	// Move to fsync state
	fmt.Printf("Moving to FSYNC_PENDING state\n")
	if err := r.stateTracker.SetState(StateFsyncPending); err != nil {
		return err
	}

	// Immediately process the fsync state to keep the state machine moving
	// This avoids getting stuck in FSYNC_PENDING state
	fmt.Printf("Directly calling FSYNC handler\n")
	return r.handleFsyncState()
}

// applyEntry applies a single WAL entry using the configured applier
func (r *Replica) applyEntry(entry *wal.Entry) error {
	fmt.Printf("Applying WAL entry: seq=%d, type=%d, key=%s\n",
		entry.SequenceNumber, entry.Type, string(entry.Key))

	// Apply the entry using the configured applier
	err := r.applier.Apply(entry)
	if err != nil {
		fmt.Printf("Error applying entry: %v\n", err)
		return fmt.Errorf("failed to apply entry: %w", err)
	}

	fmt.Printf("Successfully applied entry seq=%d\n", entry.SequenceNumber)
	return nil
}

// handleSequenceGap handles a detected sequence gap by requesting retransmission
func (r *Replica) handleSequenceGap(receivedSeq uint64) error {
	// Create a negative acknowledgment
	nack := &replication_proto.Nack{
		MissingFromSequence: r.batchApplier.GetExpectedNext(),
	}

	// Create a context with the session ID in the metadata if we have one
	ctx := r.ctx
	if r.sessionID != "" {
		md := metadata.Pairs("session-id", r.sessionID)
		ctx = metadata.NewOutgoingContext(r.ctx, md)
		fmt.Printf("Adding session ID %s to NACK metadata\n", r.sessionID)
	} else {
		fmt.Printf("Warning: No session ID available for NACK\n")
	}

	// Send the NACK with session ID in context
	_, err := r.client.NegativeAcknowledge(ctx, nack)
	if err != nil {
		return fmt.Errorf("failed to send negative acknowledgment: %w", err)
	}

	// Return to streaming state
	return nil
}

// createBackoff creates a timer for exponential backoff
func (r *Replica) createBackoff() *time.Timer {
	return time.NewTimer(r.config.Connection.RetryBaseDelay)
}

// calculateBackoff determines the next backoff duration
func (r *Replica) calculateBackoff() time.Duration {
	// Get current backoff
	state := r.stateTracker.GetState()
	if state != StateError {
		return r.config.Connection.RetryBaseDelay
	}

	// Calculate next backoff based on how long we've been in error state
	duration := r.stateTracker.GetStateDuration()
	backoff := r.config.Connection.RetryBaseDelay * time.Duration(float64(duration/r.config.Connection.RetryBaseDelay+1)*r.config.Connection.RetryMultiplier)

	// Cap at max delay
	if backoff > r.config.Connection.RetryMaxDelay {
		backoff = r.config.Connection.RetryMaxDelay
	}

	return backoff
}
