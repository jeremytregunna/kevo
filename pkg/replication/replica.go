package replication

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	replication_proto "github.com/KevoDB/kevo/pkg/replication/proto"
	"github.com/KevoDB/kevo/pkg/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// WALEntryApplier defines an interface for applying WAL entries on a replica
type WALEntryApplier interface {
	// Apply applies a single WAL entry to the local storage
	Apply(entry *wal.Entry) error

	// Sync ensures all applied entries are persisted to disk
	Sync() error
}

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
		CompressionSupported: true,
		PreferredCodec:       replication_proto.CompressionCodec_ZSTD,
		ProtocolVersion:      1,
		AckInterval:          time.Second * 5,
		MaxBatchSize:         1024 * 1024, // 1MB
		ReportMetrics:        true,
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

	// Compressor for handling compressed payloads
	compressor *Compressor

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
	compressor, err := NewCompressor()
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

	// Close connection
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}

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
	// Create a WAL stream request
	nextSeq := r.batchApplier.GetExpectedNext()
	fmt.Printf("Creating stream request, starting from sequence: %d\n", nextSeq)

	request := &replication_proto.WALStreamRequest{
		StartSequence:        nextSeq,
		ProtocolVersion:      r.config.ProtocolVersion,
		CompressionSupported: r.config.CompressionSupported,
		PreferredCodec:       r.config.PreferredCodec,
	}

	// Start streaming from the primary
	stream, err := r.client.StreamWAL(r.ctx, request)
	if err != nil {
		return fmt.Errorf("failed to start WAL stream: %w", err)
	}

	fmt.Printf("Stream established, waiting for entries\n")

	// Process the stream
	for {
		select {
		case <-r.ctx.Done():
			fmt.Printf("Context done, exiting streaming state\n")
			return nil
		default:
			// Receive next batch
			fmt.Printf("Waiting to receive next batch...\n")
			response, err := stream.Recv()
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
			fmt.Printf("Received batch with %d entries\n", len(response.Entries))
			if len(response.Entries) == 0 {
				// No entries received, wait for more
				fmt.Printf("Received empty batch, waiting for more data\n")
				if err := r.stateTracker.SetState(StateWaitingForData); err != nil {
					return err
				}
				continue
			}

			// Log sequence numbers received
			for i, entry := range response.Entries {
				fmt.Printf("Entry %d: sequence number %d\n", i, entry.SequenceNumber)
			}

			// Store the received batch for processing
			r.mu.Lock()
			// Store received batch data for processing
			receivedBatch := response
			r.mu.Unlock()

			// Move to applying state
			fmt.Printf("Moving to APPLYING_ENTRIES state\n")
			if err := r.stateTracker.SetState(StateApplyingEntries); err != nil {
				return err
			}

			// Process the entries
			fmt.Printf("Processing received entries\n")
			if err := r.processEntries(receivedBatch); err != nil {
				fmt.Printf("Error processing entries: %v\n", err)
				return err
			}
			fmt.Printf("Entries processed successfully\n")
		}
	}
}

// handleApplyingState handles the APPLYING_ENTRIES state
func (r *Replica) handleApplyingState() error {
	// This is handled by processEntries called from handleStreamingState
	// The state should have already moved to FSYNC_PENDING
	// If we're still in APPLYING_ENTRIES, it's an error
	return fmt.Errorf("invalid state: still in APPLYING_ENTRIES without active processing")
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

	// Send acknowledgment to the primary
	ack := &replication_proto.Ack{
		AcknowledgedUpTo: maxApplied,
	}

	// Update the last acknowledged sequence
	r.batchApplier.AcknowledgeUpTo(maxApplied)

	// Send the acknowledgment
	_, err := r.client.Acknowledge(r.ctx, ack)
	if err != nil {
		fmt.Printf("Failed to send acknowledgment: %v\n", err)
		return fmt.Errorf("failed to send acknowledgment: %w", err)
	}
	fmt.Printf("Acknowledgment sent successfully\n")

	// Update our tracking
	r.mu.Lock()
	r.lastAppliedSeq = maxApplied
	r.mu.Unlock()

	// Return to streaming state
	fmt.Printf("Moving back to STREAMING_ENTRIES state\n")
	return r.stateTracker.SetState(StateStreamingEntries)
}

// handleWaitingForDataState handles the WAITING_FOR_DATA state
func (r *Replica) handleWaitingForDataState() error {
	// Wait for a short period before checking again
	select {
	case <-r.ctx.Done():
		return nil
	case <-time.After(time.Second):
		// Return to streaming state
		return r.stateTracker.SetState(StateStreamingEntries)
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

	// Set up connection options
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTimeout(r.config.Connection.DialTimeout),
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
	conn, err := grpc.Dial(r.config.Connection.PrimaryAddress, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to primary at %s: %w",
			r.config.Connection.PrimaryAddress, err)
	}

	// Create client
	client := replication_proto.NewWALReplicationServiceClient(conn)

	// Store connection and client
	r.conn = conn
	r.client = client

	return nil
}

// connectToPrimary establishes a connection to the primary node
func (r *Replica) connectToPrimary() error {
	return r.connector.Connect(r)
}

// processEntries processes a batch of WAL entries
func (r *Replica) processEntries(response *replication_proto.WALStreamResponse) error {
	fmt.Printf("Processing %d entries\n", len(response.Entries))

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
	return r.applier.Apply(entry)
}

// handleSequenceGap handles a detected sequence gap by requesting retransmission
func (r *Replica) handleSequenceGap(receivedSeq uint64) error {
	// Create a negative acknowledgment
	nack := &replication_proto.Nack{
		MissingFromSequence: r.batchApplier.GetExpectedNext(),
	}

	// Send the NACK
	_, err := r.client.NegativeAcknowledge(r.ctx, nack)
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
