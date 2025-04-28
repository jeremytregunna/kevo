package replication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	proto "github.com/KevoDB/kevo/pkg/replication/proto"
	"github.com/KevoDB/kevo/pkg/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Primary implements the primary node functionality for WAL replication.
// It observes WAL entries and serves them to replica nodes.
type Primary struct {
	wal               *wal.WAL                      // Reference to the WAL
	batcher           *WALBatcher                   // Batches WAL entries for efficient transmission
	compressor        *Compressor                   // Handles compression/decompression
	sessions          map[string]*ReplicaSession    // Active replica sessions
	lastSyncedSeq     uint64                        // Highest sequence number synced to disk
	retentionConfig   WALRetentionConfig            // Configuration for WAL retention
	enableCompression bool                          // Whether compression is enabled
	defaultCodec      proto.CompressionCodec // Default compression codec
	mu                sync.RWMutex                  // Protects sessions map

	proto.UnimplementedWALReplicationServiceServer
}

// WALRetentionConfig defines WAL file retention policy
type WALRetentionConfig struct {
	MaxAgeHours    int   // Maximum age of WAL files in hours
	MinSequenceKeep uint64 // Minimum sequence number to preserve
}

// PrimaryConfig contains configuration for the primary node
type PrimaryConfig struct {
	MaxBatchSizeKB     int                           // Maximum batch size in KB
	EnableCompression  bool                          // Whether to enable compression
	CompressionCodec   proto.CompressionCodec // Compression codec to use
	RetentionConfig    WALRetentionConfig            // WAL retention configuration
	RespectTxBoundaries bool                         // Whether to respect transaction boundaries in batching
}

// DefaultPrimaryConfig returns a default configuration for primary nodes
func DefaultPrimaryConfig() *PrimaryConfig {
	return &PrimaryConfig{
		MaxBatchSizeKB:     256, // 256KB default batch size
		EnableCompression:  true,
		CompressionCodec:   proto.CompressionCodec_ZSTD,
		RetentionConfig: WALRetentionConfig{
			MaxAgeHours:    24, // Keep WAL files for 24 hours by default
			MinSequenceKeep: 0, // No sequence-based retention by default
		},
		RespectTxBoundaries: true,
	}
}

// ReplicaSession represents a connected replica
type ReplicaSession struct {
	ID               string                         // Unique session ID
	StartSequence    uint64                         // Requested start sequence
	Stream           proto.WALReplicationService_StreamWALServer // gRPC stream
	LastAckSequence  uint64                         // Last acknowledged sequence
	SupportedCodecs  []proto.CompressionCodec // Supported compression codecs
	Connected        bool                           // Whether the session is connected
	Active           bool                           // Whether the session is actively receiving WAL entries
	LastActivity     time.Time                      // Time of last activity
	mu               sync.Mutex                     // Protects session state
}

// NewPrimary creates a new primary node for replication
func NewPrimary(w *wal.WAL, config *PrimaryConfig) (*Primary, error) {
	if w == nil {
		return nil, errors.New("WAL cannot be nil")
	}

	if config == nil {
		config = DefaultPrimaryConfig()
	}

	// Create compressor
	compressor, err := NewCompressor()
	if err != nil {
		return nil, fmt.Errorf("failed to create compressor: %w", err)
	}

	// Create batcher
	batcher := NewWALBatcher(
		config.MaxBatchSizeKB,
		config.CompressionCodec,
		config.RespectTxBoundaries,
	)

	primary := &Primary{
		wal:               w,
		batcher:           batcher,
		compressor:        compressor,
		sessions:          make(map[string]*ReplicaSession),
		lastSyncedSeq:     0,
		retentionConfig:   config.RetentionConfig,
		enableCompression: config.EnableCompression,
		defaultCodec:      config.CompressionCodec,
	}

	// Register as a WAL observer
	w.RegisterObserver("primary_replication", primary)

	return primary, nil
}

// OnWALEntryWritten implements WALEntryObserver.OnWALEntryWritten
func (p *Primary) OnWALEntryWritten(entry *wal.Entry) {
	// Add to batch and broadcast if batch is full
	batchReady, err := p.batcher.AddEntry(entry)
	if err != nil {
		// Log error but continue - don't block WAL operations
		fmt.Printf("Error adding WAL entry to batch: %v\n", err)
		return
	}

	if batchReady {
		response := p.batcher.GetBatch()
		p.broadcastToReplicas(response)
	}
}

// OnWALBatchWritten implements WALEntryObserver.OnWALBatchWritten
func (p *Primary) OnWALBatchWritten(startSeq uint64, entries []*wal.Entry) {
	// Reset batcher to ensure a clean state when processing a batch
	p.batcher.Reset()

	// Process each entry in the batch
	for _, entry := range entries {
		ready, err := p.batcher.AddEntry(entry)
		if err != nil {
			fmt.Printf("Error adding batch entry to replication: %v\n", err)
			continue
		}

		// If we filled up the batch during processing, send it
		if ready {
			response := p.batcher.GetBatch()
			p.broadcastToReplicas(response)
		}
	}

	// If we have entries in the batch after processing all entries, send them
	if p.batcher.GetBatchCount() > 0 {
		response := p.batcher.GetBatch()
		p.broadcastToReplicas(response)
	}
}

// OnWALSync implements WALEntryObserver.OnWALSync
func (p *Primary) OnWALSync(upToSeq uint64) {
	p.mu.Lock()
	p.lastSyncedSeq = upToSeq
	p.mu.Unlock()

	// If we have any buffered entries, send them now that they're synced
	if p.batcher.GetBatchCount() > 0 {
		response := p.batcher.GetBatch()
		p.broadcastToReplicas(response)
	}
}

// StreamWAL implements WALReplicationServiceServer.StreamWAL
func (p *Primary) StreamWAL(
	req *proto.WALStreamRequest,
	stream proto.WALReplicationService_StreamWALServer,
) error {
	// Validate request
	if req.StartSequence < 0 {
		return status.Error(codes.InvalidArgument, "start_sequence must be non-negative")
	}

	// Create a new session for this replica
	sessionID := fmt.Sprintf("replica-%d", time.Now().UnixNano())
	session := &ReplicaSession{
		ID:               sessionID,
		StartSequence:    req.StartSequence,
		Stream:           stream,
		LastAckSequence:  req.StartSequence,
		SupportedCodecs:  []proto.CompressionCodec{proto.CompressionCodec_NONE},
		Connected:        true,
		Active:           true,
		LastActivity:     time.Now(),
	}

	// Determine compression support
	if req.CompressionSupported {
		if req.PreferredCodec != proto.CompressionCodec_NONE {
			// Use replica's preferred codec if supported
			session.SupportedCodecs = []proto.CompressionCodec{
				req.PreferredCodec,
				proto.CompressionCodec_NONE, // Always support no compression as fallback
			}
		} else {
			// Replica supports compression but has no preference, use defaults
			session.SupportedCodecs = []proto.CompressionCodec{
				p.defaultCodec,
				proto.CompressionCodec_NONE,
			}
		}
	}

	// Register the session
	p.registerReplicaSession(session)
	defer p.unregisterReplicaSession(session.ID)

	// Send initial entries if starting from a specific sequence
	if req.StartSequence > 0 {
		if err := p.sendInitialEntries(session); err != nil {
			return fmt.Errorf("failed to send initial entries: %w", err)
		}
	}

	// Keep the stream alive until client disconnects
	ctx := stream.Context()
	<-ctx.Done()

	return ctx.Err()
}

// Acknowledge implements WALReplicationServiceServer.Acknowledge
func (p *Primary) Acknowledge(
	ctx context.Context,
	req *proto.Ack,
) (*proto.AckResponse, error) {
	// Update session with acknowledgment
	sessionID := p.getSessionIDFromContext(ctx)
	if sessionID == "" {
		return &proto.AckResponse{
			Success: false,
			Message: "Unknown session",
		}, nil
	}

	// Update the session's acknowledged sequence
	if err := p.updateSessionAck(sessionID, req.AcknowledgedUpTo); err != nil {
		return &proto.AckResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	// Check if we can prune WAL files
	p.maybeManageWALRetention()

	return &proto.AckResponse{
		Success: true,
	}, nil
}

// NegativeAcknowledge implements WALReplicationServiceServer.NegativeAcknowledge
func (p *Primary) NegativeAcknowledge(
	ctx context.Context,
	req *proto.Nack,
) (*proto.NackResponse, error) {
	// Get the session ID from context
	sessionID := p.getSessionIDFromContext(ctx)
	if sessionID == "" {
		return &proto.NackResponse{
			Success: false,
			Message: "Unknown session",
		}, nil
	}

	// Get the session
	session := p.getSession(sessionID)
	if session == nil {
		return &proto.NackResponse{
			Success: false,
			Message: "Session not found",
		}, nil
	}

	// Resend WAL entries from the requested sequence
	if err := p.resendEntries(session, req.MissingFromSequence); err != nil {
		return &proto.NackResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to resend entries: %v", err),
		}, nil
	}

	return &proto.NackResponse{
		Success: true,
	}, nil
}

// broadcastToReplicas sends a WAL stream response to all connected replicas
func (p *Primary) broadcastToReplicas(response *proto.WALStreamResponse) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, session := range p.sessions {
		if !session.Connected || !session.Active {
			continue
		}

		// Copy the response for each replica to avoid race conditions
		sessionResponse := *response

		// Check if this session has requested entries from a higher sequence
		if len(sessionResponse.Entries) > 0 &&
		   sessionResponse.Entries[0].SequenceNumber <= session.StartSequence {
			continue
		}

		// Send to the replica
		p.sendToReplica(session, &sessionResponse)
	}
}

// sendToReplica sends a WAL stream response to a specific replica
func (p *Primary) sendToReplica(session *ReplicaSession, response *proto.WALStreamResponse) {
	if session == nil || !session.Connected || !session.Active {
		return
	}

	// Clone the response to avoid concurrent modification
	clonedResponse := &proto.WALStreamResponse{
		Entries:    response.Entries,
		Compressed: response.Compressed,
		Codec:      response.Codec,
	}

	// Adjust compression based on replica's capabilities
	if clonedResponse.Compressed {
		codecSupported := false
		for _, codec := range session.SupportedCodecs {
			if codec == clonedResponse.Codec {
				codecSupported = true
				break
			}
		}

		if !codecSupported {
			// Decompress and use a codec the replica supports
			decompressedEntries := make([]*proto.WALEntry, 0, len(clonedResponse.Entries))

			for _, entry := range clonedResponse.Entries {
				// Copy the entry to avoid modifying the original
				decompressedEntry := &proto.WALEntry{
					SequenceNumber: entry.SequenceNumber,
					FragmentType:   entry.FragmentType,
					Checksum:       entry.Checksum,
				}

				// Decompress if needed
				if clonedResponse.Compressed {
					decompressed, err := p.compressor.Decompress(entry.Payload, clonedResponse.Codec)
					if err != nil {
						fmt.Printf("Error decompressing entry: %v\n", err)
						continue
					}
					decompressedEntry.Payload = decompressed
				} else {
					decompressedEntry.Payload = entry.Payload
				}

				decompressedEntries = append(decompressedEntries, decompressedEntry)
			}

			// Update the response with uncompressed entries
			clonedResponse.Entries = decompressedEntries
			clonedResponse.Compressed = false
			clonedResponse.Codec = proto.CompressionCodec_NONE
		}
	}

	// Acquire lock to send to the stream
	session.mu.Lock()
	defer session.mu.Unlock()

	// Send response through the gRPC stream
	if err := session.Stream.Send(clonedResponse); err != nil {
		fmt.Printf("Error sending to replica %s: %v\n", session.ID, err)
		session.Connected = false
	} else {
		session.LastActivity = time.Now()
	}
}

// sendInitialEntries sends WAL entries from the requested start sequence to a replica
func (p *Primary) sendInitialEntries(session *ReplicaSession) error {
	// Get entries from WAL
	// Note: This is a simplified approach. A production implementation would:
	// 1. Have more efficient retrieval of WAL entries by sequence
	// 2. Handle large ranges of entries by sending in batches
	// 3. Implement proper error handling for missing WAL files

	// For now, we'll use a placeholder implementation
	entries, err := p.getWALEntriesFromSequence(session.StartSequence)
	if err != nil {
		return fmt.Errorf("failed to get WAL entries: %w", err)
	}

	if len(entries) == 0 {
		// No entries to send, that's okay
		return nil
	}

	// Convert WAL entries to protocol buffer entries
	protoEntries := make([]*proto.WALEntry, 0, len(entries))
	for _, entry := range entries {
		protoEntry, err := WALEntryToProto(entry, proto.FragmentType_FULL)
		if err != nil {
			fmt.Printf("Error converting entry %d to proto: %v\n", entry.SequenceNumber, err)
			continue
		}
		protoEntries = append(protoEntries, protoEntry)
	}

	// Create a response with the entries
	response := &proto.WALStreamResponse{
		Entries:    protoEntries,
		Compressed: false, // Initial entries are sent uncompressed for simplicity
		Codec:      proto.CompressionCodec_NONE,
	}

	// Send to the replica
	session.mu.Lock()
	defer session.mu.Unlock()

	if err := session.Stream.Send(response); err != nil {
		return fmt.Errorf("failed to send initial entries: %w", err)
	}

	session.LastActivity = time.Now()
	return nil
}

// resendEntries resends WAL entries from the requested sequence to a replica
func (p *Primary) resendEntries(session *ReplicaSession, fromSequence uint64) error {
	// Similar to sendInitialEntries but for handling NACKs
	entries, err := p.getWALEntriesFromSequence(fromSequence)
	if err != nil {
		return fmt.Errorf("failed to get WAL entries: %w", err)
	}

	if len(entries) == 0 {
		return fmt.Errorf("no entries found from sequence %d", fromSequence)
	}

	// Convert WAL entries to protocol buffer entries
	protoEntries := make([]*proto.WALEntry, 0, len(entries))
	for _, entry := range entries {
		protoEntry, err := WALEntryToProto(entry, proto.FragmentType_FULL)
		if err != nil {
			fmt.Printf("Error converting entry %d to proto: %v\n", entry.SequenceNumber, err)
			continue
		}
		protoEntries = append(protoEntries, protoEntry)
	}

	// Create a response with the entries
	response := &proto.WALStreamResponse{
		Entries:    protoEntries,
		Compressed: false, // Resent entries are uncompressed for simplicity
		Codec:      proto.CompressionCodec_NONE,
	}

	// Send to the replica
	session.mu.Lock()
	defer session.mu.Unlock()

	if err := session.Stream.Send(response); err != nil {
		return fmt.Errorf("failed to resend entries: %w", err)
	}

	session.LastActivity = time.Now()
	return nil
}

// getWALEntriesFromSequence retrieves WAL entries starting from the specified sequence
// Note: This is a placeholder implementation that needs to be connected to actual WAL retrieval
func (p *Primary) getWALEntriesFromSequence(fromSequence uint64) ([]*wal.Entry, error) {
	// TODO: Implement proper WAL entry retrieval from sequence
	// This will need to be connected to a WAL reader that can scan WAL files for entries
	// with sequence numbers >= fromSequence

	// For now, return an empty slice as a placeholder
	return []*wal.Entry{}, nil
}

// registerReplicaSession adds a new replica session
func (p *Primary) registerReplicaSession(session *ReplicaSession) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.sessions[session.ID] = session
	fmt.Printf("Registered new replica session: %s starting from sequence %d\n",
		session.ID, session.StartSequence)
}

// unregisterReplicaSession removes a replica session
func (p *Primary) unregisterReplicaSession(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.sessions[id]; exists {
		delete(p.sessions, id)
		fmt.Printf("Unregistered replica session: %s\n", id)
	}
}

// getSessionIDFromContext extracts the session ID from the gRPC context
// Note: In a real implementation, this would use proper authentication and session tracking
func (p *Primary) getSessionIDFromContext(ctx context.Context) string {
	// In a real implementation, this would extract session information
	// from authentication metadata or other context values

	// For now, we'll use a placeholder approach
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Return the first active session ID (this is just a placeholder)
	for id, session := range p.sessions {
		if session.Connected {
			return id
		}
	}

	return ""
}

// updateSessionAck updates a session's acknowledged sequence
func (p *Primary) updateSessionAck(sessionID string, ackSeq uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	session, exists := p.sessions[sessionID]
	if !exists {
		return fmt.Errorf("session %s not found", sessionID)
	}

	session.LastAckSequence = ackSeq
	session.LastActivity = time.Now()

	return nil
}

// getSession retrieves a session by ID
func (p *Primary) getSession(id string) *ReplicaSession {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.sessions[id]
}

// maybeManageWALRetention checks if WAL retention management should be triggered
func (p *Primary) maybeManageWALRetention() {
	// This method would analyze all replica acknowledgments to determine
	// the minimum acknowledged sequence across all replicas, then use that
	// to decide which WAL files can be safely deleted.

	// For now, this is a placeholder that would need to be connected to the
	// actual WAL retention management logic
	// TODO: Implement WAL retention management
}

// Close shuts down the primary, unregistering from WAL and cleaning up resources
func (p *Primary) Close() error {
	// Unregister from WAL
	p.wal.UnregisterObserver("primary_replication")

	// Close all replica sessions
	p.mu.Lock()
	for id := range p.sessions {
		session := p.sessions[id]
		session.Connected = false
		session.Active = false
	}
	p.sessions = make(map[string]*ReplicaSession)
	p.mu.Unlock()

	// Close the compressor
	if p.compressor != nil {
		p.compressor.Close()
	}

	return nil
}
