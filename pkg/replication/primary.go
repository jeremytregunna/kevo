package replication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/common/log"
	"github.com/KevoDB/kevo/pkg/wal"
	proto "github.com/KevoDB/kevo/proto/kevo/replication"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Primary implements the primary node functionality for WAL replication.
// It observes WAL entries and serves them to replica nodes.
type Primary struct {
	wal               *wal.WAL                   // Reference to the WAL
	batcher           *WALBatcher                // Batches WAL entries for efficient transmission
	compressor        *CompressionManager        // Handles compression/decompression
	sessions          map[string]*ReplicaSession // Active replica sessions
	lastSyncedSeq     uint64                     // Highest sequence number synced to disk
	retentionConfig   WALRetentionConfig         // Configuration for WAL retention
	enableCompression bool                       // Whether compression is enabled
	defaultCodec      proto.CompressionCodec     // Default compression codec
	heartbeat         *heartbeatManager          // Manages heartbeats and session monitoring
	mu                sync.RWMutex               // Protects sessions map

	proto.UnimplementedWALReplicationServiceServer
}

// WALRetentionConfig defines WAL file retention policy
type WALRetentionConfig struct {
	MaxAgeHours     int    // Maximum age of WAL files in hours
	MinSequenceKeep uint64 // Minimum sequence number to preserve
}

// PrimaryConfig contains configuration for the primary node
type PrimaryConfig struct {
	MaxBatchSizeKB      int                    // Maximum batch size in KB
	EnableCompression   bool                   // Whether to enable compression
	CompressionCodec    proto.CompressionCodec // Compression codec to use
	RetentionConfig     WALRetentionConfig     // WAL retention configuration
	RespectTxBoundaries bool                   // Whether to respect transaction boundaries in batching
	HeartbeatConfig     *HeartbeatConfig       // Configuration for heartbeat/keepalive
}

// DefaultPrimaryConfig returns a default configuration for primary nodes
func DefaultPrimaryConfig() *PrimaryConfig {
	return &PrimaryConfig{
		MaxBatchSizeKB:    256, // 256KB default batch size
		EnableCompression: true,
		CompressionCodec:  proto.CompressionCodec_ZSTD,
		RetentionConfig: WALRetentionConfig{
			MaxAgeHours:     24, // Keep WAL files for 24 hours by default
			MinSequenceKeep: 0,  // No sequence-based retention by default
		},
		RespectTxBoundaries: true,
		HeartbeatConfig:     DefaultHeartbeatConfig(),
	}
}

// ReplicaSession represents a connected replica
type ReplicaSession struct {
	ID              string                                      // Unique session ID
	StartSequence   uint64                                      // Requested start sequence
	Stream          proto.WALReplicationService_StreamWALServer // gRPC stream
	LastAckSequence uint64                                      // Last acknowledged sequence
	SupportedCodecs []proto.CompressionCodec                    // Supported compression codecs
	Connected       bool                                        // Whether the session is connected
	Active          bool                                        // Whether the session is actively receiving WAL entries
	LastActivity    time.Time                                   // Time of last activity
	ListenerAddress string                                      // Network address (host:port) the replica is listening on
	mu              sync.Mutex                                  // Protects session state
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
	compressor, err := NewCompressionManager()
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

	// Create heartbeat manager
	primary.heartbeat = newHeartbeatManager(primary, config.HeartbeatConfig)

	// Register as a WAL observer
	w.RegisterObserver("primary_replication", primary)

	// Start heartbeat monitoring
	primary.heartbeat.start()

	return primary, nil
}

// OnWALEntryWritten implements WALEntryObserver.OnWALEntryWritten
func (p *Primary) OnWALEntryWritten(entry *wal.Entry) {
	log.Info("WAL entry written: seq=%d, type=%d, key=%s",
		entry.SequenceNumber, entry.Type, string(entry.Key))

	// Add to batch and broadcast if batch is full
	batchReady, err := p.batcher.AddEntry(entry)
	if err != nil {
		// Log error but continue - don't block WAL operations
		log.Error("Error adding WAL entry to batch: %v", err)
		return
	}

	if batchReady {
		log.Info("Batch ready for broadcast with %d entries", p.batcher.GetBatchCount())
		response := p.batcher.GetBatch()
		p.broadcastToReplicas(response)
	} else {
		log.Info("Entry added to batch (not ready for broadcast yet), current count: %d",
			p.batcher.GetBatchCount())

		// Even if the batch is not technically "ready", force sending if we have entries
		// This is particularly important in low-traffic scenarios
		if p.batcher.GetBatchCount() > 0 {
			log.Info("Forcibly sending partial batch with %d entries", p.batcher.GetBatchCount())
			response := p.batcher.GetBatch()
			p.broadcastToReplicas(response)
		}
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
			log.Error("Error adding batch entry to replication: %v", err)
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

	// Get the listener address from the request
	listenerAddress := req.ListenerAddress
	if listenerAddress == "" {
		return status.Error(codes.InvalidArgument, "listener_address is required")
	}

	log.Info("Replica registered with address: %s", listenerAddress)

	session := &ReplicaSession{
		ID:              sessionID,
		StartSequence:   req.StartSequence,
		Stream:          stream,
		LastAckSequence: req.StartSequence,
		SupportedCodecs: []proto.CompressionCodec{proto.CompressionCodec_NONE},
		Connected:       true,
		Active:          true,
		LastActivity:    time.Now(),
		ListenerAddress: listenerAddress,
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

	// Send the session ID in the response header metadata
	// This is critical for the replica to identify itself in future requests
	md := metadata.Pairs("session-id", session.ID)
	if err := stream.SendHeader(md); err != nil {
		log.Error("Failed to send session ID in header: %v", err)
		return status.Errorf(codes.Internal, "Failed to send session ID: %v", err)
	}

	log.Info("Successfully sent session ID %s in stream header", session.ID)

	// Send initial entries if starting from a specific sequence
	if req.StartSequence > 0 {
		if err := p.sendInitialEntries(session); err != nil {
			return fmt.Errorf("failed to send initial entries: %w", err)
		}
	}

	// Keep the stream alive and continue sending entries as they arrive
	ctx := stream.Context()

	// Periodically check if we have more entries to send
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context was canceled, exit
			return ctx.Err()
		case <-ticker.C:
			// Check if we have new entries to send
			currentSeq := p.wal.GetNextSequence() - 1
			if currentSeq > session.LastAckSequence {
				log.Info("Checking for new entries: currentSeq=%d > lastAck=%d",
					currentSeq, session.LastAckSequence)
				if err := p.sendUpdatedEntries(session); err != nil {
					log.Error("Failed to send updated entries: %v", err)
					// Don't terminate the stream on error, just continue
				}
			}
		}
	}
}

// sendUpdatedEntries sends any new WAL entries to the replica since its last acknowledged sequence
func (p *Primary) sendUpdatedEntries(session *ReplicaSession) error {
	// Take the mutex to safely read and update session state
	session.mu.Lock()
	defer session.mu.Unlock()

	// Get the next sequence number we should send
	nextSequence := session.LastAckSequence + 1

	log.Info("Sending updated entries to replica %s starting from sequence %d",
		session.ID, nextSequence)

	// Get the next entries from WAL
	entries, err := p.getWALEntriesFromSequence(nextSequence)
	if err != nil {
		return fmt.Errorf("failed to get WAL entries: %w", err)
	}

	if len(entries) == 0 {
		// No new entries, nothing to send
		log.Info("No new entries to send to replica %s", session.ID)
		return nil
	}

	// Log what we're sending
	log.Info("Sending %d entries to replica %s, sequence range: %d to %d",
		len(entries), session.ID, entries[0].SequenceNumber, entries[len(entries)-1].SequenceNumber)

	// Convert WAL entries to protocol buffer entries
	protoEntries := make([]*proto.WALEntry, 0, len(entries))
	for _, entry := range entries {
		protoEntry, err := WALEntryToProto(entry, proto.FragmentType_FULL)
		if err != nil {
			log.Error("Error converting entry %d to proto: %v", entry.SequenceNumber, err)
			continue
		}
		protoEntries = append(protoEntries, protoEntry)
	}

	// Create a response with the entries
	response := &proto.WALStreamResponse{
		Entries:    protoEntries,
		Compressed: false, // For simplicity, not compressing these entries
		Codec:      proto.CompressionCodec_NONE,
	}

	// Send to the replica (we're already holding the lock)
	if err := session.Stream.Send(response); err != nil {
		return fmt.Errorf("failed to send entries: %w", err)
	}

	log.Info("Successfully sent %d entries to replica %s", len(protoEntries), session.ID)
	session.LastActivity = time.Now()
	return nil
}

// Acknowledge implements WALReplicationServiceServer.Acknowledge
func (p *Primary) Acknowledge(
	ctx context.Context,
	req *proto.Ack,
) (*proto.AckResponse, error) {
	// Log the acknowledgment request
	log.Info("Received acknowledgment request: AcknowledgedUpTo=%d", req.AcknowledgedUpTo)

	// Extract metadata for debugging
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		sessionIDs := md.Get("session-id")
		if len(sessionIDs) > 0 {
			log.Info("Acknowledge request contains session ID in metadata: %s", sessionIDs[0])
		} else {
			log.Warn("Acknowledge request missing session ID in metadata")
		}
	} else {
		log.Warn("No metadata in acknowledge request")
	}

	// Update session with acknowledgment
	sessionID := p.getSessionIDFromContext(ctx)
	if sessionID == "" {
		log.Error("Failed to identify session for acknowledgment")
		return &proto.AckResponse{
			Success: false,
			Message: "Unknown session",
		}, nil
	}

	log.Info("Using session ID for acknowledgment: %s", sessionID)

	// Update the session's acknowledged sequence
	if err := p.updateSessionAck(sessionID, req.AcknowledgedUpTo); err != nil {
		log.Error("Failed to update acknowledgment: %v", err)
		return &proto.AckResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	log.Info("Successfully processed acknowledgment for session %s up to sequence %d",
		sessionID, req.AcknowledgedUpTo)

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

		// Check if this session has requested entries from a higher sequence
		if len(response.Entries) > 0 &&
			response.Entries[0].SequenceNumber <= session.StartSequence {
			continue
		}

		// Send to the replica - it will create a clone inside sendToReplica
		p.sendToReplica(session, response)
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
						log.Error("Error decompressing entry: %v", err)
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
		log.Error("Error sending to replica %s: %v", session.ID, err)
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
			log.Error("Error converting entry %d to proto: %v", entry.SequenceNumber, err)
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
			log.Error("Error converting entry %d to proto: %v", entry.SequenceNumber, err)
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
// in batches of up to maxEntriesToReturn entries at a time
func (p *Primary) getWALEntriesFromSequence(fromSequence uint64) ([]*wal.Entry, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Get current sequence in WAL (next sequence - 1)
	// We subtract 1 to get the current highest assigned sequence
	currentSeq := p.wal.GetNextSequence() - 1

	log.Info("GetWALEntriesFromSequence called with fromSequence=%d, currentSeq=%d",
		fromSequence, currentSeq)

	if currentSeq == 0 || fromSequence > currentSeq {
		// No entries to return yet
		log.Info("No entries to return: currentSeq=%d, fromSequence=%d", currentSeq, fromSequence)
		return []*wal.Entry{}, nil
	}

	// Use the WAL's built-in method to get entries starting from the specified sequence
	// This preserves the original keys and values exactly as they were written
	allEntries, err := p.wal.GetEntriesFrom(fromSequence)
	if err != nil {
		log.Error("Failed to get WAL entries: %v", err)
		return nil, fmt.Errorf("failed to get WAL entries: %w", err)
	}

	log.Info("Retrieved %d entries from WAL starting at sequence %d", len(allEntries), fromSequence)

	// Debugging: Log entry details
	for i, entry := range allEntries {
		if i < 5 { // Only log first few entries to avoid excessive logging
			log.Info("Entry %d: seq=%d, type=%d, key=%s",
				i, entry.SequenceNumber, entry.Type, string(entry.Key))
		}
	}

	// Limit the number of entries to return to avoid overwhelming the network
	maxEntriesToReturn := 100
	if len(allEntries) > maxEntriesToReturn {
		allEntries = allEntries[:maxEntriesToReturn]
		log.Info("Limited entries to %d for network efficiency", maxEntriesToReturn)
	}

	log.Info("Returning %d entries starting from sequence %d", len(allEntries), fromSequence)
	return allEntries, nil
}

// registerReplicaSession adds a new replica session
func (p *Primary) registerReplicaSession(session *ReplicaSession) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.sessions[session.ID] = session
	log.Info("Registered new replica session: %s starting from sequence %d",
		session.ID, session.StartSequence)
}

// unregisterReplicaSession removes a replica session
func (p *Primary) unregisterReplicaSession(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.sessions[id]; exists {
		delete(p.sessions, id)
		log.Info("Unregistered replica session: %s", id)
	}
}

// getSessionIDFromContext extracts the session ID from the gRPC context
// Note: In a real implementation, this would use proper authentication and session tracking
func (p *Primary) getSessionIDFromContext(ctx context.Context) string {
	// Check for session ID in metadata (would be set by a proper authentication system)
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		// Look for session ID in metadata
		sessionIDs := md.Get("session-id")
		if len(sessionIDs) > 0 {
			sessionID := sessionIDs[0]
			log.Info("Found session ID in metadata: %s", sessionID)

			// Verify the session exists
			p.mu.RLock()
			defer p.mu.RUnlock()

			if _, exists := p.sessions[sessionID]; exists {
				return sessionID
			}

			log.Error("Session ID from metadata not found in sessions map: %s", sessionID)
			return ""
		}
	}

	// Fallback to first active session approach
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Log the available sessions for debugging
	log.Info("Looking for active session in %d available sessions", len(p.sessions))
	for id, session := range p.sessions {
		log.Info("Session %s: connected=%v, active=%v, lastAck=%d",
			id, session.Connected, session.Active, session.LastAckSequence)
	}

	// Return the first active session ID (this is just a placeholder)
	for id, session := range p.sessions {
		if session.Connected {
			log.Info("Selected active session %s", id)
			return id
		}
	}

	log.Error("No active session found")
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

	// We need to lock the session to safely update LastAckSequence
	session.mu.Lock()
	defer session.mu.Unlock()

	// Log the updated acknowledgement
	log.Info("Updating replica %s acknowledgement: previous=%d, new=%d",
		sessionID, session.LastAckSequence, ackSeq)

	// Only update if the new ack sequence is higher than the current one
	if ackSeq > session.LastAckSequence {
		session.LastAckSequence = ackSeq
		log.Info("Replica %s acknowledged data up to sequence %d", sessionID, ackSeq)
	} else {
		log.Warn("Received outdated acknowledgement from replica %s: got=%d, current=%d",
			sessionID, ackSeq, session.LastAckSequence)
	}

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
	p.mu.RLock()

	// Find minimum acknowledged sequence across all connected replicas
	minAcknowledgedSeq := uint64(^uint64(0)) // Start with max value
	activeReplicas := 0

	for id, session := range p.sessions {
		if session.Connected && session.Active {
			activeReplicas++
			if session.LastAckSequence < minAcknowledgedSeq {
				minAcknowledgedSeq = session.LastAckSequence
			}
			log.Info("Replica %s has acknowledged up to sequence %d",
				id, session.LastAckSequence)
		}
	}
	p.mu.RUnlock()

	// Only proceed if we have valid data and active replicas
	if minAcknowledgedSeq == uint64(^uint64(0)) || minAcknowledgedSeq == 0 {
		log.Info("No minimum acknowledged sequence found, skipping WAL retention")
		return
	}

	log.Info("WAL retention: minimum acknowledged sequence across %d active replicas: %d",
		activeReplicas, minAcknowledgedSeq)

	// Apply the retention policy using the existing WAL API
	config := wal.WALRetentionConfig{
		MaxAge:          time.Duration(p.retentionConfig.MaxAgeHours) * time.Hour,
		MinSequenceKeep: minAcknowledgedSeq,
	}

	filesDeleted, err := p.wal.ManageRetention(config)
	if err != nil {
		log.Error("Failed to manage WAL retention: %v", err)
		return
	}

	if filesDeleted > 0 {
		log.Info("WAL retention: deleted %d files, min sequence kept: %d",
			filesDeleted, minAcknowledgedSeq)
	} else {
		log.Info("WAL retention: no files eligible for deletion")
	}
}

// Close shuts down the primary, unregistering from WAL and cleaning up resources
func (p *Primary) Close() error {
	// Stop heartbeat monitoring
	if p.heartbeat != nil {
		p.heartbeat.stop()
	}

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
