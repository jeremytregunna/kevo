package service

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transport"
	"github.com/KevoDB/kevo/pkg/wal"
	"github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ReplicationServiceServer implements the gRPC ReplicationService
type ReplicationServiceServer struct {
	kevo.UnimplementedReplicationServiceServer

	// Replication components
	replicator    replication.EntryReplicator
	applier       replication.EntryApplier
	serializer    *replication.EntrySerializer
	highestLSN    uint64
	replicas      map[string]*transport.ReplicaInfo
	replicasMutex sync.RWMutex

	// For snapshot/bootstrap
	storageSnapshot  replication.StorageSnapshot
	bootstrapService *bootstrapService

	// Access control and persistence
	accessControl *transport.AccessController
	persistence   *transport.ReplicaPersistence

	// Metrics collection
	metrics *transport.ReplicationMetrics
}

// ReplicationServiceOptions contains configuration for the replication service
type ReplicationServiceOptions struct {
	// Data directory for persisting replica information
	DataDir string

	// Whether to enable access control
	EnableAccessControl bool

	// Whether to enable persistence
	EnablePersistence bool

	// Default authentication method
	DefaultAuthMethod transport.AuthMethod

	// Bootstrap service configuration
	BootstrapOptions *BootstrapServiceOptions
}

// DefaultReplicationServiceOptions returns sensible defaults
func DefaultReplicationServiceOptions() *ReplicationServiceOptions {
	return &ReplicationServiceOptions{
		DataDir:             "./replication-data",
		EnableAccessControl: false, // Disabled by default for backward compatibility
		EnablePersistence:   false, // Disabled by default for backward compatibility
		DefaultAuthMethod:   transport.AuthNone,
		BootstrapOptions:    DefaultBootstrapServiceOptions(),
	}
}

// NewReplicationService creates a new ReplicationService
func NewReplicationService(
	replicator EntryReplicator,
	applier EntryApplier,
	serializer *replication.EntrySerializer,
	storageSnapshot SnapshotProvider,
	options *ReplicationServiceOptions,
) (*ReplicationServiceServer, error) {
	if options == nil {
		options = DefaultReplicationServiceOptions()
	}

	// Create access controller
	accessControl := transport.NewAccessController(
		options.EnableAccessControl,
		options.DefaultAuthMethod,
	)

	// Create persistence manager
	persistence, err := transport.NewReplicaPersistence(
		options.DataDir,
		options.EnablePersistence,
		true, // Auto-save
	)
	if err != nil && options.EnablePersistence {
		return nil, fmt.Errorf("failed to initialize replica persistence: %w", err)
	}

	// Create metrics collector
	metrics := transport.NewReplicationMetrics()

	server := &ReplicationServiceServer{
		replicator:      replicator,
		applier:         applier,
		serializer:      serializer,
		replicas:        make(map[string]*transport.ReplicaInfo),
		storageSnapshot: storageSnapshot,
		accessControl:   accessControl,
		persistence:     persistence,
		metrics:         metrics,
	}

	// Load persisted replica data if persistence is enabled
	if options.EnablePersistence && persistence != nil {
		infoMap, credsMap, err := persistence.GetAllReplicas()
		if err != nil {
			return nil, fmt.Errorf("failed to load persisted replicas: %w", err)
		}

		// Restore replicas and credentials
		for id, info := range infoMap {
			server.replicas[id] = info

			// Register credentials
			if creds, exists := credsMap[id]; exists && options.EnableAccessControl {
				accessControl.RegisterReplica(creds)
			}
		}
	}

	// Initialize bootstrap service if bootstrap options are provided
	if options.BootstrapOptions != nil {
		if err := server.InitBootstrapService(options.BootstrapOptions); err != nil {
			// Log the error but continue - bootstrap service is optional
			fmt.Printf("Warning: Failed to initialize bootstrap service: %v\n", err)
		}
	}

	return server, nil
}

// RegisterReplica handles registration of a new replica
func (s *ReplicationServiceServer) RegisterReplica(
	ctx context.Context,
	req *kevo.RegisterReplicaRequest,
) (*kevo.RegisterReplicaResponse, error) {
	// Validate request
	if req.ReplicaId == "" {
		return nil, status.Error(codes.InvalidArgument, "replica_id is required")
	}
	if req.Address == "" {
		return nil, status.Error(codes.InvalidArgument, "address is required")
	}

	// Convert role enum to string
	role := transport.RoleReplica
	switch req.Role {
	case kevo.ReplicaRole_PRIMARY:
		role = transport.RolePrimary
	case kevo.ReplicaRole_REPLICA:
		role = transport.RoleReplica
	case kevo.ReplicaRole_READ_ONLY:
		role = transport.RoleReadOnly
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid role")
	}

	// Check if access control is enabled
	if s.accessControl.IsEnabled() {
		// For existing replicas, authenticate with token from metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing authentication metadata")
		}

		tokens := md.Get("x-replica-token")
		token := ""
		if len(tokens) > 0 {
			token = tokens[0]
		}

		// Try to authenticate if not the first registration
		existingReplicaErr := s.accessControl.AuthenticateReplica(req.ReplicaId, token)
		if existingReplicaErr != nil && existingReplicaErr != transport.ErrAccessDenied {
			return nil, status.Error(codes.Unauthenticated, "authentication failed")
		}
	}

	// Register the replica
	s.replicasMutex.Lock()
	defer s.replicasMutex.Unlock()

	var replicaInfo *transport.ReplicaInfo

	// If already registered, update address and role
	if replica, exists := s.replicas[req.ReplicaId]; exists {
		// If access control is enabled, make sure replica is authorized for the requested role
		if s.accessControl.IsEnabled() {
			// Read role requires ReadOnly access, Write role requires ReadWrite access
			var requiredLevel transport.AccessLevel
			if role == transport.RolePrimary {
				requiredLevel = transport.AccessAdmin
			} else if role == transport.RoleReplica {
				requiredLevel = transport.AccessReadWrite
			} else {
				requiredLevel = transport.AccessReadOnly
			}

			if err := s.accessControl.AuthorizeReplicaAction(req.ReplicaId, requiredLevel); err != nil {
				return nil, status.Error(codes.PermissionDenied, "not authorized for requested role")
			}
		}

		// Update existing replica
		replica.Address = req.Address
		replica.Role = role
		replica.LastSeen = time.Now()
		replica.Status = transport.StatusConnecting
		replicaInfo = replica
	} else {
		// Create new replica info
		replicaInfo = &transport.ReplicaInfo{
			ID:       req.ReplicaId,
			Address:  req.Address,
			Role:     role,
			Status:   transport.StatusConnecting,
			LastSeen: time.Now(),
		}
		s.replicas[req.ReplicaId] = replicaInfo

		// For new replicas, register with access control
		if s.accessControl.IsEnabled() {
			// Generate or use token based on settings
			token := ""
			authMethod := s.accessControl.DefaultAuthMethod()

			if authMethod == transport.AuthToken {
				// In a real system, we'd generate a secure random token
				token = fmt.Sprintf("token-%s-%d", req.ReplicaId, time.Now().UnixNano())
			}

			// Set appropriate access level based on role
			var accessLevel transport.AccessLevel
			if role == transport.RolePrimary {
				accessLevel = transport.AccessAdmin
			} else if role == transport.RoleReplica {
				accessLevel = transport.AccessReadWrite
			} else {
				accessLevel = transport.AccessReadOnly
			}

			// Register replica credentials
			creds := &transport.ReplicaCredentials{
				ReplicaID:   req.ReplicaId,
				AuthMethod:  authMethod,
				Token:       token,
				AccessLevel: accessLevel,
			}

			if err := s.accessControl.RegisterReplica(creds); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to register credentials: %v", err)
			}

			// Persist replica data with credentials
			if s.persistence != nil && s.persistence.IsEnabled() {
				if err := s.persistence.SaveReplica(replicaInfo, creds); err != nil {
					// Log error but continue
					fmt.Printf("Error persisting replica: %v\n", err)
				}
			}
		}
	}

	// Persist replica data without credentials for existing replicas
	if s.persistence != nil && s.persistence.IsEnabled() {
		if err := s.persistence.SaveReplica(replicaInfo, nil); err != nil {
			// Log error but continue
			fmt.Printf("Error persisting replica: %v\n", err)
		}
	}

	// Determine if bootstrap is needed (for now always suggest bootstrap)
	bootstrapRequired := true

	// Return current highest LSN
	currentLSN := s.replicator.GetHighestTimestamp()

	// Update metrics with primary LSN
	if s.metrics != nil {
		s.metrics.UpdatePrimaryLSN(currentLSN)
	}

	return &kevo.RegisterReplicaResponse{
		Success:           true,
		CurrentLsn:        currentLSN,
		BootstrapRequired: bootstrapRequired,
	}, nil
}

// ReplicaHeartbeat handles heartbeat requests from replicas
func (s *ReplicationServiceServer) ReplicaHeartbeat(
	ctx context.Context,
	req *kevo.ReplicaHeartbeatRequest,
) (*kevo.ReplicaHeartbeatResponse, error) {
	// Validate request
	if req.ReplicaId == "" {
		return nil, status.Error(codes.InvalidArgument, "replica_id is required")
	}

	// Check authentication if enabled
	if s.accessControl.IsEnabled() {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing authentication metadata")
		}

		tokens := md.Get("x-replica-token")
		token := ""
		if len(tokens) > 0 {
			token = tokens[0]
		}

		if err := s.accessControl.AuthenticateReplica(req.ReplicaId, token); err != nil {
			return nil, status.Error(codes.Unauthenticated, "authentication failed")
		}

		// Sending heartbeats requires at least read access
		if err := s.accessControl.AuthorizeReplicaAction(req.ReplicaId, transport.AccessReadOnly); err != nil {
			return nil, status.Error(codes.PermissionDenied, "not authorized to send heartbeats")
		}
	}

	// Lock for updating replica info
	s.replicasMutex.Lock()
	defer s.replicasMutex.Unlock()

	replica, exists := s.replicas[req.ReplicaId]
	if !exists {
		return nil, status.Error(codes.NotFound, "replica not registered")
	}

	// Update replica status
	replica.LastSeen = time.Now()

	// Convert status enum to string
	switch req.Status {
	case kevo.ReplicaStatus_CONNECTING:
		replica.Status = transport.StatusConnecting
	case kevo.ReplicaStatus_SYNCING:
		replica.Status = transport.StatusSyncing
	case kevo.ReplicaStatus_BOOTSTRAPPING:
		replica.Status = transport.StatusBootstrapping
	case kevo.ReplicaStatus_READY:
		replica.Status = transport.StatusReady
	case kevo.ReplicaStatus_DISCONNECTED:
		replica.Status = transport.StatusDisconnected
	case kevo.ReplicaStatus_ERROR:
		replica.Status = transport.StatusError
		replica.Error = fmt.Errorf("%s", req.ErrorMessage)
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid status")
	}

	// Update replica LSN
	replica.CurrentLSN = req.CurrentLsn

	// Calculate replication lag
	primaryLSN := s.replicator.GetHighestTimestamp()
	var replicationLagMs int64 = 0
	if primaryLSN > req.CurrentLsn {
		// Simple lag calculation based on LSN difference
		// In a real system, we'd use timestamps for better accuracy
		replicationLagMs = int64(primaryLSN - req.CurrentLsn)
	}

	replica.ReplicationLag = time.Duration(replicationLagMs) * time.Millisecond

	// Persist updated replica status if persistence is enabled
	if s.persistence != nil && s.persistence.IsEnabled() {
		if err := s.persistence.SaveReplica(replica, nil); err != nil {
			// Log error but continue
			fmt.Printf("Error persisting replica status: %v\n", err)
		}
	}

	// Update metrics
	if s.metrics != nil {
		// Record the heartbeat
		s.metrics.UpdateReplicaStatus(req.ReplicaId, replica.Status, replica.CurrentLSN)
		// Make sure primary LSN is current
		s.metrics.UpdatePrimaryLSN(primaryLSN)
	}

	return &kevo.ReplicaHeartbeatResponse{
		Success:          true,
		PrimaryLsn:       primaryLSN,
		ReplicationLagMs: replicationLagMs,
	}, nil
}

// GetReplicaStatus retrieves the status of a specific replica
func (s *ReplicationServiceServer) GetReplicaStatus(
	ctx context.Context,
	req *kevo.GetReplicaStatusRequest,
) (*kevo.GetReplicaStatusResponse, error) {
	// Validate request
	if req.ReplicaId == "" {
		return nil, status.Error(codes.InvalidArgument, "replica_id is required")
	}

	// Get replica info
	s.replicasMutex.RLock()
	defer s.replicasMutex.RUnlock()

	replica, exists := s.replicas[req.ReplicaId]
	if !exists {
		return nil, status.Error(codes.NotFound, "replica not found")
	}

	// Convert replica info to proto message
	pbReplica := convertReplicaInfoToProto(replica)

	return &kevo.GetReplicaStatusResponse{
		Replica: pbReplica,
	}, nil
}

// ListReplicas retrieves the status of all replicas
func (s *ReplicationServiceServer) ListReplicas(
	ctx context.Context,
	req *kevo.ListReplicasRequest,
) (*kevo.ListReplicasResponse, error) {
	s.replicasMutex.RLock()
	defer s.replicasMutex.RUnlock()

	// Convert all replicas to proto messages
	pbReplicas := make([]*kevo.ReplicaInfo, 0, len(s.replicas))
	for _, replica := range s.replicas {
		pbReplicas = append(pbReplicas, convertReplicaInfoToProto(replica))
	}

	return &kevo.ListReplicasResponse{
		Replicas: pbReplicas,
	}, nil
}

// GetWALEntries handles requests for WAL entries
func (s *ReplicationServiceServer) GetWALEntries(
	ctx context.Context,
	req *kevo.GetWALEntriesRequest,
) (*kevo.GetWALEntriesResponse, error) {
	// Validate request
	if req.ReplicaId == "" {
		return nil, status.Error(codes.InvalidArgument, "replica_id is required")
	}

	// Check if replica is registered
	s.replicasMutex.RLock()
	_, exists := s.replicas[req.ReplicaId]
	s.replicasMutex.RUnlock()

	if !exists {
		return nil, status.Error(codes.NotFound, "replica not registered")
	}

	// Get entries from replicator
	entries, err := s.replicator.GetEntriesAfter(replication.ReplicationPosition{Timestamp: req.FromLsn})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get entries: %v", err)
	}
	if len(entries) == 0 {
		return &kevo.GetWALEntriesResponse{
			Batch:   &kevo.WALEntryBatch{},
			HasMore: false,
		}, nil
	}

	// Convert entries to proto messages
	pbEntries := make([]*kevo.WALEntry, 0, len(entries))
	for _, entry := range entries {
		pbEntries = append(pbEntries, convertWALEntryToProto(entry))
	}

	// Create batch
	pbBatch := &kevo.WALEntryBatch{
		Entries:  pbEntries,
		FirstLsn: entries[0].SequenceNumber,
		LastLsn:  entries[len(entries)-1].SequenceNumber,
		Count:    uint32(len(entries)),
	}

	// Calculate batch checksum
	pbBatch.Checksum = calculateBatchChecksum(pbBatch)

	// Check if there are more entries
	hasMore := s.replicator.GetHighestTimestamp() > entries[len(entries)-1].SequenceNumber

	return &kevo.GetWALEntriesResponse{
		Batch:   pbBatch,
		HasMore: hasMore,
	}, nil
}

// StreamWALEntries handles streaming WAL entries to a replica
func (s *ReplicationServiceServer) StreamWALEntries(
	req *kevo.StreamWALEntriesRequest,
	stream kevo.ReplicationService_StreamWALEntriesServer,
) error {
	// Validate request
	if req.ReplicaId == "" {
		return status.Error(codes.InvalidArgument, "replica_id is required")
	}

	// Check if replica is registered
	s.replicasMutex.RLock()
	_, exists := s.replicas[req.ReplicaId]
	s.replicasMutex.RUnlock()

	if !exists {
		return status.Error(codes.NotFound, "replica not registered")
	}

	// Process entries in batches
	fromLSN := req.FromLsn
	batchSize := 100 // Can be configurable
	notifierCh := make(chan struct{}, 1)

	// Register for notifications of new entries
	var processor *entryNotifier
	if req.Continuous {
		processor = &entryNotifier{
			notifyCh: notifierCh,
			fromLSN:  fromLSN,
		}
		s.replicator.AddProcessor(processor)
		defer s.replicator.RemoveProcessor(processor)
	}

	// Initial send of available entries
	for {
		// Get batch of entries
		entries, err := s.replicator.GetEntriesAfter(replication.ReplicationPosition{Timestamp: fromLSN})
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			// No more entries, check if continuous streaming
			if !req.Continuous {
				break
			}
			// Wait for notification of new entries
			select {
			case <-notifierCh:
				continue
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}

		// Create batch message
		pbEntries := make([]*kevo.WALEntry, 0, len(entries))
		for _, entry := range entries {
			pbEntries = append(pbEntries, convertWALEntryToProto(entry))
		}

		pbBatch := &kevo.WALEntryBatch{
			Entries:  pbEntries,
			FirstLsn: entries[0].SequenceNumber,
			LastLsn:  entries[len(entries)-1].SequenceNumber,
			Count:    uint32(len(entries)),
		}
		// Calculate batch checksum for integrity validation
		pbBatch.Checksum = calculateBatchChecksum(pbBatch)

		// Send batch
		if err := stream.Send(pbBatch); err != nil {
			return err
		}

		// Update fromLSN for next batch
		fromLSN = entries[len(entries)-1].SequenceNumber + 1

		// If not continuous, break after sending all available entries
		if !req.Continuous && len(entries) < batchSize {
			break
		}
	}

	return nil
}

// ReportAppliedEntries handles reports of entries applied by replicas
func (s *ReplicationServiceServer) ReportAppliedEntries(
	ctx context.Context,
	req *kevo.ReportAppliedEntriesRequest,
) (*kevo.ReportAppliedEntriesResponse, error) {
	// Validate request
	if req.ReplicaId == "" {
		return nil, status.Error(codes.InvalidArgument, "replica_id is required")
	}

	// Update replica LSN
	s.replicasMutex.Lock()
	replica, exists := s.replicas[req.ReplicaId]
	if exists {
		replica.CurrentLSN = req.AppliedLsn
	}
	s.replicasMutex.Unlock()

	if !exists {
		return nil, status.Error(codes.NotFound, "replica not registered")
	}

	return &kevo.ReportAppliedEntriesResponse{
		Success:    true,
		PrimaryLsn: s.replicator.GetHighestTimestamp(),
	}, nil
}

// Legacy implementation moved to replication_service_bootstrap.go

// Helper to convert replica info to proto message
func convertReplicaInfoToProto(replica *transport.ReplicaInfo) *kevo.ReplicaInfo {
	// Convert status to proto enum
	var pbStatus kevo.ReplicaStatus
	switch replica.Status {
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

	// Convert role to proto enum
	var pbRole kevo.ReplicaRole
	switch replica.Role {
	case transport.RolePrimary:
		pbRole = kevo.ReplicaRole_PRIMARY
	case transport.RoleReplica:
		pbRole = kevo.ReplicaRole_REPLICA
	case transport.RoleReadOnly:
		pbRole = kevo.ReplicaRole_READ_ONLY
	default:
		pbRole = kevo.ReplicaRole_REPLICA
	}

	// Create proto message
	pbReplica := &kevo.ReplicaInfo{
		ReplicaId:        replica.ID,
		Address:          replica.Address,
		Role:             pbRole,
		Status:           pbStatus,
		LastSeenMs:       replica.LastSeen.UnixMilli(),
		CurrentLsn:       replica.CurrentLSN,
		ReplicationLagMs: replica.ReplicationLag.Milliseconds(),
	}

	// Add error message if any
	if replica.Error != nil {
		pbReplica.ErrorMessage = replica.Error.Error()
	}

	return pbReplica
}

// Convert WAL entry to proto message
func convertWALEntryToProto(entry *wal.Entry) *kevo.WALEntry {
	pbEntry := &kevo.WALEntry{
		SequenceNumber: entry.SequenceNumber,
		Type:           uint32(entry.Type),
		Key:            entry.Key,
		Value:          entry.Value,
	}

	// Calculate checksum for data integrity
	pbEntry.Checksum = calculateEntryChecksum(pbEntry)
	return pbEntry
}

// calculateEntryChecksum calculates a CRC32 checksum for a WAL entry
func calculateEntryChecksum(entry *kevo.WALEntry) []byte {
	// Create a checksum calculator
	hasher := crc32.NewIEEE()

	// Write all fields to the hasher
	binary.Write(hasher, binary.LittleEndian, entry.SequenceNumber)
	binary.Write(hasher, binary.LittleEndian, entry.Type)
	hasher.Write(entry.Key)
	if entry.Value != nil {
		hasher.Write(entry.Value)
	}

	// Return the checksum as a byte slice
	checksum := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksum, hasher.Sum32())
	return checksum
}

// calculateBatchChecksum calculates a CRC32 checksum for a WAL entry batch
func calculateBatchChecksum(batch *kevo.WALEntryBatch) []byte {
	// Create a checksum calculator
	hasher := crc32.NewIEEE()

	// Write batch metadata to the hasher
	binary.Write(hasher, binary.LittleEndian, batch.FirstLsn)
	binary.Write(hasher, binary.LittleEndian, batch.LastLsn)
	binary.Write(hasher, binary.LittleEndian, batch.Count)

	// Write the checksum of each entry to the hasher
	for _, entry := range batch.Entries {
		// We're using entry checksums as part of the batch checksum
		// to avoid recalculating entry data
		if entry.Checksum != nil {
			hasher.Write(entry.Checksum)
		}
	}

	// Return the checksum as a byte slice
	checksum := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksum, hasher.Sum32())
	return checksum
}

// entryNotifier is a helper struct that implements replication.EntryProcessor
// to notify when new entries are available
type entryNotifier struct {
	notifyCh chan struct{}
	fromLSN  uint64
}

func (n *entryNotifier) ProcessEntry(entry *wal.Entry) error {
	if entry.SequenceNumber >= n.fromLSN {
		select {
		case n.notifyCh <- struct{}{}:
		default:
			// Channel already has a notification, no need to send another
		}
	}
	return nil
}

func (n *entryNotifier) ProcessBatch(entries []*wal.Entry) error {
	if len(entries) > 0 && entries[len(entries)-1].SequenceNumber >= n.fromLSN {
		select {
		case n.notifyCh <- struct{}{}:
		default:
			// Channel already has a notification, no need to send another
		}
	}
	return nil
}

// Define the interface for storage snapshot operations
// This would normally be implemented by the storage engine
type StorageSnapshot interface {
	// CreateSnapshotIterator creates an iterator for a storage snapshot
	CreateSnapshotIterator() (SnapshotIterator, error)

	// KeyCount returns the approximate number of keys in storage
	KeyCount() int64
}

// SnapshotIterator provides iteration over key-value pairs in storage
type SnapshotIterator interface {
	// Next returns the next key-value pair
	Next() (key []byte, value []byte, err error)

	// Close closes the iterator
	Close() error
}

// IsReplicaStale checks if a replica is considered stale based on the last heartbeat
func (s *ReplicationServiceServer) IsReplicaStale(replicaID string, threshold time.Duration) bool {
	s.replicasMutex.RLock()
	defer s.replicasMutex.RUnlock()

	replica, exists := s.replicas[replicaID]
	if !exists {
		return true // Consider non-existent replicas as stale
	}

	// Check if the last seen time is older than the threshold
	return time.Since(replica.LastSeen) > threshold
}

// DetectStaleReplicas finds all replicas that haven't sent a heartbeat within the threshold
func (s *ReplicationServiceServer) DetectStaleReplicas(threshold time.Duration) []string {
	s.replicasMutex.RLock()
	defer s.replicasMutex.RUnlock()

	staleReplicas := make([]string, 0)
	now := time.Now()

	for id, replica := range s.replicas {
		if now.Sub(replica.LastSeen) > threshold {
			staleReplicas = append(staleReplicas, id)
		}
	}

	return staleReplicas
}

// GetMetrics returns the current replication metrics
func (s *ReplicationServiceServer) GetMetrics() map[string]interface{} {
	if s.metrics == nil {
		return map[string]interface{}{
			"error": "metrics collection is not enabled",
		}
	}

	// Get summary metrics
	summary := s.metrics.GetSummaryMetrics()

	// Add replica-specific metrics
	replicaMetrics := s.metrics.GetAllReplicaMetrics()
	replicasData := make(map[string]interface{})

	for id, metrics := range replicaMetrics {
		replicaData := map[string]interface{}{
			"status":             string(metrics.Status),
			"last_seen":          metrics.LastSeen.Format(time.RFC3339),
			"replication_lag_ms": metrics.ReplicationLag.Milliseconds(),
			"applied_lsn":        metrics.AppliedLSN,
			"connected_duration": metrics.ConnectedDuration.String(),
			"heartbeat_count":    metrics.HeartbeatCount,
			"wal_entries_sent":   metrics.WALEntriesSent,
			"bytes_sent":         metrics.BytesSent,
			"error_count":        metrics.ErrorCount,
		}

		// Add bootstrap metrics if available
		replicaData["bootstrap_count"] = metrics.BootstrapCount
		if !metrics.LastBootstrapTime.IsZero() {
			replicaData["last_bootstrap_time"] = metrics.LastBootstrapTime.Format(time.RFC3339)
			replicaData["last_bootstrap_duration"] = metrics.LastBootstrapDuration.String()
		}

		replicasData[id] = replicaData
	}

	summary["replicas"] = replicasData

	// Add bootstrap service status if available
	if s.bootstrapService != nil {
		summary["bootstrap"] = s.bootstrapService.getBootstrapStatus()
	}

	return summary
}

// GetReplicaMetrics returns metrics for a specific replica
func (s *ReplicationServiceServer) GetReplicaMetrics(replicaID string) (map[string]interface{}, error) {
	if s.metrics == nil {
		return nil, fmt.Errorf("metrics collection is not enabled")
	}

	metrics, found := s.metrics.GetReplicaMetrics(replicaID)
	if !found {
		return nil, fmt.Errorf("no metrics found for replica %s", replicaID)
	}

	result := map[string]interface{}{
		"status":             string(metrics.Status),
		"last_seen":          metrics.LastSeen.Format(time.RFC3339),
		"replication_lag_ms": metrics.ReplicationLag.Milliseconds(),
		"applied_lsn":        metrics.AppliedLSN,
		"connected_duration": metrics.ConnectedDuration.String(),
		"heartbeat_count":    metrics.HeartbeatCount,
		"wal_entries_sent":   metrics.WALEntriesSent,
		"bytes_sent":         metrics.BytesSent,
		"error_count":        metrics.ErrorCount,
		"bootstrap_count":    metrics.BootstrapCount,
	}

	// Add bootstrap time/duration if available
	if !metrics.LastBootstrapTime.IsZero() {
		result["last_bootstrap_time"] = metrics.LastBootstrapTime.Format(time.RFC3339)
		result["last_bootstrap_duration"] = metrics.LastBootstrapDuration.String()
	}

	// Add bootstrap progress if available
	if s.bootstrapService != nil {
		bootstrapStatus := s.bootstrapService.getBootstrapStatus()
		if bootstrapStatus != nil {
			if bootstrapState, ok := bootstrapStatus["bootstrap_state"].(map[string]interface{}); ok {
				if bootstrapState["replica_id"] == replicaID {
					result["bootstrap_progress"] = bootstrapState["progress"]
					result["bootstrap_status"] = map[string]interface{}{
						"started_at":   bootstrapState["started_at"],
						"completed":    bootstrapState["completed"],
						"applied_keys": bootstrapState["applied_keys"],
						"total_keys":   bootstrapState["total_keys"],
						"snapshot_lsn": bootstrapState["snapshot_lsn"],
					}
				}
			}
		}
	}

	return result, nil
}
