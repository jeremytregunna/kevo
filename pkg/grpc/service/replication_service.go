package service

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transport"
	"github.com/KevoDB/kevo/pkg/wal"
	"github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReplicationServiceServer implements the gRPC ReplicationService
type ReplicationServiceServer struct {
	kevo.UnimplementedReplicationServiceServer

	// Replication components
	replicator    *replication.WALReplicator
	applier       *replication.WALApplier
	serializer    *replication.EntrySerializer
	highestLSN    uint64
	replicas      map[string]*transport.ReplicaInfo
	replicasMutex sync.RWMutex

	// For snapshot/bootstrap
	storageSnapshot replication.StorageSnapshot
}

// NewReplicationService creates a new ReplicationService
func NewReplicationService(
	replicator *replication.WALReplicator,
	applier *replication.WALApplier,
	serializer *replication.EntrySerializer,
	storageSnapshot replication.StorageSnapshot,
) *ReplicationServiceServer {
	return &ReplicationServiceServer{
		replicator:      replicator,
		applier:         applier,
		serializer:      serializer,
		replicas:        make(map[string]*transport.ReplicaInfo),
		storageSnapshot: storageSnapshot,
	}
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

	// Register the replica
	s.replicasMutex.Lock()
	defer s.replicasMutex.Unlock()
	
	// If already registered, update address and role
	if replica, exists := s.replicas[req.ReplicaId]; exists {
		replica.Address = req.Address
		replica.Role = role
		replica.LastSeen = time.Now()
		replica.Status = transport.StatusConnecting
	} else {
		// Create new replica info
		s.replicas[req.ReplicaId] = &transport.ReplicaInfo{
			ID:      req.ReplicaId,
			Address: req.Address,
			Role:    role,
			Status:  transport.StatusConnecting,
			LastSeen: time.Now(),
		}
	}

	// Determine if bootstrap is needed (for now always suggest bootstrap)
	bootstrapRequired := true

	// Return current highest LSN
	currentLSN := s.replicator.GetHighestTimestamp()

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

	// Check if replica is registered
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

// RequestBootstrap handles bootstrap requests from replicas
func (s *ReplicationServiceServer) RequestBootstrap(
	req *kevo.BootstrapRequest,
	stream kevo.ReplicationService_RequestBootstrapServer,
) error {
	// Validate request
	if req.ReplicaId == "" {
		return status.Error(codes.InvalidArgument, "replica_id is required")
	}

	// Check if replica is registered
	s.replicasMutex.RLock()
	replica, exists := s.replicas[req.ReplicaId]
	s.replicasMutex.RUnlock()
	
	if !exists {
		return status.Error(codes.NotFound, "replica not registered")
	}

	// Update replica status
	s.replicasMutex.Lock()
	replica.Status = transport.StatusBootstrapping
	s.replicasMutex.Unlock()

	// Create snapshot of current data
	snapshotLSN := s.replicator.GetHighestTimestamp()
	iterator, err := s.storageSnapshot.CreateSnapshotIterator()
	if err != nil {
		s.replicasMutex.Lock()
		replica.Status = transport.StatusError
		replica.Error = err
		s.replicasMutex.Unlock()
		return status.Errorf(codes.Internal, "failed to create snapshot: %v", err)
	}
	defer iterator.Close()

	// Stream key-value pairs in batches
	batchSize := 100 // Can be configurable
	totalCount := s.storageSnapshot.KeyCount()
	sentCount := 0
	batch := make([]*kevo.KeyValuePair, 0, batchSize)

	for {
		// Get next key-value pair
		key, value, err := iterator.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.replicasMutex.Lock()
			replica.Status = transport.StatusError
			replica.Error = err
			s.replicasMutex.Unlock()
			return status.Errorf(codes.Internal, "error reading snapshot: %v", err)
		}

		// Add to batch
		batch = append(batch, &kevo.KeyValuePair{
			Key:   key,
			Value: value,
		})

		// Send batch if full
		if len(batch) >= batchSize {
			progress := float32(sentCount) / float32(totalCount)
			if err := stream.Send(&kevo.BootstrapBatch{
				Pairs:       batch,
				Progress:    progress,
				IsLast:      false,
				SnapshotLsn: snapshotLSN,
			}); err != nil {
				return err
			}

			// Reset batch and update count
			sentCount += len(batch)
			batch = batch[:0]
		}
	}

	// Send final batch
	if len(batch) > 0 {
		sentCount += len(batch)
		progress := float32(sentCount) / float32(totalCount)
		if err := stream.Send(&kevo.BootstrapBatch{
			Pairs:       batch,
			Progress:    progress,
			IsLast:      true,
			SnapshotLsn: snapshotLSN,
		}); err != nil {
			return err
		}
	} else if sentCount > 0 {
		// Send empty final batch to mark the end
		if err := stream.Send(&kevo.BootstrapBatch{
			Pairs:       []*kevo.KeyValuePair{},
			Progress:    1.0,
			IsLast:      true,
			SnapshotLsn: snapshotLSN,
		}); err != nil {
			return err
		}
	}

	// Update replica status
	s.replicasMutex.Lock()
	replica.Status = transport.StatusSyncing
	replica.CurrentLSN = snapshotLSN
	s.replicasMutex.Unlock()

	return nil
}

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
		ReplicaId:       replica.ID,
		Address:         replica.Address,
		Role:            pbRole,
		Status:          pbStatus,
		LastSeenMs:      replica.LastSeen.UnixMilli(),
		CurrentLsn:      replica.CurrentLSN,
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