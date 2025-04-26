package service

import (
	"fmt"
	"io"
	"time"

	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transport"
	"github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// InitBootstrapService initializes the bootstrap service component
func (s *ReplicationServiceServer) InitBootstrapService(options *BootstrapServiceOptions) error {
	// Get the storage snapshot provider from the storage snapshot
	var snapshotProvider replication.StorageSnapshotProvider
	if s.storageSnapshot != nil {
		// If we have a storage snapshot directly, create an adapter
		snapshotProvider = &storageSnapshotAdapter{
			snapshot: s.storageSnapshot,
		}
	}

	// Create the bootstrap service
	bootstrapSvc, err := newBootstrapService(
		options,
		snapshotProvider,
		s.replicator,
		s.applier,
	)
	if err != nil {
		return fmt.Errorf("failed to initialize bootstrap service: %w", err)
	}

	s.bootstrapService = bootstrapSvc
	return nil
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

	// Check access control if enabled
	if s.accessControl.IsEnabled() {
		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "missing authentication metadata")
		}

		tokens := md.Get("x-replica-token")
		token := ""
		if len(tokens) > 0 {
			token = tokens[0]
		}

		if err := s.accessControl.AuthenticateReplica(req.ReplicaId, token); err != nil {
			return status.Error(codes.Unauthenticated, "authentication failed")
		}

		// Bootstrap requires at least read access
		if err := s.accessControl.AuthorizeReplicaAction(req.ReplicaId, transport.AccessReadOnly); err != nil {
			return status.Error(codes.PermissionDenied, "not authorized for bootstrap")
		}
	}

	// Update replica status
	s.replicasMutex.Lock()
	replica.Status = transport.StatusBootstrapping
	s.replicasMutex.Unlock()

	// Update metrics
	if s.metrics != nil {
		s.metrics.UpdateReplicaStatus(req.ReplicaId, replica.Status, replica.CurrentLSN)
		// We'll add bootstrap count metrics in the future
	}

	// Pass the request to the bootstrap service
	if s.bootstrapService == nil {
		// If bootstrap service isn't initialized, use the old implementation
		return s.legacyRequestBootstrap(req, stream)
	}

	err := s.bootstrapService.handleBootstrapRequest(req, stream)

	// Update replica status based on the result
	s.replicasMutex.Lock()
	if err != nil {
		replica.Status = transport.StatusError
		replica.Error = err
	} else {
		replica.Status = transport.StatusSyncing

		// Get the snapshot LSN
		snapshot := s.bootstrapService.getBootstrapStatus()
		if activeBootstraps, ok := snapshot["active_bootstraps"].(map[string]map[string]interface{}); ok {
			if replicaInfo, ok := activeBootstraps[req.ReplicaId]; ok {
				if snapshotLSN, ok := replicaInfo["snapshot_lsn"].(uint64); ok {
					replica.CurrentLSN = snapshotLSN
				}
			}
		}
	}
	s.replicasMutex.Unlock()

	// Update metrics
	if s.metrics != nil {
		s.metrics.UpdateReplicaStatus(req.ReplicaId, replica.Status, replica.CurrentLSN)
	}

	return err
}

// legacyRequestBootstrap is the original bootstrap implementation, kept for compatibility
func (s *ReplicationServiceServer) legacyRequestBootstrap(
	req *kevo.BootstrapRequest,
	stream kevo.ReplicationService_RequestBootstrapServer,
) error {
	// Update replica status
	s.replicasMutex.Lock()
	replica, exists := s.replicas[req.ReplicaId]
	if exists {
		replica.Status = transport.StatusBootstrapping
	}
	s.replicasMutex.Unlock()

	// Create snapshot of current data
	snapshotLSN := s.replicator.GetHighestTimestamp()
	iterator, err := s.storageSnapshot.CreateSnapshotIterator()
	if err != nil {
		s.replicasMutex.Lock()
		if replica != nil {
			replica.Status = transport.StatusError
			replica.Error = err
		}
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
			if replica != nil {
				replica.Status = transport.StatusError
				replica.Error = err
			}
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
	if replica != nil {
		replica.Status = transport.StatusSyncing
		replica.CurrentLSN = snapshotLSN
	}
	s.replicasMutex.Unlock()

	// Update metrics
	if s.metrics != nil {
		s.metrics.UpdateReplicaStatus(req.ReplicaId, transport.StatusSyncing, snapshotLSN)
	}

	return nil
}

// GetBootstrapStatusMap retrieves the current status of bootstrap operations as a map
func (s *ReplicationServiceServer) GetBootstrapStatusMap() map[string]string {
	// If bootstrap service isn't initialized, return empty status
	if s.bootstrapService == nil {
		return map[string]string{
			"message": "bootstrap service not initialized",
		}
	}

	// Get bootstrap status from the service
	status := s.bootstrapService.getBootstrapStatus()

	// Convert to proto-friendly format
	protoStatus := make(map[string]string)
	convertStatusToString(status, protoStatus, "")

	return protoStatus
}

// Helper function to convert nested status map to flat string map for proto
func convertStatusToString(input map[string]interface{}, output map[string]string, prefix string) {
	for k, v := range input {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}

		switch val := v.(type) {
		case string:
			output[key] = val
		case int, int64, uint64, float64, bool:
			output[key] = fmt.Sprintf("%v", val)
		case map[string]interface{}:
			convertStatusToString(val, output, key)
		case []interface{}:
			for i, item := range val {
				itemKey := fmt.Sprintf("%s[%d]", key, i)
				if m, ok := item.(map[string]interface{}); ok {
					convertStatusToString(m, output, itemKey)
				} else {
					output[itemKey] = fmt.Sprintf("%v", item)
				}
			}
		case time.Time:
			output[key] = val.Format(time.RFC3339)
		default:
			output[key] = fmt.Sprintf("%v", val)
		}
	}
}

// storageSnapshotAdapter adapts StorageSnapshot to StorageSnapshotProvider
type storageSnapshotAdapter struct {
	snapshot replication.StorageSnapshot
}

func (a *storageSnapshotAdapter) CreateSnapshot() (replication.StorageSnapshot, error) {
	return a.snapshot, nil
}
