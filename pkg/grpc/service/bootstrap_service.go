package service

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transport"
	"github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BootstrapServiceOptions contains configuration for bootstrap operations
type BootstrapServiceOptions struct {
	// Maximum number of concurrent bootstrap operations
	MaxConcurrentBootstraps int

	// Batch size for key-value pairs in bootstrap responses
	BootstrapBatchSize int

	// Whether to enable resume for interrupted bootstraps
	EnableBootstrapResume bool

	// Directory for storing bootstrap state
	BootstrapStateDir string
}

// DefaultBootstrapServiceOptions returns sensible defaults
func DefaultBootstrapServiceOptions() *BootstrapServiceOptions {
	return &BootstrapServiceOptions{
		MaxConcurrentBootstraps: 5,
		BootstrapBatchSize:      1000,
		EnableBootstrapResume:   true,
		BootstrapStateDir:       "./bootstrap-state",
	}
}

// bootstrapService encapsulates bootstrap-related functionality for the replication service
type bootstrapService struct {
	// Bootstrap options
	options *BootstrapServiceOptions

	// Bootstrap generator for primary nodes
	bootstrapGenerator *replication.BootstrapGenerator

	// Bootstrap manager for replica nodes
	bootstrapManager *replication.BootstrapManager

	// Storage snapshot provider for generating snapshots
	snapshotProvider replication.StorageSnapshotProvider

	// Active bootstrap operations
	activeBootstraps      map[string]*bootstrapOperation
	activeBootstrapsMutex sync.RWMutex

	// WAL components
	replicator replication.EntryReplicator
	applier    replication.EntryApplier
}

// bootstrapOperation tracks a specific bootstrap operation
type bootstrapOperation struct {
	replicaID     string
	startTime     time.Time
	snapshotLSN   uint64
	totalKeys     int64
	processedKeys int64
	completed     bool
	error         error
}

// newBootstrapService creates a bootstrap service with the specified options
func newBootstrapService(
	options *BootstrapServiceOptions,
	snapshotProvider replication.StorageSnapshotProvider,
	replicator EntryReplicator,
	applier EntryApplier,
) (*bootstrapService, error) {
	if options == nil {
		options = DefaultBootstrapServiceOptions()
	}

	var bootstrapManager *replication.BootstrapManager
	var bootstrapGenerator *replication.BootstrapGenerator

	// Initialize bootstrap components based on role
	if replicator != nil {
		// Primary role - create generator
		bootstrapGenerator = replication.NewBootstrapGenerator(
			snapshotProvider,
			replicator,
			nil, // Use default logger
		)
	}

	if applier != nil {
		// Replica role - create manager
		var err error
		bootstrapManager, err = replication.NewBootstrapManager(
			nil, // Will be set later when needed
			applier,
			options.BootstrapStateDir,
			nil, // Use default logger
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create bootstrap manager: %w", err)
		}
	}

	return &bootstrapService{
		options:            options,
		bootstrapGenerator: bootstrapGenerator,
		bootstrapManager:   bootstrapManager,
		snapshotProvider:   snapshotProvider,
		activeBootstraps:   make(map[string]*bootstrapOperation),
		replicator:         replicator,
		applier:            applier,
	}, nil
}

// handleBootstrapRequest handles bootstrap requests from replicas
func (s *bootstrapService) handleBootstrapRequest(
	req *kevo.BootstrapRequest,
	stream kevo.ReplicationService_RequestBootstrapServer,
) error {
	replicaID := req.ReplicaId

	// Validate that we have a bootstrap generator (primary role)
	if s.bootstrapGenerator == nil {
		return status.Errorf(codes.FailedPrecondition, "this node is not a primary and cannot provide bootstrap")
	}

	// Check if we have too many concurrent bootstraps
	s.activeBootstrapsMutex.RLock()
	activeCount := len(s.activeBootstraps)
	s.activeBootstrapsMutex.RUnlock()

	if activeCount >= s.options.MaxConcurrentBootstraps {
		return status.Errorf(codes.ResourceExhausted, "too many concurrent bootstrap operations (max: %d)",
			s.options.MaxConcurrentBootstraps)
	}

	// Check if this replica already has an active bootstrap
	s.activeBootstrapsMutex.RLock()
	_, exists := s.activeBootstraps[replicaID]
	s.activeBootstrapsMutex.RUnlock()

	if exists {
		return status.Errorf(codes.AlreadyExists, "bootstrap already in progress for replica %s", replicaID)
	}

	// Track bootstrap operation
	operation := &bootstrapOperation{
		replicaID:     replicaID,
		startTime:     time.Now(),
		snapshotLSN:   0,
		totalKeys:     0,
		processedKeys: 0,
		completed:     false,
		error:         nil,
	}

	s.activeBootstrapsMutex.Lock()
	s.activeBootstraps[replicaID] = operation
	s.activeBootstrapsMutex.Unlock()

	// Clean up when done
	defer func() {
		// After a successful bootstrap, keep the operation record for a while
		// This helps with debugging and monitoring
		if operation.error == nil {
			go func() {
				time.Sleep(1 * time.Hour)
				s.activeBootstrapsMutex.Lock()
				delete(s.activeBootstraps, replicaID)
				s.activeBootstrapsMutex.Unlock()
			}()
		} else {
			// For failed bootstraps, remove immediately
			s.activeBootstrapsMutex.Lock()
			delete(s.activeBootstraps, replicaID)
			s.activeBootstrapsMutex.Unlock()
		}
	}()

	// Start bootstrap generation
	ctx := stream.Context()
	iterator, snapshotLSN, err := s.bootstrapGenerator.StartBootstrapGeneration(ctx, replicaID)
	if err != nil {
		operation.error = err
		return status.Errorf(codes.Internal, "failed to start bootstrap generation: %v", err)
	}

	// Update operation with snapshot LSN
	operation.snapshotLSN = snapshotLSN

	// Stream key-value pairs in batches
	batchSize := s.options.BootstrapBatchSize
	batch := make([]*kevo.KeyValuePair, 0, batchSize)
	pairsProcessed := int64(0)

	// Get an estimate of total keys if available
	snapshot, err := s.snapshotProvider.CreateSnapshot()
	if err == nil {
		operation.totalKeys = snapshot.KeyCount()
	}

	// Stream data in batches
	for {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			operation.error = ctx.Err()
			return status.Error(codes.Canceled, "bootstrap cancelled by client")
		default:
			// Continue
		}

		// Get next key-value pair
		key, value, err := iterator.Next()
		if err == io.EOF {
			// End of data, send any remaining pairs
			break
		}
		if err != nil {
			operation.error = err
			return status.Errorf(codes.Internal, "error reading from snapshot: %v", err)
		}

		// Add to batch
		batch = append(batch, &kevo.KeyValuePair{
			Key:   key,
			Value: value,
		})

		pairsProcessed++
		operation.processedKeys = pairsProcessed

		// Send batch if full
		if len(batch) >= batchSize {
			progress := float32(0)
			if operation.totalKeys > 0 {
				progress = float32(pairsProcessed) / float32(operation.totalKeys)
			}

			if err := stream.Send(&kevo.BootstrapBatch{
				Pairs:       batch,
				Progress:    progress,
				IsLast:      false,
				SnapshotLsn: snapshotLSN,
			}); err != nil {
				operation.error = err
				return err
			}

			// Reset batch
			batch = batch[:0]
		}
	}

	// Send any remaining pairs in the final batch
	progress := float32(1.0)
	if operation.totalKeys > 0 {
		progress = float32(pairsProcessed) / float32(operation.totalKeys)
	}

	// If there are no remaining pairs, send an empty batch with isLast=true
	if err := stream.Send(&kevo.BootstrapBatch{
		Pairs:       batch,
		Progress:    progress,
		IsLast:      true,
		SnapshotLsn: snapshotLSN,
	}); err != nil {
		operation.error = err
		return err
	}

	// Mark as completed
	operation.completed = true

	return nil
}

// handleClientBootstrap handles bootstrap process for a client replica
func (s *bootstrapService) handleClientBootstrap(
	ctx context.Context,
	bootstrapIterator transport.BootstrapIterator,
	replicaID string,
	storageApplier replication.StorageApplier,
) error {
	// Validate that we have a bootstrap manager (replica role)
	if s.bootstrapManager == nil {
		return fmt.Errorf("bootstrap manager not initialized")
	}

	// Create a storage applier adapter if needed
	if storageApplier == nil {
		return fmt.Errorf("storage applier not provided")
	}

	// Start the bootstrap process
	err := s.bootstrapManager.StartBootstrap(
		replicaID,
		bootstrapIterator,
		s.options.BootstrapBatchSize,
	)
	if err != nil {
		return fmt.Errorf("failed to start bootstrap: %w", err)
	}

	return nil
}

// isBootstrapInProgress checks if a bootstrap operation is in progress
func (s *bootstrapService) isBootstrapInProgress() bool {
	if s.bootstrapManager != nil {
		return s.bootstrapManager.IsBootstrapInProgress()
	}

	// If no bootstrap manager (primary node), check active operations
	s.activeBootstrapsMutex.RLock()
	defer s.activeBootstrapsMutex.RUnlock()

	for _, op := range s.activeBootstraps {
		if !op.completed && op.error == nil {
			return true
		}
	}

	return false
}

// getBootstrapStatus returns the status of bootstrap operations
func (s *bootstrapService) getBootstrapStatus() map[string]interface{} {
	result := make(map[string]interface{})

	// For primary role, return active bootstraps
	if s.bootstrapGenerator != nil {
		result["role"] = "primary"
		result["active_bootstraps"] = s.bootstrapGenerator.GetActiveBootstraps()
	}

	// For replica role, return bootstrap state
	if s.bootstrapManager != nil {
		result["role"] = "replica"

		state := s.bootstrapManager.GetBootstrapState()
		if state != nil {
			result["bootstrap_state"] = map[string]interface{}{
				"replica_id":      state.ReplicaID,
				"started_at":      state.StartedAt.Format(time.RFC3339),
				"last_updated_at": state.LastUpdatedAt.Format(time.RFC3339),
				"snapshot_lsn":    state.SnapshotLSN,
				"applied_keys":    state.AppliedKeys,
				"total_keys":      state.TotalKeys,
				"progress":        state.Progress,
				"completed":       state.Completed,
				"error":           state.Error,
			}
		}
	}

	return result
}

// transitionToWALReplication transitions from bootstrap to WAL replication
func (s *bootstrapService) transitionToWALReplication() error {
	if s.bootstrapManager != nil {
		return s.bootstrapManager.TransitionToWALReplication()
	}

	// If no bootstrap manager, we don't need to transition
	return nil
}
