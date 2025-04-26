package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/common/log"
)

var (
	// ErrBootstrapGenerationCancelled indicates the bootstrap generation was cancelled
	ErrBootstrapGenerationCancelled = errors.New("bootstrap generation was cancelled")

	// ErrBootstrapGenerationFailed indicates the bootstrap generation failed
	ErrBootstrapGenerationFailed = errors.New("bootstrap generation failed")
)

// BootstrapGenerator manages the creation of storage snapshots for bootstrapping replicas
type BootstrapGenerator struct {
	// Storage snapshot provider
	snapshotProvider StorageSnapshotProvider

	// Replicator for getting current LSN
	replicator EntryReplicator

	// Active bootstrap operations
	activeBootstraps      map[string]*bootstrapOperation
	activeBootstrapsMutex sync.RWMutex

	// Logger
	logger log.Logger
}

// bootstrapOperation tracks a specific bootstrap operation
type bootstrapOperation struct {
	replicaID      string
	startTime      time.Time
	keyCount       int64
	processedCount int64
	snapshotLSN    uint64
	cancelled      bool
	completed      bool
	cancelFunc     context.CancelFunc
}

// NewBootstrapGenerator creates a new bootstrap generator
func NewBootstrapGenerator(
	snapshotProvider StorageSnapshotProvider,
	replicator EntryReplicator,
	logger log.Logger,
) *BootstrapGenerator {
	if logger == nil {
		logger = log.GetDefaultLogger().WithField("component", "bootstrap_generator")
	}

	return &BootstrapGenerator{
		snapshotProvider: snapshotProvider,
		replicator:       replicator,
		activeBootstraps: make(map[string]*bootstrapOperation),
		logger:           logger,
	}
}

// StartBootstrapGeneration begins generating a bootstrap snapshot for a replica
func (g *BootstrapGenerator) StartBootstrapGeneration(
	ctx context.Context,
	replicaID string,
) (SnapshotIterator, uint64, error) {
	// Create a cancellable context
	bootstrapCtx, cancelFunc := context.WithCancel(ctx)

	// Get current LSN from replicator
	snapshotLSN := uint64(0)
	if g.replicator != nil {
		snapshotLSN = g.replicator.GetHighestTimestamp()
	}

	// Create snapshot
	snapshot, err := g.snapshotProvider.CreateSnapshot()
	if err != nil {
		cancelFunc()
		return nil, 0, fmt.Errorf("failed to create storage snapshot: %w", err)
	}

	// Get key count estimate
	keyCount := snapshot.KeyCount()

	// Create bootstrap operation tracking
	operation := &bootstrapOperation{
		replicaID:      replicaID,
		startTime:      time.Now(),
		keyCount:       keyCount,
		processedCount: 0,
		snapshotLSN:    snapshotLSN,
		cancelled:      false,
		completed:      false,
		cancelFunc:     cancelFunc,
	}

	// Register the bootstrap operation
	g.activeBootstrapsMutex.Lock()
	g.activeBootstraps[replicaID] = operation
	g.activeBootstrapsMutex.Unlock()

	// Create snapshot iterator
	iterator, err := snapshot.CreateSnapshotIterator()
	if err != nil {
		cancelFunc()
		g.activeBootstrapsMutex.Lock()
		delete(g.activeBootstraps, replicaID)
		g.activeBootstrapsMutex.Unlock()
		return nil, 0, fmt.Errorf("failed to create snapshot iterator: %w", err)
	}

	g.logger.Info("Started bootstrap generation for replica %s (estimated keys: %d, snapshot LSN: %d)",
		replicaID, keyCount, snapshotLSN)

	// Create a tracking iterator that updates progress
	trackingIterator := &trackingSnapshotIterator{
		iterator:  iterator,
		ctx:       bootstrapCtx,
		operation: operation,
		processedKey: func(count int64) {
			atomic.AddInt64(&operation.processedCount, 1)
		},
		completedCallback: func() {
			g.activeBootstrapsMutex.Lock()
			defer g.activeBootstrapsMutex.Unlock()

			operation.completed = true
			g.logger.Info("Completed bootstrap generation for replica %s (keys: %d, duration: %v)",
				replicaID, operation.processedCount, time.Since(operation.startTime))
		},
		cancelledCallback: func() {
			g.activeBootstrapsMutex.Lock()
			defer g.activeBootstrapsMutex.Unlock()

			operation.cancelled = true
			g.logger.Info("Cancelled bootstrap generation for replica %s (keys processed: %d)",
				replicaID, operation.processedCount)
		},
	}

	return trackingIterator, snapshotLSN, nil
}

// CancelBootstrapGeneration cancels an in-progress bootstrap generation
func (g *BootstrapGenerator) CancelBootstrapGeneration(replicaID string) bool {
	g.activeBootstrapsMutex.Lock()
	defer g.activeBootstrapsMutex.Unlock()

	operation, exists := g.activeBootstraps[replicaID]
	if !exists {
		return false
	}

	if operation.completed || operation.cancelled {
		return false
	}

	// Cancel the operation
	operation.cancelled = true
	operation.cancelFunc()

	g.logger.Info("Cancelled bootstrap generation for replica %s", replicaID)
	return true
}

// GetActiveBootstraps returns information about all active bootstrap operations
func (g *BootstrapGenerator) GetActiveBootstraps() map[string]map[string]interface{} {
	g.activeBootstrapsMutex.RLock()
	defer g.activeBootstrapsMutex.RUnlock()

	result := make(map[string]map[string]interface{})

	for replicaID, operation := range g.activeBootstraps {
		// Skip completed operations after a certain time
		if operation.completed && time.Since(operation.startTime) > 1*time.Hour {
			continue
		}

		// Calculate progress
		progress := float64(0)
		if operation.keyCount > 0 {
			progress = float64(operation.processedCount) / float64(operation.keyCount)
		}

		result[replicaID] = map[string]interface{}{
			"start_time":      operation.startTime,
			"duration":        time.Since(operation.startTime).String(),
			"key_count":       operation.keyCount,
			"processed_count": operation.processedCount,
			"progress":        progress,
			"snapshot_lsn":    operation.snapshotLSN,
			"completed":       operation.completed,
			"cancelled":       operation.cancelled,
		}
	}

	return result
}

// CleanupCompletedBootstraps removes tracking information for completed bootstrap operations
func (g *BootstrapGenerator) CleanupCompletedBootstraps() int {
	g.activeBootstrapsMutex.Lock()
	defer g.activeBootstrapsMutex.Unlock()

	removed := 0
	for replicaID, operation := range g.activeBootstraps {
		// Remove operations that are completed or cancelled and older than 1 hour
		if (operation.completed || operation.cancelled) && time.Since(operation.startTime) > 1*time.Hour {
			delete(g.activeBootstraps, replicaID)
			removed++
		}
	}

	return removed
}

// trackingSnapshotIterator wraps a snapshot iterator to track progress
type trackingSnapshotIterator struct {
	iterator          SnapshotIterator
	ctx               context.Context
	operation         *bootstrapOperation
	processedKey      func(count int64)
	completedCallback func()
	cancelledCallback func()
	closed            bool
	mu                sync.Mutex
}

// Next returns the next key-value pair
func (t *trackingSnapshotIterator) Next() ([]byte, []byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, nil, io.EOF
	}

	// Check for cancellation
	select {
	case <-t.ctx.Done():
		if !t.closed {
			t.closed = true
			t.cancelledCallback()
		}
		return nil, nil, ErrBootstrapGenerationCancelled
	default:
		// Continue
	}

	// Get next pair
	key, value, err := t.iterator.Next()
	if err == io.EOF {
		if !t.closed {
			t.closed = true
			t.completedCallback()
		}
		return nil, nil, io.EOF
	}
	if err != nil {
		return nil, nil, err
	}

	// Track progress
	t.processedKey(1)

	return key, value, nil
}

// Close closes the iterator
func (t *trackingSnapshotIterator) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true

	// Call appropriate callback
	select {
	case <-t.ctx.Done():
		t.cancelledCallback()
	default:
		t.completedCallback()
	}

	return t.iterator.Close()
}
