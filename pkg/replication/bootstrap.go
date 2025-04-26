package replication

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/common/log"
	"github.com/KevoDB/kevo/pkg/transport"
)

var (
	// ErrBootstrapInterrupted indicates the bootstrap process was interrupted
	ErrBootstrapInterrupted = errors.New("bootstrap process was interrupted")

	// ErrBootstrapFailed indicates the bootstrap process failed
	ErrBootstrapFailed = errors.New("bootstrap process failed")
)

// BootstrapManager handles the bootstrap process for replicas
type BootstrapManager struct {
	// Storage-related components
	storageApplier StorageApplier
	walApplier     EntryApplier

	// State tracking
	bootstrapState     *BootstrapState
	bootstrapStatePath string
	snapshotLSN        uint64

	// Mutex for synchronization
	mu sync.RWMutex

	// Logger instance
	logger log.Logger
}

// StorageApplier defines an interface for applying key-value pairs to storage
type StorageApplier interface {
	// Apply applies a key-value pair to storage
	Apply(key, value []byte) error

	// ApplyBatch applies multiple key-value pairs to storage
	ApplyBatch(pairs []KeyValuePair) error

	// Flush ensures all applied changes are persisted
	Flush() error
}

// BootstrapState tracks the state of an ongoing bootstrap operation
type BootstrapState struct {
	ReplicaID       string    `json:"replica_id"`
	StartedAt       time.Time `json:"started_at"`
	LastUpdatedAt   time.Time `json:"last_updated_at"`
	SnapshotLSN     uint64    `json:"snapshot_lsn"`
	AppliedKeys     int       `json:"applied_keys"`
	TotalKeys       int       `json:"total_keys"`
	Progress        float64   `json:"progress"`
	Completed       bool      `json:"completed"`
	Error           string    `json:"error,omitempty"`
	CurrentChecksum uint32    `json:"current_checksum"`
}

// NewBootstrapManager creates a new bootstrap manager
func NewBootstrapManager(
	storageApplier StorageApplier,
	walApplier EntryApplier,
	dataDir string,
	logger log.Logger,
) (*BootstrapManager, error) {
	if logger == nil {
		logger = log.GetDefaultLogger().WithField("component", "bootstrap_manager")
	}

	// Create bootstrap directory if it doesn't exist
	bootstrapDir := filepath.Join(dataDir, "bootstrap")
	if err := os.MkdirAll(bootstrapDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create bootstrap directory: %w", err)
	}

	bootstrapStatePath := filepath.Join(bootstrapDir, "bootstrap_state.json")

	manager := &BootstrapManager{
		storageApplier:     storageApplier,
		walApplier:         walApplier,
		bootstrapStatePath: bootstrapStatePath,
		logger:             logger,
	}

	// Try to load existing bootstrap state
	state, err := manager.loadBootstrapState()
	if err == nil && state != nil {
		manager.bootstrapState = state
		logger.Info("Loaded existing bootstrap state (progress: %.2f%%)", state.Progress*100)
	}

	return manager, nil
}

// loadBootstrapState loads the bootstrap state from disk
func (m *BootstrapManager) loadBootstrapState() (*BootstrapState, error) {
	// Check if the state file exists
	if _, err := os.Stat(m.bootstrapStatePath); os.IsNotExist(err) {
		return nil, nil
	}

	// Read and parse the state file
	file, err := os.Open(m.bootstrapStatePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var state BootstrapState
	if err := readJSONFile(file, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

// saveBootstrapState saves the bootstrap state to disk
func (m *BootstrapManager) saveBootstrapState() error {
	m.mu.RLock()
	state := m.bootstrapState
	m.mu.RUnlock()

	if state == nil {
		return nil
	}

	// Update the last updated timestamp
	state.LastUpdatedAt = time.Now()

	// Create a temporary file
	tempFile, err := os.CreateTemp(filepath.Dir(m.bootstrapStatePath), "bootstrap_state_*.json")
	if err != nil {
		return err
	}
	tempFilePath := tempFile.Name()

	// Write state to the temporary file
	if err := writeJSONFile(tempFile, state); err != nil {
		tempFile.Close()
		os.Remove(tempFilePath)
		return err
	}

	// Close the temporary file
	tempFile.Close()

	// Atomically replace the state file
	return os.Rename(tempFilePath, m.bootstrapStatePath)
}

// StartBootstrap begins the bootstrap process
func (m *BootstrapManager) StartBootstrap(
	replicaID string,
	bootstrapIterator transport.BootstrapIterator,
	batchSize int,
) error {
	m.mu.Lock()

	// Initialize bootstrap state
	m.bootstrapState = &BootstrapState{
		ReplicaID:       replicaID,
		StartedAt:       time.Now(),
		LastUpdatedAt:   time.Now(),
		SnapshotLSN:     0,
		AppliedKeys:     0,
		TotalKeys:       0, // Will be updated during the process
		Progress:        0.0,
		Completed:       false,
		CurrentChecksum: 0,
	}

	m.mu.Unlock()

	// Save initial state
	if err := m.saveBootstrapState(); err != nil {
		m.logger.Warn("Failed to save initial bootstrap state: %v", err)
	}

	// Start bootstrap process in a goroutine
	go func() {
		err := m.runBootstrap(bootstrapIterator, batchSize)
		if err != nil && err != io.EOF {
			m.mu.Lock()
			m.bootstrapState.Error = err.Error()
			m.mu.Unlock()

			m.logger.Error("Bootstrap failed: %v", err)
			if err := m.saveBootstrapState(); err != nil {
				m.logger.Error("Failed to save failed bootstrap state: %v", err)
			}
		}
	}()

	return nil
}

// runBootstrap executes the bootstrap process
func (m *BootstrapManager) runBootstrap(
	bootstrapIterator transport.BootstrapIterator,
	batchSize int,
) error {
	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	m.logger.Info("Starting bootstrap process")

	// If we have an existing state, check if we need to resume
	m.mu.RLock()
	state := m.bootstrapState
	appliedKeys := state.AppliedKeys
	m.mu.RUnlock()

	// Track batch for efficient application
	batch := make([]KeyValuePair, 0, batchSize)
	appliedInBatch := 0
	lastSaveTime := time.Now()
	saveThreshold := 5 * time.Second // Save state every 5 seconds

	// Process all key-value pairs from the iterator
	for {
		// Check progress periodically
		progress := bootstrapIterator.Progress()

		// Update progress in state
		m.mu.Lock()
		m.bootstrapState.Progress = progress
		m.mu.Unlock()

		// Save state periodically
		if time.Since(lastSaveTime) > saveThreshold {
			if err := m.saveBootstrapState(); err != nil {
				m.logger.Warn("Failed to save bootstrap state: %v", err)
			}
			lastSaveTime = time.Now()

			// Log progress
			m.logger.Info("Bootstrap progress: %.2f%% (%d keys applied)",
				progress*100, appliedKeys)
		}

		// Get next key-value pair
		key, value, err := bootstrapIterator.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error getting next key-value pair: %w", err)
		}

		// Skip keys if we're resuming and haven't reached the last applied key
		if appliedInBatch < appliedKeys {
			appliedInBatch++
			continue
		}

		// Add to batch
		batch = append(batch, KeyValuePair{
			Key:   key,
			Value: value,
		})

		// Apply batch if full
		if len(batch) >= batchSize {
			if err := m.storageApplier.ApplyBatch(batch); err != nil {
				return fmt.Errorf("error applying batch: %w", err)
			}

			// Update applied count
			appliedInBatch += len(batch)

			m.mu.Lock()
			m.bootstrapState.AppliedKeys = appliedInBatch
			m.mu.Unlock()

			// Clear batch
			batch = batch[:0]
		}
	}

	// Apply any remaining items in the batch
	if len(batch) > 0 {
		if err := m.storageApplier.ApplyBatch(batch); err != nil {
			return fmt.Errorf("error applying final batch: %w", err)
		}

		appliedInBatch += len(batch)

		m.mu.Lock()
		m.bootstrapState.AppliedKeys = appliedInBatch
		m.mu.Unlock()
	}

	// Flush changes to storage
	if err := m.storageApplier.Flush(); err != nil {
		return fmt.Errorf("error flushing storage: %w", err)
	}

	// Update WAL applier with snapshot LSN
	m.mu.RLock()
	snapshotLSN := m.snapshotLSN
	m.mu.RUnlock()

	// Reset the WAL applier to start from the snapshot LSN
	if m.walApplier != nil {
		m.walApplier.ResetHighestApplied(snapshotLSN)
		m.logger.Info("Reset WAL applier to snapshot LSN: %d", snapshotLSN)
	}

	// Update and save final state
	m.mu.Lock()
	m.bootstrapState.Completed = true
	m.bootstrapState.Progress = 1.0
	m.bootstrapState.TotalKeys = appliedInBatch
	m.bootstrapState.SnapshotLSN = snapshotLSN
	m.mu.Unlock()

	if err := m.saveBootstrapState(); err != nil {
		m.logger.Warn("Failed to save final bootstrap state: %v", err)
	}

	m.logger.Info("Bootstrap completed successfully: %d keys applied, snapshot LSN: %d",
		appliedInBatch, snapshotLSN)

	return nil
}

// IsBootstrapInProgress checks if a bootstrap operation is in progress
func (m *BootstrapManager) IsBootstrapInProgress() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.bootstrapState != nil && !m.bootstrapState.Completed && m.bootstrapState.Error == ""
}

// GetBootstrapState returns the current bootstrap state
func (m *BootstrapManager) GetBootstrapState() *BootstrapState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.bootstrapState == nil {
		return nil
	}

	// Return a copy to avoid concurrent modification
	stateCopy := *m.bootstrapState
	return &stateCopy
}

// SetSnapshotLSN sets the LSN of the snapshot being bootstrapped
func (m *BootstrapManager) SetSnapshotLSN(lsn uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.snapshotLSN = lsn

	if m.bootstrapState != nil {
		m.bootstrapState.SnapshotLSN = lsn
	}
}

// ClearBootstrapState clears any existing bootstrap state
func (m *BootstrapManager) ClearBootstrapState() error {
	m.mu.Lock()
	m.bootstrapState = nil
	m.mu.Unlock()

	// Remove state file if it exists
	if _, err := os.Stat(m.bootstrapStatePath); err == nil {
		if err := os.Remove(m.bootstrapStatePath); err != nil {
			return fmt.Errorf("error removing bootstrap state file: %w", err)
		}
	}

	return nil
}

// TransitionToWALReplication transitions from bootstrap to WAL replication
func (m *BootstrapManager) TransitionToWALReplication() error {
	m.mu.RLock()
	state := m.bootstrapState
	m.mu.RUnlock()

	if state == nil || !state.Completed {
		return ErrBootstrapInterrupted
	}

	// Ensure WAL applier is properly initialized with the snapshot LSN
	if m.walApplier != nil {
		m.walApplier.ResetHighestApplied(state.SnapshotLSN)
		m.logger.Info("Transitioned to WAL replication from LSN: %d", state.SnapshotLSN)
	}

	return nil
}

// JSON file handling functions
var writeJSONFile = writeJSONFileImpl
var readJSONFile = readJSONFileImpl

// writeJSONFileImpl writes a JSON object to a file
func writeJSONFileImpl(file *os.File, v interface{}) error {
	encoder := json.NewEncoder(file)
	return encoder.Encode(v)
}

// readJSONFileImpl reads a JSON object from a file
func readJSONFileImpl(file *os.File, v interface{}) error {
	decoder := json.NewDecoder(file)
	return decoder.Decode(v)
}
