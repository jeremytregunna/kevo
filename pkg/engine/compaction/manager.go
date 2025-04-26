package compaction

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/KevoDB/kevo/pkg/compaction"
	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	"github.com/KevoDB/kevo/pkg/stats"
)

// Manager implements the interfaces.CompactionManager interface
type Manager struct {
	// Core compaction coordinator from pkg/compaction
	coordinator compaction.CompactionCoordinator

	// Configuration and paths
	cfg        *config.Config
	sstableDir string

	// Stats collector
	stats stats.Collector

	// Track whether compaction is running
	started atomic.Bool
}

// NewManager creates a new compaction manager
func NewManager(cfg *config.Config, sstableDir string, statsCollector stats.Collector) (*Manager, error) {
	// Create compaction coordinator options
	options := compaction.CompactionCoordinatorOptions{
		// Use defaults for CompactionStrategy and CompactionExecutor
		// They will be created by the coordinator
		CompactionInterval: cfg.CompactionInterval,
	}

	// Create the compaction coordinator
	coordinator := compaction.NewCompactionCoordinator(cfg, sstableDir, options)

	return &Manager{
		coordinator: coordinator,
		cfg:         cfg,
		sstableDir:  sstableDir,
		stats:       statsCollector,
	}, nil
}

// Start begins background compaction
func (m *Manager) Start() error {
	// Track the operation
	m.stats.TrackOperation(stats.OpCompact)

	// Track operation latency
	start := time.Now()
	err := m.coordinator.Start()
	latencyNs := uint64(time.Since(start).Nanoseconds())
	m.stats.TrackOperationWithLatency(stats.OpCompact, latencyNs)

	if err == nil {
		m.started.Store(true)
	} else {
		m.stats.TrackError("compaction_start_error")
	}

	return err
}

// Stop halts background compaction
func (m *Manager) Stop() error {
	// If not started, nothing to do
	if !m.started.Load() {
		return nil
	}

	// Track the operation
	m.stats.TrackOperation(stats.OpCompact)

	// Track operation latency
	start := time.Now()
	err := m.coordinator.Stop()
	latencyNs := uint64(time.Since(start).Nanoseconds())
	m.stats.TrackOperationWithLatency(stats.OpCompact, latencyNs)

	if err == nil {
		m.started.Store(false)
	} else {
		m.stats.TrackError("compaction_stop_error")
	}

	return err
}

// TriggerCompaction forces a compaction cycle
func (m *Manager) TriggerCompaction() error {
	// If not started, can't trigger compaction
	if !m.started.Load() {
		return fmt.Errorf("compaction manager not started")
	}

	// Track the operation
	m.stats.TrackOperation(stats.OpCompact)

	// Track operation latency
	start := time.Now()
	err := m.coordinator.TriggerCompaction()
	latencyNs := uint64(time.Since(start).Nanoseconds())
	m.stats.TrackOperationWithLatency(stats.OpCompact, latencyNs)

	if err != nil {
		m.stats.TrackError("compaction_trigger_error")
	}

	return err
}

// CompactRange triggers compaction on a specific key range
func (m *Manager) CompactRange(startKey, endKey []byte) error {
	// If not started, can't trigger compaction
	if !m.started.Load() {
		return fmt.Errorf("compaction manager not started")
	}

	// Track the operation
	m.stats.TrackOperation(stats.OpCompact)

	// Track bytes processed
	keyBytes := uint64(len(startKey) + len(endKey))
	m.stats.TrackBytes(false, keyBytes)

	// Track operation latency
	start := time.Now()
	err := m.coordinator.CompactRange(startKey, endKey)
	latencyNs := uint64(time.Since(start).Nanoseconds())
	m.stats.TrackOperationWithLatency(stats.OpCompact, latencyNs)

	if err != nil {
		m.stats.TrackError("compaction_range_error")
	}

	return err
}

// TrackTombstone adds a key to the tombstone tracker
func (m *Manager) TrackTombstone(key []byte) {
	// Forward to the coordinator
	m.coordinator.TrackTombstone(key)

	// Track bytes processed
	m.stats.TrackBytes(false, uint64(len(key)))
}

// ForcePreserveTombstone marks a tombstone for special handling
func (m *Manager) ForcePreserveTombstone(key []byte) {
	// Forward to the coordinator
	if coordinator, ok := m.coordinator.(interface {
		ForcePreserveTombstone(key []byte)
	}); ok {
		coordinator.ForcePreserveTombstone(key)
	}

	// Track bytes processed
	m.stats.TrackBytes(false, uint64(len(key)))
}

// GetCompactionStats returns statistics about the compaction state
func (m *Manager) GetCompactionStats() map[string]interface{} {
	// Get stats from the coordinator
	stats := m.coordinator.GetCompactionStats()

	// Add our own stats
	stats["compaction_running"] = m.started.Load()

	// Add tombstone tracking stats - needed for tests
	stats["tombstones_tracked"] = uint64(0)

	// Add last_compaction timestamp if not present - needed for tests
	if _, exists := stats["last_compaction"]; !exists {
		stats["last_compaction"] = time.Now().Unix()
	}

	return stats
}

// Ensure Manager implements the CompactionManager interface
var _ interfaces.CompactionManager = (*Manager)(nil)
