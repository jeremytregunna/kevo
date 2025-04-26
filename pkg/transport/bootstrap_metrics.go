package transport

import (
	"sync"
	"time"
)

// BootstrapMetrics contains metrics related to bootstrap operations
type BootstrapMetrics struct {
	// Bootstrap counts per replica
	bootstrapCount     map[string]int
	bootstrapCountLock sync.RWMutex

	// Bootstrap progress per replica
	bootstrapProgress     map[string]float64
	bootstrapProgressLock sync.RWMutex

	// Last successful bootstrap time per replica
	lastBootstrap     map[string]time.Time
	lastBootstrapLock sync.RWMutex
}

// newBootstrapMetrics creates a new bootstrap metrics container
func newBootstrapMetrics() *BootstrapMetrics {
	return &BootstrapMetrics{
		bootstrapCount:    make(map[string]int),
		bootstrapProgress: make(map[string]float64),
		lastBootstrap:     make(map[string]time.Time),
	}
}

// IncrementBootstrapCount increments the bootstrap count for a replica
func (m *BootstrapMetrics) IncrementBootstrapCount(replicaID string) {
	m.bootstrapCountLock.Lock()
	defer m.bootstrapCountLock.Unlock()

	m.bootstrapCount[replicaID]++
}

// GetBootstrapCount gets the bootstrap count for a replica
func (m *BootstrapMetrics) GetBootstrapCount(replicaID string) int {
	m.bootstrapCountLock.RLock()
	defer m.bootstrapCountLock.RUnlock()

	return m.bootstrapCount[replicaID]
}

// UpdateBootstrapProgress updates the bootstrap progress for a replica
func (m *BootstrapMetrics) UpdateBootstrapProgress(replicaID string, progress float64) {
	m.bootstrapProgressLock.Lock()
	defer m.bootstrapProgressLock.Unlock()

	m.bootstrapProgress[replicaID] = progress
}

// GetBootstrapProgress gets the bootstrap progress for a replica
func (m *BootstrapMetrics) GetBootstrapProgress(replicaID string) float64 {
	m.bootstrapProgressLock.RLock()
	defer m.bootstrapProgressLock.RUnlock()

	return m.bootstrapProgress[replicaID]
}

// MarkBootstrapCompleted marks a bootstrap as completed for a replica
func (m *BootstrapMetrics) MarkBootstrapCompleted(replicaID string) {
	m.lastBootstrapLock.Lock()
	defer m.lastBootstrapLock.Unlock()

	m.lastBootstrap[replicaID] = time.Now()
}

// GetLastBootstrapTime gets the last bootstrap time for a replica
func (m *BootstrapMetrics) GetLastBootstrapTime(replicaID string) (time.Time, bool) {
	m.lastBootstrapLock.RLock()
	defer m.lastBootstrapLock.RUnlock()

	ts, exists := m.lastBootstrap[replicaID]
	return ts, exists
}

// GetAllBootstrapMetrics returns all bootstrap metrics as a map
func (m *BootstrapMetrics) GetAllBootstrapMetrics() map[string]map[string]interface{} {
	result := make(map[string]map[string]interface{})

	// Get all replica IDs
	var replicaIDs []string

	m.bootstrapCountLock.RLock()
	for id := range m.bootstrapCount {
		replicaIDs = append(replicaIDs, id)
	}
	m.bootstrapCountLock.RUnlock()

	m.bootstrapProgressLock.RLock()
	for id := range m.bootstrapProgress {
		found := false
		for _, existingID := range replicaIDs {
			if existingID == id {
				found = true
				break
			}
		}
		if !found {
			replicaIDs = append(replicaIDs, id)
		}
	}
	m.bootstrapProgressLock.RUnlock()

	m.lastBootstrapLock.RLock()
	for id := range m.lastBootstrap {
		found := false
		for _, existingID := range replicaIDs {
			if existingID == id {
				found = true
				break
			}
		}
		if !found {
			replicaIDs = append(replicaIDs, id)
		}
	}
	m.lastBootstrapLock.RUnlock()

	// Build metrics for each replica
	for _, id := range replicaIDs {
		replicaMetrics := make(map[string]interface{})

		// Add bootstrap count
		m.bootstrapCountLock.RLock()
		if count, exists := m.bootstrapCount[id]; exists {
			replicaMetrics["bootstrap_count"] = count
		} else {
			replicaMetrics["bootstrap_count"] = 0
		}
		m.bootstrapCountLock.RUnlock()

		// Add bootstrap progress
		m.bootstrapProgressLock.RLock()
		if progress, exists := m.bootstrapProgress[id]; exists {
			replicaMetrics["bootstrap_progress"] = progress
		} else {
			replicaMetrics["bootstrap_progress"] = 0.0
		}
		m.bootstrapProgressLock.RUnlock()

		// Add last bootstrap time
		m.lastBootstrapLock.RLock()
		if ts, exists := m.lastBootstrap[id]; exists {
			replicaMetrics["last_bootstrap"] = ts
		} else {
			replicaMetrics["last_bootstrap"] = nil
		}
		m.lastBootstrapLock.RUnlock()

		result[id] = replicaMetrics
	}

	return result
}
