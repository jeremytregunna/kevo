package transport

import (
	"sync"
	"time"
)

// ReplicaMetrics contains metrics for a single replica
type ReplicaMetrics struct {
	ReplicaID         string        // ID of the replica
	Status            ReplicaStatus // Current status
	ConnectedDuration time.Duration // How long the replica has been connected
	LastSeen          time.Time     // Last time a heartbeat was received
	ReplicationLag    time.Duration // Current replication lag
	AppliedLSN        uint64        // Last LSN applied on the replica
	WALEntriesSent    uint64        // Number of WAL entries sent to this replica
	HeartbeatCount    uint64        // Number of heartbeats received
	ErrorCount        uint64        // Number of errors encountered

	// For bandwidth metrics
	BytesSent        uint64 // Total bytes sent to this replica
	BytesReceived    uint64 // Total bytes received from this replica
	LastTransferRate uint64 // Bytes/second in the last measurement period

	// Bootstrap metrics
	BootstrapCount        uint64        // Number of times bootstrapped
	LastBootstrapTime     time.Time     // Last time a bootstrap was completed
	LastBootstrapDuration time.Duration // Duration of the last bootstrap
}

// NewReplicaMetrics creates a new metrics collector for a replica
func NewReplicaMetrics(replicaID string) *ReplicaMetrics {
	return &ReplicaMetrics{
		ReplicaID: replicaID,
		Status:    StatusDisconnected,
		LastSeen:  time.Now(),
	}
}

// ReplicationMetrics collects and provides metrics about replication
type ReplicationMetrics struct {
	mu             sync.RWMutex
	replicaMetrics map[string]*ReplicaMetrics // Metrics by replica ID

	// Overall replication metrics
	PrimaryLSN            uint64        // Current LSN on primary
	TotalWALEntriesSent   uint64        // Total WAL entries sent to all replicas
	TotalBytesTransferred uint64        // Total bytes transferred
	ActiveReplicaCount    int           // Number of currently active replicas
	TotalErrorCount       uint64        // Total error count across all replicas
	TotalHeartbeatCount   uint64        // Total heartbeats processed
	AverageReplicationLag time.Duration // Average lag across replicas
	MaxReplicationLag     time.Duration // Maximum lag across replicas

	// For performance tracking
	processingTime  map[string]time.Duration // Processing time by operation type
	processingCount map[string]uint64        // Operation counts
	lastSampleTime  time.Time                // Last time metrics were sampled

	// Bootstrap metrics
	bootstrapMetrics *BootstrapMetrics
}

// NewReplicationMetrics creates a new metrics collector
func NewReplicationMetrics() *ReplicationMetrics {
	return &ReplicationMetrics{
		replicaMetrics:   make(map[string]*ReplicaMetrics),
		processingTime:   make(map[string]time.Duration),
		processingCount:  make(map[string]uint64),
		lastSampleTime:   time.Now(),
		bootstrapMetrics: newBootstrapMetrics(),
	}
}

// GetOrCreateReplicaMetrics gets metrics for a replica, creating if needed
func (rm *ReplicationMetrics) GetOrCreateReplicaMetrics(replicaID string) *ReplicaMetrics {
	rm.mu.RLock()
	metrics, exists := rm.replicaMetrics[replicaID]
	rm.mu.RUnlock()

	if exists {
		return metrics
	}

	// Create new metrics
	metrics = NewReplicaMetrics(replicaID)

	rm.mu.Lock()
	rm.replicaMetrics[replicaID] = metrics
	rm.mu.Unlock()

	return metrics
}

// UpdateReplicaStatus updates a replica's status and metrics
func (rm *ReplicationMetrics) UpdateReplicaStatus(replicaID string, status ReplicaStatus, lsn uint64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	metrics, exists := rm.replicaMetrics[replicaID]
	if !exists {
		metrics = NewReplicaMetrics(replicaID)
		rm.replicaMetrics[replicaID] = metrics
	}

	// Update last seen
	now := time.Now()
	metrics.LastSeen = now

	// Update status
	oldStatus := metrics.Status
	metrics.Status = status

	// If just connected, start tracking connected duration
	if oldStatus != StatusReady && status == StatusReady {
		metrics.ConnectedDuration = 0
	}

	// Update LSN and calculate lag
	if lsn > 0 {
		metrics.AppliedLSN = lsn

		// Calculate lag (primary LSN - replica LSN)
		if rm.PrimaryLSN > lsn {
			lag := rm.PrimaryLSN - lsn
			// Convert to a time.Duration (assuming LSN ~ timestamp)
			metrics.ReplicationLag = time.Duration(lag) * time.Millisecond
		} else {
			metrics.ReplicationLag = 0
		}
	}

	// Increment heartbeat count
	metrics.HeartbeatCount++
	rm.TotalHeartbeatCount++

	// Count active replicas and update aggregate metrics
	rm.updateAggregateMetrics()
}

// RecordWALEntries records WAL entries sent to a replica
func (rm *ReplicationMetrics) RecordWALEntries(replicaID string, count uint64, bytes uint64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	metrics, exists := rm.replicaMetrics[replicaID]
	if !exists {
		metrics = NewReplicaMetrics(replicaID)
		rm.replicaMetrics[replicaID] = metrics
	}

	// Update WAL entries count
	metrics.WALEntriesSent += count
	rm.TotalWALEntriesSent += count

	// Update bytes transferred
	metrics.BytesSent += bytes
	rm.TotalBytesTransferred += bytes

	// Calculate transfer rate
	now := time.Now()
	elapsed := now.Sub(rm.lastSampleTime)
	if elapsed > time.Second {
		metrics.LastTransferRate = uint64(float64(bytes) / elapsed.Seconds())
		rm.lastSampleTime = now
	}
}

// RecordBootstrap records a bootstrap operation
func (rm *ReplicationMetrics) RecordBootstrap(replicaID string, duration time.Duration) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	metrics, exists := rm.replicaMetrics[replicaID]
	if !exists {
		metrics = NewReplicaMetrics(replicaID)
		rm.replicaMetrics[replicaID] = metrics
	}

	metrics.BootstrapCount++
	metrics.LastBootstrapTime = time.Now()
	metrics.LastBootstrapDuration = duration
}

// RecordError records an error for a replica
func (rm *ReplicationMetrics) RecordError(replicaID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	metrics, exists := rm.replicaMetrics[replicaID]
	if !exists {
		metrics = NewReplicaMetrics(replicaID)
		rm.replicaMetrics[replicaID] = metrics
	}

	metrics.ErrorCount++
	rm.TotalErrorCount++
}

// RecordOperationDuration records the duration of a replication operation
func (rm *ReplicationMetrics) RecordOperationDuration(operation string, duration time.Duration) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.processingTime[operation] += duration
	rm.processingCount[operation]++
}

// GetAverageOperationDuration returns the average duration for an operation
func (rm *ReplicationMetrics) GetAverageOperationDuration(operation string) time.Duration {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	count := rm.processingCount[operation]
	if count == 0 {
		return 0
	}

	return time.Duration(int64(rm.processingTime[operation]) / int64(count))
}

// GetAllReplicaMetrics returns a copy of all replica metrics
func (rm *ReplicationMetrics) GetAllReplicaMetrics() map[string]ReplicaMetrics {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	result := make(map[string]ReplicaMetrics, len(rm.replicaMetrics))
	for id, metrics := range rm.replicaMetrics {
		result[id] = *metrics // Make a copy
	}

	return result
}

// GetReplicaMetrics returns metrics for a specific replica
func (rm *ReplicationMetrics) GetReplicaMetrics(replicaID string) (ReplicaMetrics, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	metrics, exists := rm.replicaMetrics[replicaID]
	if !exists {
		return ReplicaMetrics{}, false
	}

	return *metrics, true
}

// GetSummaryMetrics returns summary metrics for all replicas
func (rm *ReplicationMetrics) GetSummaryMetrics() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	return map[string]interface{}{
		"primary_lsn":             rm.PrimaryLSN,
		"active_replicas":         rm.ActiveReplicaCount,
		"total_wal_entries_sent":  rm.TotalWALEntriesSent,
		"total_bytes_transferred": rm.TotalBytesTransferred,
		"avg_replication_lag_ms":  rm.AverageReplicationLag.Milliseconds(),
		"max_replication_lag_ms":  rm.MaxReplicationLag.Milliseconds(),
		"total_errors":            rm.TotalErrorCount,
		"total_heartbeats":        rm.TotalHeartbeatCount,
	}
}

// UpdatePrimaryLSN updates the current primary LSN
func (rm *ReplicationMetrics) UpdatePrimaryLSN(lsn uint64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.PrimaryLSN = lsn

	// Update lag for all replicas based on new primary LSN
	for _, metrics := range rm.replicaMetrics {
		if rm.PrimaryLSN > metrics.AppliedLSN {
			lag := rm.PrimaryLSN - metrics.AppliedLSN
			metrics.ReplicationLag = time.Duration(lag) * time.Millisecond
		} else {
			metrics.ReplicationLag = 0
		}
	}

	// Update aggregate metrics
	rm.updateAggregateMetrics()
}

// updateAggregateMetrics updates aggregate metrics based on all replicas
func (rm *ReplicationMetrics) updateAggregateMetrics() {
	// Count active replicas
	activeCount := 0
	var totalLag time.Duration
	maxLag := time.Duration(0)

	for _, metrics := range rm.replicaMetrics {
		if metrics.Status == StatusReady {
			activeCount++
			totalLag += metrics.ReplicationLag
			if metrics.ReplicationLag > maxLag {
				maxLag = metrics.ReplicationLag
			}
		}
	}

	rm.ActiveReplicaCount = activeCount

	// Calculate average lag
	if activeCount > 0 {
		rm.AverageReplicationLag = totalLag / time.Duration(activeCount)
	} else {
		rm.AverageReplicationLag = 0
	}

	rm.MaxReplicationLag = maxLag
}

// UpdateConnectedDurations updates connected durations for all replicas
func (rm *ReplicationMetrics) UpdateConnectedDurations() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	now := time.Now()

	for _, metrics := range rm.replicaMetrics {
		if metrics.Status == StatusReady {
			metrics.ConnectedDuration = now.Sub(metrics.LastSeen) + metrics.ConnectedDuration
		}
	}
}

// IncrementBootstrapCount increments the bootstrap count for a replica
func (rm *ReplicationMetrics) IncrementBootstrapCount(replicaID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Update per-replica metrics
	metrics, exists := rm.replicaMetrics[replicaID]
	if !exists {
		metrics = NewReplicaMetrics(replicaID)
		rm.replicaMetrics[replicaID] = metrics
	}

	metrics.BootstrapCount++

	// Also update dedicated bootstrap metrics if available
	if rm.bootstrapMetrics != nil {
		rm.bootstrapMetrics.IncrementBootstrapCount(replicaID)
	}
}

// UpdateBootstrapProgress updates the bootstrap progress for a replica
func (rm *ReplicationMetrics) UpdateBootstrapProgress(replicaID string, progress float64) {
	if rm.bootstrapMetrics != nil {
		rm.bootstrapMetrics.UpdateBootstrapProgress(replicaID, progress)
	}
}
