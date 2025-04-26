package service

import (
	"context"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transport"
	"github.com/KevoDB/kevo/proto/kevo"
)

func TestReplicaStateTracking(t *testing.T) {
	// Create test service with metrics enabled
	replicator := &MockRegWALReplicator{highestTimestamp: 12345}
	options := DefaultReplicationServiceOptions()

	service, err := NewReplicationService(
		replicator,
		nil, // No applier needed for this test
		replication.NewEntrySerializer(),
		&MockRegStorageSnapshot{},
		options,
	)
	if err != nil {
		t.Fatalf("Failed to create replication service: %v", err)
	}

	// Register some replicas
	replicas := []struct {
		id     string
		role   kevo.ReplicaRole
		status kevo.ReplicaStatus
		lsn    uint64
	}{
		{"replica1", kevo.ReplicaRole_REPLICA, kevo.ReplicaStatus_READY, 12000},
		{"replica2", kevo.ReplicaRole_REPLICA, kevo.ReplicaStatus_SYNCING, 10000},
		{"replica3", kevo.ReplicaRole_READ_ONLY, kevo.ReplicaStatus_READY, 11500},
	}

	for _, r := range replicas {
		// Register replica
		req := &kevo.RegisterReplicaRequest{
			ReplicaId: r.id,
			Address:   "localhost:500" + r.id[len(r.id)-1:], // localhost:5001, etc.
			Role:      r.role,
		}

		_, err := service.RegisterReplica(context.Background(), req)
		if err != nil {
			t.Fatalf("Failed to register replica %s: %v", r.id, err)
		}

		// Send initial heartbeat with status and LSN
		hbReq := &kevo.ReplicaHeartbeatRequest{
			ReplicaId:    r.id,
			Status:       r.status,
			CurrentLsn:   r.lsn,
			ErrorMessage: "",
		}

		_, err = service.ReplicaHeartbeat(context.Background(), hbReq)
		if err != nil {
			t.Fatalf("Failed to send heartbeat for replica %s: %v", r.id, err)
		}
	}

	// Test 1: Verify lag monitoring based on Lamport timestamps
	t.Run("ReplicationLagMonitoring", func(t *testing.T) {
		// Check if lag is calculated correctly for each replica
		metrics := service.GetMetrics()
		replicasMetrics, ok := metrics["replicas"].(map[string]interface{})
		if !ok {
			t.Fatalf("Expected replicas metrics to be a map")
		}

		// Replica 1 (345 lag)
		replica1Metrics, ok := replicasMetrics["replica1"].(map[string]interface{})
		if !ok {
			t.Fatalf("Expected replica1 metrics to be a map")
		}

		lagMs1 := replica1Metrics["replication_lag_ms"]
		if lagMs1 != int64(345) {
			t.Errorf("Expected replica1 lag to be 345ms, got %v", lagMs1)
		}

		// Replica 2 (2345 lag)
		replica2Metrics, ok := replicasMetrics["replica2"].(map[string]interface{})
		if !ok {
			t.Fatalf("Expected replica2 metrics to be a map")
		}

		lagMs2 := replica2Metrics["replication_lag_ms"]
		if lagMs2 != int64(2345) {
			t.Errorf("Expected replica2 lag to be 2345ms, got %v", lagMs2)
		}
	})

	// Test 2: Detect stale replicas
	t.Run("StaleReplicaDetection", func(t *testing.T) {
		// Make replica1 stale by setting its LastSeen to 1 minute ago
		service.replicasMutex.Lock()
		service.replicas["replica1"].LastSeen = time.Now().Add(-1 * time.Minute)
		service.replicasMutex.Unlock()

		// Detect stale replicas with 30-second threshold
		staleReplicas := service.DetectStaleReplicas(30 * time.Second)

		// Verify replica1 is marked as stale
		if len(staleReplicas) != 1 || staleReplicas[0] != "replica1" {
			t.Errorf("Expected replica1 to be stale, got %v", staleReplicas)
		}

		// Verify with IsReplicaStale
		if !service.IsReplicaStale("replica1", 30*time.Second) {
			t.Error("Expected IsReplicaStale to return true for replica1")
		}

		if service.IsReplicaStale("replica2", 30*time.Second) {
			t.Error("Expected IsReplicaStale to return false for replica2")
		}
	})

	// Test 3: Verify metrics collection
	t.Run("MetricsCollection", func(t *testing.T) {
		// Get initial metrics
		_ = service.GetMetrics() // Initial metrics

		// Send some more heartbeats
		for i := 0; i < 5; i++ {
			hbReq := &kevo.ReplicaHeartbeatRequest{
				ReplicaId:    "replica2",
				Status:       kevo.ReplicaStatus_READY,
				CurrentLsn:   10500 + uint64(i*100), // Increasing LSN
				ErrorMessage: "",
			}

			_, err = service.ReplicaHeartbeat(context.Background(), hbReq)
			if err != nil {
				t.Fatalf("Failed to send heartbeat: %v", err)
			}
		}

		// Get updated metrics
		updatedMetrics := service.GetMetrics()

		// Check replica metrics
		replicasMetrics := updatedMetrics["replicas"].(map[string]interface{})
		replica2Metrics := replicasMetrics["replica2"].(map[string]interface{})

		// Check heartbeat count increased
		heartbeatCount := replica2Metrics["heartbeat_count"].(uint64)
		if heartbeatCount < 6 { // Initial + 5 more
			t.Errorf("Expected at least 6 heartbeats for replica2, got %d", heartbeatCount)
		}

		// Check LSN increased
		appliedLSN := replica2Metrics["applied_lsn"].(uint64)
		if appliedLSN < 10900 {
			t.Errorf("Expected LSN to increase to at least 10900, got %d", appliedLSN)
		}

		// Check status changed to READY
		status := replica2Metrics["status"].(string)
		if status != string(transport.StatusReady) {
			t.Errorf("Expected status to be READY, got %s", status)
		}
	})

	// Test 4: Get metrics for a specific replica
	t.Run("GetReplicaMetrics", func(t *testing.T) {
		metrics, err := service.GetReplicaMetrics("replica3")
		if err != nil {
			t.Fatalf("Failed to get replica metrics: %v", err)
		}

		// Check some fields
		if metrics["applied_lsn"].(uint64) != 11500 {
			t.Errorf("Expected LSN 11500, got %v", metrics["applied_lsn"])
		}

		if metrics["status"].(string) != string(transport.StatusReady) {
			t.Errorf("Expected status READY, got %v", metrics["status"])
		}

		// Test non-existent replica
		_, err = service.GetReplicaMetrics("nonexistent")
		if err == nil {
			t.Error("Expected error for non-existent replica, got nil")
		}
	})
}
