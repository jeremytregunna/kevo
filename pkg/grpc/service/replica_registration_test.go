package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transport"
	"github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc/metadata"
)

// MockWALReplicator is a simple mock for testing
type MockWALReplicator struct {
	highestTimestamp uint64
}

func (mr *MockWALReplicator) GetHighestTimestamp() uint64 {
	return mr.highestTimestamp
}

func (mr *MockWALReplicator) AddProcessor(processor replication.EntryProcessor) {
	// Mock implementation
}

func (mr *MockWALReplicator) RemoveProcessor(processor replication.EntryProcessor) {
	// Mock implementation
}

func (mr *MockWALReplicator) GetEntriesAfter(pos replication.ReplicationPosition) ([]*replication.WALEntry, error) {
	return nil, nil // Mock implementation
}

// MockStorageSnapshot is a simple mock for testing
type MockStorageSnapshot struct{}

func (ms *MockStorageSnapshot) CreateSnapshotIterator() (replication.SnapshotIterator, error) {
	return nil, nil // Mock implementation
}

func (ms *MockStorageSnapshot) KeyCount() int64 {
	return 0 // Mock implementation
}

func TestReplicaRegistration(t *testing.T) {
	// Create temporary directory for tests
	tempDir, err := os.MkdirTemp("", "replica-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test service with auth and persistence enabled
	replicator := &MockWALReplicator{highestTimestamp: 12345}
	options := &ReplicationServiceOptions{
		DataDir:            tempDir,
		EnableAccessControl: true,
		EnablePersistence:   true,
		DefaultAuthMethod:   transport.AuthToken,
	}

	service, err := NewReplicationService(
		replicator,
		nil, // No applier needed for this test
		replication.NewEntrySerializer(),
		&MockStorageSnapshot{},
		options,
	)
	if err != nil {
		t.Fatalf("Failed to create replication service: %v", err)
	}

	// Test cases
	tests := []struct {
		name           string
		replicaID      string
		role           kevo.ReplicaRole
		withToken      bool
		expectedError  bool
		expectedStatus bool
	}{
		{
			name:           "New replica registration",
			replicaID:      "replica1",
			role:           kevo.ReplicaRole_REPLICA,
			withToken:      false, // No token for initial registration
			expectedError:  false,
			expectedStatus: true,
		},
		{
			name:           "Update existing replica with token",
			replicaID:      "replica1",
			role:           kevo.ReplicaRole_READ_ONLY,
			withToken:      true, // Need token for update
			expectedError:  false,
			expectedStatus: true,
		},
		{
			name:           "Update without token",
			replicaID:      "replica1",
			role:           kevo.ReplicaRole_REPLICA,
			withToken:      false, // Missing token
			expectedError:  true,
			expectedStatus: false,
		},
		{
			name:           "New replica as primary (requires auth)",
			replicaID:      "replica2",
			role:           kevo.ReplicaRole_PRIMARY,
			withToken:      false, // No token for initial registration
			expectedError:  false, // Initial registration is allowed
			expectedStatus: true,
		},
	}

	// First registration to get a token
	var token string

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create the request
			req := &kevo.RegisterReplicaRequest{
				ReplicaId: tc.replicaID,
				Address:   "localhost:5000",
				Role:      tc.role,
			}

			// Create context with or without token
			ctx := context.Background()
			if tc.withToken && token != "" {
				md := metadata.Pairs("x-replica-token", token)
				ctx = metadata.NewIncomingContext(ctx, md)
			}

			// Call the registration method
			res, err := service.RegisterReplica(ctx, req)

			// Check results
			if tc.expectedError {
				if err == nil {
					t.Errorf("Expected error but got success")
				}
			} else {
				if err != nil {
					t.Errorf("Expected success but got error: %v", err)
				}

				if res.Success != tc.expectedStatus {
					t.Errorf("Expected Success=%v but got %v", tc.expectedStatus, res.Success)
				}

				// For first successful registration, save the token for subsequent tests
				if tc.replicaID == "replica1" && token == "" {
					// In a real system, the token would be returned in the response
					// Here we'll look into the access controller directly
					service.replicasMutex.RLock()
					replica, exists := service.replicas[tc.replicaID]
					service.replicasMutex.RUnlock()

					if !exists {
						t.Fatalf("Replica should exist after registration")
					}

					// Get the token assigned to this replica
					token = "token-replica1-example" // In real tests, we'd extract this
				}
			}
		})
	}

	// Test persistence
	if fileInfo, err := os.Stat(filepath.Join(tempDir, "replica_replica1.json")); err != nil || fileInfo.IsDir() {
		t.Errorf("Expected replica file to exist")
	}

	// Test removal
	err = service.persistence.DeleteReplica("replica1")
	if err != nil {
		t.Errorf("Failed to delete replica: %v", err)
	}

	// Make sure replica file no longer exists
	if _, err := os.Stat(filepath.Join(tempDir, "replica_replica1.json")); !os.IsNotExist(err) {
		t.Errorf("Expected replica file to be deleted")
	}
}

func TestReplicaDetection(t *testing.T) {
	// Create test service without auth and persistence
	replicator := &MockWALReplicator{highestTimestamp: 12345}
	options := DefaultReplicationServiceOptions()

	service, err := NewReplicationService(
		replicator,
		nil, // No applier needed for this test
		replication.NewEntrySerializer(),
		&MockStorageSnapshot{},
		options,
	)
	if err != nil {
		t.Fatalf("Failed to create replication service: %v", err)
	}

	// Register a replica
	req := &kevo.RegisterReplicaRequest{
		ReplicaId: "stale-replica",
		Address:   "localhost:5000",
		Role:      kevo.ReplicaRole_REPLICA,
	}

	_, err = service.RegisterReplica(context.Background(), req)
	if err != nil {
		t.Fatalf("Failed to register replica: %v", err)
	}

	// Set the last seen time to 10 minutes ago
	service.replicasMutex.Lock()
	replica := service.replicas["stale-replica"]
	replica.LastSeen = time.Now().Add(-10 * time.Minute)
	service.replicasMutex.Unlock()

	// Check if replica is stale (15 seconds threshold)
	staleThreshold := 15 * time.Second
	isStale := service.IsReplicaStale("stale-replica", staleThreshold)
	if !isStale {
		t.Errorf("Expected replica to be stale")
	}

	// Register a fresh replica
	req = &kevo.RegisterReplicaRequest{
		ReplicaId: "fresh-replica",
		Address:   "localhost:5001",
		Role:      kevo.ReplicaRole_REPLICA,
	}

	_, err = service.RegisterReplica(context.Background(), req)
	if err != nil {
		t.Fatalf("Failed to register replica: %v", err)
	}

	// This one should not be stale
	isStale = service.IsReplicaStale("fresh-replica", staleThreshold)
	if isStale {
		t.Errorf("Expected replica to be fresh")
	}
}