package client

import (
	"context"
	"testing"
)

// Renamed from TestClientConnectWithTopology to avoid duplicate function name
func TestClientConnectWithReplicationTopology(t *testing.T) {
	// Create mock client
	mock := newMockClient()
	mock.setResponse("GetNodeInfo", []byte(`{
		"node_role": 0,
		"primary_address": "",
		"replicas": [],
		"last_sequence": 0,
		"read_only": false
	}`))

	// Create and override client
	options := DefaultClientOptions()
	options.TransportType = "mock"
	client, err := NewClient(options)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Replace the transport with our manually configured mock
	client.client = mock

	// Connect and discover topology
	err = client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Verify node info was collected correctly
	if client.nodeInfo == nil {
		t.Fatal("Expected nodeInfo to be set")
	}
	if client.nodeInfo.Role != "standalone" {
		t.Errorf("Expected role to be standalone, got %s", client.nodeInfo.Role)
	}
}

// Test simple replica check
func TestIsReplicaMethod(t *testing.T) {
	// Setup client with replica node info
	client := &Client{
		options: DefaultClientOptions(),
		nodeInfo: &NodeInfo{
			Role:        "replica",
			PrimaryAddr: "primary:50051",
		},
	}

	// Verify IsReplica returns true
	if !client.IsReplica() {
		t.Error("Expected IsReplica() to return true for a replica node")
	}

	// Verify IsPrimary returns false
	if client.IsPrimary() {
		t.Error("Expected IsPrimary() to return false for a replica node")
	}

	// Verify IsStandalone returns false
	if client.IsStandalone() {
		t.Error("Expected IsStandalone() to return false for a replica node")
	}
}

// Test simple primary check
func TestIsPrimaryMethod(t *testing.T) {
	// Setup client with primary node info
	client := &Client{
		options: DefaultClientOptions(),
		nodeInfo: &NodeInfo{
			Role: "primary",
		},
	}

	// Verify IsPrimary returns true
	if !client.IsPrimary() {
		t.Error("Expected IsPrimary() to return true for a primary node")
	}

	// Verify IsReplica returns false
	if client.IsReplica() {
		t.Error("Expected IsReplica() to return false for a primary node")
	}

	// Verify IsStandalone returns false
	if client.IsStandalone() {
		t.Error("Expected IsStandalone() to return false for a primary node")
	}
}

// Test simple standalone check
func TestIsStandaloneMethod(t *testing.T) {
	// Setup client with standalone node info
	client := &Client{
		options: DefaultClientOptions(),
		nodeInfo: &NodeInfo{
			Role: "standalone",
		},
	}

	// Verify IsStandalone returns true
	if !client.IsStandalone() {
		t.Error("Expected IsStandalone() to return true for a standalone node")
	}

	// Verify IsPrimary returns false
	if client.IsPrimary() {
		t.Error("Expected IsPrimary() to return false for a standalone node")
	}

	// Verify IsReplica returns false
	if client.IsReplica() {
		t.Error("Expected IsReplica() to return false for a standalone node")
	}

	// Test with nil nodeInfo should also return true for standalone
	client = &Client{
		options:  DefaultClientOptions(),
		nodeInfo: nil,
	}
	if !client.IsStandalone() {
		t.Error("Expected IsStandalone() to return true when nodeInfo is nil")
	}
}
