package transport

import (
	"context"
	"testing"
	"time"

	pb "github.com/jeremytregunna/kevo/proto/kevo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCTransportManager(t *testing.T) {
	// Create transport manager with default options
	manager, err := NewGRPCTransportManager(DefaultGRPCTransportOptions())
	if err != nil {
		t.Fatalf("Failed to create gRPC transport manager: %v", err)
	}

	// Start the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start gRPC server: %v", err)
	}
	defer manager.Stop(ctx)

	// Ensure server is running before proceeding
	time.Sleep(100 * time.Millisecond)

	// Test connecting to the server
	conn, err := grpc.DialContext(
		ctx,
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	// Create a client
	client := pb.NewKevoServiceClient(conn)

	// At this point, we can only verify that the connection works
	// We'll need a mock service implementation to test actual RPC calls
	t.Log("Successfully connected to gRPC server")
}

func TestConnectionPool(t *testing.T) {
	// Create transport manager with default options
	manager, err := NewGRPCTransportManager(DefaultGRPCTransportOptions())
	if err != nil {
		t.Fatalf("Failed to create gRPC transport manager: %v", err)
	}

	// Create connection pool
	pool := NewConnectionPool(manager, "localhost:50051", 2, 5, 5*time.Minute)
	defer pool.Close()

	// Test getting connections from pool
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// This should fail because we haven't started the server
	_, err = pool.Get(ctx, false)
	if err == nil {
		t.Fatal("Expected error when getting connection from pool with no server running")
	}
}

func TestConnectionPoolManager(t *testing.T) {
	// Create transport manager with default options
	manager, err := NewGRPCTransportManager(DefaultGRPCTransportOptions())
	if err != nil {
		t.Fatalf("Failed to create gRPC transport manager: %v", err)
	}

	// Create pool manager
	poolManager := NewConnectionPoolManager(manager, 2, 5, 5*time.Minute)
	defer poolManager.CloseAll()

	// Test getting pools for different addresses
	pool1 := poolManager.GetPool("localhost:50051")
	pool2 := poolManager.GetPool("localhost:50052")
	pool3 := poolManager.GetPool("localhost:50051") // Same as pool1

	if pool1 == nil || pool2 == nil || pool3 == nil {
		t.Fatal("Failed to get connection pools")
	}

	// pool1 and pool3 should be the same object
	if pool1 != pool3 {
		t.Fatal("Expected pool1 and pool3 to be the same object")
	}

	// pool1 and pool2 should be different objects
	if pool1 == pool2 {
		t.Fatal("Expected pool1 and pool2 to be different objects")
	}
}

func TestTLSConfig(t *testing.T) {
	// Just test the TLS configuration functions
	// We'll skip actually loading certificates since that would require test files

	// Test with nil config
	_, err := LoadServerTLSConfig(nil)
	if err != nil {
		t.Fatalf("Expected nil error for nil server TLS config, got: %v", err)
	}

	_, err = LoadClientTLSConfig(nil)
	if err != nil {
		t.Fatalf("Expected nil error for nil client TLS config, got: %v", err)
	}

	// Test with incomplete config
	incompleteConfig := &TLSConfig{
		CertFile: "cert.pem",
		// Missing KeyFile
	}

	_, err = LoadServerTLSConfig(incompleteConfig)
	if err == nil {
		t.Fatal("Expected error for incomplete server TLS config")
	}
}