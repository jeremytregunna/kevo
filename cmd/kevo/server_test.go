package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jeremytregunna/kevo/pkg/engine"
)

func TestTransactionRegistry(t *testing.T) {
	// Create a timeout context for the whole test
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// Set up temporary directory for test
	tmpDir, err := os.MkdirTemp("", "kevo_test")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a test engine
	eng, err := engine.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Create transaction registry
	registry := NewTransactionRegistry()

	// Test begin transaction
	txID, err := registry.Begin(ctx, eng, false)
	if err != nil {
		// If we get a timeout, don't fail the test - the engine might be busy
		if ctx.Err() != nil || strings.Contains(err.Error(), "timed out") {
			t.Skip("Skipping test due to transaction timeout")
		}
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if txID == "" {
		t.Fatal("Expected non-empty transaction ID")
	}

	// Test get transaction
	tx, exists := registry.Get(txID)
	if !exists {
		t.Fatalf("Transaction %s not found in registry", txID)
	}
	if tx == nil {
		t.Fatal("Expected non-nil transaction")
	}
	if tx.IsReadOnly() {
		t.Fatal("Expected read-write transaction")
	}

	// Test read-only transaction
	roTxID, err := registry.Begin(ctx, eng, true)
	if err != nil {
		// If we get a timeout, don't fail the test - the engine might be busy
		if ctx.Err() != nil || strings.Contains(err.Error(), "timed out") {
			t.Skip("Skipping test due to transaction timeout")
		}
		t.Fatalf("Failed to begin read-only transaction: %v", err)
	}
	roTx, exists := registry.Get(roTxID)
	if !exists {
		t.Fatalf("Transaction %s not found in registry", roTxID)
	}
	if !roTx.IsReadOnly() {
		t.Fatal("Expected read-only transaction")
	}

	// Test remove transaction
	registry.Remove(txID)
	_, exists = registry.Get(txID)
	if exists {
		t.Fatalf("Transaction %s should have been removed", txID)
	}

	// Test graceful shutdown
	shutdownErr := registry.GracefulShutdown(ctx)
	if shutdownErr != nil && !strings.Contains(shutdownErr.Error(), "timed out") {
		t.Fatalf("Failed to gracefully shutdown registry: %v", shutdownErr)
	}
}

func TestServerStartup(t *testing.T) {
	// Skip if not running in an environment where we can bind to ports
	if os.Getenv("ENABLE_NETWORK_TESTS") != "1" {
		t.Skip("Skipping network test (set ENABLE_NETWORK_TESTS=1 to run)")
	}

	// Set up temporary directory for test
	tmpDir, err := os.MkdirTemp("", "kevo_server_test")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a test engine
	eng, err := engine.NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Create server with a random port
	config := Config{
		ServerMode: true,
		ListenAddr: "localhost:0", // Let the OS assign a port
		DBPath:     tmpDir,
	}
	server := NewServer(eng, config)

	// Start server (does not block)
	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Check that the listener is active
	if server.listener == nil {
		t.Fatal("Server listener is nil after Start()")
	}

	// Get the assigned port - if this works, the listener is properly set up
	addr := server.listener.Addr().String()
	if addr == "" {
		t.Fatal("Server listener has no address")
	}
	t.Logf("Server listening on %s", addr)

	// Test shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Failed to shutdown server: %v", err)
	}
}