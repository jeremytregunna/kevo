package transport

import (
	"testing"
)

// Simple smoke test for the gRPC transport
func TestNewGRPCTransportManager(t *testing.T) {
	opts := DefaultGRPCTransportOptions()
	
	// Override the listen address to avoid port conflicts
	opts.ListenAddr = ":0" // use random available port
	
	manager, err := NewGRPCTransportManager(opts)
	if err != nil {
		t.Fatalf("Failed to create transport manager: %v", err)
	}
	
	// Verify the manager was created
	if manager == nil {
		t.Fatal("Expected non-nil manager")
	}
}

// Test for the server TLS configuration
func TestLoadServerTLSConfig(t *testing.T) {
	// Skip actual loading, just test validation
	_, err := LoadServerTLSConfig("", "", "")
	if err == nil {
		t.Fatal("Expected error for empty cert/key")
	}
}

// Test for the client TLS configuration
func TestLoadClientTLSConfig(t *testing.T) {
	// Test with insecure config
	config, err := LoadClientTLSConfig("", "", "", true)
	if err != nil {
		t.Fatalf("Failed to create insecure TLS config: %v", err)
	}
	if config == nil {
		t.Fatal("Expected non-nil TLS config")
	}
	if !config.InsecureSkipVerify {
		t.Fatal("Expected InsecureSkipVerify to be true")
	}
}

// Skip actual TLS certificate loading by providing empty values
func TestLoadClientTLSConfigFromStruct(t *testing.T) {
	config, err := LoadClientTLSConfigFromStruct(&TLSConfig{
		SkipVerify: true,
	})
	if err != nil {
		t.Fatalf("Failed to create TLS config from struct: %v", err)
	}
	if config == nil {
		t.Fatal("Expected non-nil TLS config")
	}
	if !config.InsecureSkipVerify {
		t.Fatal("Expected InsecureSkipVerify to be true")
	}
}