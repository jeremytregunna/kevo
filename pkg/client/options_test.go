package client

import (
	"testing"
	"time"
)

func TestDefaultClientOptions(t *testing.T) {
	options := DefaultClientOptions()

	// Verify the default options have sensible values
	if options.Endpoint != "localhost:50051" {
		t.Errorf("Expected default endpoint to be localhost:50051, got %s", options.Endpoint)
	}

	if options.ConnectTimeout != 5*time.Second {
		t.Errorf("Expected default connect timeout to be 5s, got %s", options.ConnectTimeout)
	}

	if options.RequestTimeout != 10*time.Second {
		t.Errorf("Expected default request timeout to be 10s, got %s", options.RequestTimeout)
	}

	if options.TransportType != "grpc" {
		t.Errorf("Expected default transport type to be grpc, got %s", options.TransportType)
	}

	if options.PoolSize != 5 {
		t.Errorf("Expected default pool size to be 5, got %d", options.PoolSize)
	}

	if options.TLSEnabled != false {
		t.Errorf("Expected default TLS enabled to be false")
	}

	if options.MaxRetries != 3 {
		t.Errorf("Expected default max retries to be 3, got %d", options.MaxRetries)
	}
}
