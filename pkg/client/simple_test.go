package client

import (
	"testing"

	"github.com/jeremytregunna/kevo/pkg/transport"
)

// mockTransport is a simple mock for testing
type mockTransport struct{}

// Create a simple mock client factory for testing
func mockClientFactory(endpoint string, options transport.TransportOptions) (transport.Client, error) {
	return &mockClient{}, nil
}

func TestClientCreation(t *testing.T) {
	// First, register our mock transport
	transport.RegisterClientTransport("mock_test", mockClientFactory)
	
	// Create client options using our mock transport
	options := DefaultClientOptions()
	options.TransportType = "mock_test"
	
	// Create a client
	client, err := NewClient(options)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	
	// Verify the client was created
	if client == nil {
		t.Fatal("Client is nil")
	}
}