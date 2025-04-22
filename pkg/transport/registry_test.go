package transport

import (
	"context"
	"errors"
	"testing"
	"time"
)

// mockClient implements the Client interface for testing
type mockClient struct {
	connected bool
	endpoint  string
	options   TransportOptions
}

func (m *mockClient) Connect(ctx context.Context) error {
	m.connected = true
	return nil
}

func (m *mockClient) Close() error {
	m.connected = false
	return nil
}

func (m *mockClient) IsConnected() bool {
	return m.connected
}

func (m *mockClient) Status() TransportStatus {
	return TransportStatus{
		Connected: m.connected,
	}
}

func (m *mockClient) Send(ctx context.Context, request Request) (Response, error) {
	if !m.connected {
		return nil, ErrNotConnected
	}
	return &BasicResponse{
		ResponseType: request.Type() + "_response",
		ResponseData: []byte("mock response"),
	}, nil
}

func (m *mockClient) Stream(ctx context.Context) (Stream, error) {
	if !m.connected {
		return nil, ErrNotConnected
	}
	return nil, errors.New("streaming not implemented in mock")
}

// mockClientFactory creates a new mock client
func mockClientFactory(endpoint string, options TransportOptions) (Client, error) {
	return &mockClient{
		endpoint: endpoint,
		options:  options,
	}, nil
}

// mockServer implements the Server interface for testing
type mockServer struct {
	started bool
	address string
	options TransportOptions
	handler RequestHandler
}

func (m *mockServer) Start() error {
	m.started = true
	return nil
}

func (m *mockServer) Serve() error {
	m.started = true
	return nil
}

func (m *mockServer) Stop(ctx context.Context) error {
	m.started = false
	return nil
}

func (m *mockServer) SetRequestHandler(handler RequestHandler) {
	m.handler = handler
}

// mockServerFactory creates a new mock server
func mockServerFactory(address string, options TransportOptions) (Server, error) {
	return &mockServer{
		address: address,
		options: options,
	}, nil
}

// TestRegistry tests the transport registry
func TestRegistry(t *testing.T) {
	registry := NewRegistry()

	// Register transports
	registry.RegisterClient("mock", mockClientFactory)
	registry.RegisterServer("mock", mockServerFactory)

	// Test listing transports
	transports := registry.ListTransports()
	if len(transports) != 1 || transports[0] != "mock" {
		t.Errorf("Expected [mock], got %v", transports)
	}

	// Test creating client
	client, err := registry.CreateClient("mock", "localhost:8080", TransportOptions{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Test client methods
	if client.IsConnected() {
		t.Error("Expected client to be disconnected initially")
	}

	err = client.Connect(context.Background())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	if !client.IsConnected() {
		t.Error("Expected client to be connected after Connect()")
	}

	// Test server creation
	server, err := registry.CreateServer("mock", "localhost:8080", TransportOptions{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Test server methods
	err = server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	mockServer := server.(*mockServer)
	if !mockServer.started {
		t.Error("Expected server to be started")
	}

	// Test non-existent transport
	_, err = registry.CreateClient("nonexistent", "", TransportOptions{})
	if err == nil {
		t.Error("Expected error creating non-existent client")
	}

	_, err = registry.CreateServer("nonexistent", "", TransportOptions{})
	if err == nil {
		t.Error("Expected error creating non-existent server")
	}
}
