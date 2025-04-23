package client

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/transport"
)

// mockClient implements the transport.Client interface for testing
type mockClient struct {
	connected bool
	responses map[string][]byte
	errors    map[string]error
}

func newMockClient() *mockClient {
	return &mockClient{
		connected: false,
		responses: make(map[string][]byte),
		errors:    make(map[string]error),
	}
}

func (m *mockClient) Connect(ctx context.Context) error {
	if m.errors["connect"] != nil {
		return m.errors["connect"]
	}
	m.connected = true
	return nil
}

func (m *mockClient) Close() error {
	if m.errors["close"] != nil {
		return m.errors["close"]
	}
	m.connected = false
	return nil
}

func (m *mockClient) IsConnected() bool {
	return m.connected
}

func (m *mockClient) Status() transport.TransportStatus {
	return transport.TransportStatus{
		Connected: m.connected,
	}
}

func (m *mockClient) Send(ctx context.Context, request transport.Request) (transport.Response, error) {
	if !m.connected {
		return nil, errors.New("not connected")
	}

	reqType := request.Type()
	if m.errors[reqType] != nil {
		return nil, m.errors[reqType]
	}

	if payload, ok := m.responses[reqType]; ok {
		return transport.NewResponse(reqType, payload, nil), nil
	}

	return nil, errors.New("unexpected request type")
}

func (m *mockClient) Stream(ctx context.Context) (transport.Stream, error) {
	if !m.connected {
		return nil, errors.New("not connected")
	}

	if m.errors["stream"] != nil {
		return nil, m.errors["stream"]
	}

	return nil, errors.New("stream not implemented in mock")
}

// Set up a mock response for a specific request type
func (m *mockClient) setResponse(reqType string, payload []byte) {
	m.responses[reqType] = payload
}

// Set up a mock error for a specific request type
func (m *mockClient) setError(reqType string, err error) {
	m.errors[reqType] = err
}

// TestMain is used to set up test environment
func TestMain(m *testing.M) {
	// Register mock client with the transport registry for testing
	transport.RegisterClientTransport("mock", func(endpoint string, options transport.TransportOptions) (transport.Client, error) {
		return newMockClient(), nil
	})

	// Run tests
	os.Exit(m.Run())
}

func TestClientConnect(t *testing.T) {
	// Modify default options to use mock transport
	options := DefaultClientOptions()
	options.TransportType = "mock"

	// Create a client with the mock transport
	client, err := NewClient(options)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Get the underlying mock client for test assertions
	mock := client.client.(*mockClient)

	ctx := context.Background()

	// Test successful connection
	err = client.Connect(ctx)
	if err != nil {
		t.Errorf("Expected successful connection, got error: %v", err)
	}

	if !client.IsConnected() {
		t.Error("Expected client to be connected")
	}

	// Test connection error
	mock.setError("connect", errors.New("connection refused"))
	err = client.Connect(ctx)
	if err == nil {
		t.Error("Expected connection error, got nil")
	}
}

func TestClientGet(t *testing.T) {
	// Create a client with the mock transport
	options := DefaultClientOptions()
	options.TransportType = "mock"

	client, err := NewClient(options)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Get the underlying mock client for test assertions
	mock := client.client.(*mockClient)
	mock.connected = true

	ctx := context.Background()

	// Test successful get
	mock.setResponse(transport.TypeGet, []byte(`{"value": "dGVzdHZhbHVl", "found": true}`))
	val, found, err := client.Get(ctx, []byte("testkey"))
	if err != nil {
		t.Errorf("Expected successful get, got error: %v", err)
	}
	if !found {
		t.Error("Expected found to be true")
	}
	if string(val) != "testvalue" {
		t.Errorf("Expected value 'testvalue', got '%s'", val)
	}

	// Test key not found
	mock.setResponse(transport.TypeGet, []byte(`{"value": null, "found": false}`))
	_, found, err = client.Get(ctx, []byte("nonexistent"))
	if err != nil {
		t.Errorf("Expected successful get with not found, got error: %v", err)
	}
	if found {
		t.Error("Expected found to be false")
	}

	// Test get error
	mock.setError(transport.TypeGet, errors.New("get error"))
	_, _, err = client.Get(ctx, []byte("testkey"))
	if err == nil {
		t.Error("Expected get error, got nil")
	}
}

func TestClientPut(t *testing.T) {
	// Create a client with the mock transport
	options := DefaultClientOptions()
	options.TransportType = "mock"

	client, err := NewClient(options)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Get the underlying mock client for test assertions
	mock := client.client.(*mockClient)
	mock.connected = true

	ctx := context.Background()

	// Test successful put
	mock.setResponse(transport.TypePut, []byte(`{"success": true}`))
	success, err := client.Put(ctx, []byte("testkey"), []byte("testvalue"), true)
	if err != nil {
		t.Errorf("Expected successful put, got error: %v", err)
	}
	if !success {
		t.Error("Expected success to be true")
	}

	// Test put error
	mock.setError(transport.TypePut, errors.New("put error"))
	_, err = client.Put(ctx, []byte("testkey"), []byte("testvalue"), true)
	if err == nil {
		t.Error("Expected put error, got nil")
	}
}

func TestClientDelete(t *testing.T) {
	// Create a client with the mock transport
	options := DefaultClientOptions()
	options.TransportType = "mock"

	client, err := NewClient(options)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Get the underlying mock client for test assertions
	mock := client.client.(*mockClient)
	mock.connected = true

	ctx := context.Background()

	// Test successful delete
	mock.setResponse(transport.TypeDelete, []byte(`{"success": true}`))
	success, err := client.Delete(ctx, []byte("testkey"), true)
	if err != nil {
		t.Errorf("Expected successful delete, got error: %v", err)
	}
	if !success {
		t.Error("Expected success to be true")
	}

	// Test delete error
	mock.setError(transport.TypeDelete, errors.New("delete error"))
	_, err = client.Delete(ctx, []byte("testkey"), true)
	if err == nil {
		t.Error("Expected delete error, got nil")
	}
}

func TestClientBatchWrite(t *testing.T) {
	// Create a client with the mock transport
	options := DefaultClientOptions()
	options.TransportType = "mock"

	client, err := NewClient(options)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Get the underlying mock client for test assertions
	mock := client.client.(*mockClient)
	mock.connected = true

	ctx := context.Background()

	// Create batch operations
	operations := []BatchOperation{
		{Type: "put", Key: []byte("key1"), Value: []byte("value1")},
		{Type: "put", Key: []byte("key2"), Value: []byte("value2")},
		{Type: "delete", Key: []byte("key3")},
	}

	// Test successful batch write
	mock.setResponse(transport.TypeBatchWrite, []byte(`{"success": true}`))
	success, err := client.BatchWrite(ctx, operations, true)
	if err != nil {
		t.Errorf("Expected successful batch write, got error: %v", err)
	}
	if !success {
		t.Error("Expected success to be true")
	}

	// Test batch write error
	mock.setError(transport.TypeBatchWrite, errors.New("batch write error"))
	_, err = client.BatchWrite(ctx, operations, true)
	if err == nil {
		t.Error("Expected batch write error, got nil")
	}
}

func TestClientGetStats(t *testing.T) {
	// Create a client with the mock transport
	options := DefaultClientOptions()
	options.TransportType = "mock"

	client, err := NewClient(options)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Get the underlying mock client for test assertions
	mock := client.client.(*mockClient)
	mock.connected = true

	ctx := context.Background()

	// Test successful get stats
	statsJSON := `{
		"key_count": 1000,
		"storage_size": 1048576,
		"memtable_count": 1,
		"sstable_count": 5,
		"write_amplification": 1.5,
		"read_amplification": 2.0
	}`
	mock.setResponse(transport.TypeGetStats, []byte(statsJSON))

	stats, err := client.GetStats(ctx)
	if err != nil {
		t.Errorf("Expected successful get stats, got error: %v", err)
	}

	if stats.KeyCount != 1000 {
		t.Errorf("Expected KeyCount 1000, got %d", stats.KeyCount)
	}
	if stats.StorageSize != 1048576 {
		t.Errorf("Expected StorageSize 1048576, got %d", stats.StorageSize)
	}
	if stats.MemtableCount != 1 {
		t.Errorf("Expected MemtableCount 1, got %d", stats.MemtableCount)
	}
	if stats.SstableCount != 5 {
		t.Errorf("Expected SstableCount 5, got %d", stats.SstableCount)
	}
	if stats.WriteAmplification != 1.5 {
		t.Errorf("Expected WriteAmplification 1.5, got %f", stats.WriteAmplification)
	}
	if stats.ReadAmplification != 2.0 {
		t.Errorf("Expected ReadAmplification 2.0, got %f", stats.ReadAmplification)
	}

	// Test get stats error
	mock.setError(transport.TypeGetStats, errors.New("get stats error"))
	_, err = client.GetStats(ctx)
	if err == nil {
		t.Error("Expected get stats error, got nil")
	}
}

func TestClientCompact(t *testing.T) {
	// Create a client with the mock transport
	options := DefaultClientOptions()
	options.TransportType = "mock"

	client, err := NewClient(options)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Get the underlying mock client for test assertions
	mock := client.client.(*mockClient)
	mock.connected = true

	ctx := context.Background()

	// Test successful compact
	mock.setResponse(transport.TypeCompact, []byte(`{"success": true}`))
	success, err := client.Compact(ctx, true)
	if err != nil {
		t.Errorf("Expected successful compact, got error: %v", err)
	}
	if !success {
		t.Error("Expected success to be true")
	}

	// Test compact error
	mock.setError(transport.TypeCompact, errors.New("compact error"))
	_, err = client.Compact(ctx, true)
	if err == nil {
		t.Error("Expected compact error, got nil")
	}
}

func TestClientPutDeletePutSequence(t *testing.T) {
	// Create a client with the mock transport
	options := DefaultClientOptions()
	options.TransportType = "mock"

	client, err := NewClient(options)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Get the underlying mock client for test assertions
	mock := client.client.(*mockClient)
	mock.connected = true

	ctx := context.Background()

	// Define test key and values
	key := []byte("sequence-test-key")
	initialValue := []byte("initial-value")
	newValue := []byte("new-value-after-delete")

	// 1. Put the initial value
	mock.setResponse(transport.TypePut, []byte(`{"success": true}`))
	success, err := client.Put(ctx, key, initialValue, true)
	if err != nil {
		t.Fatalf("Failed to put initial value: %v", err)
	}
	if !success {
		t.Fatal("Expected Put success to be true")
	}

	// 2. Get and verify the initial value
	mock.setResponse(transport.TypeGet, []byte(`{"value": "aW5pdGlhbC12YWx1ZQ==", "found": true}`))
	val, found, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get key after initial put: %v", err)
	}
	if !found {
		t.Fatal("Expected key to be found after initial put")
	}
	if string(val) != string(initialValue) {
		t.Errorf("Expected value '%s', got '%s'", initialValue, val)
	}

	// 3. Delete the key
	mock.setResponse(transport.TypeDelete, []byte(`{"success": true}`))
	success, err = client.Delete(ctx, key, true)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}
	if !success {
		t.Fatal("Expected Delete success to be true")
	}

	// 4. Verify the key is deleted
	mock.setResponse(transport.TypeGet, []byte(`{"value": null, "found": false}`))
	_, found, err = client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get key after delete: %v", err)
	}
	if found {
		t.Fatal("Expected key to not be found after delete")
	}

	// 5. Put a new value for the same key
	mock.setResponse(transport.TypePut, []byte(`{"success": true}`))
	success, err = client.Put(ctx, key, newValue, true)
	if err != nil {
		t.Fatalf("Failed to put new value after delete: %v", err)
	}
	if !success {
		t.Fatal("Expected Put success to be true")
	}

	// 6. Get and verify the new value
	mock.setResponse(transport.TypeGet, []byte(`{"value": "bmV3LXZhbHVlLWFmdGVyLWRlbGV0ZQ==", "found": true}`))
	val, found, err = client.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get key after put-delete-put sequence: %v", err)
	}
	if !found {
		t.Fatal("Expected key to be found after put-delete-put sequence")
	}
	if string(val) != string(newValue) {
		t.Errorf("Expected value '%s', got '%s'", newValue, val)
	}
}

func TestRetryWithBackoff(t *testing.T) {
	ctx := context.Background()

	// Test successful retry
	attempts := 0
	err := RetryWithBackoff(
		ctx,
		func() error {
			attempts++
			if attempts < 3 {
				return ErrTimeout
			}
			return nil
		},
		5,                    // maxRetries
		10*time.Millisecond,  // initialBackoff
		100*time.Millisecond, // maxBackoff
		2.0,                  // backoffFactor
		0.1,                  // jitter
	)

	if err != nil {
		t.Errorf("Expected successful retry, got error: %v", err)
	}
	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}

	// Test max retries exceeded
	attempts = 0
	err = RetryWithBackoff(
		ctx,
		func() error {
			attempts++
			return ErrTimeout
		},
		3,                    // maxRetries
		10*time.Millisecond,  // initialBackoff
		100*time.Millisecond, // maxBackoff
		2.0,                  // backoffFactor
		0.1,                  // jitter
	)

	if err == nil {
		t.Error("Expected error after max retries, got nil")
	}
	if attempts != 4 { // Initial + 3 retries
		t.Errorf("Expected 4 attempts, got %d", attempts)
	}

	// Test non-retryable error
	attempts = 0
	err = RetryWithBackoff(
		ctx,
		func() error {
			attempts++
			return errors.New("non-retryable error")
		},
		3,                    // maxRetries
		10*time.Millisecond,  // initialBackoff
		100*time.Millisecond, // maxBackoff
		2.0,                  // backoffFactor
		0.1,                  // jitter
	)

	if err == nil {
		t.Error("Expected non-retryable error to be returned, got nil")
	}
	if attempts != 1 {
		t.Errorf("Expected 1 attempt for non-retryable error, got %d", attempts)
	}

	// Test context cancellation
	attempts = 0
	cancelCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err = RetryWithBackoff(
		cancelCtx,
		func() error {
			attempts++
			return ErrTimeout
		},
		10,                   // maxRetries
		50*time.Millisecond,  // initialBackoff
		500*time.Millisecond, // maxBackoff
		2.0,                  // backoffFactor
		0.1,                  // jitter
	)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled error, got: %v", err)
	}
}
