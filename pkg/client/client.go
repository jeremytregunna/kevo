package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KevoDB/kevo/pkg/transport"
)

// CompressionType represents a compression algorithm
type CompressionType = transport.CompressionType

// Compression options
const (
	CompressionNone   = transport.CompressionNone
	CompressionGzip   = transport.CompressionGzip
	CompressionSnappy = transport.CompressionSnappy
)

// ClientOptions configures a Kevo client
type ClientOptions struct {
	// Connection options
	Endpoint        string        // Server address
	ConnectTimeout  time.Duration // Timeout for connection attempts
	RequestTimeout  time.Duration // Default timeout for requests
	TransportType   string        // Transport type (e.g. "grpc")
	PoolSize        int           // Connection pool size

	// Security options
	TLSEnabled bool   // Enable TLS
	CertFile   string // Client certificate file
	KeyFile    string // Client key file
	CAFile     string // CA certificate file

	// Retry options
	MaxRetries     int           // Maximum number of retries
	InitialBackoff time.Duration // Initial retry backoff
	MaxBackoff     time.Duration // Maximum retry backoff
	BackoffFactor  float64       // Backoff multiplier
	RetryJitter    float64       // Random jitter factor

	// Performance options
	Compression    CompressionType // Compression algorithm
	MaxMessageSize int             // Maximum message size
}

// DefaultClientOptions returns sensible default client options
func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		Endpoint:        "localhost:50051",
		ConnectTimeout:  time.Second * 5,
		RequestTimeout:  time.Second * 10,
		TransportType:   "grpc",
		PoolSize:        5,
		TLSEnabled:      false,
		MaxRetries:      3,
		InitialBackoff:  time.Millisecond * 100,
		MaxBackoff:      time.Second * 2,
		BackoffFactor:   1.5,
		RetryJitter:     0.2,
		Compression:     CompressionNone,
		MaxMessageSize:  16 * 1024 * 1024, // 16MB
	}
}

// Client represents a connection to a Kevo database server
type Client struct {
	options ClientOptions
	client  transport.Client
}

// NewClient creates a new Kevo client with the given options
func NewClient(options ClientOptions) (*Client, error) {
	if options.Endpoint == "" {
		return nil, errors.New("endpoint is required")
	}

	transportOpts := transport.TransportOptions{
		Timeout:        options.ConnectTimeout,
		MaxMessageSize: options.MaxMessageSize,
		Compression:    options.Compression,
		TLSEnabled:     options.TLSEnabled,
		CertFile:       options.CertFile,
		KeyFile:        options.KeyFile,
		CAFile:         options.CAFile,
		RetryPolicy: transport.RetryPolicy{
			MaxRetries:     options.MaxRetries,
			InitialBackoff: options.InitialBackoff,
			MaxBackoff:     options.MaxBackoff,
			BackoffFactor:  options.BackoffFactor,
			Jitter:         options.RetryJitter,
		},
	}

	transportClient, err := transport.GetClient(options.TransportType, options.Endpoint, transportOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport client: %w", err)
	}

	return &Client{
		options: options,
		client:  transportClient,
	}, nil
}

// Connect establishes a connection to the server
func (c *Client) Connect(ctx context.Context) error {
	return c.client.Connect(ctx)
}

// Close closes the connection to the server
func (c *Client) Close() error {
	return c.client.Close()
}

// IsConnected returns whether the client is connected to the server
func (c *Client) IsConnected() bool {
	return c.client != nil && c.client.IsConnected()
}

// Get retrieves a value by key
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if !c.IsConnected() {
		return nil, false, errors.New("not connected to server")
	}

	req := struct {
		Key []byte `json:"key"`
	}{
		Key: key,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, false, fmt.Errorf("failed to marshal request: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
	defer cancel()

	resp, err := c.client.Send(timeoutCtx, transport.NewRequest(transport.TypeGet, reqData))
	if err != nil {
		return nil, false, fmt.Errorf("failed to send request: %w", err)
	}

	var getResp struct {
		Value []byte `json:"value"`
		Found bool   `json:"found"`
	}

	if err := json.Unmarshal(resp.Payload(), &getResp); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return getResp.Value, getResp.Found, nil
}

// Put stores a key-value pair
func (c *Client) Put(ctx context.Context, key, value []byte, sync bool) (bool, error) {
	if !c.IsConnected() {
		return false, errors.New("not connected to server")
	}

	req := struct {
		Key   []byte `json:"key"`
		Value []byte `json:"value"`
		Sync  bool   `json:"sync"`
	}{
		Key:   key,
		Value: value,
		Sync:  sync,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return false, fmt.Errorf("failed to marshal request: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
	defer cancel()

	resp, err := c.client.Send(timeoutCtx, transport.NewRequest(transport.TypePut, reqData))
	if err != nil {
		return false, fmt.Errorf("failed to send request: %w", err)
	}

	var putResp struct {
		Success bool `json:"success"`
	}

	if err := json.Unmarshal(resp.Payload(), &putResp); err != nil {
		return false, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return putResp.Success, nil
}

// Delete removes a key-value pair
func (c *Client) Delete(ctx context.Context, key []byte, sync bool) (bool, error) {
	if !c.IsConnected() {
		return false, errors.New("not connected to server")
	}

	req := struct {
		Key  []byte `json:"key"`
		Sync bool   `json:"sync"`
	}{
		Key:  key,
		Sync: sync,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return false, fmt.Errorf("failed to marshal request: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
	defer cancel()

	resp, err := c.client.Send(timeoutCtx, transport.NewRequest(transport.TypeDelete, reqData))
	if err != nil {
		return false, fmt.Errorf("failed to send request: %w", err)
	}

	var deleteResp struct {
		Success bool `json:"success"`
	}

	if err := json.Unmarshal(resp.Payload(), &deleteResp); err != nil {
		return false, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return deleteResp.Success, nil
}

// BatchOperation represents a single operation in a batch
type BatchOperation struct {
	Type  string // "put" or "delete"
	Key   []byte
	Value []byte // only used for "put" operations
}

// BatchWrite performs multiple operations in a single atomic batch
func (c *Client) BatchWrite(ctx context.Context, operations []BatchOperation, sync bool) (bool, error) {
	if !c.IsConnected() {
		return false, errors.New("not connected to server")
	}

	req := struct {
		Operations []struct {
			Type  string `json:"type"`
			Key   []byte `json:"key"`
			Value []byte `json:"value"`
		} `json:"operations"`
		Sync bool `json:"sync"`
	}{
		Sync: sync,
	}

	for _, op := range operations {
		req.Operations = append(req.Operations, struct {
			Type  string `json:"type"`
			Key   []byte `json:"key"`
			Value []byte `json:"value"`
		}{
			Type:  op.Type,
			Key:   op.Key,
			Value: op.Value,
		})
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return false, fmt.Errorf("failed to marshal request: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
	defer cancel()

	resp, err := c.client.Send(timeoutCtx, transport.NewRequest(transport.TypeBatchWrite, reqData))
	if err != nil {
		return false, fmt.Errorf("failed to send request: %w", err)
	}

	var batchResp struct {
		Success bool `json:"success"`
	}

	if err := json.Unmarshal(resp.Payload(), &batchResp); err != nil {
		return false, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return batchResp.Success, nil
}

// GetStats retrieves database statistics
func (c *Client) GetStats(ctx context.Context) (*Stats, error) {
	if !c.IsConnected() {
		return nil, errors.New("not connected to server")
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
	defer cancel()

	// GetStats doesn't require a payload
	resp, err := c.client.Send(timeoutCtx, transport.NewRequest(transport.TypeGetStats, nil))
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	var statsResp struct {
		KeyCount           int64   `json:"key_count"`
		StorageSize        int64   `json:"storage_size"`
		MemtableCount      int32   `json:"memtable_count"`
		SstableCount       int32   `json:"sstable_count"`
		WriteAmplification float64 `json:"write_amplification"`
		ReadAmplification  float64 `json:"read_amplification"`
	}

	if err := json.Unmarshal(resp.Payload(), &statsResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &Stats{
		KeyCount:           statsResp.KeyCount,
		StorageSize:        statsResp.StorageSize,
		MemtableCount:      statsResp.MemtableCount,
		SstableCount:       statsResp.SstableCount,
		WriteAmplification: statsResp.WriteAmplification,
		ReadAmplification:  statsResp.ReadAmplification,
	}, nil
}

// Compact triggers compaction of the database
func (c *Client) Compact(ctx context.Context, force bool) (bool, error) {
	if !c.IsConnected() {
		return false, errors.New("not connected to server")
	}

	req := struct {
		Force bool `json:"force"`
	}{
		Force: force,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return false, fmt.Errorf("failed to marshal request: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
	defer cancel()

	resp, err := c.client.Send(timeoutCtx, transport.NewRequest(transport.TypeCompact, reqData))
	if err != nil {
		return false, fmt.Errorf("failed to send request: %w", err)
	}

	var compactResp struct {
		Success bool `json:"success"`
	}

	if err := json.Unmarshal(resp.Payload(), &compactResp); err != nil {
		return false, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return compactResp.Success, nil
}

// Stats contains database statistics
type Stats struct {
	KeyCount           int64
	StorageSize        int64
	MemtableCount      int32
	SstableCount       int32
	WriteAmplification float64
	ReadAmplification  float64
}