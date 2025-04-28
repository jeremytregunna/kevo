package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
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
	Endpoint       string        // Server address
	ConnectTimeout time.Duration // Timeout for connection attempts
	RequestTimeout time.Duration // Default timeout for requests
	TransportType  string        // Transport type (e.g. "grpc")
	PoolSize       int           // Connection pool size

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
		Endpoint:       "localhost:50051",
		ConnectTimeout: time.Second * 5,
		RequestTimeout: time.Second * 10,
		TransportType:  "grpc",
		PoolSize:       5,
		TLSEnabled:     false,
		MaxRetries:     3,
		InitialBackoff: time.Millisecond * 100,
		MaxBackoff:     time.Second * 2,
		BackoffFactor:  1.5,
		RetryJitter:    0.2,
		Compression:    CompressionNone,
		MaxMessageSize: 16 * 1024 * 1024, // 16MB
	}
}

// ReplicaInfo represents information about a replica node
type ReplicaInfo struct {
	Address      string            // Host:port of the replica
	LastSequence uint64            // Last applied sequence number
	Available    bool              // Whether the replica is available
	Region       string            // Optional region information
	Meta         map[string]string // Additional metadata
}

// NodeInfo contains information about the server node and topology
type NodeInfo struct {
	Role         string        // "primary", "replica", or "standalone"
	PrimaryAddr  string        // Address of the primary node
	Replicas     []ReplicaInfo // Available replica nodes
	LastSequence uint64        // Last applied sequence number
	ReadOnly     bool          // Whether the node is in read-only mode
}

// Client represents a connection to a Kevo database server
type Client struct {
	options     ClientOptions
	client      transport.Client
	primaryConn transport.Client   // Connection to primary (when connected to replica)
	replicaConn []transport.Client // Connections to replicas (when connected to primary)
	nodeInfo    *NodeInfo          // Information about the current node and topology
	connMutex   sync.RWMutex       // Protects connections
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
// and discovers the replication topology if available
func (c *Client) Connect(ctx context.Context) error {
	// First connect to the primary endpoint
	if err := c.client.Connect(ctx); err != nil {
		return err
	}

	// Query node information to discover the topology
	return c.discoverTopology(ctx)
}

// discoverTopology queries the node for replication information
// and establishes additional connections if needed
func (c *Client) discoverTopology(ctx context.Context) error {
	// Get node info from the connected server
	nodeInfo, err := c.getNodeInfo(ctx)
	if err != nil {
		// If GetNodeInfo isn't supported, assume it's standalone
		// This ensures backward compatibility with older servers
		nodeInfo = &NodeInfo{
			Role:     "standalone",
			ReadOnly: false,
		}
	}

	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	// Store the node info
	c.nodeInfo = nodeInfo

	// Based on the role, establish additional connections as needed
	switch nodeInfo.Role {
	case "replica":
		// If connected to a replica and a primary is available, connect to it
		if nodeInfo.PrimaryAddr != "" && nodeInfo.PrimaryAddr != c.options.Endpoint {
			primaryOptions := c.options
			primaryOptions.Endpoint = nodeInfo.PrimaryAddr

			// Create client connection to primary
			primaryClient, err := transport.GetClient(
				primaryOptions.TransportType,
				primaryOptions.Endpoint,
				c.createTransportOptions(primaryOptions),
			)
			if err == nil {
				// Try to connect to primary
				if err := primaryClient.Connect(ctx); err == nil {
					c.primaryConn = primaryClient
				}
			}
		}

	case "primary":
		// If connected to a primary and replicas are available, connect to some of them
		c.replicaConn = make([]transport.Client, 0, len(nodeInfo.Replicas))

		// Connect to up to 2 replicas (to avoid too many connections)
		for i, replica := range nodeInfo.Replicas {
			if i >= 2 || !replica.Available {
				continue
			}

			replicaOptions := c.options
			replicaOptions.Endpoint = replica.Address

			// Create client connection to replica
			replicaClient, err := transport.GetClient(
				replicaOptions.TransportType,
				replicaOptions.Endpoint,
				c.createTransportOptions(replicaOptions),
			)
			if err == nil {
				// Try to connect to replica
				if err := replicaClient.Connect(ctx); err == nil {
					c.replicaConn = append(c.replicaConn, replicaClient)
				}
			}
		}
	}

	return nil
}

// createTransportOptions converts client options to transport options
func (c *Client) createTransportOptions(options ClientOptions) transport.TransportOptions {
	return transport.TransportOptions{
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
}

// Close closes all connections to servers
func (c *Client) Close() error {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	// Close primary connection
	if c.primaryConn != nil {
		c.primaryConn.Close()
		c.primaryConn = nil
	}

	// Close replica connections
	for _, replica := range c.replicaConn {
		replica.Close()
	}
	c.replicaConn = nil

	// Close main connection
	return c.client.Close()
}

// getNodeInfo retrieves node information from the server
func (c *Client) getNodeInfo(ctx context.Context) (*NodeInfo, error) {
	// Create a request to the GetNodeInfo endpoint
	req := transport.NewRequest("GetNodeInfo", nil)

	// Send the request
	timeoutCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
	defer cancel()

	resp, err := c.client.Send(timeoutCtx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get node info: %w", err)
	}

	// Parse the response
	var nodeInfoResp struct {
		NodeRole       int               `json:"node_role"`
		PrimaryAddress string            `json:"primary_address"`
		Replicas       []json.RawMessage `json:"replicas"`
		LastSequence   uint64            `json:"last_sequence"`
		ReadOnly       bool              `json:"read_only"`
	}

	if err := json.Unmarshal(resp.Payload(), &nodeInfoResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal node info response: %w", err)
	}

	// Convert role from int to string
	var role string
	switch nodeInfoResp.NodeRole {
	case 0:
		role = "standalone"
	case 1:
		role = "primary"
	case 2:
		role = "replica"
	default:
		role = "unknown"
	}

	// Parse replica information
	replicas := make([]ReplicaInfo, 0, len(nodeInfoResp.Replicas))
	for _, rawReplica := range nodeInfoResp.Replicas {
		var replica struct {
			Address      string            `json:"address"`
			LastSequence uint64            `json:"last_sequence"`
			Available    bool              `json:"available"`
			Region       string            `json:"region"`
			Meta         map[string]string `json:"meta"`
		}

		if err := json.Unmarshal(rawReplica, &replica); err != nil {
			continue // Skip replicas that can't be parsed
		}

		replicas = append(replicas, ReplicaInfo{
			Address:      replica.Address,
			LastSequence: replica.LastSequence,
			Available:    replica.Available,
			Region:       replica.Region,
			Meta:         replica.Meta,
		})
	}

	return &NodeInfo{
		Role:         role,
		PrimaryAddr:  nodeInfoResp.PrimaryAddress,
		Replicas:     replicas,
		LastSequence: nodeInfoResp.LastSequence,
		ReadOnly:     nodeInfoResp.ReadOnly,
	}, nil
}

// IsConnected returns whether the client is connected to the server
func (c *Client) IsConnected() bool {
	return c.client != nil && c.client.IsConnected()
}

// Get retrieves a value by key
// If connected to a primary with replicas, it will route reads to a replica
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if !c.IsConnected() {
		return nil, false, errors.New("not connected to server")
	}

	// Check if we should route to replica
	c.connMutex.RLock()
	shouldUseReplica := c.nodeInfo != nil &&
		c.nodeInfo.Role == "primary" &&
		len(c.replicaConn) > 0
	c.connMutex.RUnlock()

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

	var resp transport.Response
	var sendErr error

	if shouldUseReplica {
		// Select a replica for reading
		c.connMutex.RLock()
		selectedReplica := c.replicaConn[0] // Simple selection: always use first replica
		c.connMutex.RUnlock()

		// Try the replica first
		resp, sendErr = selectedReplica.Send(timeoutCtx, transport.NewRequest(transport.TypeGet, reqData))

		// If replica fails, fall back to primary
		if sendErr != nil {
			resp, sendErr = c.client.Send(timeoutCtx, transport.NewRequest(transport.TypeGet, reqData))
		}
	} else {
		// Use default connection
		resp, sendErr = c.client.Send(timeoutCtx, transport.NewRequest(transport.TypeGet, reqData))
	}

	if sendErr != nil {
		return nil, false, fmt.Errorf("failed to send request: %w", sendErr)
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
// If connected to a replica, it will automatically route the write to the primary
func (c *Client) Put(ctx context.Context, key, value []byte, sync bool) (bool, error) {
	if !c.IsConnected() {
		return false, errors.New("not connected to server")
	}

	// Check if we should route to primary
	c.connMutex.RLock()
	shouldUsePrimary := c.nodeInfo != nil &&
		c.nodeInfo.Role == "replica" &&
		c.primaryConn != nil
	c.connMutex.RUnlock()

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

	var resp transport.Response
	var sendErr error

	if shouldUsePrimary {
		// Use primary connection for writes when connected to replica
		c.connMutex.RLock()
		primaryConn := c.primaryConn
		c.connMutex.RUnlock()

		resp, sendErr = primaryConn.Send(timeoutCtx, transport.NewRequest(transport.TypePut, reqData))
	} else {
		// Use default connection
		resp, sendErr = c.client.Send(timeoutCtx, transport.NewRequest(transport.TypePut, reqData))

		// If we get a read-only error and we have node info, try to extract primary address
		if sendErr != nil && c.nodeInfo == nil {
			// Try to discover topology to get primary address
			if discoverErr := c.discoverTopology(ctx); discoverErr == nil {
				// Check again if we now have a primary connection
				c.connMutex.RLock()
				primaryAvailable := c.nodeInfo != nil &&
					c.nodeInfo.Role == "replica" &&
					c.primaryConn != nil
				primaryConn := c.primaryConn
				c.connMutex.RUnlock()

				// If we now have a primary connection, retry the write
				if primaryAvailable && primaryConn != nil {
					resp, sendErr = primaryConn.Send(timeoutCtx, transport.NewRequest(transport.TypePut, reqData))
				}
			}
		}
	}

	if sendErr != nil {
		return false, fmt.Errorf("failed to send request: %w", sendErr)
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
// If connected to a replica, it will automatically route the delete to the primary
func (c *Client) Delete(ctx context.Context, key []byte, sync bool) (bool, error) {
	if !c.IsConnected() {
		return false, errors.New("not connected to server")
	}

	// Check if we should route to primary
	c.connMutex.RLock()
	shouldUsePrimary := c.nodeInfo != nil &&
		c.nodeInfo.Role == "replica" &&
		c.primaryConn != nil
	c.connMutex.RUnlock()

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

	var resp transport.Response
	var sendErr error

	if shouldUsePrimary {
		// Use primary connection for writes when connected to replica
		c.connMutex.RLock()
		primaryConn := c.primaryConn
		c.connMutex.RUnlock()

		resp, sendErr = primaryConn.Send(timeoutCtx, transport.NewRequest(transport.TypeDelete, reqData))
	} else {
		// Use default connection
		resp, sendErr = c.client.Send(timeoutCtx, transport.NewRequest(transport.TypeDelete, reqData))

		// If we get a read-only error and we have node info, try to extract primary address
		if sendErr != nil && c.nodeInfo == nil {
			// Try to discover topology to get primary address
			if discoverErr := c.discoverTopology(ctx); discoverErr == nil {
				// Check again if we now have a primary connection
				c.connMutex.RLock()
				primaryAvailable := c.nodeInfo != nil &&
					c.nodeInfo.Role == "replica" &&
					c.primaryConn != nil
				primaryConn := c.primaryConn
				c.connMutex.RUnlock()

				// If we now have a primary connection, retry the delete
				if primaryAvailable && primaryConn != nil {
					resp, sendErr = primaryConn.Send(timeoutCtx, transport.NewRequest(transport.TypeDelete, reqData))
				}
			}
		}
	}

	if sendErr != nil {
		return false, fmt.Errorf("failed to send request: %w", sendErr)
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
// If connected to a replica, it will automatically route the batch to the primary
func (c *Client) BatchWrite(ctx context.Context, operations []BatchOperation, sync bool) (bool, error) {
	if !c.IsConnected() {
		return false, errors.New("not connected to server")
	}

	// Check if we should route to primary
	c.connMutex.RLock()
	shouldUsePrimary := c.nodeInfo != nil &&
		c.nodeInfo.Role == "replica" &&
		c.primaryConn != nil
	c.connMutex.RUnlock()

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

	var resp transport.Response
	var sendErr error

	if shouldUsePrimary {
		// Use primary connection for writes when connected to replica
		c.connMutex.RLock()
		primaryConn := c.primaryConn
		c.connMutex.RUnlock()

		resp, sendErr = primaryConn.Send(timeoutCtx, transport.NewRequest(transport.TypeBatchWrite, reqData))
	} else {
		// Use default connection
		resp, sendErr = c.client.Send(timeoutCtx, transport.NewRequest(transport.TypeBatchWrite, reqData))

		// If we get a read-only error and we have node info, try to extract primary address
		if sendErr != nil && c.nodeInfo == nil {
			// Try to discover topology to get primary address
			if discoverErr := c.discoverTopology(ctx); discoverErr == nil {
				// Check again if we now have a primary connection
				c.connMutex.RLock()
				primaryAvailable := c.nodeInfo != nil &&
					c.nodeInfo.Role == "replica" &&
					c.primaryConn != nil
				primaryConn := c.primaryConn
				c.connMutex.RUnlock()

				// If we now have a primary connection, retry the batch
				if primaryAvailable && primaryConn != nil {
					resp, sendErr = primaryConn.Send(timeoutCtx, transport.NewRequest(transport.TypeBatchWrite, reqData))
				}
			}
		}
	}

	if sendErr != nil {
		return false, fmt.Errorf("failed to send request: %w", sendErr)
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

// GetNodeInfo returns information about the current node and replication topology
func (c *Client) GetReplicationInfo() (*NodeInfo, error) {
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()

	if c.nodeInfo == nil {
		return nil, errors.New("replication information not available")
	}

	// Return a copy to avoid concurrent access issues
	return &NodeInfo{
		Role:         c.nodeInfo.Role,
		PrimaryAddr:  c.nodeInfo.PrimaryAddr,
		Replicas:     c.nodeInfo.Replicas,
		LastSequence: c.nodeInfo.LastSequence,
		ReadOnly:     c.nodeInfo.ReadOnly,
	}, nil
}

// RefreshTopology updates the replication topology information
func (c *Client) RefreshTopology(ctx context.Context) error {
	return c.discoverTopology(ctx)
}

// IsPrimary returns true if the connected node is a primary
func (c *Client) IsPrimary() bool {
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()

	return c.nodeInfo != nil && c.nodeInfo.Role == "primary"
}

// IsReplica returns true if the connected node is a replica
func (c *Client) IsReplica() bool {
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()

	return c.nodeInfo != nil && c.nodeInfo.Role == "replica"
}

// IsStandalone returns true if the connected node is standalone (not part of replication)
func (c *Client) IsStandalone() bool {
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()

	return c.nodeInfo == nil || c.nodeInfo.Role == "standalone"
}
