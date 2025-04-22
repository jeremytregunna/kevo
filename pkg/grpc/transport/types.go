package transport

import (
	"crypto/tls"
	"sync"
	"time"

	pb "github.com/jeremytregunna/kevo/proto/kevo"
	"github.com/jeremytregunna/kevo/pkg/transport"
	"google.golang.org/grpc"
)

// GRPCConnection implements the transport.Connection interface for gRPC connections
type GRPCConnection struct {
	conn     *grpc.ClientConn
	address  string
	metrics  *transport.ExtendedMetricsCollector
	lastUsed time.Time
	mu       sync.RWMutex
	reqCount int
	errCount int
}

// Execute runs a function with the gRPC client
func (c *GRPCConnection) Execute(fn func(interface{}) error) error {
	c.mu.Lock()
	c.lastUsed = time.Now()
	c.reqCount++
	c.mu.Unlock()

	// Create a new client from the connection
	client := pb.NewKevoServiceClient(c.conn)
	
	// Execute the provided function with the client
	err := fn(client)
	
	// Update metrics if there was an error
	if err != nil {
		c.mu.Lock()
		c.errCount++
		c.mu.Unlock()
	}
	
	return err
}

// Close closes the gRPC connection
func (c *GRPCConnection) Close() error {
	return c.conn.Close()
}

// Address returns the endpoint address
func (c *GRPCConnection) Address() string {
	return c.address
}

// Status returns the current connection status
func (c *GRPCConnection) Status() transport.ConnectionStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	// Check the connection state
	isConnected := c.conn != nil
	
	return transport.ConnectionStatus{
		Connected:    isConnected,
		LastActivity: c.lastUsed,
		ErrorCount:   c.errCount,
		RequestCount: c.reqCount,
	}
}

// GRPCTransportOptions configuration for gRPC transport
type GRPCTransportOptions struct {
	ListenAddr         string
	TLSConfig          *tls.Config
	ConnectionTimeout  time.Duration
	DialTimeout        time.Duration
	KeepAliveTime      time.Duration
	KeepAliveTimeout   time.Duration
	MaxConnectionIdle  time.Duration
	MaxConnectionAge   time.Duration
	MaxPoolConnections int
}