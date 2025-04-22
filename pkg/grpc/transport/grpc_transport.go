package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/jeremytregunna/kevo/proto/kevo"
	"github.com/jeremytregunna/kevo/pkg/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	defaultDialTimeout     = 5 * time.Second
	defaultConnectTimeout  = 5 * time.Second
	defaultKeepAliveTime   = 15 * time.Second
	defaultKeepAlivePolicy = 5 * time.Second
	defaultMaxConnIdle     = 60 * time.Second
	defaultMaxConnAge      = 5 * time.Minute
)

type GRPCTransportManager struct {
	opts        *GRPCTransportOptions
	server      *grpc.Server
	listener    net.Listener
	connections sync.Map // map[string]*grpc.ClientConn
	mu          sync.RWMutex
	metrics     *transport.Metrics
}

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

var _ transport.TransportManager = (*GRPCTransportManager)(nil)

func DefaultGRPCTransportOptions() *GRPCTransportOptions {
	return &GRPCTransportOptions{
		ListenAddr:        ":50051",
		ConnectionTimeout: defaultConnectTimeout,
		DialTimeout:       defaultDialTimeout,
		KeepAliveTime:     defaultKeepAliveTime,
		KeepAliveTimeout:  defaultKeepAlivePolicy,
		MaxConnectionIdle: defaultMaxConnIdle,
		MaxConnectionAge:  defaultMaxConnAge,
	}
}

func NewGRPCTransportManager(opts *GRPCTransportOptions) (*GRPCTransportManager, error) {
	if opts == nil {
		opts = DefaultGRPCTransportOptions()
	}

	metrics := transport.NewMetrics("grpc")

	return &GRPCTransportManager{
		opts:    opts,
		metrics: metrics,
	}, nil
}

func (g *GRPCTransportManager) Start(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.server != nil {
		return fmt.Errorf("gRPC transport already started")
	}

	var serverOpts []grpc.ServerOption

	// Configure TLS if provided
	if g.opts.TLSConfig != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(g.opts.TLSConfig)))
	}

	// Configure keepalive parameters
	kaProps := keepalive.ServerParameters{
		MaxConnectionIdle: g.opts.MaxConnectionIdle,
		MaxConnectionAge:  g.opts.MaxConnectionAge,
		Time:              g.opts.KeepAliveTime,
		Timeout:           g.opts.KeepAliveTimeout,
	}
	kaPolicy := keepalive.EnforcementPolicy{
		MinTime:             g.opts.KeepAliveTime / 2,
		PermitWithoutStream: true,
	}
	serverOpts = append(serverOpts,
		grpc.KeepaliveParams(kaProps),
		grpc.KeepaliveEnforcementPolicy(kaPolicy),
	)

	// Create and start the gRPC server
	g.server = grpc.NewServer(serverOpts...)

	// Register service implementations
	// This will be implemented later once we have the service implementation
	// pb.RegisterKevoServiceServer(g.server, &kevoServiceServer{})

	// Start listening
	listener, err := net.Listen("tcp", g.opts.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", g.opts.ListenAddr, err)
	}
	g.listener = listener

	// Start server in a goroutine
	go func() {
		g.metrics.ServerStarted()
		if err := g.server.Serve(listener); err != nil {
			g.metrics.ServerErrored()
			// Just log the error, as this is running in a goroutine
			// and we can't return it
			fmt.Printf("gRPC server stopped: %v\n", err)
		}
	}()

	return nil
}

func (g *GRPCTransportManager) Stop(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.server == nil {
		return nil
	}

	// Close all client connections
	g.connections.Range(func(key, value interface{}) bool {
		conn := value.(*grpc.ClientConn)
		conn.Close()
		g.connections.Delete(key)
		return true
	})

	// Gracefully stop the server
	stopped := make(chan struct{})
	go func() {
		g.server.GracefulStop()
		close(stopped)
	}()

	// Wait for graceful stop or context deadline
	select {
	case <-stopped:
		// Server stopped gracefully
	case <-ctx.Done():
		// Context deadline exceeded, force stop
		g.server.Stop()
	}

	g.metrics.ServerStopped()
	g.server = nil
	return nil
}

func (g *GRPCTransportManager) Connect(ctx context.Context, address string) (transport.Connection, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Check if we already have a connection to this address
	if conn, ok := g.connections.Load(address); ok {
		return &GRPCConnection{
			conn:    conn.(*grpc.ClientConn),
			address: address,
			metrics: g.metrics,
		}, nil
	}

	// Set connection options
	dialOptions := []grpc.DialOption{
		grpc.WithBlock(),
	}

	// Add TLS if configured
	if g.opts.TLSConfig != nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(
			credentials.NewTLS(g.opts.TLSConfig),
		))
	} else {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Add keepalive options
	dialOptions = append(dialOptions, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                g.opts.KeepAliveTime,
		Timeout:             g.opts.KeepAliveTimeout,
		PermitWithoutStream: true,
	}))

	// Set timeout for connection
	dialCtx, cancel := context.WithTimeout(ctx, g.opts.DialTimeout)
	defer cancel()

	// Dial the server
	conn, err := grpc.DialContext(dialCtx, address, dialOptions...)
	if err != nil {
		g.metrics.ConnectionFailed()
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	// Store the connection
	g.connections.Store(address, conn)
	g.metrics.ConnectionOpened()

	return &GRPCConnection{
		conn:    conn,
		address: address,
		metrics: g.metrics,
	}, nil
}

func (g *GRPCTransportManager) RegisterService(service interface{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.server == nil {
		return fmt.Errorf("server not started, cannot register service")
	}

	// Type assert to KevoServiceServer and register
	kevoService, ok := service.(pb.KevoServiceServer)
	if !ok {
		return fmt.Errorf("service is not a valid KevoServiceServer implementation")
	}

	pb.RegisterKevoServiceServer(g.server, kevoService)
	return nil
}

// GRPCConnection represents a gRPC client connection
type GRPCConnection struct {
	conn    *grpc.ClientConn
	address string
	metrics *transport.Metrics
}

func (c *GRPCConnection) Close() error {
	err := c.conn.Close()
	c.metrics.ConnectionClosed()
	return err
}

func (c *GRPCConnection) GetClient() interface{} {
	return pb.NewKevoServiceClient(c.conn)
}

func (c *GRPCConnection) Address() string {
	return c.address
}

func init() {
	transport.RegisterTransport("grpc", func(opts map[string]interface{}) (transport.TransportManager, error) {
		// Convert generic options map to GRPCTransportOptions
		options := DefaultGRPCTransportOptions()

		if addr, ok := opts["listen_addr"].(string); ok {
			options.ListenAddr = addr
		}

		if timeout, ok := opts["connection_timeout"].(time.Duration); ok {
			options.ConnectionTimeout = timeout
		}

		if timeout, ok := opts["dial_timeout"].(time.Duration); ok {
			options.DialTimeout = timeout
		}

		if tlsConfig, ok := opts["tls_config"].(*tls.Config); ok {
			options.TLSConfig = tlsConfig
		}

		return NewGRPCTransportManager(options)
	})
}