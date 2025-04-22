package transport

import (
	"context"
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

// Constants for default timeout values
const (
	defaultDialTimeout     = 5 * time.Second
	defaultConnectTimeout  = 5 * time.Second
	defaultKeepAliveTime   = 15 * time.Second
	defaultKeepAlivePolicy = 5 * time.Second
	defaultMaxConnIdle     = 60 * time.Second
	defaultMaxConnAge      = 5 * time.Minute
)

// GRPCTransportManager manages gRPC connections
type GRPCTransportManager struct {
	opts        *GRPCTransportOptions
	server      *grpc.Server
	listener    net.Listener
	connections sync.Map // map[string]*grpc.ClientConn
	mu          sync.RWMutex
	metrics     *transport.ExtendedMetricsCollector
}

// Ensure GRPCTransportManager implements TransportManager
var _ transport.TransportManager = (*GRPCTransportManager)(nil)

// DefaultGRPCTransportOptions returns default transport options
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

// NewGRPCTransportManager creates a new gRPC transport manager
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

// Start starts the gRPC server
// Serve starts the server and blocks until it's stopped
func (g *GRPCTransportManager) Serve() error {
	ctx := context.Background()
	if err := g.Start(); err != nil {
		return err
	}
	
	// Block until server is stopped
	<-ctx.Done()
	return nil
}

// Start starts the server and returns immediately
func (g *GRPCTransportManager) Start() error {
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
			fmt.Printf("gRPC server stopped: %v\n", err)
		}
	}()

	return nil
}

// Stop stops the gRPC server
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

// Connect creates a connection to the specified address
func (g *GRPCTransportManager) Connect(ctx context.Context, address string) (transport.Connection, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Check if we already have a connection to this address
	if conn, ok := g.connections.Load(address); ok {
		return &GRPCConnection{
			conn:     conn.(*grpc.ClientConn),
			address:  address,
			metrics:  g.metrics,
			lastUsed: time.Now(),
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

	// Connect with timeout
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
		conn:     conn,
		address:  address,
		metrics:  g.metrics,
		lastUsed: time.Now(),
	}, nil
}

// SetRequestHandler sets the request handler for the server
func (g *GRPCTransportManager) SetRequestHandler(handler transport.RequestHandler) {
	// This would be implemented in a real server
}

// RegisterService registers a service with the gRPC server
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

// Register the transport with the registry
func init() {
	transport.RegisterServerTransport("grpc", func(address string, options transport.TransportOptions) (transport.Server, error) {
		// Convert the generic options to our specific options
		grpcOpts := &GRPCTransportOptions{
			ListenAddr:        address,
			TLSConfig:         nil, // We'll set this up if TLS is enabled
			ConnectionTimeout: options.Timeout,
			DialTimeout:       options.Timeout,
			KeepAliveTime:     defaultKeepAliveTime,
			KeepAliveTimeout:  defaultKeepAlivePolicy,
			MaxConnectionIdle: defaultMaxConnIdle,
			MaxConnectionAge:  defaultMaxConnAge,
		}

		// Set up TLS if enabled
		if options.TLSEnabled && options.CertFile != "" && options.KeyFile != "" {
			tlsConfig, err := LoadServerTLSConfig(options.CertFile, options.KeyFile, options.CAFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load TLS config: %w", err)
			}
			grpcOpts.TLSConfig = tlsConfig
		}

		return NewGRPCTransportManager(grpcOpts)
	})

	transport.RegisterClientTransport("grpc", func(endpoint string, options transport.TransportOptions) (transport.Client, error) {
		return NewGRPCClient(endpoint, options)
	})
}