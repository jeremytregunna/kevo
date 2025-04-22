package transport

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/jeremytregunna/kevo/proto/kevo"
	"github.com/jeremytregunna/kevo/pkg/grpc/service"
	"github.com/jeremytregunna/kevo/pkg/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// GRPCServer implements the transport.Server interface for gRPC
type GRPCServer struct {
	address   string
	options   transport.TransportOptions
	server    *grpc.Server
	listener  net.Listener
	handler   transport.RequestHandler
	metrics   transport.MetricsCollector
	mu        sync.Mutex
	started   bool
	kevoImpl  *service.KevoServiceServer
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(address string, options transport.TransportOptions) (transport.Server, error) {
	return &GRPCServer{
		address: address,
		options: options,
		metrics: transport.NewMetricsCollector(),
	}, nil
}

// Start starts the server and returns immediately
func (s *GRPCServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.started {
		return fmt.Errorf("server already started")
	}
	
	var serverOpts []grpc.ServerOption
	
	// Configure TLS if enabled
	if s.options.TLSEnabled {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		
		// Load server certificate if provided
		if s.options.CertFile != "" && s.options.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(s.options.CertFile, s.options.KeyFile)
			if err != nil {
				return fmt.Errorf("failed to load server certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		
		// Add credentials to server options
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	
	// Configure keepalive parameters
	keepaliveParams := keepalive.ServerParameters{
		MaxConnectionIdle:     60 * time.Second,
		MaxConnectionAge:      5 * time.Minute,
		MaxConnectionAgeGrace: 5 * time.Second,
		Time:                  15 * time.Second,
		Timeout:               5 * time.Second,
	}
	
	keepalivePolicy := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}
	
	serverOpts = append(serverOpts,
		grpc.KeepaliveParams(keepaliveParams),
		grpc.KeepaliveEnforcementPolicy(keepalivePolicy),
	)
	
	// Create gRPC server
	s.server = grpc.NewServer(serverOpts...)
	
	// Create listener
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}
	s.listener = listener
	
	// Set up service implementation
	// Note: This is currently a placeholder. The actual implementation
	// would require initializing the engine and transaction registry
	// with real components. For now, we'll just register the "empty" service.
	pb.RegisterKevoServiceServer(s.server, &placeholderKevoService{})
	
	// Start serving in a goroutine
	go func() {
		if err := s.server.Serve(listener); err != nil {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()
	
	s.started = true
	return nil
}

// Serve starts the server and blocks until it's stopped
func (s *GRPCServer) Serve() error {
	s.mu.Lock()
	
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}
	
	var serverOpts []grpc.ServerOption
	
	// Configure TLS if enabled
	if s.options.TLSEnabled {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		
		// Load server certificate if provided
		if s.options.CertFile != "" && s.options.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(s.options.CertFile, s.options.KeyFile)
			if err != nil {
				s.mu.Unlock()
				return fmt.Errorf("failed to load server certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		
		// Add credentials to server options
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	
	// Configure keepalive parameters
	keepaliveParams := keepalive.ServerParameters{
		MaxConnectionIdle:     60 * time.Second,
		MaxConnectionAge:      5 * time.Minute,
		MaxConnectionAgeGrace: 5 * time.Second,
		Time:                  15 * time.Second,
		Timeout:               5 * time.Second,
	}
	
	keepalivePolicy := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}
	
	serverOpts = append(serverOpts,
		grpc.KeepaliveParams(keepaliveParams),
		grpc.KeepaliveEnforcementPolicy(keepalivePolicy),
	)
	
	// Create gRPC server
	s.server = grpc.NewServer(serverOpts...)
	
	// Create listener
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}
	s.listener = listener
	
	// Set up service implementation
	// Note: This is currently a placeholder. The actual implementation
	// would require initializing the engine and transaction registry
	// with real components. For now, we'll just register the "empty" service.
	pb.RegisterKevoServiceServer(s.server, &placeholderKevoService{})
	
	s.started = true
	s.mu.Unlock()
	
	// This will block until the server is stopped
	return s.server.Serve(listener)
}

// Stop stops the server gracefully
func (s *GRPCServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if !s.started {
		return nil
	}
	
	stopped := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(stopped)
	}()
	
	select {
	case <-stopped:
		// Server stopped gracefully
	case <-ctx.Done():
		// Context deadline exceeded, force stop
		s.server.Stop()
	}
	
	s.started = false
	return nil
}

// SetRequestHandler sets the handler for incoming requests
func (s *GRPCServer) SetRequestHandler(handler transport.RequestHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.handler = handler
}

// placeholderKevoService is a minimal implementation of KevoServiceServer for testing
type placeholderKevoService struct {
	pb.UnimplementedKevoServiceServer
}

// Register server factory with transport registry
func init() {
	transport.RegisterServerTransport("grpc", NewGRPCServer)
}