package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	pb "github.com/jeremytregunna/kevo/proto/kevo"
	"github.com/jeremytregunna/kevo/pkg/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// GRPCServer implements the transport.Server interface for gRPC
type GRPCServer struct {
	address       string
	tlsConfig     *tls.Config
	server        *grpc.Server
	requestHandler transport.RequestHandler
	started       bool
	mu            sync.Mutex
	metrics       *transport.ExtendedMetricsCollector
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(address string, options transport.TransportOptions) (transport.Server, error) {
	// Create server options
	var serverOpts []grpc.ServerOption
	
	// Configure TLS if enabled
	if options.TLSEnabled {
		tlsConfig, err := LoadServerTLSConfig(options.CertFile, options.KeyFile, options.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS config: %w", err)
		}
		
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	
	// Configure keepalive parameters
	kaProps := keepalive.ServerParameters{
		MaxConnectionIdle: 30 * time.Minute,
		MaxConnectionAge:  5 * time.Minute,
		Time:              15 * time.Second,
		Timeout:           5 * time.Second,
	}
	
	kaPolicy := keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}
	
	serverOpts = append(serverOpts,
		grpc.KeepaliveParams(kaProps),
		grpc.KeepaliveEnforcementPolicy(kaPolicy),
	)
	
	// Create the server
	server := grpc.NewServer(serverOpts...)
	
	return &GRPCServer{
		address: address,
		server:  server,
		metrics: transport.NewMetrics("grpc"),
	}, nil
}

// Start starts the server and returns immediately
func (s *GRPCServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.started {
		return fmt.Errorf("server already started")
	}
	
	// Start the server in a goroutine
	go func() {
		if err := s.Serve(); err != nil {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()
	
	s.started = true
	return nil
}

// Serve starts the server and blocks until it's stopped
func (s *GRPCServer) Serve() error {
	if s.requestHandler == nil {
		return fmt.Errorf("no request handler set")
	}
	
	// Create the service implementation
	service := &kevoServiceServer{
		handler: s.requestHandler,
	}
	
	// Register the service
	pb.RegisterKevoServiceServer(s.server, service)
	
	// Start listening
	listener, err := transport.CreateListener("tcp", s.address, s.tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}
	
	s.metrics.ServerStarted()
	
	// Serve requests
	err = s.server.Serve(listener)
	
	if err != nil {
		s.metrics.ServerErrored()
		return fmt.Errorf("failed to serve: %w", err)
	}
	
	s.metrics.ServerStopped()
	return nil
}

// Stop stops the server gracefully
func (s *GRPCServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if !s.started {
		return nil
	}
	
	s.server.GracefulStop()
	s.started = false
	
	return nil
}

// SetRequestHandler sets the handler for incoming requests
func (s *GRPCServer) SetRequestHandler(handler transport.RequestHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.requestHandler = handler
}

// kevoServiceServer implements the KevoService gRPC service
type kevoServiceServer struct {
	pb.UnimplementedKevoServiceServer
	handler transport.RequestHandler
}

// TODO: Implement service methods