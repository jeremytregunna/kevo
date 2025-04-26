package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	"github.com/KevoDB/kevo/pkg/engine/transaction"
	grpcservice "github.com/KevoDB/kevo/pkg/grpc/service"
	pb "github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// Server represents the Kevo server
type Server struct {
	eng         interfaces.Engine
	txRegistry  interfaces.TxRegistry
	listener    net.Listener
	grpcServer  *grpc.Server
	kevoService *grpcservice.KevoServiceServer
	config      Config
}

// NewServer creates a new server instance
func NewServer(eng interfaces.Engine, config Config) *Server {
	return &Server{
		eng:        eng,
		txRegistry: transaction.NewRegistry(),
		config:     config,
	}
}

// Start initializes and starts the server
func (s *Server) Start() error {
	// Create a listener on the specified address
	var err error
	s.listener, err = net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.ListenAddr, err)
	}

	fmt.Printf("Listening on %s\n", s.config.ListenAddr)

	// Configure gRPC server options
	var serverOpts []grpc.ServerOption

	// Add TLS if configured
	if s.config.TLSEnabled {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		// Load server certificate if provided
		if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(s.config.TLSCertFile, s.config.TLSKeyFile)
			if err != nil {
				return fmt.Errorf("failed to load TLS certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Add credentials to server options
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	// Configure keepalive parameters
	kaProps := keepalive.ServerParameters{
		MaxConnectionIdle:     60 * time.Second,
		MaxConnectionAge:      5 * time.Minute,
		MaxConnectionAgeGrace: 5 * time.Second,
		Time:                  15 * time.Second,
		Timeout:               5 * time.Second,
	}

	kaPolicy := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}

	serverOpts = append(serverOpts,
		grpc.KeepaliveParams(kaProps),
		grpc.KeepaliveEnforcementPolicy(kaPolicy),
	)

	// Create gRPC server with options
	s.grpcServer = grpc.NewServer(serverOpts...)

	// Create and register the Kevo service implementation
	s.kevoService = grpcservice.NewKevoServiceServer(s.eng, s.txRegistry)
	pb.RegisterKevoServiceServer(s.grpcServer, s.kevoService)

	fmt.Println("gRPC server initialized")
	return nil
}

// Serve starts serving requests (blocking)
func (s *Server) Serve() error {
	if s.grpcServer == nil {
		return fmt.Errorf("server not initialized, call Start() first")
	}

	fmt.Println("Starting gRPC server")
	return s.grpcServer.Serve(s.listener)
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	// First, gracefully stop the gRPC server if it exists
	if s.grpcServer != nil {
		fmt.Println("Gracefully stopping gRPC server...")

		// Create a channel to signal when the server has stopped
		stopped := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(stopped)
		}()

		// Wait for graceful stop or context deadline
		select {
		case <-stopped:
			fmt.Println("gRPC server stopped gracefully")
		case <-ctx.Done():
			fmt.Println("Context deadline exceeded, forcing server stop")
			s.grpcServer.Stop()
		}
	}

	// Shut down the listener if it's still open
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}

	// Clean up any active transactions
	if registry, ok := s.txRegistry.(interface {
		GracefulShutdown(context.Context) error
	}); ok {
		if err := registry.GracefulShutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown transaction registry: %w", err)
		}
	}

	return nil
}
