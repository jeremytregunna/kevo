package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/KevoDB/kevo/pkg/engine"
	grpcservice "github.com/KevoDB/kevo/pkg/grpc/service"
	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transaction"
	pb "github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// Server represents the Kevo server
type Server struct {
	eng                *engine.EngineFacade
	txRegistry         transaction.Registry
	listener           net.Listener
	grpcServer         *grpc.Server
	kevoService        *grpcservice.KevoServiceServer
	config             Config
	replicationManager *replication.Manager
}

// NewServer creates a new server instance
func NewServer(eng *engine.EngineFacade, config Config) *Server {
	// Create a transaction registry directly from the transaction package
	// The transaction registry can work with any type that implements BeginTransaction
	txRegistry := transaction.NewRegistry()
	
	return &Server{
		eng:        eng,
		txRegistry: txRegistry,
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
	var tlsConfig *tls.Config
	if s.config.TLSEnabled {
		tlsConfig = &tls.Config{
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
		Time:                  30 * time.Second, // Send pings every 30 seconds if there is no activity
		Timeout:               10 * time.Second, // Wait 10 seconds for ping ack before assuming connection is dead
	}

	kaPolicy := keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second, // Minimum time a client should wait between pings
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	serverOpts = append(serverOpts,
		grpc.KeepaliveParams(kaProps),
		grpc.KeepaliveEnforcementPolicy(kaPolicy),
	)

	// Create gRPC server with options
	s.grpcServer = grpc.NewServer(serverOpts...)

	// Initialize replication if enabled
	if s.config.ReplicationEnabled {
		// Create replication manager config
		replicationConfig := &replication.ManagerConfig{
			Enabled:       true,
			Mode:          s.config.ReplicationMode,
			PrimaryAddr:   s.config.PrimaryAddr,
			ListenAddr:    s.config.ReplicationAddr,
			TLSConfig:     tlsConfig,
			ForceReadOnly: true,
		}

		// Create the replication manager
		s.replicationManager, err = replication.NewManager(s.eng, replicationConfig)
		if err != nil {
			return fmt.Errorf("failed to create replication manager: %w", err)
		}

		// Start the replication service
		if err := s.replicationManager.Start(); err != nil {
			return fmt.Errorf("failed to start replication: %w", err)
		}

		fmt.Printf("Replication started in %s mode\n", s.config.ReplicationMode)

		// If in replica mode, the engine should now be read-only
		if s.config.ReplicationMode == "replica" {
			fmt.Println("Running as replica: database is in read-only mode")
		}
	}

	// Create and register the Kevo service implementation
	// Only pass replicationManager if it's properly initialized
	var repManager grpcservice.ReplicationInfoProvider
	if s.replicationManager != nil && s.config.ReplicationEnabled {
		fmt.Printf("DEBUG: Using replication manager for role %s\n", s.config.ReplicationMode)
		repManager = s.replicationManager
	} else {
		fmt.Printf("DEBUG: No replication manager available. ReplicationEnabled: %v, Manager nil: %v\n",
			s.config.ReplicationEnabled, s.replicationManager == nil)
	}

	s.kevoService = grpcservice.NewKevoServiceServer(s.eng, s.txRegistry, repManager)
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
	// First, stop the replication manager if it exists
	if s.replicationManager != nil {
		fmt.Println("Stopping replication manager...")
		if err := s.replicationManager.Stop(); err != nil {
			fmt.Printf("Warning: Failed to stop replication manager: %v\n", err)
		} else {
			fmt.Println("Replication manager stopped")
		}
	}

	// Next, gracefully stop the gRPC server if it exists
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
