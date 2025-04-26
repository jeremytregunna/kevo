package transport

import (
	"context"
	"fmt"
	"sync"

	"github.com/KevoDB/kevo/pkg/grpc/service"
	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/transport"
)

// ReplicationGRPCServer implements the ReplicationServer interface using gRPC
type ReplicationGRPCServer struct {
	transportManager *GRPCTransportManager
	replicationService *service.ReplicationServiceServer
	options         transport.TransportOptions
	replicas        map[string]*transport.ReplicaInfo
	mu              sync.RWMutex
}

// NewReplicationGRPCServer creates a new ReplicationGRPCServer
func NewReplicationGRPCServer(
	transportManager *GRPCTransportManager,
	replicator *replication.WALReplicator,
	applier *replication.WALApplier,
	serializer *replication.EntrySerializer,
	storageSnapshot replication.StorageSnapshot,
	options transport.TransportOptions,
) (*ReplicationGRPCServer, error) {
	// Create replication service
	replicationService := service.NewReplicationService(
		replicator,
		applier,
		serializer,
		storageSnapshot,
	)

	return &ReplicationGRPCServer{
		transportManager:    transportManager,
		replicationService:  replicationService,
		options:             options,
		replicas:            make(map[string]*transport.ReplicaInfo),
	}, nil
}

// Start starts the server and returns immediately
func (s *ReplicationGRPCServer) Start() error {
	// Register the replication service with the transport manager
	if err := s.transportManager.RegisterService(s.replicationService); err != nil {
		return fmt.Errorf("failed to register replication service: %w", err)
	}

	// Start the transport manager if it's not already started
	if err := s.transportManager.Start(); err != nil {
		return fmt.Errorf("failed to start transport manager: %w", err)
	}

	return nil
}

// Serve starts the server and blocks until it's stopped
func (s *ReplicationGRPCServer) Serve() error {
	if err := s.Start(); err != nil {
		return err
	}

	// This will block until the context is cancelled
	<-context.Background().Done()
	return nil
}

// Stop stops the server gracefully
func (s *ReplicationGRPCServer) Stop(ctx context.Context) error {
	return s.transportManager.Stop(ctx)
}

// SetRequestHandler sets the handler for incoming requests
// Not used for the replication server as it uses a dedicated service
func (s *ReplicationGRPCServer) SetRequestHandler(handler transport.RequestHandler) {
	// No-op for replication server
}

// RegisterReplica registers a new replica
func (s *ReplicationGRPCServer) RegisterReplica(replicaInfo *transport.ReplicaInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.replicas[replicaInfo.ID] = replicaInfo
	return nil
}

// UpdateReplicaStatus updates the status of a replica
func (s *ReplicationGRPCServer) UpdateReplicaStatus(replicaID string, status transport.ReplicaStatus, lsn uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	replica, exists := s.replicas[replicaID]
	if !exists {
		return fmt.Errorf("replica not found: %s", replicaID)
	}
	
	replica.Status = status
	replica.CurrentLSN = lsn
	return nil
}

// GetReplicaInfo returns information about a specific replica
func (s *ReplicationGRPCServer) GetReplicaInfo(replicaID string) (*transport.ReplicaInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	replica, exists := s.replicas[replicaID]
	if !exists {
		return nil, fmt.Errorf("replica not found: %s", replicaID)
	}
	
	return replica, nil
}

// ListReplicas returns information about all connected replicas
func (s *ReplicationGRPCServer) ListReplicas() ([]*transport.ReplicaInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	result := make([]*transport.ReplicaInfo, 0, len(s.replicas))
	for _, replica := range s.replicas {
		result = append(result, replica)
	}
	
	return result, nil
}

// StreamWALEntriesToReplica streams WAL entries to a specific replica
func (s *ReplicationGRPCServer) StreamWALEntriesToReplica(ctx context.Context, replicaID string, fromLSN uint64) error {
	// This is handled by the gRPC service directly
	return nil
}

// RegisterReplicationTransport registers the gRPC replication transport with the registry
func init() {
	// Register replication server factory
	transport.RegisterReplicationServerTransport("grpc", func(address string, options transport.TransportOptions) (transport.ReplicationServer, error) {
		// Create gRPC transport manager
		grpcOptions := &GRPCTransportOptions{
			ListenAddr:        address,
			ConnectionTimeout: options.Timeout,
			DialTimeout:       options.Timeout,
		}
		
		// Add TLS configuration if enabled
		if options.TLSEnabled {
			tlsConfig, err := LoadServerTLSConfig(options.CertFile, options.KeyFile, options.CAFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load TLS config: %w", err)
			}
			grpcOptions.TLSConfig = tlsConfig
		}
		
		// Create transport manager
		manager, err := NewGRPCTransportManager(grpcOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to create gRPC transport manager: %w", err)
		}
		
		// For registration, we return a placeholder that will be properly initialized
		// by the caller with the required components
		return &ReplicationGRPCServer{
			transportManager: manager,
			options:          options,
			replicas:         make(map[string]*transport.ReplicaInfo),
		}, nil
	})
	
	// Register replication client factory
	transport.RegisterReplicationClientTransport("grpc", func(endpoint string, options transport.TransportOptions) (transport.ReplicationClient, error) {
		// For registration, we return a placeholder that will be properly initialized
		// by the caller with the required components
		return &ReplicationGRPCClient{
			endpoint: endpoint,
			options:  options,
		}, nil
	})
}

// WithReplicator adds a replicator to the replication server
func (s *ReplicationGRPCServer) WithReplicator(
	replicator *replication.WALReplicator,
	applier *replication.WALApplier,
	serializer *replication.EntrySerializer,
	storageSnapshot replication.StorageSnapshot,
) *ReplicationGRPCServer {
	s.replicationService = service.NewReplicationService(
		replicator,
		applier,
		serializer,
		storageSnapshot,
	)
	return s
}