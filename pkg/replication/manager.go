// Package replication implements the primary-replica replication protocol for the Kevo database.
package replication

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/common/log"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	proto "github.com/KevoDB/kevo/pkg/replication/proto"
	"github.com/KevoDB/kevo/pkg/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// ManagerConfig defines the configuration for the replication manager
type ManagerConfig struct {
	// Whether replication is enabled
	Enabled bool

	// The replication mode: "primary", "replica", or "standalone"
	Mode string

	// Address of the primary node (for replicas)
	PrimaryAddr string

	// Address to listen on (for primaries)
	ListenAddr string

	// Configuration for primary node
	PrimaryConfig *PrimaryConfig

	// Configuration for replica node
	ReplicaConfig *ReplicaConfig

	// TLS configuration
	TLSConfig *tls.Config

	// Read-only mode enforcement for replicas
	ForceReadOnly bool
}

// DefaultManagerConfig returns a default configuration for the replication manager
func DefaultManagerConfig() *ManagerConfig {
	return &ManagerConfig{
		Enabled:       false,
		Mode:          "standalone",
		PrimaryAddr:   "localhost:50052",
		ListenAddr:    ":50052",
		PrimaryConfig: DefaultPrimaryConfig(),
		ReplicaConfig: DefaultReplicaConfig(),
		ForceReadOnly: true,
	}
}

// Manager handles the setup and management of replication
type Manager struct {
	config        *ManagerConfig
	engine        interfaces.Engine
	primary       *Primary
	replica       *Replica
	grpcServer    *grpc.Server
	serviceStatus bool
	walApplier    *EngineApplier
	lastApplied   uint64
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
}

// EngineApplier implements the WALEntryApplier interface for applying WAL entries to an engine
type EngineApplier struct {
	engine interfaces.Engine
}

// NewEngineApplier creates a new engine applier
func NewEngineApplier(engine interfaces.Engine) *EngineApplier {
	return &EngineApplier{engine: engine}
}

// Apply applies a WAL entry to the engine
func (e *EngineApplier) Apply(entry *wal.Entry) error {
	switch entry.Type {
	case wal.OpTypePut:
		return e.engine.Put(entry.Key, entry.Value)
	case wal.OpTypeDelete:
		return e.engine.Delete(entry.Key)
	case wal.OpTypeBatch:
		return e.engine.ApplyBatch([]*wal.Entry{entry})
	default:
		return fmt.Errorf("unsupported WAL entry type: %d", entry.Type)
	}
}

// Sync ensures all applied entries are persisted
func (e *EngineApplier) Sync() error {
	// Force a flush of in-memory tables to ensure durability
	return e.engine.FlushImMemTables()
}

// NewManager creates a new replication manager
func NewManager(engine interfaces.Engine, config *ManagerConfig) (*Manager, error) {
	if config == nil {
		config = DefaultManagerConfig()
	}

	if !config.Enabled {
		return &Manager{
			config:        config,
			engine:        engine,
			serviceStatus: false,
		}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		config:        config,
		engine:        engine,
		serviceStatus: false,
		walApplier:    NewEngineApplier(engine),
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

// Start initializes and starts the replication service
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.config.Enabled {
		log.Info("Replication not enabled, skipping initialization")
		return nil
	}

	log.Info("Starting replication in %s mode", m.config.Mode)

	switch m.config.Mode {
	case "primary":
		return m.startPrimary()
	case "replica":
		return m.startReplica()
	case "standalone":
		log.Info("Running in standalone mode (no replication)")
		return nil
	default:
		return fmt.Errorf("invalid replication mode: %s", m.config.Mode)
	}
}

// Stop halts the replication service
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.serviceStatus {
		return nil
	}

	// Cancel the context to signal shutdown to all goroutines
	if m.cancel != nil {
		m.cancel()
	}

	// Shut down gRPC server
	if m.grpcServer != nil {
		m.grpcServer.GracefulStop()
		m.grpcServer = nil
	}

	// Stop the replica
	if m.replica != nil {
		if err := m.replica.Stop(); err != nil {
			log.Error("Error stopping replica: %v", err)
		}
		m.replica = nil
	}

	// Close the primary
	if m.primary != nil {
		if err := m.primary.Close(); err != nil {
			log.Error("Error closing primary: %v", err)
		}
		m.primary = nil
	}

	m.serviceStatus = false
	log.Info("Replication service stopped")
	return nil
}

// Status returns the current status of the replication service
func (m *Manager) Status() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := map[string]interface{}{
		"enabled": m.config.Enabled,
		"mode":    m.config.Mode,
		"active":  m.serviceStatus,
	}

	// Add mode-specific status
	switch m.config.Mode {
	case "primary":
		if m.primary != nil {
			// Add information about connected replicas, etc.
			status["listen_address"] = m.config.ListenAddr
			// TODO: Add more detailed primary status
		}
	case "replica":
		if m.replica != nil {
			status["primary_address"] = m.config.PrimaryAddr
			status["last_applied_sequence"] = m.lastApplied
			status["state"] = m.replica.GetCurrentState().String()
			// TODO: Add more detailed replica status
		}
	}

	return status
}

// startPrimary initializes the primary node
func (m *Manager) startPrimary() error {
	// Access the WAL from the engine
	// This requires the engine to expose its WAL - might need interface enhancement
	wal, err := m.getWAL()
	if err != nil {
		return fmt.Errorf("failed to access WAL: %w", err)
	}

	// Create primary replication service
	primary, err := NewPrimary(wal, m.config.PrimaryConfig)
	if err != nil {
		return fmt.Errorf("failed to create primary node: %w", err)
	}

	// Configure gRPC server options
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second, // Send pings every 10 seconds if there is no activity
			Timeout: 5 * time.Second,  // Wait 5 seconds for ping ack before assuming connection is dead
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second, // Minimum time a client should wait before sending a ping
			PermitWithoutStream: true,            // Allow pings even when there are no active streams
		}),
		grpc.MaxRecvMsgSize(16 * 1024 * 1024), // 16MB max message size
		grpc.MaxSendMsgSize(16 * 1024 * 1024), // 16MB max message size
	}

	// Add TLS if configured
	if m.config.TLSConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(m.config.TLSConfig)))
	}

	// Create gRPC server
	server := grpc.NewServer(opts...)

	// Register primary service
	proto.RegisterWALReplicationServiceServer(server, primary)

	// Start server in a separate goroutine
	go func() {
		// Start listening
		listener, err := createListener(m.config.ListenAddr)
		if err != nil {
			log.Error("Failed to create listener for primary: %v", err)
			return
		}

		log.Info("Primary node listening on %s", m.config.ListenAddr)
		if err := server.Serve(listener); err != nil {
			log.Error("Primary gRPC server error: %v", err)
		}
	}()

	// Store references
	m.primary = primary
	m.grpcServer = server
	m.serviceStatus = true

	return nil
}

// startReplica initializes the replica node
func (m *Manager) startReplica() error {
	// Check last applied sequence (ideally from persistent storage)
	// For now, we'll start from 0
	lastApplied := uint64(0)

	// Adjust replica config for connection
	replicaConfig := m.config.ReplicaConfig
	if replicaConfig == nil {
		replicaConfig = DefaultReplicaConfig()
	}

	// Configure the connection to the primary
	replicaConfig.Connection.PrimaryAddress = m.config.PrimaryAddr
	replicaConfig.Connection.UseTLS = m.config.TLSConfig != nil

	// Set TLS credentials if configured
	if m.config.TLSConfig != nil {
		replicaConfig.Connection.TLSCredentials = credentials.NewTLS(m.config.TLSConfig)
	} else {
		// Use insecure credentials if TLS is not configured
		replicaConfig.Connection.TLSCredentials = credentials.NewTLS(nil)
	}

	// Create replica instance
	replica, err := NewReplica(lastApplied, m.walApplier, replicaConfig)
	if err != nil {
		return fmt.Errorf("failed to create replica node: %w", err)
	}

	// Start replication
	if err := replica.Start(); err != nil {
		return fmt.Errorf("failed to start replica: %w", err)
	}

	// Set read-only mode on the engine if configured
	if m.config.ForceReadOnly {
		if err := m.setEngineReadOnly(true); err != nil {
			log.Warn("Failed to set engine to read-only mode: %v", err)
		} else {
			log.Info("Engine set to read-only mode (replica)")
		}
	}

	// Store references
	m.replica = replica
	m.lastApplied = lastApplied
	m.serviceStatus = true

	log.Info("Replica connected to primary at %s", m.config.PrimaryAddr)
	return nil
}

// setEngineReadOnly sets the read-only mode on the engine (if supported)
func (m *Manager) setEngineReadOnly(readOnly bool) error {
	// Try to access the SetReadOnly method if available
	// This would be engine-specific and may require interface enhancement
	// For now, we'll assume this is implemented via type assertion
	type readOnlySetter interface {
		SetReadOnly(bool)
	}

	if setter, ok := m.engine.(readOnlySetter); ok {
		setter.SetReadOnly(readOnly)
		return nil
	}

	return fmt.Errorf("engine does not support read-only mode setting")
}

// getWAL retrieves the WAL from the engine
func (m *Manager) getWAL() (*wal.WAL, error) {
	// This would be engine-specific and may require interface enhancement
	// For now, we'll assume this is implemented via type assertion
	type walProvider interface {
		GetWAL() *wal.WAL
	}

	if provider, ok := m.engine.(walProvider); ok {
		wal := provider.GetWAL()
		if wal == nil {
			return nil, fmt.Errorf("engine returned nil WAL")
		}
		return wal, nil
	}

	return nil, fmt.Errorf("engine does not provide WAL access")
}

// createListener creates a network listener for the gRPC server
func createListener(address string) (net.Listener, error) {
	return net.Listen("tcp", address)
}
