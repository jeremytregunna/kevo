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
	"github.com/KevoDB/kevo/pkg/wal"
	proto "github.com/KevoDB/kevo/proto/kevo/replication"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// ManagerConfig defines the configuration for the replication manager
type ManagerConfig struct {
	// Whether replication is enabled
	Enabled bool

	// The replication mode: ReplicationModePrimary, ReplicationModeReplica, or
	// ReplicationModeStandalone
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

// Manager using EngineApplier from engine_applier.go for WAL entry application

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
	case ReplicationModePrimary:
		return m.startPrimary()
	case ReplicationModeReplica:
		return m.startReplica()
	case ReplicationModeStandalone:
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

// getPrimaryStatus returns detailed status information for a primary node
func (m *Manager) getPrimaryStatus(status map[string]interface{}) map[string]interface{} {
	if m.primary == nil {
		return status
	}

	status["listen_address"] = m.config.ListenAddr
	
	// Get detailed primary status
	m.primary.mu.RLock()
	defer m.primary.mu.RUnlock()
	
	// Add information about connected replicas
	replicaCount := len(m.primary.sessions)
	activeReplicas := 0
	connectedReplicas := 0
	
	// Create a replicas list with detailed information
	replicas := make([]map[string]interface{}, 0, replicaCount)
	
	for id, session := range m.primary.sessions {
		// Track active and connected counts
		if session.Connected {
			connectedReplicas++
		}
		if session.Active && session.Connected {
			activeReplicas++
		}
		
		// Create detailed replica info
		replicaInfo := map[string]interface{}{
			"id":                id,
			"connected":         session.Connected,
			"active":            session.Active,
			"last_activity":     session.LastActivity.UnixNano() / int64(time.Millisecond),
			"last_ack_sequence": session.LastAckSequence,
			"listener_address":  session.ListenerAddress,
			"start_sequence":    session.StartSequence,
			"idle_time_seconds": time.Since(session.LastActivity).Seconds(),
		}
		
		replicas = append(replicas, replicaInfo)
	}
	
	// Get WAL sequence information
	currentWalSeq := uint64(0)
	if m.primary.wal != nil {
		currentWalSeq = m.primary.wal.GetNextSequence() - 1 // Last used sequence
	}
	
	// Add primary-specific information to status
	status["replica_count"] = replicaCount
	status["connected_replica_count"] = connectedReplicas
	status["active_replica_count"] = activeReplicas
	status["replicas"] = replicas
	status["current_wal_sequence"] = currentWalSeq
	status["last_synced_sequence"] = m.primary.lastSyncedSeq
	status["retention_config"] = map[string]interface{}{
		"max_age_hours":     m.primary.retentionConfig.MaxAgeHours,
		"min_sequence_keep": m.primary.retentionConfig.MinSequenceKeep,
	}
	status["compression_enabled"] = m.primary.enableCompression
	status["default_codec"] = m.primary.defaultCodec.String()
	
	return status
}

// getReplicaStatus returns detailed status information for a replica node
func (m *Manager) getReplicaStatus(status map[string]interface{}) map[string]interface{} {
	if m.replica == nil {
		return status
	}

	// Basic replica information
	status["primary_address"] = m.config.PrimaryAddr
	status["last_applied_sequence"] = m.lastApplied
	
	// Detailed state information
	currentState := m.replica.GetStateString()
	status["state"] = currentState
	
	// Get the state tracker for more detailed information
	stateTracker := m.replica.stateTracker
	if stateTracker != nil {
		// Add state duration
		stateTime := stateTracker.GetStateDuration()
		status["state_duration_seconds"] = stateTime.Seconds()
		
		// Add error information if in error state
		if currentState == "ERROR" {
			if err := stateTracker.GetError(); err != nil {
				status["last_error"] = err.Error()
			}
		}
		
		// Get state transitions
		transitions := stateTracker.GetTransitions()
		if len(transitions) > 0 {
			stateHistory := make([]map[string]interface{}, 0, len(transitions))
			
			for _, t := range transitions {
				stateHistory = append(stateHistory, map[string]interface{}{
					"from":      t.From.String(),
					"to":        t.To.String(),
					"timestamp": t.Timestamp.UnixNano() / int64(time.Millisecond),
				})
			}
			
			// Only include the last 10 transitions to keep the response size reasonable
			if len(stateHistory) > 10 {
				stateHistory = stateHistory[len(stateHistory)-10:]
			}
			
			status["state_history"] = stateHistory
		}
	}
	
	// Add connection information
	if m.replica.conn != nil {
		status["connection_status"] = "connected"
	} else {
		status["connection_status"] = "disconnected"
	}
	
	// Add replication listener information
	status["replication_listener_address"] = m.config.ListenAddr
	
	// Include statistics
	if m.replica.stats != nil {
		status["entries_received"] = m.replica.stats.GetEntriesReceived()
		status["entries_applied"] = m.replica.stats.GetEntriesApplied()
		status["bytes_received"] = m.replica.stats.GetBytesReceived()
		status["batch_count"] = m.replica.stats.GetBatchCount()
		status["errors"] = m.replica.stats.GetErrorCount()
		
		// Add last batch time information
		lastBatchTime := m.replica.stats.GetLastBatchTime()
		if lastBatchTime > 0 {
			status["last_batch_time"] = lastBatchTime
			status["seconds_since_last_batch"] = m.replica.stats.GetLastBatchTimeDuration().Seconds()
		}
	}
	
	// Add configuration information
	status["force_read_only"] = m.config.ForceReadOnly
	
	return status
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
	case ReplicationModePrimary:
		status = m.getPrimaryStatus(status)
	case ReplicationModeReplica:
		status = m.getReplicaStatus(status)
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
			Time:    30 * time.Second, // Send pings every 30 seconds if there is no activity
			Timeout: 10 * time.Second,  // Wait 10 seconds for ping ack before assuming connection is dead
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second, // Minimum time a client should wait between pings
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
	replicaConfig.ReplicationListenerAddr = m.config.ListenAddr // Set replica's own listener address
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
// This only affects client operations, not internal replication operations
func (m *Manager) setEngineReadOnly(readOnly bool) error {
	// Try to access the SetReadOnly method if available
	// This would be engine-specific and may require interface enhancement
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
