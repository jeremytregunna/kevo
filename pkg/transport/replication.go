package transport

import (
	"context"
	"time"
	
	"github.com/KevoDB/kevo/pkg/wal"
)

// Standard constants for replication message types
const (
	// Request types
	TypeReplicaRegister      = "replica_register"
	TypeReplicaHeartbeat     = "replica_heartbeat"
	TypeReplicaWALSync       = "replica_wal_sync"
	TypeReplicaBootstrap     = "replica_bootstrap"
	TypeReplicaStatus        = "replica_status"
	
	// Response types
	TypeReplicaACK           = "replica_ack"
	TypeReplicaWALEntries    = "replica_wal_entries"
	TypeReplicaBootstrapData = "replica_bootstrap_data"
	TypeReplicaStatusData    = "replica_status_data"
)

// ReplicaRole defines the role of a node in replication
type ReplicaRole string

// Replica roles
const (
	RolePrimary  ReplicaRole = "primary"
	RoleReplica  ReplicaRole = "replica"
	RoleReadOnly ReplicaRole = "readonly"
)

// ReplicaStatus defines the current status of a replica
type ReplicaStatus string

// Replica statuses
const (
	StatusConnecting   ReplicaStatus = "connecting"
	StatusSyncing      ReplicaStatus = "syncing"
	StatusBootstrapping ReplicaStatus = "bootstrapping"
	StatusReady        ReplicaStatus = "ready"
	StatusDisconnected ReplicaStatus = "disconnected"
	StatusError        ReplicaStatus = "error"
)

// ReplicaInfo contains information about a replica
type ReplicaInfo struct {
	ID            string
	Address       string
	Role          ReplicaRole
	Status        ReplicaStatus
	LastSeen      time.Time
	CurrentLSN    uint64  // Lamport Sequence Number
	ReplicationLag time.Duration
	Error         error
}

// ReplicationStreamDirection defines the direction of a replication stream
type ReplicationStreamDirection int

const (
	DirectionPrimaryToReplica ReplicationStreamDirection = iota
	DirectionReplicaToPrimary
	DirectionBidirectional
)

// ReplicationConnection provides methods specific to replication connections
type ReplicationConnection interface {
	Connection

	// GetReplicaInfo returns information about the remote replica
	GetReplicaInfo() (*ReplicaInfo, error)
	
	// SendWALEntries sends WAL entries to the replica
	SendWALEntries(ctx context.Context, entries []*wal.Entry) error
	
	// ReceiveWALEntries receives WAL entries from the replica
	ReceiveWALEntries(ctx context.Context) ([]*wal.Entry, error)
	
	// StartReplicationStream starts a stream for WAL entries
	StartReplicationStream(ctx context.Context, direction ReplicationStreamDirection) (ReplicationStream, error)
}

// ReplicationStream provides a bidirectional stream of WAL entries
type ReplicationStream interface {
	// SendEntries sends WAL entries through the stream
	SendEntries(entries []*wal.Entry) error
	
	// ReceiveEntries receives WAL entries from the stream
	ReceiveEntries() ([]*wal.Entry, error)
	
	// Close closes the replication stream
	Close() error
	
	// SetHighWatermark updates the highest applied Lamport sequence number
	SetHighWatermark(lsn uint64) error
	
	// GetHighWatermark returns the highest applied Lamport sequence number
	GetHighWatermark() (uint64, error)
}

// ReplicationClient extends the Client interface with replication-specific methods
type ReplicationClient interface {
	Client
	
	// RegisterAsReplica registers this client as a replica with the primary
	RegisterAsReplica(ctx context.Context, replicaID string) error
	
	// SendHeartbeat sends a heartbeat to the primary
	SendHeartbeat(ctx context.Context, status *ReplicaInfo) error
	
	// RequestWALEntries requests WAL entries from the primary starting from a specific LSN
	RequestWALEntries(ctx context.Context, fromLSN uint64) ([]*wal.Entry, error)
	
	// RequestBootstrap requests a snapshot for bootstrap purposes
	RequestBootstrap(ctx context.Context) (BootstrapIterator, error)
}

// ReplicationServer extends the Server interface with replication-specific methods
type ReplicationServer interface {
	Server
	
	// RegisterReplica registers a new replica
	RegisterReplica(replicaInfo *ReplicaInfo) error
	
	// UpdateReplicaStatus updates the status of a replica
	UpdateReplicaStatus(replicaID string, status ReplicaStatus, lsn uint64) error
	
	// GetReplicaInfo returns information about a specific replica
	GetReplicaInfo(replicaID string) (*ReplicaInfo, error)
	
	// ListReplicas returns information about all connected replicas
	ListReplicas() ([]*ReplicaInfo, error)
	
	// StreamWALEntriesToReplica streams WAL entries to a specific replica
	StreamWALEntriesToReplica(ctx context.Context, replicaID string, fromLSN uint64) error
}

// BootstrapIterator provides an iterator over key-value pairs for bootstrapping a replica
type BootstrapIterator interface {
	// Next returns the next key-value pair
	Next() (key []byte, value []byte, err error)
	
	// Close closes the iterator
	Close() error
	
	// Progress returns the progress of the bootstrap operation (0.0-1.0)
	Progress() float64
}

// ReplicationRequestHandler processes replication-specific requests
type ReplicationRequestHandler interface {
	// HandleReplicaRegister handles replica registration requests
	HandleReplicaRegister(ctx context.Context, replicaID string, address string) error
	
	// HandleReplicaHeartbeat handles heartbeat requests
	HandleReplicaHeartbeat(ctx context.Context, status *ReplicaInfo) error
	
	// HandleWALRequest handles requests for WAL entries
	HandleWALRequest(ctx context.Context, fromLSN uint64) ([]*wal.Entry, error)
	
	// HandleBootstrapRequest handles bootstrap requests
	HandleBootstrapRequest(ctx context.Context) (BootstrapIterator, error)
}

// ReplicationClientFactory creates a new replication client
type ReplicationClientFactory func(endpoint string, options TransportOptions) (ReplicationClient, error)

// ReplicationServerFactory creates a new replication server
type ReplicationServerFactory func(address string, options TransportOptions) (ReplicationServer, error)

// RegisterReplicationClient registers a replication client implementation
func RegisterReplicationClient(name string, factory ReplicationClientFactory) {
	// This would be implemented to register with the transport registry
}

// RegisterReplicationServer registers a replication server implementation
func RegisterReplicationServer(name string, factory ReplicationServerFactory) {
	// This would be implemented to register with the transport registry
}