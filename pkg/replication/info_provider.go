package replication

import (
	"fmt"
)

const (
	ReplicationModeStandalone = "standalone"
	ReplicationModePrimary    = "primary"
	ReplicationModeReplica    = "replica"
)

// ReplicationNodeInfo contains information about a node in the replication topology
type ReplicationNodeInfo struct {
	Address      string            // Host:port of the node
	LastSequence uint64            // Last applied sequence number
	Available    bool              // Whether the node is available
	Region       string            // Optional region information
	Meta         map[string]string // Additional metadata
}

// GetNodeInfo exposes replication topology information to the client service
func (m *Manager) GetNodeInfo() (string, string, []ReplicationNodeInfo, uint64, bool) {
	// Return information about the current node and replication topology
	var role string
	var primaryAddr string
	var replicas []ReplicationNodeInfo
	var lastSequence uint64
	var readOnly bool

	// Safety check - the manager itself cannot be nil here (as this is a method on it),
	// but we need to make sure we have valid internal state
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if we have a valid configuration
	if m.config == nil {
		fmt.Printf("DEBUG[GetNodeInfo]: Replication manager has nil config\n")
		// Return safe default values if config is nil
		return "standalone", "", nil, 0, false
	}

	fmt.Printf("DEBUG[GetNodeInfo]: Replication mode: %s, Enabled: %v\n",
		m.config.Mode, m.config.Enabled)

	// Set role
	role = m.config.Mode

	// Set primary address
	if role == ReplicationModeReplica {
		primaryAddr = m.config.PrimaryAddr
	} else if role == ReplicationModePrimary {
		primaryAddr = m.config.ListenAddr
	}

	// Set last sequence
	if role == ReplicationModePrimary && m.primary != nil {
		lastSequence = m.primary.GetLastSequence()
	} else if role == ReplicationModeReplica && m.replica != nil {
		lastSequence = m.replica.GetLastAppliedSequence()
	}

	// Gather replica information
	if role == ReplicationModePrimary && m.primary != nil {
		// Get replica sessions from primary
		replicas = m.primary.GetReplicaInfo()
	} else if role == ReplicationModeReplica {
		// Add self as a replica
		replicas = append(replicas, ReplicationNodeInfo{
			Address:      m.config.ListenAddr,
			LastSequence: lastSequence,
			Available:    true,
			Region:       "",
			Meta:         map[string]string{},
		})
	}

	// Check for a valid engine before calling IsReadOnly
	if m.engine != nil {
		readOnly = m.engine.IsReadOnly()
	}

	return role, primaryAddr, replicas, lastSequence, readOnly
}
