package replication

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

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Set role
	role = m.config.Mode

	// Set primary address
	if role == "replica" {
		primaryAddr = m.config.PrimaryAddr
	} else if role == "primary" {
		primaryAddr = m.config.ListenAddr
	}

	// Set last sequence
	if role == "primary" && m.primary != nil {
		lastSequence = m.primary.GetLastSequence()
	} else if role == "replica" && m.replica != nil {
		lastSequence = m.replica.GetLastAppliedSequence()
	}

	// Gather replica information
	if role == "primary" && m.primary != nil {
		// Get replica sessions from primary
		replicas = m.primary.GetReplicaInfo()
	} else if role == "replica" {
		// Add self as a replica
		replicas = append(replicas, ReplicationNodeInfo{
			Address:      m.config.ListenAddr,
			LastSequence: lastSequence,
			Available:    true,
			Region:       "",
			Meta:         map[string]string{},
		})
	}

	return role, primaryAddr, replicas, lastSequence, m.engine.IsReadOnly()
}
