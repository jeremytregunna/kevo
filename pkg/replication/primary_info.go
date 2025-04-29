package replication

// GetReplicaInfo returns information about all connected replicas
func (p *Primary) GetReplicaInfo() []ReplicationNodeInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var replicas []ReplicationNodeInfo

	// Convert replica sessions to ReplicationNodeInfo
	for _, session := range p.sessions {
		if !session.Connected {
			continue
		}

		replica := ReplicationNodeInfo{
			Address:      session.ListenerAddress, // Use actual listener address
			LastSequence: session.LastAckSequence,
			Available:    session.Active,
			Region:       "",
			Meta:         map[string]string{},
		}

		replicas = append(replicas, replica)
	}

	return replicas
}

// GetLastSequence returns the highest sequence number that has been synced to disk
func (p *Primary) GetLastSequence() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastSyncedSeq
}
