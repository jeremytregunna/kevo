package replication

import (
	"context"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/common/log"
	proto "github.com/KevoDB/kevo/pkg/replication/proto"
)

// HeartbeatConfig contains configuration for heartbeat/keepalive.
type HeartbeatConfig struct {
	// Interval between heartbeat checks
	Interval time.Duration
	// Timeout after which a session is considered dead if no activity
	Timeout time.Duration
	// Whether to send periodic empty WALStreamResponse as heartbeats
	SendEmptyResponses bool
}

// DefaultHeartbeatConfig returns the default heartbeat configuration.
func DefaultHeartbeatConfig() *HeartbeatConfig {
	return &HeartbeatConfig{
		Interval:           10 * time.Second,
		Timeout:            30 * time.Second,
		SendEmptyResponses: true,
	}
}

// heartbeatManager handles heartbeat and session monitoring for the primary node.
type heartbeatManager struct {
	config    *HeartbeatConfig
	primary   *Primary
	stopChan  chan struct{}
	waitGroup sync.WaitGroup
	mu        sync.Mutex
	running   bool
}

// newHeartbeatManager creates a new heartbeat manager.
func newHeartbeatManager(primary *Primary, config *HeartbeatConfig) *heartbeatManager {
	if config == nil {
		config = DefaultHeartbeatConfig()
	}

	return &heartbeatManager{
		config:   config,
		primary:  primary,
		stopChan: make(chan struct{}),
	}
}

// start begins the heartbeat monitoring.
func (h *heartbeatManager) start() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.running {
		return
	}

	h.running = true
	h.waitGroup.Add(1)

	go h.monitorLoop()
}

// stop halts the heartbeat monitoring.
func (h *heartbeatManager) stop() {
	h.mu.Lock()
	if !h.running {
		h.mu.Unlock()
		return
	}

	h.running = false
	close(h.stopChan)
	h.mu.Unlock()

	h.waitGroup.Wait()
}

// monitorLoop periodically checks replica sessions for activity and sends heartbeats.
func (h *heartbeatManager) monitorLoop() {
	defer h.waitGroup.Done()

	ticker := time.NewTicker(h.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopChan:
			return
		case <-ticker.C:
			h.checkSessions()
		}
	}
}

// checkSessions verifies activity on all sessions and sends heartbeats as needed.
func (h *heartbeatManager) checkSessions() {
	now := time.Now()
	deadSessions := make([]string, 0)

	// Get a snapshot of current sessions
	h.primary.mu.RLock()
	sessions := make(map[string]*ReplicaSession)
	for id, session := range h.primary.sessions {
		sessions[id] = session
	}
	h.primary.mu.RUnlock()

	for id, session := range sessions {
		// Skip already disconnected sessions
		if !session.Connected || !session.Active {
			continue
		}

		// Check if session has timed out
		session.mu.Lock()
		lastActivity := session.LastActivity
		if now.Sub(lastActivity) > h.config.Timeout {
			log.Warn("Session %s timed out after %.1fs of inactivity",
				id, now.Sub(lastActivity).Seconds())
			session.Connected = false
			session.Active = false
			deadSessions = append(deadSessions, id)
			session.mu.Unlock()
			continue
		}

		// If sending empty responses is enabled, send a heartbeat
		if h.config.SendEmptyResponses && now.Sub(lastActivity) > h.config.Interval {
			// Create empty WALStreamResponse as heartbeat
			heartbeat := &proto.WALStreamResponse{
				Entries:    []*proto.WALEntry{},
				Compressed: false,
				Codec:      proto.CompressionCodec_NONE,
			}

			// Send heartbeat (don't block on lock for too long)
			if err := session.Stream.Send(heartbeat); err != nil {
				log.Error("Failed to send heartbeat to session %s: %v", id, err)
				session.Connected = false
				session.Active = false
				deadSessions = append(deadSessions, id)
			} else {
				session.LastActivity = now
				log.Debug("Sent heartbeat to session %s", id)
			}
		}
		session.mu.Unlock()
	}

	// Clean up dead sessions
	for _, id := range deadSessions {
		h.primary.unregisterReplicaSession(id)
	}
}

// pingSession sends a single heartbeat ping to a specific session
func (h *heartbeatManager) pingSession(sessionID string) bool {
	session := h.primary.getSession(sessionID)
	if session == nil || !session.Connected || !session.Active {
		return false
	}

	// Create empty WALStreamResponse as heartbeat
	heartbeat := &proto.WALStreamResponse{
		Entries:    []*proto.WALEntry{},
		Compressed: false,
		Codec:      proto.CompressionCodec_NONE,
	}

	// Attempt to send a heartbeat
	session.mu.Lock()
	defer session.mu.Unlock()

	if err := session.Stream.Send(heartbeat); err != nil {
		log.Error("Failed to ping session %s: %v", sessionID, err)
		session.Connected = false
		session.Active = false
		return false
	}

	session.LastActivity = time.Now()
	return true
}

// checkSessionActive verifies if a session is active
func (h *heartbeatManager) checkSessionActive(sessionID string) bool {
	session := h.primary.getSession(sessionID)
	if session == nil {
		return false
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	return session.Connected && session.Active &&
		time.Since(session.LastActivity) <= h.config.Timeout
}

// sessionContext returns a context that is canceled when the session becomes inactive
func (h *heartbeatManager) sessionContext(sessionID string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	// Start a goroutine to monitor session and cancel if it becomes inactive
	go func() {
		ticker := time.NewTicker(h.config.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				// Context was canceled elsewhere
				return
			case <-ticker.C:
				// Check if session is still active
				if !h.checkSessionActive(sessionID) {
					cancel()
					return
				}
			}
		}
	}()

	return ctx, cancel
}
