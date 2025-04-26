package transport

import (
	"context"
	"math"
	"time"

	"github.com/KevoDB/kevo/pkg/transport"
)

// reconnectLoop continuously attempts to reconnect the client
func (c *ReplicationGRPCClient) reconnectLoop(initialDelay time.Duration) {
	// If we're shutting down, don't attempt to reconnect
	if c.shuttingDown {
		return
	}

	// Start with initial delay
	delay := initialDelay

	// Reset reconnect attempt counter on first try
	c.reconnectAttempt = 0

	for {
		// Check if we're shutting down
		if c.shuttingDown {
			return
		}

		// Wait for the delay
		time.Sleep(delay)

		// Attempt to reconnect
		c.reconnectAttempt++
		maxAttempts := c.options.RetryPolicy.MaxRetries

		c.logger.Info("Attempting to reconnect (%d/%d)", c.reconnectAttempt, maxAttempts)

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), c.options.Timeout)

		// Attempt connection
		err := c.Connect(ctx)
		cancel()

		if err == nil {
			// Connection successful
			c.logger.Info("Successfully reconnected after %d attempts", c.reconnectAttempt)

			// Reset circuit breaker
			c.circuitBreaker.Reset()

			// Register with primary if we have a replica ID
			if c.replicaID != "" {
				ctx, cancel := context.WithTimeout(context.Background(), c.options.Timeout)
				defer cancel()

				err := c.RegisterAsReplica(ctx, c.replicaID)
				if err != nil {
					c.logger.Error("Failed to re-register as replica: %v", err)
				} else {
					c.logger.Info("Successfully re-registered as replica %s", c.replicaID)
				}
			}

			return
		}

		// Log the reconnection failure
		c.logger.Error("Failed to reconnect (attempt %d/%d): %v",
			c.reconnectAttempt, maxAttempts, err)

		// Check if we've exceeded the maximum number of reconnection attempts
		if maxAttempts > 0 && c.reconnectAttempt >= maxAttempts {
			c.logger.Error("Maximum reconnection attempts (%d) exceeded", maxAttempts)
			// Trip the circuit breaker to prevent further attempts for a while
			c.circuitBreaker.Trip()
			return
		}

		// Increase delay for next attempt (with jitter)
		delay = calculateBackoff(c.reconnectAttempt, c.options.RetryPolicy)
	}
}

// calculateBackoff calculates the backoff duration for the next reconnection attempt
func calculateBackoff(attempt int, policy transport.RetryPolicy) time.Duration {
	// Calculate base backoff using exponential formula
	backoff := float64(policy.InitialBackoff) *
		math.Pow(2, float64(attempt-1)) // 2^(attempt-1)

	// Apply backoff factor if specified
	if policy.BackoffFactor > 0 {
		backoff *= policy.BackoffFactor
	}

	// Apply jitter if specified
	if policy.Jitter > 0 {
		jitter := 1.0 - policy.Jitter/2 + policy.Jitter*float64(time.Now().UnixNano()%1000)/1000.0
		backoff *= jitter
	}

	// Cap at max backoff
	if policy.MaxBackoff > 0 && time.Duration(backoff) > policy.MaxBackoff {
		return policy.MaxBackoff
	}

	return time.Duration(backoff)
}

// maybeReconnect checks if the connection is alive, and starts a reconnection
// loop if it's not
func (c *ReplicationGRPCClient) maybeReconnect() {
	// Check if we're connected
	if c.IsConnected() {
		return
	}

	// Check if the circuit breaker is open
	if c.circuitBreaker.IsOpen() {
		c.logger.Warn("Circuit breaker is open, not attempting to reconnect")
		return
	}

	// Start reconnection loop in a new goroutine
	go c.reconnectLoop(c.options.RetryPolicy.InitialBackoff)
}

// handleConnectionError processes a connection error and triggers reconnection if needed
func (c *ReplicationGRPCClient) handleConnectionError(err error) error {
	if err == nil {
		return nil
	}

	// Update status
	c.mu.Lock()
	c.status.LastError = err
	wasConnected := c.status.Connected
	c.status.Connected = false
	c.mu.Unlock()

	// Log the error
	c.logger.Error("Connection error: %v", err)

	// Check if we should attempt to reconnect
	if wasConnected && !c.shuttingDown {
		c.logger.Info("Connection lost, attempting to reconnect")
		go c.reconnectLoop(c.options.RetryPolicy.InitialBackoff)
	}

	return err
}
