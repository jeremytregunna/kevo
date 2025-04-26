package transport

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/common/log"
	"github.com/KevoDB/kevo/pkg/transport"
)

func TestCalculateBackoff(t *testing.T) {
	policy := transport.RetryPolicy{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		BackoffFactor:  2.0,
		Jitter:         0.0, // Disable jitter for deterministic tests
	}

	tests := []struct {
		name          string
		attempt       int
		expectedRange [2]time.Duration // Min and max expected duration
	}{
		{
			name:          "First attempt",
			attempt:       1,
			expectedRange: [2]time.Duration{100 * time.Millisecond, 200 * time.Millisecond},
		},
		{
			name:          "Second attempt",
			attempt:       2,
			expectedRange: [2]time.Duration{200 * time.Millisecond, 400 * time.Millisecond},
		},
		{
			name:          "Third attempt",
			attempt:       3,
			expectedRange: [2]time.Duration{400 * time.Millisecond, 800 * time.Millisecond},
		},
		{
			name:          "Tenth attempt",
			attempt:       10,
			expectedRange: [2]time.Duration{5 * time.Second, 5 * time.Second}, // Capped at max
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backoff := calculateBackoff(tt.attempt, policy)
			if backoff < tt.expectedRange[0] || backoff > tt.expectedRange[1] {
				t.Errorf("Expected backoff between %v and %v, got %v",
					tt.expectedRange[0], tt.expectedRange[1], backoff)
			}
		})
	}

	// Test with jitter
	t.Run("With jitter", func(t *testing.T) {
		jitterPolicy := transport.RetryPolicy{
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     5 * time.Second,
			BackoffFactor:  2.0,
			Jitter:         0.5, // 50% jitter
		}

		// Run multiple times to check for variation
		var values []time.Duration
		for i := 0; i < 10; i++ {
			values = append(values, calculateBackoff(2, jitterPolicy))
		}

		// Check if we have at least some variation (jitter is working)
		allSame := true
		for i := 1; i < len(values); i++ {
			if values[i] != values[0] {
				allSame = false
				break
			}
		}

		if allSame {
			t.Error("Expected variation with jitter enabled, but all values are the same")
		}
	})
}

// mockClient is a standalone struct for testing that doesn't rely on the reconnectLoop method
type mockClient struct {
	logger          log.Logger
	circuitBreaker  *transport.CircuitBreaker
	status          transport.TransportStatus
	options         transport.TransportOptions
	shuttingDown    bool
	reconnectCalled atomic.Bool
}

// handleConnectionError is a copy of the real implementation but uses reconnectCalled instead
func (c *mockClient) handleConnectionError(err error) error {
	if err == nil {
		return nil
	}

	// Update status
	c.status.LastError = err
	wasConnected := c.status.Connected
	c.status.Connected = false

	// Log the error
	c.logger.Error("Connection error: %v", err)

	// Check if we should attempt to reconnect
	if wasConnected && !c.shuttingDown {
		c.logger.Info("Connection lost, attempting to reconnect")
		// Instead of calling reconnectLoop, set the flag
		c.reconnectCalled.Store(true)
	}

	return err
}

// maybeReconnect is a copy of the real implementation but uses reconnectCalled instead
func (c *mockClient) maybeReconnect() {
	// Check if we're connected
	if c.status.Connected {
		return
	}

	// Check if the circuit breaker is open
	if c.circuitBreaker.IsOpen() {
		c.logger.Warn("Circuit breaker is open, not attempting to reconnect")
		return
	}

	// Mark that reconnect was called
	c.reconnectCalled.Store(true)
}

// IsConnected returns whether the client is connected
func (c *mockClient) IsConnected() bool {
	return c.status.Connected
}

func TestHandleConnectionError(t *testing.T) {
	// Create a mock client
	client := &mockClient{
		logger:         log.NewStandardLogger(),
		circuitBreaker: transport.NewCircuitBreaker(3, 100*time.Millisecond),
		status: transport.TransportStatus{
			Connected: true,
		},
		options: transport.TransportOptions{
			RetryPolicy: transport.RetryPolicy{
				InitialBackoff: 1 * time.Millisecond,
				MaxBackoff:     10 * time.Millisecond,
				MaxRetries:     2,
			},
		},
	}

	// Test with a connection error
	testErr := errors.New("test connection error")
	result := client.handleConnectionError(testErr)

	// Check results
	if result != testErr {
		t.Errorf("Expected error to be returned, got: %v", result)
	}

	if client.status.Connected {
		t.Error("Expected connected status to be false")
	}

	if client.status.LastError != testErr {
		t.Errorf("Expected LastError to be set, got: %v", client.status.LastError)
	}

	// Check if reconnect was attempted
	if !client.reconnectCalled.Load() {
		t.Error("Expected reconnect to be attempted")
	}

	// Test with nil error
	client.reconnectCalled.Store(false)
	result = client.handleConnectionError(nil)

	if result != nil {
		t.Errorf("Expected nil error to be returned, got: %v", result)
	}

	if client.reconnectCalled.Load() {
		t.Error("Expected no reconnect attempt for nil error")
	}
}

func TestMaybeReconnect(t *testing.T) {
	// Test case 1: Already connected
	t.Run("Already connected", func(t *testing.T) {
		client := &mockClient{
			logger:         log.NewStandardLogger(),
			circuitBreaker: transport.NewCircuitBreaker(3, 100*time.Millisecond),
			status: transport.TransportStatus{
				Connected: true,
			},
		}

		client.maybeReconnect()

		if client.reconnectCalled.Load() {
			t.Error("Expected no reconnect attempt when already connected")
		}
	})

	// Test case 2: Not connected but circuit breaker open
	t.Run("Circuit breaker open", func(t *testing.T) {
		client := &mockClient{
			logger:         log.NewStandardLogger(),
			circuitBreaker: transport.NewCircuitBreaker(3, 100*time.Millisecond),
			status: transport.TransportStatus{
				Connected: false,
			},
		}

		// Trip the circuit breaker
		client.circuitBreaker.Trip()

		client.maybeReconnect()

		if client.reconnectCalled.Load() {
			t.Error("Expected no reconnect attempt when circuit breaker is open")
		}
	})

	// Test case 3: Not connected and circuit breaker closed
	t.Run("Not connected, circuit closed", func(t *testing.T) {
		client := &mockClient{
			logger:         log.NewStandardLogger(),
			circuitBreaker: transport.NewCircuitBreaker(3, 100*time.Millisecond),
			status: transport.TransportStatus{
				Connected: false,
			},
			options: transport.TransportOptions{
				RetryPolicy: transport.RetryPolicy{
					InitialBackoff: 1 * time.Millisecond,
				},
			},
		}

		client.maybeReconnect()

		if !client.reconnectCalled.Load() {
			t.Error("Expected reconnect attempt when not connected and circuit breaker closed")
		}
	})
}
