package transport

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithRetrySuccess(t *testing.T) {
	callCount := 0
	successOnAttempt := 3

	fn := func(ctx context.Context) error {
		callCount++
		if callCount >= successOnAttempt {
			return nil
		}
		return errors.New("temporary error")
	}

	policy := RetryPolicy{
		MaxRetries:     5,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         0.1,
	}

	err := WithRetry(context.Background(), policy, fn)

	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}

	if callCount != successOnAttempt {
		t.Errorf("Expected %d calls, got %d", successOnAttempt, callCount)
	}
}

func TestWithRetryExceedMaxRetries(t *testing.T) {
	callCount := 0

	fn := func(ctx context.Context) error {
		callCount++
		return errors.New("persistent error")
	}

	policy := RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         0.0, // Disable jitter for deterministic tests
	}

	err := WithRetry(context.Background(), policy, fn)

	if err == nil {
		t.Error("Expected error, got nil")
	}

	expectedCalls := policy.MaxRetries + 1 // Initial try + retries
	if callCount != expectedCalls {
		t.Errorf("Expected %d calls, got %d", expectedCalls, callCount)
	}
}

func TestWithRetryContextCancellation(t *testing.T) {
	callCount := 0

	fn := func(ctx context.Context) error {
		callCount++
		return errors.New("error")
	}

	policy := RetryPolicy{
		MaxRetries:     10,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		BackoffFactor:  2.0,
		Jitter:         0.0,
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after a short time
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := WithRetry(ctx, policy, fn)

	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled error, got: %v", err)
	}
}

func TestExponentialBackoff(t *testing.T) {
	initialBackoff := 100 * time.Millisecond
	maxBackoff := 10 * time.Second
	factor := 2.0

	tests := []struct {
		name     string
		attempt  int
		expected time.Duration
	}{
		{"FirstAttempt", 0, 100 * time.Millisecond},
		{"SecondAttempt", 1, 200 * time.Millisecond},
		{"ThirdAttempt", 2, 400 * time.Millisecond},
		{"FourthAttempt", 3, 800 * time.Millisecond},
		{"MaxBackoff", 10, maxBackoff}, // This would exceed maxBackoff
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExponentialBackoff(tt.attempt, initialBackoff, maxBackoff, factor)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestCircuitBreaker(t *testing.T) {
	t.Run("Initially Closed", func(t *testing.T) {
		cb := NewCircuitBreaker(3, 100*time.Millisecond)
		if cb.IsOpen() {
			t.Error("Circuit breaker should be closed initially")
		}
	})

	t.Run("Opens After Failures", func(t *testing.T) {
		cb := NewCircuitBreaker(3, 100*time.Millisecond)

		failingFn := func(ctx context.Context) error {
			return errors.New("error")
		}

		// Execute with failures
		for i := 0; i < 3; i++ {
			_ = cb.Execute(context.Background(), failingFn)
		}

		if !cb.IsOpen() {
			t.Error("Circuit breaker should be open after threshold failures")
		}
	})

	t.Run("Stays Open Until Timeout", func(t *testing.T) {
		resetTimeout := 100 * time.Millisecond
		cb := NewCircuitBreaker(1, resetTimeout)

		// Trip the circuit
		cb.Trip()

		if !cb.IsOpen() {
			t.Error("Circuit breaker should be open after tripping")
		}

		// Execute should fail fast
		err := cb.Execute(context.Background(), func(ctx context.Context) error {
			return nil
		})

		if err != ErrCircuitOpen {
			t.Errorf("Expected ErrCircuitOpen, got: %v", err)
		}

		// Wait for reset timeout
		time.Sleep(resetTimeout + 10*time.Millisecond)

		// Now it should be half-open and attempt the function
		successFn := func(ctx context.Context) error {
			return nil
		}

		err = cb.Execute(context.Background(), successFn)

		if err != nil {
			t.Errorf("Expected successful execution, got: %v", err)
		}

		if cb.IsOpen() {
			t.Error("Circuit breaker should be closed after successful execution in half-open state")
		}
	})

	t.Run("Resets After Success", func(t *testing.T) {
		cb := NewCircuitBreaker(3, 100*time.Millisecond)

		// Trip the circuit manually
		cb.Trip()

		// Manually reset
		cb.Reset()

		if cb.IsOpen() {
			t.Error("Circuit breaker should be closed after reset")
		}
	})
}
