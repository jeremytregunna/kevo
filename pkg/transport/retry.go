package transport

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// RetryableFunc is a function that can be retried
type RetryableFunc func(ctx context.Context) error

// WithRetry executes a function with retry logic based on the provided policy
func WithRetry(ctx context.Context, policy RetryPolicy, fn RetryableFunc) error {
	var err error
	backoff := policy.InitialBackoff

	for attempt := 0; attempt <= policy.MaxRetries; attempt++ {
		// Execute the function
		err = fn(ctx)
		if err == nil {
			// Success
			return nil
		}

		// Check if we should continue retrying
		if attempt == policy.MaxRetries {
			break
		}

		// Check if context is done
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Continue
		}

		// Add jitter to prevent thundering herd
		jitter := 1.0
		if policy.Jitter > 0 {
			jitter = 1.0 + rand.Float64()*policy.Jitter
		}

		// Calculate next backoff with jitter
		backoffWithJitter := time.Duration(float64(backoff) * jitter)
		if backoffWithJitter > policy.MaxBackoff {
			backoffWithJitter = policy.MaxBackoff
		}

		// Wait for backoff period
		timer := time.NewTimer(backoffWithJitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			// Continue with next attempt
		}

		// Increase backoff for next attempt
		backoff = time.Duration(float64(backoff) * policy.BackoffFactor)
		if backoff > policy.MaxBackoff {
			backoff = policy.MaxBackoff
		}
	}

	return err
}

// DefaultRetryPolicy returns a sensible default retry policy
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		BackoffFactor:  2.0,
		Jitter:         0.2,
	}
}

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int

const (
	// CircuitClosed means the circuit is closed and operations are permitted
	CircuitClosed CircuitBreakerState = iota
	// CircuitOpen means the circuit is open and operations will fail fast
	CircuitOpen
	// CircuitHalfOpen means the circuit is allowing a test operation
	CircuitHalfOpen
)

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	state             CircuitBreakerState
	failureThreshold  int
	resetTimeout      time.Duration
	failureCount      int
	lastFailure       time.Time
	lastStateChange   time.Time
	successThreshold  int
	halfOpenSuccesses int
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(failureThreshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            CircuitClosed,
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
		successThreshold: 1, // Default to 1 success required to close circuit
	}
}

// Execute attempts to execute a function with circuit breaker protection
func (cb *CircuitBreaker) Execute(ctx context.Context, fn RetryableFunc) error {
	// Check if circuit is open
	if cb.IsOpen() && !cb.shouldAttemptReset() {
		return ErrCircuitOpen
	}

	// Mark as half-open if we're attempting a reset
	if cb.state == CircuitOpen {
		cb.state = CircuitHalfOpen
		cb.halfOpenSuccesses = 0
		cb.lastStateChange = time.Now()
	}

	// Execute the function
	err := fn(ctx)
	
	// Handle result
	if err != nil {
		// Record failure
		cb.recordFailure()
		return err
	}
	
	// Record success
	cb.recordSuccess()
	return nil
}

// IsOpen returns whether the circuit is open
func (cb *CircuitBreaker) IsOpen() bool {
	return cb.state == CircuitOpen || cb.state == CircuitHalfOpen
}

// Trip manually opens the circuit
func (cb *CircuitBreaker) Trip() {
	cb.state = CircuitOpen
	cb.lastStateChange = time.Now()
}

// Reset manually closes the circuit
func (cb *CircuitBreaker) Reset() {
	cb.state = CircuitClosed
	cb.failureCount = 0
	cb.lastStateChange = time.Now()
}

// recordFailure records a failure and potentially opens the circuit
func (cb *CircuitBreaker) recordFailure() {
	cb.lastFailure = time.Now()
	
	switch cb.state {
	case CircuitClosed:
		cb.failureCount++
		if cb.failureCount >= cb.failureThreshold {
			cb.state = CircuitOpen
			cb.lastStateChange = time.Now()
		}
	case CircuitHalfOpen:
		cb.state = CircuitOpen
		cb.lastStateChange = time.Now()
	}
}

// recordSuccess records a success and potentially closes the circuit
func (cb *CircuitBreaker) recordSuccess() {
	switch cb.state {
	case CircuitHalfOpen:
		cb.halfOpenSuccesses++
		if cb.halfOpenSuccesses >= cb.successThreshold {
			cb.state = CircuitClosed
			cb.failureCount = 0
			cb.lastStateChange = time.Now()
		}
	case CircuitClosed:
		// Reset failure count after a success
		cb.failureCount = 0
	}
}

// shouldAttemptReset determines if enough time has passed to attempt a reset
func (cb *CircuitBreaker) shouldAttemptReset() bool {
	return cb.state == CircuitOpen &&
		time.Since(cb.lastStateChange) >= cb.resetTimeout
}

// ExponentialBackoff calculates the next backoff duration
func ExponentialBackoff(attempt int, initialBackoff time.Duration, maxBackoff time.Duration, factor float64) time.Duration {
	backoff := float64(initialBackoff) * math.Pow(factor, float64(attempt))
	if backoff > float64(maxBackoff) {
		return maxBackoff
	}
	return time.Duration(backoff)
}