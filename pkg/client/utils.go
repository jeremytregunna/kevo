package client

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

// RetryableFunc is a function that can be retried
type RetryableFunc func() error

// Errors that can occur during client operations
var (
	// ErrNotConnected indicates the client is not connected to the server
	ErrNotConnected = errors.New("not connected to server")

	// ErrInvalidOptions indicates invalid client options
	ErrInvalidOptions = errors.New("invalid client options")

	// ErrTimeout indicates a request timed out
	ErrTimeout = errors.New("request timed out")

	// ErrKeyNotFound indicates a key was not found
	ErrKeyNotFound = errors.New("key not found")

	// ErrTransactionConflict indicates a transaction conflict occurred
	ErrTransactionConflict = errors.New("transaction conflict detected")
)

// IsRetryableError returns true if the error is considered retryable
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// These errors are considered transient and can be retried
	if errors.Is(err, ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Other errors are considered permanent
	return false
}

// RetryWithBackoff executes a function with exponential backoff and jitter
func RetryWithBackoff(
	ctx context.Context,
	fn RetryableFunc,
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
	backoffFactor float64,
	jitter float64,
) error {
	var err error
	backoff := initialBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Execute the function
		err = fn()
		if err == nil {
			return nil
		}

		// Check if the error is retryable
		if !IsRetryableError(err) {
			return err
		}

		// Check if we've reached the retry limit
		if attempt >= maxRetries {
			return err
		}

		// Calculate next backoff with jitter
		jitterRange := float64(backoff) * jitter
		jitterAmount := int64(rand.Float64() * jitterRange)
		sleepTime := backoff + time.Duration(jitterAmount)

		// Check context before sleeping
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepTime):
			// Continue with next attempt
		}

		// Increase backoff for next attempt
		backoff = time.Duration(float64(backoff) * backoffFactor)
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return err
}

// CalculateExponentialBackoff calculates the backoff time for a given attempt
func CalculateExponentialBackoff(
	attempt int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
	backoffFactor float64,
	jitter float64,
) time.Duration {
	backoff := initialBackoff * time.Duration(math.Pow(backoffFactor, float64(attempt)))
	if backoff > maxBackoff {
		backoff = maxBackoff
	}

	if jitter > 0 {
		jitterRange := float64(backoff) * jitter
		jitterAmount := int64(rand.Float64() * jitterRange)
		backoff = backoff + time.Duration(jitterAmount)
	}

	return backoff
}
