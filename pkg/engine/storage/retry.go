package storage

import (
	"math/rand"
	"time"

	"github.com/KevoDB/kevo/pkg/wal"
)

// RetryConfig defines parameters for retry operations
type RetryConfig struct {
	MaxRetries     int           // Maximum number of retries
	InitialBackoff time.Duration // Initial backoff duration
	MaxBackoff     time.Duration // Maximum backoff duration
}

// DefaultRetryConfig returns default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
	}
}

// Commented out due to duplicate declaration with the one in manager.go
// func (m *Manager) RetryOnWALRotating(operation func() error) error {
// 	config := DefaultRetryConfig()
// 	return m.RetryWithConfig(operation, config, isWALRotating)
// }

// RetryWithConfig retries an operation with the given configuration
func (m *Manager) RetryWithConfig(operation func() error, config *RetryConfig, isRetryable func(error) bool) error {
	backoff := config.InitialBackoff

	for i := 0; i <= config.MaxRetries; i++ {
		// Attempt the operation
		err := operation()
		if err == nil {
			return nil
		}

		// Check if we should retry
		if !isRetryable(err) || i == config.MaxRetries {
			return err
		}

		// Add some jitter to the backoff
		jitter := time.Duration(rand.Int63n(int64(backoff / 10)))
		backoff = backoff + jitter

		// Wait before retrying
		time.Sleep(backoff)

		// Increase backoff for next attempt, but cap it
		backoff = 2 * backoff
		if backoff > config.MaxBackoff {
			backoff = config.MaxBackoff
		}
	}

	// Should never get here, but just in case
	return nil
}

// isWALRotating checks if the error is due to WAL rotation or closure
func isWALRotating(err error) bool {
	// Both ErrWALRotating and ErrWALClosed can occur during WAL rotation
	// Since WAL rotation is a normal operation, we should retry in both cases
	return err == wal.ErrWALRotating || err == wal.ErrWALClosed
}
