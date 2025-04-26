package transport

import (
	"errors"
	"fmt"
)

// Common error types for transport and reliability
var (
	// ErrMaxRetriesExceeded indicates the operation failed after all retries
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")
	
	// ErrCircuitOpen indicates the circuit breaker is open
	ErrCircuitOpen = errors.New("circuit breaker is open")
	
	// ErrConnectionFailed indicates a connection failure
	ErrConnectionFailed = errors.New("connection failed")
	
	// ErrDisconnected indicates the connection was lost
	ErrDisconnected = errors.New("connection was lost")
	
	// ErrReconnectionFailed indicates reconnection attempts failed
	ErrReconnectionFailed = errors.New("reconnection failed")
	
	// ErrStreamClosed indicates the stream was closed
	ErrStreamClosed = errors.New("stream was closed")
	
	// ErrInvalidState indicates an invalid state
	ErrInvalidState = errors.New("invalid state")
	
	// ErrReplicaNotRegistered indicates the replica is not registered
	ErrReplicaNotRegistered = errors.New("replica not registered")
)

// TemporaryError wraps an error with information about whether it's temporary
type TemporaryError struct {
	Err        error
	IsTemp     bool
	RetryAfter int // Suggested retry after duration in milliseconds
}

// Error returns the error string
func (e *TemporaryError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("%v (temporary: %v, retry after: %dms)", e.Err, e.IsTemp, e.RetryAfter)
	}
	return fmt.Sprintf("%v (temporary: %v)", e.Err, e.IsTemp)
}

// Unwrap returns the wrapped error
func (e *TemporaryError) Unwrap() error {
	return e.Err
}

// IsTemporary returns whether the error is temporary
func (e *TemporaryError) IsTemporary() bool {
	return e.IsTemp
}

// GetRetryAfter returns the suggested retry after duration
func (e *TemporaryError) GetRetryAfter() int {
	return e.RetryAfter
}

// NewTemporaryError creates a new temporary error
func NewTemporaryError(err error, isTemp bool) *TemporaryError {
	return &TemporaryError{
		Err:    err,
		IsTemp: isTemp,
	}
}

// NewTemporaryErrorWithRetry creates a new temporary error with retry hint
func NewTemporaryErrorWithRetry(err error, isTemp bool, retryAfter int) *TemporaryError {
	return &TemporaryError{
		Err:        err,
		IsTemp:     isTemp,
		RetryAfter: retryAfter,
	}
}

// IsTemporary checks if an error is a temporary error
func IsTemporary(err error) bool {
	var tempErr *TemporaryError
	if errors.As(err, &tempErr) {
		return tempErr.IsTemporary()
	}
	return false
}

// GetRetryAfter extracts the retry hint from an error
func GetRetryAfter(err error) int {
	var tempErr *TemporaryError
	if errors.As(err, &tempErr) {
		return tempErr.GetRetryAfter()
	}
	return 0
}