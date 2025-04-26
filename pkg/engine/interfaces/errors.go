package interfaces

import "errors"

// Common error types used throughout the engine
// Note: Some errors are defined as constants in engine.go
var (
	// ErrReadOnlyTransaction is returned when attempting to write in a read-only transaction
	ErrReadOnlyTransaction = errors.New("transaction is read-only")

	// ErrTransactionClosed is returned when operations are performed on a completed transaction
	ErrTransactionClosed = errors.New("transaction is already committed or rolled back")
)
