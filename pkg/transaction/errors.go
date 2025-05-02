package transaction

import "errors"

// Common errors for transaction operations
var (
	// ErrReadOnlyTransaction is returned when a write operation is attempted on a read-only transaction
	ErrReadOnlyTransaction = errors.New("cannot write to a read-only transaction")

	// ErrTransactionClosed is returned when an operation is attempted on a closed transaction
	ErrTransactionClosed = errors.New("transaction already committed or rolled back")

	// ErrKeyNotFound is returned when a key doesn't exist
	ErrKeyNotFound = errors.New("key not found")

	// ErrInvalidEngine is returned when an incompatible engine type is provided
	ErrInvalidEngine = errors.New("invalid engine type")
)
