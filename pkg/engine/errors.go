package engine

import "errors"

var (
	// ErrEngineClosed is returned when operations are performed on a closed engine
	ErrEngineClosed = errors.New("engine is closed")
	// ErrKeyNotFound is returned when a key is not found
	ErrKeyNotFound = errors.New("key not found")
	// ErrReadOnlyMode is returned when write operations are attempted while the engine is in read-only mode
	ErrReadOnlyMode = errors.New("engine is in read-only mode (replica)")
)
