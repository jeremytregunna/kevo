package transaction

import (
	"github.com/KevoDB/kevo/pkg/engine"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"
)

// TransactionCreatorImpl implements the interfaces.TransactionCreator interface
type TransactionCreatorImpl struct{}

// CreateTransaction creates a new transaction
func (tc *TransactionCreatorImpl) CreateTransaction(e interface{}, readOnly bool) (interfaces.Transaction, error) {
	// Convert the interface to the engine.Engine type
	eng, ok := e.(*engine.Engine)
	if !ok {
		return nil, ErrInvalidEngine
	}

	// Determine transaction mode
	var mode TransactionMode
	if readOnly {
		mode = ReadOnly
	} else {
		mode = ReadWrite
	}

	// Create a new transaction
	tx, err := NewTransaction(eng, mode)
	if err != nil {
		return nil, err
	}
	
	// Return the transaction as an interfaces.Transaction
	return tx, nil
}

// For backward compatibility, register with the old mechanism too
// This can be removed once all code is migrated
func init() {
	// In the new approach, we should use dependency injection rather than global registration
}
