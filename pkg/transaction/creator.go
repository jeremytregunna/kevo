package transaction

import (
	"github.com/jer/kevo/pkg/engine"
)

// TransactionCreatorImpl implements the engine.TransactionCreator interface
type TransactionCreatorImpl struct{}

// CreateTransaction creates a new transaction
func (tc *TransactionCreatorImpl) CreateTransaction(e interface{}, readOnly bool) (engine.Transaction, error) {
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
	return NewTransaction(eng, mode)
}

// Register the transaction creator with the engine
func init() {
	engine.RegisterTransactionCreator(&TransactionCreatorImpl{})
}
