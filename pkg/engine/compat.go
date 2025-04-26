package engine

import (
	"errors"
	"sync"

	"github.com/KevoDB/kevo/pkg/common/iterator"
)

// Compatibility layer for the legacy engine API

// LegacyTransaction interface is kept for backward compatibility
type LegacyTransaction interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	NewIterator() iterator.Iterator
	NewRangeIterator(startKey, endKey []byte) iterator.Iterator
	Commit() error
	Rollback() error
	IsReadOnly() bool
}

// LegacyTransactionCreator is kept for backward compatibility
type LegacyTransactionCreator interface {
	CreateTransaction(engine interface{}, readOnly bool) (LegacyTransaction, error)
}

var (
	// legacyTransactionCreatorFunc holds the function that creates transactions
	legacyTransactionCreatorFunc LegacyTransactionCreator
	transactionCreatorMu         sync.RWMutex
)

// RegisterTransactionCreator registers a function that can create transactions
// This is kept for backward compatibility
func RegisterTransactionCreator(creator LegacyTransactionCreator) {
	transactionCreatorMu.Lock()
	defer transactionCreatorMu.Unlock()
	legacyTransactionCreatorFunc = creator
}

// GetRegisteredTransactionCreator returns the registered transaction creator
// This is for internal use by the engine facade
func GetRegisteredTransactionCreator() LegacyTransactionCreator {
	transactionCreatorMu.RLock()
	defer transactionCreatorMu.RUnlock()
	return legacyTransactionCreatorFunc
}

// CreateTransactionWithCreator creates a transaction using the registered creator
// This is for internal use by the engine facade
func CreateTransactionWithCreator(engine interface{}, readOnly bool) (LegacyTransaction, error) {
	transactionCreatorMu.RLock()
	creator := legacyTransactionCreatorFunc
	transactionCreatorMu.RUnlock()

	if creator == nil {
		return nil, errors.New("no transaction creator registered")
	}

	return creator.CreateTransaction(engine, readOnly)
}

// GetRWLock is a compatibility method for the engine facade
// It returns a sync.RWMutex for use by the legacy transaction code
func (e *EngineFacade) GetRWLock() *sync.RWMutex {
	// Forward to the transaction manager's lock
	return e.txManager.GetRWLock()
}

// IncrementTxCompleted is a compatibility method for the engine facade
func (e *EngineFacade) IncrementTxCompleted() {
	e.txManager.IncrementTxCompleted()
}

// IncrementTxAborted is a compatibility method for the engine facade
func (e *EngineFacade) IncrementTxAborted() {
	e.txManager.IncrementTxAborted()
}
