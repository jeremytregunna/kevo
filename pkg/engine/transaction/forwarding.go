package transaction

import (
	"context"
	"sync"
	
	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/engine/interfaces"
	"github.com/KevoDB/kevo/pkg/stats"
	tx "github.com/KevoDB/kevo/pkg/transaction"
	"github.com/KevoDB/kevo/pkg/wal"
)

// Forward engine transaction functions to the new implementation
// This is a transitional approach until all call sites are updated

// storageAdapter adapts the engine storage interface to the new transaction package
type storageAdapter struct {
	storage interfaces.StorageManager
}

// Implement the transaction.StorageBackend interface
func (a *storageAdapter) Get(key []byte) ([]byte, error) {
	return a.storage.Get(key)
}

func (a *storageAdapter) ApplyBatch(entries []*wal.Entry) error {
	return a.storage.ApplyBatch(entries)
}

func (a *storageAdapter) GetIterator() (iterator.Iterator, error) {
	return a.storage.GetIterator()
}

func (a *storageAdapter) GetRangeIterator(startKey, endKey []byte) (iterator.Iterator, error) {
	return a.storage.GetRangeIterator(startKey, endKey)
}

// Create a wrapper for the transaction manager interface
type managerWrapper struct {
	inner *tx.Manager
}

// Implement interfaces.TransactionManager methods
func (w *managerWrapper) BeginTransaction(readOnly bool) (interfaces.Transaction, error) {
	transaction, err := w.inner.BeginTransaction(readOnly)
	if err != nil {
		return nil, err
	}
	// Since our transaction implements the same interface, wrap it
	return &transactionWrapper{transaction}, nil
}

func (w *managerWrapper) GetRWLock() *sync.RWMutex {
	return w.inner.GetRWLock()
}

func (w *managerWrapper) IncrementTxCompleted() {
	w.inner.IncrementTxCompleted()
}

func (w *managerWrapper) IncrementTxAborted() {
	w.inner.IncrementTxAborted()
}

func (w *managerWrapper) GetTransactionStats() map[string]interface{} {
	return w.inner.GetTransactionStats()
}

// Create a wrapper for the transaction interface
type transactionWrapper struct {
	inner tx.Transaction
}

// Implement interfaces.Transaction methods
func (w *transactionWrapper) Get(key []byte) ([]byte, error) {
	return w.inner.Get(key)
}

func (w *transactionWrapper) Put(key, value []byte) error {
	return w.inner.Put(key, value)
}

func (w *transactionWrapper) Delete(key []byte) error {
	return w.inner.Delete(key)
}

func (w *transactionWrapper) NewIterator() iterator.Iterator {
	return w.inner.NewIterator()
}

func (w *transactionWrapper) NewRangeIterator(startKey, endKey []byte) iterator.Iterator {
	return w.inner.NewRangeIterator(startKey, endKey)
}

func (w *transactionWrapper) Commit() error {
	return w.inner.Commit()
}

func (w *transactionWrapper) Rollback() error {
	return w.inner.Rollback()
}

func (w *transactionWrapper) IsReadOnly() bool {
	return w.inner.IsReadOnly()
}

// Create a wrapper for the registry interface
type registryWrapper struct {
	inner tx.Registry
}

// Implement interfaces.TxRegistry methods
func (w *registryWrapper) Begin(ctx context.Context, eng interfaces.Engine, readOnly bool) (string, error) {
	return w.inner.Begin(ctx, eng, readOnly)
}

func (w *registryWrapper) Get(txID string) (interfaces.Transaction, bool) {
	transaction, found := w.inner.Get(txID)
	if !found {
		return nil, false
	}
	return &transactionWrapper{transaction}, true
}

func (w *registryWrapper) Remove(txID string) {
	w.inner.Remove(txID)
}

func (w *registryWrapper) CleanupConnection(connectionID string) {
	w.inner.CleanupConnection(connectionID)
}

func (w *registryWrapper) GracefulShutdown(ctx context.Context) error {
	return w.inner.GracefulShutdown(ctx)
}

// NewManager forwards to the new implementation while maintaining the same signature
func NewManager(storage interfaces.StorageManager, statsCollector stats.Collector) interfaces.TransactionManager {
	// Create a storage adapter that works with our new transaction implementation
	adapter := &storageAdapter{storage: storage}
	
	// Create the new transaction manager and wrap it
	return &managerWrapper{
		inner: tx.NewManager(adapter, statsCollector),
	}
}

// NewRegistry forwards to the new implementation while maintaining the same signature
func NewRegistry() interfaces.TxRegistry {
	// Create the new registry and wrap it
	return &registryWrapper{
		inner: tx.NewRegistry(),
	}
}