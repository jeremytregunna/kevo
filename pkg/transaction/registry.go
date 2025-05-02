package transaction

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Registry manages transaction lifecycle and connections
type RegistryImpl struct {
	mu            sync.RWMutex
	transactions  map[string]Transaction
	nextID        uint64
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
	connectionTxs map[string]map[string]struct{}
	txTTL         time.Duration
}

// NewRegistry creates a new transaction registry
func NewRegistry() Registry {
	r := &RegistryImpl{
		transactions:  make(map[string]Transaction),
		connectionTxs: make(map[string]map[string]struct{}),
		stopCleanup:   make(chan struct{}),
		txTTL:         5 * time.Minute, // Default TTL
	}

	// Start periodic cleanup
	r.cleanupTicker = time.NewTicker(30 * time.Second)
	go r.cleanupStaleTx()

	return r
}

// NewRegistryWithTTL creates a new transaction registry with a specific TTL
func NewRegistryWithTTL(ttl time.Duration) Registry {
	r := &RegistryImpl{
		transactions:  make(map[string]Transaction),
		connectionTxs: make(map[string]map[string]struct{}),
		stopCleanup:   make(chan struct{}),
		txTTL:         ttl,
	}

	// Start periodic cleanup
	r.cleanupTicker = time.NewTicker(30 * time.Second)
	go r.cleanupStaleTx()

	return r
}

// cleanupStaleTx periodically checks for and removes stale transactions
func (r *RegistryImpl) cleanupStaleTx() {
	for {
		select {
		case <-r.cleanupTicker.C:
			r.cleanupStaleTransactions()
		case <-r.stopCleanup:
			r.cleanupTicker.Stop()
			return
		}
	}
}

// cleanupStaleTransactions removes transactions that have been idle for too long
func (r *RegistryImpl) cleanupStaleTransactions() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Use the configured TTL (TODO: Add TTL tracking)

	// Find stale transactions
	var staleIDs []string
	for id := range r.transactions {
		// For simplicity, we don't check the creation time for now
		// A more sophisticated implementation would track last activity time
		staleIDs = append(staleIDs, id)
	}

	if len(staleIDs) > 0 {
		fmt.Printf("Cleaning up %d potentially stale transactions\n", len(staleIDs))
	}

	// Clean up stale transactions
	for _, id := range staleIDs {
		if tx, exists := r.transactions[id]; exists {
			// Try to rollback the transaction
			_ = tx.Rollback() // Ignore errors during cleanup

			// Remove from connection tracking
			for connID, txs := range r.connectionTxs {
				if _, ok := txs[id]; ok {
					delete(txs, id)
					// If connection has no more transactions, remove it
					if len(txs) == 0 {
						delete(r.connectionTxs, connID)
					}
					break
				}
			}

			// Remove from main transactions map
			delete(r.transactions, id)
			fmt.Printf("Removed stale transaction: %s\n", id)
		}
	}
}

// Begin starts a new transaction
func (r *RegistryImpl) Begin(ctx context.Context, engine interface{}, readOnly bool) (string, error) {
	// Extract connection ID from context
	connectionID := "unknown"
	if p, ok := ctx.Value("peer").(string); ok {
		connectionID = p
	}

	// Create a timeout context for transaction creation
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Create a channel to receive the transaction result
	type txResult struct {
		tx  Transaction
		err error
	}
	resultCh := make(chan txResult, 1)

	// Start transaction in a goroutine
	go func() {
		var tx Transaction
		var err error

		// Attempt to cast to different engine types
		if manager, ok := engine.(TransactionManager); ok {
			tx, err = manager.BeginTransaction(readOnly)
		} else {
			err = fmt.Errorf("unsupported engine type for transactions")
		}

		select {
		case resultCh <- txResult{tx, err}:
			// Successfully sent result
		case <-timeoutCtx.Done():
			// Context timed out, but try to rollback if we got a transaction
			if tx != nil {
				tx.Rollback()
			}
		}
	}()

	// Wait for result or timeout
	select {
	case result := <-resultCh:
		if result.err != nil {
			return "", fmt.Errorf("failed to begin transaction: %w", result.err)
		}

		r.mu.Lock()
		defer r.mu.Unlock()

		// Generate transaction ID
		r.nextID++
		txID := fmt.Sprintf("tx-%d", r.nextID)

		// Store the transaction in the registry
		r.transactions[txID] = result.tx

		// Track by connection ID
		if _, exists := r.connectionTxs[connectionID]; !exists {
			r.connectionTxs[connectionID] = make(map[string]struct{})
		}
		r.connectionTxs[connectionID][txID] = struct{}{}

		fmt.Printf("Created transaction: %s (connection: %s)\n", txID, connectionID)
		return txID, nil

	case <-timeoutCtx.Done():
		return "", fmt.Errorf("transaction creation timed out: %w", timeoutCtx.Err())
	}
}

// Get retrieves a transaction by ID
func (r *RegistryImpl) Get(txID string) (Transaction, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tx, exists := r.transactions[txID]
	if !exists {
		return nil, false
	}

	return tx, true
}

// Remove removes a transaction from the registry
func (r *RegistryImpl) Remove(txID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, exists := r.transactions[txID]
	if !exists {
		return
	}

	// Remove from connection tracking
	for connID, txs := range r.connectionTxs {
		if _, ok := txs[txID]; ok {
			delete(txs, txID)
			// If connection has no more transactions, remove it
			if len(txs) == 0 {
				delete(r.connectionTxs, connID)
			}
			break
		}
	}

	// Remove from transactions map
	delete(r.transactions, txID)
}

// CleanupConnection rolls back and removes all transactions for a connection
func (r *RegistryImpl) CleanupConnection(connectionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	txIDs, exists := r.connectionTxs[connectionID]
	if !exists {
		return
	}

	fmt.Printf("Cleaning up %d transactions for disconnected connection %s\n",
		len(txIDs), connectionID)

	// Rollback each transaction
	for txID := range txIDs {
		if tx, ok := r.transactions[txID]; ok {
			// Rollback and ignore errors since we're cleaning up
			_ = tx.Rollback()
			// Remove from transactions map
			delete(r.transactions, txID)
		}
	}

	// Remove the connection entry
	delete(r.connectionTxs, connectionID)
}

// GracefulShutdown cleans up all transactions
func (r *RegistryImpl) GracefulShutdown(ctx context.Context) error {
	// Stop the cleanup goroutine
	close(r.stopCleanup)
	r.cleanupTicker.Stop()

	r.mu.Lock()
	defer r.mu.Unlock()

	var lastErr error

	// Copy transaction IDs to avoid modifying during iteration
	ids := make([]string, 0, len(r.transactions))
	for id := range r.transactions {
		ids = append(ids, id)
	}

	// Rollback each transaction with a timeout
	for _, id := range ids {
		tx, exists := r.transactions[id]
		if !exists {
			continue
		}

		// Use a timeout for each rollback operation
		rollbackCtx, cancel := context.WithTimeout(ctx, 1*time.Second)

		// Create a channel for the rollback result
		doneCh := make(chan error, 1)

		// Execute rollback in goroutine to handle potential hangs
		go func(t Transaction) {
			doneCh <- t.Rollback()
		}(tx)

		// Wait for rollback or timeout
		var err error
		select {
		case err = <-doneCh:
			// Rollback completed
		case <-rollbackCtx.Done():
			err = fmt.Errorf("rollback timed out: %w", rollbackCtx.Err())
		}

		cancel() // Clean up context

		// Record error if any
		if err != nil {
			lastErr = fmt.Errorf("failed to rollback transaction %s: %w", id, err)
		}

		// Always remove transaction from map
		delete(r.transactions, id)
	}

	// Clear the connection tracking map
	r.connectionTxs = make(map[string]map[string]struct{})

	return lastErr
}