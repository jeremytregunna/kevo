package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jeremytregunna/kevo/pkg/engine"
)

// TransactionRegistry manages active transactions on the server
type TransactionRegistry struct {
	mu           sync.RWMutex
	transactions map[string]engine.Transaction
	nextID       uint64
}

// NewTransactionRegistry creates a new transaction registry
func NewTransactionRegistry() *TransactionRegistry {
	return &TransactionRegistry{
		transactions: make(map[string]engine.Transaction),
	}
}

// Begin creates a new transaction and registers it
func (tr *TransactionRegistry) Begin(ctx context.Context, eng *engine.Engine, readOnly bool) (string, error) {
	// Create context with timeout to prevent potential hangs
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	
	// Create a channel to receive the transaction result
	type txResult struct {
		tx  engine.Transaction
		err error
	}
	resultCh := make(chan txResult, 1)
	
	// Start transaction in a goroutine to prevent potential blocking
	go func() {
		tx, err := eng.BeginTransaction(readOnly)
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
		
		tr.mu.Lock()
		defer tr.mu.Unlock()
		
		// Generate a transaction ID
		tr.nextID++
		txID := fmt.Sprintf("tx-%d", tr.nextID)
		
		// Register the transaction
		tr.transactions[txID] = result.tx
		
		return txID, nil
		
	case <-timeoutCtx.Done():
		return "", fmt.Errorf("transaction creation timed out: %w", timeoutCtx.Err())
	}
}

// Get retrieves a transaction by ID
func (tr *TransactionRegistry) Get(txID string) (engine.Transaction, bool) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	tx, exists := tr.transactions[txID]
	return tx, exists
}

// Remove removes a transaction from the registry
func (tr *TransactionRegistry) Remove(txID string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	delete(tr.transactions, txID)
}

// GracefulShutdown attempts to cleanly shut down all transactions
func (tr *TransactionRegistry) GracefulShutdown(ctx context.Context) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	var lastErr error
	
	// Copy transaction IDs to avoid modifying the map during iteration
	ids := make([]string, 0, len(tr.transactions))
	for id := range tr.transactions {
		ids = append(ids, id)
	}
	
	// Rollback each transaction with a timeout
	for _, id := range ids {
		tx, exists := tr.transactions[id]
		if !exists {
			continue
		}
		
		// Use a timeout for each rollback operation
		rollbackCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		
		// Create a channel for the rollback result
		doneCh := make(chan error, 1)
		
		// Execute rollback in goroutine
		go func(t engine.Transaction) {
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
		delete(tr.transactions, id)
	}

	return lastErr
}

// Server represents the Kevo server
type Server struct {
	eng        *engine.Engine
	txRegistry *TransactionRegistry
	listener   net.Listener
	grpcServer interface{} // Will be replaced with actual gRPC server
	config     Config
}

// NewServer creates a new server instance
func NewServer(eng *engine.Engine, config Config) *Server {
	return &Server{
		eng:        eng,
		txRegistry: NewTransactionRegistry(),
		config:     config,
	}
}

// Start initializes and starts the server
func (s *Server) Start() error {
	// Create a listener on the specified address
	var err error
	s.listener, err = net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.ListenAddr, err)
	}

	fmt.Printf("Listening on %s\n", s.config.ListenAddr)

	// TODO: Initialize gRPC server with our service implementation
	// This will be implemented in Phase 3 when we add gRPC support
	// For now, just hold the listener open

	return nil
}

// Serve starts serving requests (blocking)
func (s *Server) Serve() error {
	// TODO: Start the gRPC server
	// This will be implemented in Phase 3 when we add gRPC support

	// For now, just block until the listener is closed
	select {}
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	// First, shut down the listener to stop accepting new connections
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}

	// TODO: Gracefully shutdown gRPC server
	// This will be implemented in Phase 3 when we add gRPC support

	// Clean up any active transactions
	if err := s.txRegistry.GracefulShutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown transaction registry: %w", err)
	}

	return nil
}