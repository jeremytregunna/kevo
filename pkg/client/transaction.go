package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/KevoDB/kevo/pkg/transport"
)

// Transaction represents a database transaction
type Transaction struct {
	client   *Client
	id       string
	readOnly bool
	closed   bool
	mu       sync.RWMutex
}

// ErrTransactionClosed is returned when attempting to use a closed transaction
var ErrTransactionClosed = errors.New("transaction is closed")

// BeginTransaction starts a new transaction
func (c *Client) BeginTransaction(ctx context.Context, readOnly bool) (*Transaction, error) {
	if !c.IsConnected() {
		return nil, errors.New("not connected to server")
	}

	req := struct {
		ReadOnly bool `json:"read_only"`
	}{
		ReadOnly: readOnly,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
	defer cancel()

	resp, err := c.client.Send(timeoutCtx, transport.NewRequest(transport.TypeBeginTx, reqData))
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	var txResp struct {
		TransactionID string `json:"transaction_id"`
	}

	if err := json.Unmarshal(resp.Payload(), &txResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &Transaction{
		client:   c,
		id:       txResp.TransactionID,
		readOnly: readOnly,
		closed:   false,
	}, nil
}

// Commit commits the transaction
func (tx *Transaction) Commit(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return ErrTransactionClosed
	}

	req := struct {
		TransactionID string `json:"transaction_id"`
	}{
		TransactionID: tx.id,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, tx.client.options.RequestTimeout)
	defer cancel()

	resp, err := tx.client.client.Send(timeoutCtx, transport.NewRequest(transport.TypeCommitTx, reqData))
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	var commitResp struct {
		Success bool `json:"success"`
	}

	if err := json.Unmarshal(resp.Payload(), &commitResp); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	tx.closed = true

	if !commitResp.Success {
		return errors.New("transaction commit failed")
	}

	return nil
}

// Rollback aborts the transaction
func (tx *Transaction) Rollback(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return ErrTransactionClosed
	}

	req := struct {
		TransactionID string `json:"transaction_id"`
	}{
		TransactionID: tx.id,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, tx.client.options.RequestTimeout)
	defer cancel()

	resp, err := tx.client.client.Send(timeoutCtx, transport.NewRequest(transport.TypeRollbackTx, reqData))
	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	var rollbackResp struct {
		Success bool `json:"success"`
	}

	if err := json.Unmarshal(resp.Payload(), &rollbackResp); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	tx.closed = true

	if !rollbackResp.Success {
		return errors.New("transaction rollback failed")
	}

	return nil
}

// Get retrieves a value by key within the transaction
func (tx *Transaction) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if tx.closed {
		return nil, false, ErrTransactionClosed
	}

	req := struct {
		TransactionID string `json:"transaction_id"`
		Key           []byte `json:"key"`
	}{
		TransactionID: tx.id,
		Key:           key,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, false, fmt.Errorf("failed to marshal request: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, tx.client.options.RequestTimeout)
	defer cancel()

	resp, err := tx.client.client.Send(timeoutCtx, transport.NewRequest(transport.TypeTxGet, reqData))
	if err != nil {
		return nil, false, fmt.Errorf("failed to send request: %w", err)
	}

	var getResp struct {
		Value []byte `json:"value"`
		Found bool   `json:"found"`
	}

	if err := json.Unmarshal(resp.Payload(), &getResp); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return getResp.Value, getResp.Found, nil
}

// Put stores a key-value pair within the transaction
func (tx *Transaction) Put(ctx context.Context, key, value []byte) (bool, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if tx.closed {
		return false, ErrTransactionClosed
	}

	if tx.readOnly {
		return false, errors.New("cannot write to a read-only transaction")
	}

	req := struct {
		TransactionID string `json:"transaction_id"`
		Key           []byte `json:"key"`
		Value         []byte `json:"value"`
	}{
		TransactionID: tx.id,
		Key:           key,
		Value:         value,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return false, fmt.Errorf("failed to marshal request: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, tx.client.options.RequestTimeout)
	defer cancel()

	resp, err := tx.client.client.Send(timeoutCtx, transport.NewRequest(transport.TypeTxPut, reqData))
	if err != nil {
		return false, fmt.Errorf("failed to send request: %w", err)
	}

	var putResp struct {
		Success bool `json:"success"`
	}

	if err := json.Unmarshal(resp.Payload(), &putResp); err != nil {
		return false, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return putResp.Success, nil
}

// Delete removes a key-value pair within the transaction
func (tx *Transaction) Delete(ctx context.Context, key []byte) (bool, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if tx.closed {
		return false, ErrTransactionClosed
	}

	if tx.readOnly {
		return false, errors.New("cannot delete in a read-only transaction")
	}

	req := struct {
		TransactionID string `json:"transaction_id"`
		Key           []byte `json:"key"`
	}{
		TransactionID: tx.id,
		Key:           key,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return false, fmt.Errorf("failed to marshal request: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, tx.client.options.RequestTimeout)
	defer cancel()

	resp, err := tx.client.client.Send(timeoutCtx, transport.NewRequest(transport.TypeTxDelete, reqData))
	if err != nil {
		return false, fmt.Errorf("failed to send request: %w", err)
	}

	var deleteResp struct {
		Success bool `json:"success"`
	}

	if err := json.Unmarshal(resp.Payload(), &deleteResp); err != nil {
		return false, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return deleteResp.Success, nil
}
