package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/KevoDB/kevo/pkg/transport"
)

// ScanOptions configures a scan operation
type ScanOptions struct {
	// Prefix limit the scan to keys with this prefix
	Prefix []byte
	// Suffix limit the scan to keys with this suffix
	Suffix []byte
	// StartKey sets the starting point for the scan (inclusive)
	StartKey []byte
	// EndKey sets the ending point for the scan (exclusive)
	EndKey []byte
	// Limit sets the maximum number of key-value pairs to return
	Limit int32
}

// KeyValue represents a key-value pair from a scan
type KeyValue struct {
	Key   []byte
	Value []byte
}

// Scanner interface for iterating through keys and values
type Scanner interface {
	// Next advances the scanner to the next key-value pair
	Next() bool
	// Key returns the current key
	Key() []byte
	// Value returns the current value
	Value() []byte
	// Error returns any error that occurred during iteration
	Error() error
	// Close releases resources associated with the scanner
	Close() error
}

// scanIterator implements the Scanner interface for regular scans
type scanIterator struct {
	client     *Client
	options    ScanOptions
	stream     transport.Stream
	current    *KeyValue
	err        error
	closed     bool
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// Scan creates a scanner to iterate over keys in the database
func (c *Client) Scan(ctx context.Context, options ScanOptions) (Scanner, error) {
	if !c.IsConnected() {
		return nil, errors.New("not connected to server")
	}

	// Use the provided context directly for streaming operations

	// Implement stream request
	streamCtx, streamCancel := context.WithCancel(ctx)

	stream, err := c.client.Stream(streamCtx)
	if err != nil {
		streamCancel()
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	// Create the scan request
	req := struct {
		Prefix   []byte `json:"prefix"`
		Suffix   []byte `json:"suffix"`
		StartKey []byte `json:"start_key"`
		EndKey   []byte `json:"end_key"`
		Limit    int32  `json:"limit"`
	}{
		Prefix:   options.Prefix,
		Suffix:   options.Suffix,
		StartKey: options.StartKey,
		EndKey:   options.EndKey,
		Limit:    options.Limit,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		streamCancel()
		stream.Close()
		return nil, fmt.Errorf("failed to marshal scan request: %w", err)
	}

	// Send the scan request
	if err := stream.Send(transport.NewRequest(transport.TypeScan, reqData)); err != nil {
		streamCancel()
		stream.Close()
		return nil, fmt.Errorf("failed to send scan request: %w", err)
	}

	// Create the iterator
	iter := &scanIterator{
		client:     c,
		options:    options,
		stream:     stream,
		ctx:        streamCtx,
		cancelFunc: streamCancel,
	}

	return iter, nil
}

// Next advances the iterator to the next key-value pair
func (s *scanIterator) Next() bool {
	if s.closed || s.err != nil {
		return false
	}

	resp, err := s.stream.Recv()
	if err != nil {
		if err != io.EOF {
			s.err = fmt.Errorf("error receiving scan response: %w", err)
		}
		return false
	}

	// Parse the response
	var scanResp struct {
		Key   []byte `json:"key"`
		Value []byte `json:"value"`
	}

	if err := json.Unmarshal(resp.Payload(), &scanResp); err != nil {
		s.err = fmt.Errorf("failed to unmarshal scan response: %w", err)
		return false
	}

	s.current = &KeyValue{
		Key:   scanResp.Key,
		Value: scanResp.Value,
	}
	return true
}

// Key returns the current key
func (s *scanIterator) Key() []byte {
	if s.current == nil {
		return nil
	}
	return s.current.Key
}

// Value returns the current value
func (s *scanIterator) Value() []byte {
	if s.current == nil {
		return nil
	}
	return s.current.Value
}

// Error returns any error that occurred during iteration
func (s *scanIterator) Error() error {
	return s.err
}

// Close releases resources associated with the scanner
func (s *scanIterator) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.cancelFunc()
	return s.stream.Close()
}

// transactionScanIterator implements the Scanner interface for transaction scans
type transactionScanIterator struct {
	tx         *Transaction
	options    ScanOptions
	stream     transport.Stream
	current    *KeyValue
	err        error
	closed     bool
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// Scan creates a scanner to iterate over keys in the transaction
func (tx *Transaction) Scan(ctx context.Context, options ScanOptions) (Scanner, error) {
	if tx.closed {
		return nil, ErrTransactionClosed
	}

	// Use the provided context directly for streaming operations

	// Implement transaction stream request
	streamCtx, streamCancel := context.WithCancel(ctx)

	stream, err := tx.client.client.Stream(streamCtx)
	if err != nil {
		streamCancel()
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	// Create the transaction scan request
	req := struct {
		TransactionID string `json:"transaction_id"`
		Prefix        []byte `json:"prefix"`
		Suffix        []byte `json:"suffix"`
		StartKey      []byte `json:"start_key"`
		EndKey        []byte `json:"end_key"`
		Limit         int32  `json:"limit"`
	}{
		TransactionID: tx.id,
		Prefix:        options.Prefix,
		Suffix:        options.Suffix,
		StartKey:      options.StartKey,
		EndKey:        options.EndKey,
		Limit:         options.Limit,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		streamCancel()
		stream.Close()
		return nil, fmt.Errorf("failed to marshal transaction scan request: %w", err)
	}

	// Send the transaction scan request
	if err := stream.Send(transport.NewRequest(transport.TypeTxScan, reqData)); err != nil {
		streamCancel()
		stream.Close()
		return nil, fmt.Errorf("failed to send transaction scan request: %w", err)
	}

	// Create the iterator
	iter := &transactionScanIterator{
		tx:         tx,
		options:    options,
		stream:     stream,
		ctx:        streamCtx,
		cancelFunc: streamCancel,
	}

	return iter, nil
}

// Next advances the iterator to the next key-value pair
func (s *transactionScanIterator) Next() bool {
	if s.closed || s.err != nil {
		return false
	}

	resp, err := s.stream.Recv()
	if err != nil {
		if err != io.EOF {
			s.err = fmt.Errorf("error receiving transaction scan response: %w", err)
		}
		return false
	}

	// Parse the response
	var scanResp struct {
		Key   []byte `json:"key"`
		Value []byte `json:"value"`
	}

	if err := json.Unmarshal(resp.Payload(), &scanResp); err != nil {
		s.err = fmt.Errorf("failed to unmarshal transaction scan response: %w", err)
		return false
	}

	s.current = &KeyValue{
		Key:   scanResp.Key,
		Value: scanResp.Value,
	}
	return true
}

// Key returns the current key
func (s *transactionScanIterator) Key() []byte {
	if s.current == nil {
		return nil
	}
	return s.current.Key
}

// Value returns the current value
func (s *transactionScanIterator) Value() []byte {
	if s.current == nil {
		return nil
	}
	return s.current.Value
}

// Error returns any error that occurred during iteration
func (s *transactionScanIterator) Error() error {
	return s.err
}

// Close releases resources associated with the scanner
func (s *transactionScanIterator) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.cancelFunc()
	return s.stream.Close()
}
