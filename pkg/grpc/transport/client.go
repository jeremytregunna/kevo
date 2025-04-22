package transport

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	pb "github.com/jeremytregunna/kevo/proto/kevo"
	"github.com/jeremytregunna/kevo/pkg/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// GRPCClient implements the transport.Client interface for gRPC
type GRPCClient struct {
	endpoint   string
	options    transport.TransportOptions
	conn       *grpc.ClientConn
	client     pb.KevoServiceClient
	status     transport.TransportStatus
	statusMu   sync.RWMutex
	metrics    transport.MetricsCollector
}

// NewGRPCClient creates a new gRPC client
func NewGRPCClient(endpoint string, options transport.TransportOptions) (transport.Client, error) {
	return &GRPCClient{
		endpoint: endpoint,
		options:  options,
		metrics:  transport.NewMetricsCollector(),
		status: transport.TransportStatus{
			Connected: false,
		},
	}, nil
}

// Connect establishes a connection to the server
func (c *GRPCClient) Connect(ctx context.Context) error {
	dialOptions := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                15 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	// Configure TLS if enabled
	if c.options.TLSEnabled {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		// Load client certificate if provided
		if c.options.CertFile != "" && c.options.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(c.options.CertFile, c.options.KeyFile)
			if err != nil {
				c.metrics.RecordConnection(false)
				return fmt.Errorf("failed to load client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Add credentials to dial options
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		// Use insecure credentials if TLS is not enabled
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Set timeout for connection
	dialCtx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()

	// Connect to the server
	conn, err := grpc.DialContext(dialCtx, c.endpoint, dialOptions...)
	if err != nil {
		c.metrics.RecordConnection(false)
		c.setStatus(false, err)
		return fmt.Errorf("failed to connect to %s: %w", c.endpoint, err)
	}

	c.conn = conn
	c.client = pb.NewKevoServiceClient(conn)
	c.metrics.RecordConnection(true)
	c.setStatus(true, nil)

	return nil
}

// Close closes the connection
func (c *GRPCClient) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.client = nil
		c.setStatus(false, nil)
		return err
	}
	return nil
}

// IsConnected returns whether the client is connected
func (c *GRPCClient) IsConnected() bool {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.status.Connected
}

// Status returns the current status of the connection
func (c *GRPCClient) Status() transport.TransportStatus {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.status
}

// setStatus updates the client status
func (c *GRPCClient) setStatus(connected bool, err error) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	
	c.status.Connected = connected
	c.status.LastError = err
	
	if connected {
		c.status.LastConnected = time.Now()
	}
}

// Send sends a request and waits for a response
func (c *GRPCClient) Send(ctx context.Context, request transport.Request) (transport.Response, error) {
	if !c.IsConnected() {
		return nil, transport.ErrNotConnected
	}

	// Record request metrics
	startTime := time.Now()
	requestType := request.Type()
	
	// Record bytes sent
	requestPayload := request.Payload()
	c.metrics.RecordSend(len(requestPayload))
	
	var resp transport.Response
	var err error

	// Handle request based on type
	switch requestType {
	case transport.TypeGet:
		resp, err = c.handleGet(ctx, requestPayload)
	case transport.TypePut:
		resp, err = c.handlePut(ctx, requestPayload)
	case transport.TypeDelete:
		resp, err = c.handleDelete(ctx, requestPayload)
	case transport.TypeBatchWrite:
		resp, err = c.handleBatchWrite(ctx, requestPayload)
	case transport.TypeBeginTx:
		resp, err = c.handleBeginTransaction(ctx, requestPayload)
	case transport.TypeCommitTx:
		resp, err = c.handleCommitTransaction(ctx, requestPayload)
	case transport.TypeRollbackTx:
		resp, err = c.handleRollbackTransaction(ctx, requestPayload)
	case transport.TypeTxGet:
		resp, err = c.handleTxGet(ctx, requestPayload)
	case transport.TypeTxPut:
		resp, err = c.handleTxPut(ctx, requestPayload)
	case transport.TypeTxDelete:
		resp, err = c.handleTxDelete(ctx, requestPayload)
	case transport.TypeGetStats:
		resp, err = c.handleGetStats(ctx, requestPayload)
	case transport.TypeCompact:
		resp, err = c.handleCompact(ctx, requestPayload)
	default:
		err = fmt.Errorf("unsupported request type: %s", requestType)
		resp = transport.NewErrorResponse(err)
	}

	// Record metrics for the request
	c.metrics.RecordRequest(requestType, startTime, err)
	
	// If we got a response, record received bytes
	if resp != nil {
		c.metrics.RecordReceive(len(resp.Payload()))
	}
	
	return resp, err
}

// Stream opens a bidirectional stream
func (c *GRPCClient) Stream(ctx context.Context) (transport.Stream, error) {
	if !c.IsConnected() {
		return nil, transport.ErrNotConnected
	}

	// For now, we'll implement streaming only for scan operations
	return nil, fmt.Errorf("streaming not fully implemented yet")
}

// Request handler methods
func (c *GRPCClient) handleGet(ctx context.Context, payload []byte) (transport.Response, error) {
	var req struct {
		Key []byte `json:"key"`
	}
	
	if err := json.Unmarshal(payload, &req); err != nil {
		return transport.NewErrorResponse(fmt.Errorf("invalid get request payload: %w", err)), err
	}
	
	grpcReq := &pb.GetRequest{
		Key: req.Key,
	}
	
	grpcResp, err := c.client.Get(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		Value []byte `json:"value"`
		Found bool   `json:"found"`
	}{
		Value: grpcResp.Value,
		Found: grpcResp.Found,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypeGet, respData, nil), nil
}

func (c *GRPCClient) handlePut(ctx context.Context, payload []byte) (transport.Response, error) {
	var req struct {
		Key   []byte `json:"key"`
		Value []byte `json:"value"`
		Sync  bool   `json:"sync"`
	}
	
	if err := json.Unmarshal(payload, &req); err != nil {
		return transport.NewErrorResponse(fmt.Errorf("invalid put request payload: %w", err)), err
	}
	
	grpcReq := &pb.PutRequest{
		Key:   req.Key,
		Value: req.Value,
		Sync:  req.Sync,
	}
	
	grpcResp, err := c.client.Put(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		Success bool `json:"success"`
	}{
		Success: grpcResp.Success,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypePut, respData, nil), nil
}

func (c *GRPCClient) handleDelete(ctx context.Context, payload []byte) (transport.Response, error) {
	var req struct {
		Key  []byte `json:"key"`
		Sync bool   `json:"sync"`
	}
	
	if err := json.Unmarshal(payload, &req); err != nil {
		return transport.NewErrorResponse(fmt.Errorf("invalid delete request payload: %w", err)), err
	}
	
	grpcReq := &pb.DeleteRequest{
		Key:  req.Key,
		Sync: req.Sync,
	}
	
	grpcResp, err := c.client.Delete(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		Success bool `json:"success"`
	}{
		Success: grpcResp.Success,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypeDelete, respData, nil), nil
}

func (c *GRPCClient) handleBatchWrite(ctx context.Context, payload []byte) (transport.Response, error) {
	var req struct {
		Operations []struct {
			Type  string `json:"type"`
			Key   []byte `json:"key"`
			Value []byte `json:"value"`
		} `json:"operations"`
		Sync bool `json:"sync"`
	}
	
	if err := json.Unmarshal(payload, &req); err != nil {
		return transport.NewErrorResponse(fmt.Errorf("invalid batch write request payload: %w", err)), err
	}
	
	operations := make([]*pb.Operation, len(req.Operations))
	for i, op := range req.Operations {
		pbOp := &pb.Operation{
			Key:   op.Key,
			Value: op.Value,
		}
		
		switch op.Type {
		case "put":
			pbOp.Type = pb.Operation_PUT
		case "delete":
			pbOp.Type = pb.Operation_DELETE
		default:
			return transport.NewErrorResponse(fmt.Errorf("invalid operation type: %s", op.Type)), fmt.Errorf("invalid operation type: %s", op.Type)
		}
		
		operations[i] = pbOp
	}
	
	grpcReq := &pb.BatchWriteRequest{
		Operations: operations,
		Sync:       req.Sync,
	}
	
	grpcResp, err := c.client.BatchWrite(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		Success bool `json:"success"`
	}{
		Success: grpcResp.Success,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypeBatchWrite, respData, nil), nil
}

func (c *GRPCClient) handleBeginTransaction(ctx context.Context, payload []byte) (transport.Response, error) {
	var req struct {
		ReadOnly bool `json:"read_only"`
	}
	
	if err := json.Unmarshal(payload, &req); err != nil {
		return transport.NewErrorResponse(fmt.Errorf("invalid begin transaction request payload: %w", err)), err
	}
	
	grpcReq := &pb.BeginTransactionRequest{
		ReadOnly: req.ReadOnly,
	}
	
	grpcResp, err := c.client.BeginTransaction(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		TransactionID string `json:"transaction_id"`
	}{
		TransactionID: grpcResp.TransactionId,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypeBeginTx, respData, nil), nil
}

func (c *GRPCClient) handleCommitTransaction(ctx context.Context, payload []byte) (transport.Response, error) {
	var req struct {
		TransactionID string `json:"transaction_id"`
	}
	
	if err := json.Unmarshal(payload, &req); err != nil {
		return transport.NewErrorResponse(fmt.Errorf("invalid commit transaction request payload: %w", err)), err
	}
	
	grpcReq := &pb.CommitTransactionRequest{
		TransactionId: req.TransactionID,
	}
	
	grpcResp, err := c.client.CommitTransaction(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		Success bool `json:"success"`
	}{
		Success: grpcResp.Success,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypeCommitTx, respData, nil), nil
}

func (c *GRPCClient) handleRollbackTransaction(ctx context.Context, payload []byte) (transport.Response, error) {
	var req struct {
		TransactionID string `json:"transaction_id"`
	}
	
	if err := json.Unmarshal(payload, &req); err != nil {
		return transport.NewErrorResponse(fmt.Errorf("invalid rollback transaction request payload: %w", err)), err
	}
	
	grpcReq := &pb.RollbackTransactionRequest{
		TransactionId: req.TransactionID,
	}
	
	grpcResp, err := c.client.RollbackTransaction(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		Success bool `json:"success"`
	}{
		Success: grpcResp.Success,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypeRollbackTx, respData, nil), nil
}

func (c *GRPCClient) handleTxGet(ctx context.Context, payload []byte) (transport.Response, error) {
	var req struct {
		TransactionID string `json:"transaction_id"`
		Key           []byte `json:"key"`
	}
	
	if err := json.Unmarshal(payload, &req); err != nil {
		return transport.NewErrorResponse(fmt.Errorf("invalid tx get request payload: %w", err)), err
	}
	
	grpcReq := &pb.TxGetRequest{
		TransactionId: req.TransactionID,
		Key:           req.Key,
	}
	
	grpcResp, err := c.client.TxGet(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		Value []byte `json:"value"`
		Found bool   `json:"found"`
	}{
		Value: grpcResp.Value,
		Found: grpcResp.Found,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypeTxGet, respData, nil), nil
}

func (c *GRPCClient) handleTxPut(ctx context.Context, payload []byte) (transport.Response, error) {
	var req struct {
		TransactionID string `json:"transaction_id"`
		Key           []byte `json:"key"`
		Value         []byte `json:"value"`
	}
	
	if err := json.Unmarshal(payload, &req); err != nil {
		return transport.NewErrorResponse(fmt.Errorf("invalid tx put request payload: %w", err)), err
	}
	
	grpcReq := &pb.TxPutRequest{
		TransactionId: req.TransactionID,
		Key:           req.Key,
		Value:         req.Value,
	}
	
	grpcResp, err := c.client.TxPut(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		Success bool `json:"success"`
	}{
		Success: grpcResp.Success,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypeTxPut, respData, nil), nil
}

func (c *GRPCClient) handleTxDelete(ctx context.Context, payload []byte) (transport.Response, error) {
	var req struct {
		TransactionID string `json:"transaction_id"`
		Key           []byte `json:"key"`
	}
	
	if err := json.Unmarshal(payload, &req); err != nil {
		return transport.NewErrorResponse(fmt.Errorf("invalid tx delete request payload: %w", err)), err
	}
	
	grpcReq := &pb.TxDeleteRequest{
		TransactionId: req.TransactionID,
		Key:           req.Key,
	}
	
	grpcResp, err := c.client.TxDelete(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		Success bool `json:"success"`
	}{
		Success: grpcResp.Success,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypeTxDelete, respData, nil), nil
}

func (c *GRPCClient) handleGetStats(ctx context.Context, payload []byte) (transport.Response, error) {
	grpcReq := &pb.GetStatsRequest{}
	
	grpcResp, err := c.client.GetStats(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		KeyCount           int64   `json:"key_count"`
		StorageSize        int64   `json:"storage_size"`
		MemtableCount      int32   `json:"memtable_count"`
		SstableCount       int32   `json:"sstable_count"`
		WriteAmplification float64 `json:"write_amplification"`
		ReadAmplification  float64 `json:"read_amplification"`
	}{
		KeyCount:           grpcResp.KeyCount,
		StorageSize:        grpcResp.StorageSize,
		MemtableCount:      grpcResp.MemtableCount,
		SstableCount:       grpcResp.SstableCount,
		WriteAmplification: grpcResp.WriteAmplification,
		ReadAmplification:  grpcResp.ReadAmplification,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypeGetStats, respData, nil), nil
}

func (c *GRPCClient) handleCompact(ctx context.Context, payload []byte) (transport.Response, error) {
	var req struct {
		Force bool `json:"force"`
	}
	
	if err := json.Unmarshal(payload, &req); err != nil {
		return transport.NewErrorResponse(fmt.Errorf("invalid compact request payload: %w", err)), err
	}
	
	grpcReq := &pb.CompactRequest{
		Force: req.Force,
	}
	
	grpcResp, err := c.client.Compact(ctx, grpcReq)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	resp := struct {
		Success bool `json:"success"`
	}{
		Success: grpcResp.Success,
	}
	
	respData, err := json.Marshal(resp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	return transport.NewResponse(transport.TypeCompact, respData, nil), nil
}

// GRPCScanStream implements the transport.Stream interface for scan operations
type GRPCScanStream struct {
	ctx        context.Context
	cancel     context.CancelFunc
	stream     pb.KevoService_ScanClient
	client     *GRPCClient
	streamType string
}

func (s *GRPCScanStream) Send(request transport.Request) error {
	return fmt.Errorf("sending to scan stream not supported")
}

func (s *GRPCScanStream) Recv() (transport.Response, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return transport.NewErrorResponse(err), err
	}
	
	// Build response based on scan type
	scanResp := struct {
		Key   []byte `json:"key"`
		Value []byte `json:"value"`
	}{
		Key:   resp.Key,
		Value: resp.Value,
	}
	
	respData, err := json.Marshal(scanResp)
	if err != nil {
		return transport.NewErrorResponse(err), err
	}
	
	s.client.metrics.RecordReceive(len(respData))
	return transport.NewResponse(s.streamType, respData, nil), nil
}

func (s *GRPCScanStream) Close() error {
	s.cancel()
	return nil
}