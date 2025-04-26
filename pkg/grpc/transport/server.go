package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/transport"
	pb "github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// GRPCServer implements the transport.Server interface for gRPC
type GRPCServer struct {
	address        string
	tlsConfig      *tls.Config
	server         *grpc.Server
	requestHandler transport.RequestHandler
	started        bool
	mu             sync.Mutex
	metrics        *transport.ExtendedMetricsCollector
	connTracker    *connectionTracker
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(address string, options transport.TransportOptions) (transport.Server, error) {
	// Create server options
	var serverOpts []grpc.ServerOption

	// Configure TLS if enabled
	if options.TLSEnabled {
		tlsConfig, err := LoadServerTLSConfig(options.CertFile, options.KeyFile, options.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS config: %w", err)
		}

		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	// Configure keepalive parameters
	kaProps := keepalive.ServerParameters{
		MaxConnectionIdle: 30 * time.Minute,
		MaxConnectionAge:  5 * time.Minute,
		Time:              15 * time.Second,
		Timeout:           5 * time.Second,
	}

	kaPolicy := keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}

	// Add connection tracking interceptor
	connTracker := newConnectionTracker()

	serverOpts = append(serverOpts,
		grpc.KeepaliveParams(kaProps),
		grpc.KeepaliveEnforcementPolicy(kaPolicy),
		grpc.UnaryInterceptor(connTracker.unaryInterceptor),
		grpc.StreamInterceptor(connTracker.streamInterceptor),
	)

	// Create the server
	server := grpc.NewServer(serverOpts...)

	return &GRPCServer{
		address:     address,
		server:      server,
		metrics:     transport.NewMetrics("grpc"),
		connTracker: connTracker,
	}, nil
}

// Start starts the server and returns immediately
func (s *GRPCServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("server already started")
	}

	// Start the server in a goroutine
	go func() {
		if err := s.Serve(); err != nil {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()

	s.started = true
	return nil
}

// Serve starts the server and blocks until it's stopped
func (s *GRPCServer) Serve() error {
	if s.requestHandler == nil {
		return fmt.Errorf("no request handler set")
	}

	// Create the service implementation
	service := &kevoServiceServer{
		handler: s.requestHandler,
	}

	// Register the service
	pb.RegisterKevoServiceServer(s.server, service)

	// Start listening
	listener, err := transport.CreateListener("tcp", s.address, s.tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}

	s.metrics.ServerStarted()

	// Serve requests
	err = s.server.Serve(listener)

	if err != nil {
		s.metrics.ServerErrored()
		return fmt.Errorf("failed to serve: %w", err)
	}

	s.metrics.ServerStopped()
	return nil
}

// Stop stops the server gracefully
func (s *GRPCServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	s.server.GracefulStop()
	s.started = false

	return nil
}

// SetRequestHandler sets the handler for incoming requests
func (s *GRPCServer) SetRequestHandler(handler transport.RequestHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requestHandler = handler

	// Connect the connection tracker to the request handler
	// so it can clean up transactions on disconnection
	if s.connTracker != nil {
		s.connTracker.setRegistry(handler)

		// Set up an interceptor for incoming requests that get the peer info
		fmt.Println("Setting up connection tracking for automatic transaction cleanup")
	}
}

// kevoServiceServer implements the KevoService gRPC service
type kevoServiceServer struct {
	pb.UnimplementedKevoServiceServer
	handler transport.RequestHandler
}

// ConnectionCleanup interface for transaction cleanup on disconnection
type ConnectionCleanup interface {
	CleanupConnection(connectionID string)
}

// ConnectionTracker tracks gRPC connections and notifies of disconnections
type connectionTracker struct {
	connections     sync.Map
	registry        transport.RequestHandler
	cleanupRegistry ConnectionCleanup
}

func newConnectionTracker() *connectionTracker {
	return &connectionTracker{}
}

// setRegistry sets the request handler/registry for cleanup notifications
func (ct *connectionTracker) setRegistry(registry transport.RequestHandler) {
	ct.registry = registry

	// If the registry implements ConnectionCleanup, store it
	if cleaner, ok := registry.(ConnectionCleanup); ok {
		ct.cleanupRegistry = cleaner
	}
}

// generateConnectionID creates a unique connection ID from peer info
func (ct *connectionTracker) generateConnectionID(ctx context.Context) string {
	// Try to get peer info from context
	p, ok := peer.FromContext(ctx)
	if !ok {
		return fmt.Sprintf("unknown-%d", time.Now().UnixNano())
	}
	return p.Addr.String()
}

// trackConnection adds a connection to tracking
func (ct *connectionTracker) trackConnection(ctx context.Context) context.Context {
	connID := ct.generateConnectionID(ctx)
	ct.connections.Store(connID, true)

	// Add connection ID to context for transaction tracking
	return context.WithValue(ctx, "peer", connID)
}

// untrackConnection removes a connection from tracking and cleans up
func (ct *connectionTracker) untrackConnection(ctx context.Context) {
	connID, ok := ctx.Value("peer").(string)
	if !ok {
		return
	}

	ct.connections.Delete(connID)

	// Log the disconnection
	fmt.Printf("Client disconnected: %s\n", connID)

	// Notify registry to clean up transactions for this connection
	if ct.cleanupRegistry != nil {
		fmt.Printf("Cleaning up transactions for connection: %s\n", connID)
		ct.cleanupRegistry.CleanupConnection(connID)
	}
}

// unaryInterceptor is the gRPC interceptor for unary calls
func (ct *connectionTracker) unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	// Track connection
	newCtx := ct.trackConnection(ctx)

	// Handle the request
	resp, err := handler(newCtx, req)

	// Check for errors indicating disconnection
	if err != nil && (err == context.Canceled ||
		status.Code(err) == codes.Canceled ||
		status.Code(err) == codes.Unavailable) {
		ct.untrackConnection(newCtx)
	}

	// If this is a disconnection-related method, trigger cleanup
	if info.FullMethod == "/kevo.KevoService/Close" {
		ct.untrackConnection(newCtx)
	}

	return resp, err
}

// streamInterceptor is the gRPC interceptor for streaming calls
func (ct *connectionTracker) streamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	// Track connection
	newCtx := ct.trackConnection(ss.Context())

	// Wrap the stream with our tracked context
	wrappedStream := &wrappedServerStream{
		ServerStream: ss,
		ctx:          newCtx,
	}

	// Handle the stream
	err := handler(srv, wrappedStream)

	// Check for errors or EOF indicating disconnection
	if err != nil && (err == context.Canceled ||
		status.Code(err) == codes.Canceled ||
		status.Code(err) == codes.Unavailable ||
		err == io.EOF) {
		ct.untrackConnection(newCtx)
	} else if err == nil && info.IsClientStream {
		// For client streams, an EOF without error is normal
		// Let's consider this a client disconnection
		ct.untrackConnection(newCtx)
	}

	return err
}

// wrappedServerStream wraps a grpc.ServerStream with a new context
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the wrapped context
func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// TODO: Implement service methods
