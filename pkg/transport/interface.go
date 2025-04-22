package transport

import (
	"context"
	"time"
)

// CompressionType defines the compression algorithm used
type CompressionType string

// Standard compression options
const (
	CompressionNone   CompressionType = "none"
	CompressionGzip   CompressionType = "gzip"
	CompressionSnappy CompressionType = "snappy"
)

// RetryPolicy defines how retries are handled
type RetryPolicy struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	Jitter         float64
}

// TransportOptions contains common configuration across all transport types
type TransportOptions struct {
	Timeout        time.Duration
	RetryPolicy    RetryPolicy
	Compression    CompressionType
	MaxMessageSize int
	TLSEnabled     bool
	CertFile       string
	KeyFile        string
	CAFile         string
}

// TransportStatus contains information about the current transport state
type TransportStatus struct {
	Connected     bool
	LastConnected time.Time
	LastError     error
	BytesSent     uint64
	BytesReceived uint64
	RTT           time.Duration
}

// Request represents a generic request to the transport layer
type Request interface {
	// Type returns the type of request
	Type() string
	
	// Payload returns the payload of the request
	Payload() []byte
}

// Response represents a generic response from the transport layer
type Response interface {
	// Type returns the type of response
	Type() string
	
	// Payload returns the payload of the response
	Payload() []byte
	
	// Error returns any error associated with the response
	Error() error
}

// Stream represents a bidirectional stream of messages
type Stream interface {
	// Send sends a request over the stream
	Send(request Request) error
	
	// Recv receives a response from the stream
	Recv() (Response, error)
	
	// Close closes the stream
	Close() error
}

// Client defines the client interface for any transport implementation
type Client interface {
	// Connect establishes a connection to the server
	Connect(ctx context.Context) error
	
	// Close closes the connection
	Close() error
	
	// IsConnected returns whether the client is connected
	IsConnected() bool
	
	// Status returns the current status of the connection
	Status() TransportStatus
	
	// Send sends a request and waits for a response
	Send(ctx context.Context, request Request) (Response, error)
	
	// Stream opens a bidirectional stream
	Stream(ctx context.Context) (Stream, error)
}

// RequestHandler processes incoming requests
type RequestHandler interface {
	// HandleRequest processes a request and returns a response
	HandleRequest(ctx context.Context, request Request) (Response, error)
	
	// HandleStream processes a bidirectional stream
	HandleStream(stream Stream) error
}

// Server defines the server interface for any transport implementation
type Server interface {
	// Start starts the server and returns immediately
	Start() error
	
	// Serve starts the server and blocks until it's stopped
	Serve() error
	
	// Stop stops the server gracefully
	Stop(ctx context.Context) error
	
	// SetRequestHandler sets the handler for incoming requests
	SetRequestHandler(handler RequestHandler)
}

// ClientFactory creates a new client
type ClientFactory func(endpoint string, options TransportOptions) (Client, error)

// ServerFactory creates a new server
type ServerFactory func(address string, options TransportOptions) (Server, error)

// Registry keeps track of available transport implementations
type Registry interface {
	// RegisterClient adds a new client implementation to the registry
	RegisterClient(name string, factory ClientFactory)
	
	// RegisterServer adds a new server implementation to the registry
	RegisterServer(name string, factory ServerFactory)
	
	// CreateClient instantiates a client by name
	CreateClient(name, endpoint string, options TransportOptions) (Client, error)
	
	// CreateServer instantiates a server by name
	CreateServer(name, address string, options TransportOptions) (Server, error)
	
	// ListTransports returns all available transport names
	ListTransports() []string
}