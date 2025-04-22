package transport

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jeremytregunna/kevo/pkg/transport"
)

var (
	ErrPoolClosed      = errors.New("connection pool is closed")
	ErrPoolFull        = errors.New("connection pool is full")
	ErrPoolEmptyNoWait = errors.New("connection pool is empty and wait is disabled")
)

// ConnectionPool manages a pool of gRPC connections
type ConnectionPool struct {
	manager    *GRPCTransportManager
	address    string
	maxIdle    int
	maxActive  int
	idlePool   chan *GRPCConnection
	activePool chan struct{}
	mu         sync.Mutex
	closed     bool
	idleTime   time.Duration
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(manager *GRPCTransportManager, address string, maxIdle, maxActive int, idleTime time.Duration) *ConnectionPool {
	if maxIdle <= 0 {
		maxIdle = 2
	}
	if maxActive <= 0 {
		maxActive = 10
	}
	if idleTime <= 0 {
		idleTime = 5 * time.Minute
	}

	pool := &ConnectionPool{
		manager:    manager,
		address:    address,
		maxIdle:    maxIdle,
		maxActive:  maxActive,
		idlePool:   make(chan *GRPCConnection, maxIdle),
		activePool: make(chan struct{}, maxActive),
		idleTime:   idleTime,
	}

	return pool
}

// Get retrieves a connection from the pool or creates a new one
func (p *ConnectionPool) Get(ctx context.Context, wait bool) (*GRPCConnection, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	p.mu.Unlock()

	// Try to get an idle connection
	select {
	case conn := <-p.idlePool:
		return conn, nil
	default:
		// No idle connections available
	}

	// Check if we can create a new connection
	select {
	case p.activePool <- struct{}{}:
		// We acquired a slot to create a new connection
		conn, err := p.createConnection(ctx)
		if err != nil {
			// If connection creation fails, release the active slot
			<-p.activePool
			return nil, err
		}
		return conn, nil
	default:
		// Pool is full, check if we should wait
		if !wait {
			return nil, ErrPoolEmptyNoWait
		}
	}

	// Wait for a connection to become available or context to expire
	select {
	case conn := <-p.idlePool:
		// Got an idle connection
		return conn, nil
	case p.activePool <- struct{}{}:
		// Got permission to create a new connection
		conn, err := p.createConnection(ctx)
		if err != nil {
			<-p.activePool
			return nil, err
		}
		return conn, nil
	case <-ctx.Done():
		// Context deadline exceeded or canceled
		return nil, ctx.Err()
	}
}

// createConnection creates a new connection to the server
func (p *ConnectionPool) createConnection(ctx context.Context) (*GRPCConnection, error) {
	conn, err := p.manager.Connect(ctx, p.address)
	if err != nil {
		return nil, err
	}

	// Type assert to our concrete connection type
	grpcConn, ok := conn.(*GRPCConnection)
	if !ok {
		conn.Close()
		return nil, errors.New("received incorrect connection type from transport manager")
	}

	return grpcConn, nil
}

// Put returns a connection to the pool
func (p *ConnectionPool) Put(conn *GRPCConnection) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close() // Close the connection since the pool is closed
		<-p.activePool
		return nil
	}
	p.mu.Unlock()

	// Try to add to the idle pool
	select {
	case p.idlePool <- conn:
		// Successfully returned to idle pool
		return nil
	default:
		// Idle pool full, close the connection
		conn.Close()
		<-p.activePool
		return nil
	}
}

// Close closes the connection pool and all idle connections
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPoolClosed
	}

	p.closed = true

	// Close all idle connections
	close(p.idlePool)
	for conn := range p.idlePool {
		conn.Close()
		<-p.activePool
	}

	return nil
}

// ConnectionPoolManager manages multiple connection pools
type ConnectionPoolManager struct {
	manager       *GRPCTransportManager
	pools         sync.Map // map[string]*ConnectionPool
	defaultMaxIdle    int
	defaultMaxActive  int
	defaultIdleTime   time.Duration
}

// NewConnectionPoolManager creates a new connection pool manager
func NewConnectionPoolManager(manager *GRPCTransportManager, maxIdle, maxActive int, idleTime time.Duration) *ConnectionPoolManager {
	return &ConnectionPoolManager{
		manager:          manager,
		defaultMaxIdle:   maxIdle,
		defaultMaxActive: maxActive,
		defaultIdleTime:  idleTime,
	}
}

// GetPool gets or creates a connection pool for the given address
func (m *ConnectionPoolManager) GetPool(address string) *ConnectionPool {
	// Check if pool exists
	if pool, ok := m.pools.Load(address); ok {
		return pool.(*ConnectionPool)
	}

	// Create new pool
	pool := NewConnectionPool(m.manager, address, m.defaultMaxIdle, m.defaultMaxActive, m.defaultIdleTime)
	m.pools.Store(address, pool)
	return pool
}

// CloseAll closes all connection pools
func (m *ConnectionPoolManager) CloseAll() {
	m.pools.Range(func(key, value interface{}) bool {
		pool := value.(*ConnectionPool)
		pool.Close()
		m.pools.Delete(key)
		return true
	})
}

// Client is a wrapper around the gRPC client that supports connection pooling and retries
type Client struct {
	pool      *ConnectionPool
	maxRetries int
	retryDelay time.Duration
}

// NewClient creates a new client with the given connection pool
func NewClient(pool *ConnectionPool, maxRetries int, retryDelay time.Duration) *Client {
	if maxRetries <= 0 {
		maxRetries = 3
	}
	if retryDelay <= 0 {
		retryDelay = 100 * time.Millisecond
	}

	return &Client{
		pool:       pool,
		maxRetries: maxRetries,
		retryDelay: retryDelay,
	}
}

// Execute executes the given function with a connection from the pool
func (c *Client) Execute(ctx context.Context, fn func(ctx context.Context, client interface{}) (interface{}, error)) (interface{}, error) {
	var conn *GRPCConnection
	var err error
	var result interface{}

	// Get a connection from the pool
	for i := 0; i < c.maxRetries; i++ {
		if i > 0 {
			// Wait before retrying
			select {
			case <-time.After(c.retryDelay):
				// Continue with retry
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		conn, err = c.pool.Get(ctx, true)
		if err != nil {
			continue // Try to get another connection
		}

		// Execute the function with the connection
		client := conn.GetClient()
		result, err = fn(ctx, client)
		
		// Return connection to the pool regardless of error
		putErr := c.pool.Put(conn)
		if putErr != nil {
			// Log the error but continue with the original error
			transport.LogError("Failed to return connection to pool: %v", putErr)
		}

		if err == nil {
			// Success, return the result
			return result, nil
		}
	}

	return nil, err
}