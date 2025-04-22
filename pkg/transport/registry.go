package transport

import (
	"fmt"
	"sync"
)

// registry implements the Registry interface
type registry struct {
	mu              sync.RWMutex
	clientFactories map[string]ClientFactory
	serverFactories map[string]ServerFactory
}

// NewRegistry creates a new transport registry
func NewRegistry() Registry {
	return &registry{
		clientFactories: make(map[string]ClientFactory),
		serverFactories: make(map[string]ServerFactory),
	}
}

// DefaultRegistry is the default global registry instance
var DefaultRegistry = NewRegistry()

// RegisterClient adds a new client implementation to the registry
func (r *registry) RegisterClient(name string, factory ClientFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clientFactories[name] = factory
}

// RegisterServer adds a new server implementation to the registry
func (r *registry) RegisterServer(name string, factory ServerFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.serverFactories[name] = factory
}

// CreateClient instantiates a client by name
func (r *registry) CreateClient(name, endpoint string, options TransportOptions) (Client, error) {
	r.mu.RLock()
	factory, exists := r.clientFactories[name]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("transport client %q not registered", name)
	}

	return factory(endpoint, options)
}

// CreateServer instantiates a server by name
func (r *registry) CreateServer(name, address string, options TransportOptions) (Server, error) {
	r.mu.RLock()
	factory, exists := r.serverFactories[name]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("transport server %q not registered", name)
	}

	return factory(address, options)
}

// ListTransports returns all available transport names
func (r *registry) ListTransports() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Get unique transport names
	names := make(map[string]struct{})
	for name := range r.clientFactories {
		names[name] = struct{}{}
	}
	for name := range r.serverFactories {
		names[name] = struct{}{}
	}

	// Convert to slice
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}

	return result
}

// Helper functions for global registry

// RegisterClientTransport registers a client transport with the default registry
func RegisterClientTransport(name string, factory ClientFactory) {
	DefaultRegistry.RegisterClient(name, factory)
}

// RegisterServerTransport registers a server transport with the default registry
func RegisterServerTransport(name string, factory ServerFactory) {
	DefaultRegistry.RegisterServer(name, factory)
}

// GetClient creates a client using the default registry
func GetClient(name, endpoint string, options TransportOptions) (Client, error) {
	return DefaultRegistry.CreateClient(name, endpoint, options)
}

// GetServer creates a server using the default registry
func GetServer(name, address string, options TransportOptions) (Server, error) {
	return DefaultRegistry.CreateServer(name, address, options)
}

// AvailableTransports lists all available transports in the default registry
func AvailableTransports() []string {
	return DefaultRegistry.ListTransports()
}
