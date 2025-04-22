package transport

import (
	"crypto/tls"
	"net"
)

// CreateListener creates a network listener with optional TLS
func CreateListener(network, address string, tlsConfig *tls.Config) (net.Listener, error) {
	// Create the listener
	listener, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}

	// If TLS is configured, wrap the listener
	if tlsConfig != nil {
		listener = tls.NewListener(listener, tlsConfig)
	}

	return listener, nil
}
