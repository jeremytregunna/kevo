package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

// TLSConfig holds TLS configuration settings
type TLSConfig struct {
	CertFile   string
	KeyFile    string
	CAFile     string
	SkipVerify bool
}

// LoadServerTLSConfig loads TLS configuration for server
func LoadServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	// Check if both cert and key files are provided
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("both certificate and key files must be provided")
	}

	// Load server certificate and key
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server key pair: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Load CA if provided for client authentication
	if caFile != "" {
		caBytes, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caBytes) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.ClientCAs = certPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsConfig, nil
}

// LoadClientTLSConfig loads TLS configuration for client
func LoadClientTLSConfig(certFile, keyFile, caFile string, skipVerify bool) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: skipVerify,
	}

	// Load client certificate and key if provided
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA if provided for server authentication
	if caFile != "" {
		caBytes, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caBytes) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.RootCAs = certPool
	}

	return tlsConfig, nil
}

// LoadClientTLSConfigFromStruct is a convenience method to load TLS config from TLSConfig struct
func LoadClientTLSConfigFromStruct(config *TLSConfig) (*tls.Config, error) {
	if config == nil {
		return &tls.Config{MinVersion: tls.VersionTLS12}, nil
	}
	return LoadClientTLSConfig(config.CertFile, config.KeyFile, config.CAFile, config.SkipVerify)
}