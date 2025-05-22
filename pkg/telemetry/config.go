// ABOUTME: Configuration structures for telemetry setup including exporters, sampling, and validation
// ABOUTME: Supports environment variable overrides and provides sensible defaults for all telemetry options

package telemetry

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for telemetry providers and exporters.
type Config struct {
	// ServiceName identifies the service in telemetry data
	ServiceName string `json:"service_name"`

	// ServiceVersion identifies the service version in telemetry data
	ServiceVersion string `json:"service_version"`

	// Enabled controls whether telemetry is active
	Enabled bool `json:"enabled"`

	// Exporters specifies which exporters to use (prometheus, otlp, jaeger, stdout)
	Exporters []string `json:"exporters"`

	// SampleRate controls trace sampling (0.0 to 1.0)
	SampleRate float64 `json:"sample_rate"`

	// PrometheusPort specifies the port for Prometheus metrics endpoint
	PrometheusPort int `json:"prometheus_port"`

	// OTLPEndpoint specifies the OTLP collector endpoint
	OTLPEndpoint string `json:"otlp_endpoint"`

	// JaegerEndpoint specifies the Jaeger collector endpoint
	JaegerEndpoint string `json:"jaeger_endpoint"`

	// ExportTimeout controls how long to wait for exports
	ExportTimeout time.Duration `json:"export_timeout"`

	// BatchTimeout controls how long to wait before exporting a batch
	BatchTimeout time.Duration `json:"batch_timeout"`

	// MaxQueueSize controls the maximum queue size for pending exports
	MaxQueueSize int `json:"max_queue_size"`

	// MaxExportBatchSize controls the maximum batch size for exports
	MaxExportBatchSize int `json:"max_export_batch_size"`
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() Config {
	return Config{
		ServiceName:        "kevo",
		ServiceVersion:     "development",
		Enabled:            true,
		Exporters:          []string{"stdout"},
		SampleRate:         1.0,
		PrometheusPort:     9090,
		OTLPEndpoint:       "http://localhost:4317",
		JaegerEndpoint:     "http://localhost:14268/api/traces",
		ExportTimeout:      30 * time.Second,
		BatchTimeout:       5 * time.Second,
		MaxQueueSize:       2048,
		MaxExportBatchSize: 512,
	}
}

// LoadFromEnv loads configuration from environment variables, overriding defaults.
func (c *Config) LoadFromEnv() {
	if val := os.Getenv("KEVO_TELEMETRY_SERVICE_NAME"); val != "" {
		c.ServiceName = val
	}

	if val := os.Getenv("KEVO_TELEMETRY_SERVICE_VERSION"); val != "" {
		c.ServiceVersion = val
	}

	if val := os.Getenv("KEVO_TELEMETRY_ENABLED"); val != "" {
		if enabled, err := strconv.ParseBool(val); err == nil {
			c.Enabled = enabled
		}
	}

	if val := os.Getenv("KEVO_TELEMETRY_EXPORTERS"); val != "" {
		c.Exporters = strings.Split(val, ",")
		for i := range c.Exporters {
			c.Exporters[i] = strings.TrimSpace(c.Exporters[i])
		}
	}

	if val := os.Getenv("KEVO_TELEMETRY_SAMPLE_RATE"); val != "" {
		if rate, err := strconv.ParseFloat(val, 64); err == nil {
			c.SampleRate = rate
		}
	}

	if val := os.Getenv("KEVO_TELEMETRY_PROMETHEUS_PORT"); val != "" {
		if port, err := strconv.Atoi(val); err == nil {
			c.PrometheusPort = port
		}
	}

	if val := os.Getenv("KEVO_TELEMETRY_OTLP_ENDPOINT"); val != "" {
		c.OTLPEndpoint = val
	}

	if val := os.Getenv("KEVO_TELEMETRY_JAEGER_ENDPOINT"); val != "" {
		c.JaegerEndpoint = val
	}

	if val := os.Getenv("KEVO_TELEMETRY_EXPORT_TIMEOUT"); val != "" {
		if timeout, err := time.ParseDuration(val); err == nil {
			c.ExportTimeout = timeout
		}
	}

	if val := os.Getenv("KEVO_TELEMETRY_BATCH_TIMEOUT"); val != "" {
		if timeout, err := time.ParseDuration(val); err == nil {
			c.BatchTimeout = timeout
		}
	}

	if val := os.Getenv("KEVO_TELEMETRY_MAX_QUEUE_SIZE"); val != "" {
		if size, err := strconv.Atoi(val); err == nil {
			c.MaxQueueSize = size
		}
	}

	if val := os.Getenv("KEVO_TELEMETRY_MAX_EXPORT_BATCH_SIZE"); val != "" {
		if size, err := strconv.Atoi(val); err == nil {
			c.MaxExportBatchSize = size
		}
	}
}

// Validate checks the configuration for invalid values and returns an error if found.
func (c *Config) Validate() error {
	if c.ServiceName == "" {
		return fmt.Errorf("service_name cannot be empty")
	}

	if c.ServiceVersion == "" {
		return fmt.Errorf("service_version cannot be empty")
	}

	if c.SampleRate < 0.0 || c.SampleRate > 1.0 {
		return fmt.Errorf("sample_rate must be between 0.0 and 1.0, got %f", c.SampleRate)
	}

	if c.PrometheusPort < 1 || c.PrometheusPort > 65535 {
		return fmt.Errorf("prometheus_port must be between 1 and 65535, got %d", c.PrometheusPort)
	}

	if c.ExportTimeout <= 0 {
		return fmt.Errorf("export_timeout must be positive, got %s", c.ExportTimeout)
	}

	if c.BatchTimeout <= 0 {
		return fmt.Errorf("batch_timeout must be positive, got %s", c.BatchTimeout)
	}

	if c.MaxQueueSize <= 0 {
		return fmt.Errorf("max_queue_size must be positive, got %d", c.MaxQueueSize)
	}

	if c.MaxExportBatchSize <= 0 {
		return fmt.Errorf("max_export_batch_size must be positive, got %d", c.MaxExportBatchSize)
	}

	// Validate exporter names
	validExporters := map[string]bool{
		"prometheus": true,
		"otlp":       true,
		"jaeger":     true,
		"stdout":     true,
	}

	for _, exporter := range c.Exporters {
		if !validExporters[exporter] {
			return fmt.Errorf("invalid exporter: %s, valid options are: prometheus, otlp, jaeger, stdout", exporter)
		}
	}

	return nil
}

// HasExporter returns true if the specified exporter is configured.
func (c *Config) HasExporter(name string) bool {
	for _, exporter := range c.Exporters {
		if exporter == name {
			return true
		}
	}
	return false
}
