// ABOUTME: Tests for telemetry configuration validation, environment variable loading, and default values
// ABOUTME: Ensures configuration behaves correctly with valid and invalid inputs using real config operations

package telemetry

import (
	"os"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Verify default values
	if cfg.ServiceName != "kevo" {
		t.Errorf("Expected default service name 'kevo', got '%s'", cfg.ServiceName)
	}

	if cfg.ServiceVersion != "development" {
		t.Errorf("Expected default service version 'development', got '%s'", cfg.ServiceVersion)
	}

	if !cfg.Enabled {
		t.Error("Expected telemetry to be enabled by default")
	}

	if len(cfg.Exporters) != 1 || cfg.Exporters[0] != "stdout" {
		t.Errorf("Expected default exporters ['stdout'], got %v", cfg.Exporters)
	}

	if cfg.SampleRate != 1.0 {
		t.Errorf("Expected default sample rate 1.0, got %f", cfg.SampleRate)
	}

	if cfg.PrometheusPort != 9090 {
		t.Errorf("Expected default prometheus port 9090, got %d", cfg.PrometheusPort)
	}

	if cfg.OTLPEndpoint != "http://localhost:4317" {
		t.Errorf("Expected default OTLP endpoint 'http://localhost:4317', got '%s'", cfg.OTLPEndpoint)
	}

	if cfg.ExportTimeout != 30*time.Second {
		t.Errorf("Expected default export timeout 30s, got %s", cfg.ExportTimeout)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name:    "valid default config",
			cfg:     DefaultConfig(),
			wantErr: false,
		},
		{
			name: "empty service name",
			cfg: Config{
				ServiceName:        "",
				ServiceVersion:     "1.0.0",
				Enabled:            true,
				Exporters:          []string{"stdout"},
				SampleRate:         1.0,
				PrometheusPort:     9090,
				ExportTimeout:      30 * time.Second,
				BatchTimeout:       5 * time.Second,
				MaxQueueSize:       2048,
				MaxExportBatchSize: 512,
			},
			wantErr: true,
		},
		{
			name: "empty service version",
			cfg: Config{
				ServiceName:        "test",
				ServiceVersion:     "",
				Enabled:            true,
				Exporters:          []string{"stdout"},
				SampleRate:         1.0,
				PrometheusPort:     9090,
				ExportTimeout:      30 * time.Second,
				BatchTimeout:       5 * time.Second,
				MaxQueueSize:       2048,
				MaxExportBatchSize: 512,
			},
			wantErr: true,
		},
		{
			name: "invalid sample rate negative",
			cfg: Config{
				ServiceName:        "test",
				ServiceVersion:     "1.0.0",
				Enabled:            true,
				Exporters:          []string{"stdout"},
				SampleRate:         -0.1,
				PrometheusPort:     9090,
				ExportTimeout:      30 * time.Second,
				BatchTimeout:       5 * time.Second,
				MaxQueueSize:       2048,
				MaxExportBatchSize: 512,
			},
			wantErr: true,
		},
		{
			name: "invalid sample rate too high",
			cfg: Config{
				ServiceName:        "test",
				ServiceVersion:     "1.0.0",
				Enabled:            true,
				Exporters:          []string{"stdout"},
				SampleRate:         1.1,
				PrometheusPort:     9090,
				ExportTimeout:      30 * time.Second,
				BatchTimeout:       5 * time.Second,
				MaxQueueSize:       2048,
				MaxExportBatchSize: 512,
			},
			wantErr: true,
		},
		{
			name: "invalid prometheus port",
			cfg: Config{
				ServiceName:        "test",
				ServiceVersion:     "1.0.0",
				Enabled:            true,
				Exporters:          []string{"stdout"},
				SampleRate:         1.0,
				PrometheusPort:     0,
				ExportTimeout:      30 * time.Second,
				BatchTimeout:       5 * time.Second,
				MaxQueueSize:       2048,
				MaxExportBatchSize: 512,
			},
			wantErr: true,
		},
		{
			name: "invalid exporter",
			cfg: Config{
				ServiceName:        "test",
				ServiceVersion:     "1.0.0",
				Enabled:            true,
				Exporters:          []string{"invalid"},
				SampleRate:         1.0,
				PrometheusPort:     9090,
				ExportTimeout:      30 * time.Second,
				BatchTimeout:       5 * time.Second,
				MaxQueueSize:       2048,
				MaxExportBatchSize: 512,
			},
			wantErr: true,
		},
		{
			name: "invalid export timeout",
			cfg: Config{
				ServiceName:        "test",
				ServiceVersion:     "1.0.0",
				Enabled:            true,
				Exporters:          []string{"stdout"},
				SampleRate:         1.0,
				PrometheusPort:     9090,
				ExportTimeout:      0,
				BatchTimeout:       5 * time.Second,
				MaxQueueSize:       2048,
				MaxExportBatchSize: 512,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfigLoadFromEnv(t *testing.T) {
	// Save original environment
	originalEnv := make(map[string]string)
	envVars := []string{
		"KEVO_TELEMETRY_SERVICE_NAME",
		"KEVO_TELEMETRY_SERVICE_VERSION",
		"KEVO_TELEMETRY_ENABLED",
		"KEVO_TELEMETRY_EXPORTERS",
		"KEVO_TELEMETRY_SAMPLE_RATE",
		"KEVO_TELEMETRY_PROMETHEUS_PORT",
		"KEVO_TELEMETRY_OTLP_ENDPOINT",
		"KEVO_TELEMETRY_EXPORT_TIMEOUT",
	}

	for _, envVar := range envVars {
		originalEnv[envVar] = os.Getenv(envVar)
	}

	// Clean up environment after test
	defer func() {
		for _, envVar := range envVars {
			if originalValue, exists := originalEnv[envVar]; exists {
				os.Setenv(envVar, originalValue)
			} else {
				os.Unsetenv(envVar)
			}
		}
	}()

	// Set test environment variables
	os.Setenv("KEVO_TELEMETRY_SERVICE_NAME", "test-service")
	os.Setenv("KEVO_TELEMETRY_SERVICE_VERSION", "2.0.0")
	os.Setenv("KEVO_TELEMETRY_ENABLED", "false")
	os.Setenv("KEVO_TELEMETRY_EXPORTERS", "prometheus,otlp")
	os.Setenv("KEVO_TELEMETRY_SAMPLE_RATE", "0.5")
	os.Setenv("KEVO_TELEMETRY_PROMETHEUS_PORT", "8080")
	os.Setenv("KEVO_TELEMETRY_OTLP_ENDPOINT", "http://test:4317")
	os.Setenv("KEVO_TELEMETRY_EXPORT_TIMEOUT", "60s")

	cfg := DefaultConfig()
	cfg.LoadFromEnv()

	// Verify environment variables were loaded
	if cfg.ServiceName != "test-service" {
		t.Errorf("Expected service name 'test-service', got '%s'", cfg.ServiceName)
	}

	if cfg.ServiceVersion != "2.0.0" {
		t.Errorf("Expected service version '2.0.0', got '%s'", cfg.ServiceVersion)
	}

	if cfg.Enabled {
		t.Error("Expected telemetry to be disabled")
	}

	expectedExporters := []string{"prometheus", "otlp"}
	if len(cfg.Exporters) != len(expectedExporters) {
		t.Errorf("Expected exporters %v, got %v", expectedExporters, cfg.Exporters)
	}

	if cfg.SampleRate != 0.5 {
		t.Errorf("Expected sample rate 0.5, got %f", cfg.SampleRate)
	}

	if cfg.PrometheusPort != 8080 {
		t.Errorf("Expected prometheus port 8080, got %d", cfg.PrometheusPort)
	}

	if cfg.OTLPEndpoint != "http://test:4317" {
		t.Errorf("Expected OTLP endpoint 'http://test:4317', got '%s'", cfg.OTLPEndpoint)
	}

	if cfg.ExportTimeout != 60*time.Second {
		t.Errorf("Expected export timeout 60s, got %s", cfg.ExportTimeout)
	}
}

func TestConfigHasExporter(t *testing.T) {
	cfg := Config{
		Exporters: []string{"prometheus", "stdout"},
	}

	if !cfg.HasExporter("prometheus") {
		t.Error("Expected HasExporter('prometheus') to return true")
	}

	if !cfg.HasExporter("stdout") {
		t.Error("Expected HasExporter('stdout') to return true")
	}

	if cfg.HasExporter("otlp") {
		t.Error("Expected HasExporter('otlp') to return false")
	}

	if cfg.HasExporter("invalid") {
		t.Error("Expected HasExporter('invalid') to return false")
	}
}

func TestConfigLoadFromEnvInvalidValues(t *testing.T) {
	// Save original environment
	originalEnabled := os.Getenv("KEVO_TELEMETRY_ENABLED")
	originalSampleRate := os.Getenv("KEVO_TELEMETRY_SAMPLE_RATE")
	originalPort := os.Getenv("KEVO_TELEMETRY_PROMETHEUS_PORT")

	defer func() {
		os.Setenv("KEVO_TELEMETRY_ENABLED", originalEnabled)
		os.Setenv("KEVO_TELEMETRY_SAMPLE_RATE", originalSampleRate)
		os.Setenv("KEVO_TELEMETRY_PROMETHEUS_PORT", originalPort)
	}()

	// Test invalid boolean
	os.Setenv("KEVO_TELEMETRY_ENABLED", "invalid")
	cfg := DefaultConfig()
	originalEnabledValue := cfg.Enabled
	cfg.LoadFromEnv()
	if cfg.Enabled != originalEnabledValue {
		t.Error("Invalid boolean should not change the value")
	}

	// Test invalid sample rate
	os.Setenv("KEVO_TELEMETRY_SAMPLE_RATE", "invalid")
	cfg = DefaultConfig()
	originalSampleRateValue := cfg.SampleRate
	cfg.LoadFromEnv()
	if cfg.SampleRate != originalSampleRateValue {
		t.Error("Invalid sample rate should not change the value")
	}

	// Test invalid port
	os.Setenv("KEVO_TELEMETRY_PROMETHEUS_PORT", "invalid")
	cfg = DefaultConfig()
	originalPortValue := cfg.PrometheusPort
	cfg.LoadFromEnv()
	if cfg.PrometheusPort != originalPortValue {
		t.Error("Invalid port should not change the value")
	}
}
