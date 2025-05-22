// ABOUTME: Tests for telemetry provider creation and configuration handling using real provider operations
// ABOUTME: Validates provider initialization, configuration validation, and no-op fallback behavior

package telemetry

import (
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Config
		expectNoop  bool
		expectError bool
	}{
		{
			name:        "disabled telemetry returns noop",
			cfg:         Config{Enabled: false},
			expectNoop:  true,
			expectError: false,
		},
		{
			name: "invalid config returns error",
			cfg: Config{
				Enabled:     true,
				ServiceName: "", // Invalid: empty service name
			},
			expectNoop:  false,
			expectError: true,
		},
		{
			name: "valid config returns noop (current implementation)",
			cfg: Config{
				ServiceName:        "test",
				ServiceVersion:     "1.0.0",
				Enabled:            true,
				Exporters:          []string{"stdout"},
				SampleRate:         1.0,
				PrometheusPort:     9090,
				OTLPEndpoint:       "http://localhost:4317",
				ExportTimeout:      DefaultConfig().ExportTimeout,
				BatchTimeout:       DefaultConfig().BatchTimeout,
				MaxQueueSize:       DefaultConfig().MaxQueueSize,
				MaxExportBatchSize: DefaultConfig().MaxExportBatchSize,
			},
			expectNoop:  true, // Current implementation returns noop
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tel, err := New(tt.cfg)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tel == nil {
				t.Error("Expected telemetry instance but got nil")
				return
			}

			// For now, all valid configs return noop implementation
			// This test will need to be updated when full implementation is added
			if tt.expectNoop {
				// Verify it behaves like noop by testing operations don't panic
				tel.RecordHistogram(nil, "test", 1.0)
				tel.RecordCounter(nil, "test", 1)
			}
		})
	}
}

func TestNewWithDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	tel, err := New(cfg)

	if err != nil {
		t.Errorf("Unexpected error with default config: %v", err)
	}

	if tel == nil {
		t.Error("Expected telemetry instance but got nil")
	}

	// Test that operations work without panicking
	tel.RecordHistogram(nil, "test.histogram", 1.5)
	tel.RecordCounter(nil, "test.counter", 10)

	if err := tel.Shutdown(nil); err != nil {
		t.Errorf("Shutdown failed: %v", err)
	}
}

func TestNewWithInvalidConfigs(t *testing.T) {
	invalidConfigs := []Config{
		{
			Enabled:     true,
			ServiceName: "", // Empty service name
		},
		{
			Enabled:        true,
			ServiceName:    "test",
			ServiceVersion: "", // Empty service version
		},
		{
			Enabled:        true,
			ServiceName:    "test",
			ServiceVersion: "1.0.0",
			SampleRate:     -0.1, // Invalid sample rate
		},
		{
			Enabled:        true,
			ServiceName:    "test",
			ServiceVersion: "1.0.0",
			SampleRate:     1.1, // Invalid sample rate
		},
		{
			Enabled:        true,
			ServiceName:    "test",
			ServiceVersion: "1.0.0",
			SampleRate:     1.0,
			PrometheusPort: 0, // Invalid port
		},
	}

	for i, cfg := range invalidConfigs {
		t.Run(fmt.Sprintf("invalid_config_%d", i), func(t *testing.T) {
			tel, err := New(cfg)

			if err == nil {
				t.Error("Expected error for invalid config but got none")
			}

			if tel != nil {
				t.Error("Expected nil telemetry for invalid config but got instance")
			}
		})
	}
}
