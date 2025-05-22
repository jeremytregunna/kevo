// ABOUTME: Tests for engine facade telemetry integration using real engine operations with telemetry
// ABOUTME: Validates telemetry initialization, configuration loading, and proper cleanup using real engine components

package engine

import (
	"os"
	"testing"
)

func TestEngineFacadeWithTelemetry(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "engine-telemetry-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create engine - should initialize telemetry from config
	engine, err := NewEngineFacade(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Verify telemetry was initialized
	if engine.telemetry == nil {
		t.Error("Expected telemetry to be initialized")
	}

	// Test that engine operations work with telemetry
	testKey := []byte("test-key")
	testValue := []byte("test-value")

	// Test Put operation
	if err := engine.Put(testKey, testValue); err != nil {
		t.Errorf("Put operation failed: %v", err)
	}

	// Test Get operation
	retrievedValue, err := engine.Get(testKey)
	if err != nil {
		t.Errorf("Get operation failed: %v", err)
	}

	if string(retrievedValue) != string(testValue) {
		t.Errorf("Expected value '%s', got '%s'", testValue, retrievedValue)
	}

	// Test Delete operation
	if err := engine.Delete(testKey); err != nil {
		t.Errorf("Delete operation failed: %v", err)
	}
}

func TestEngineFacadeWithDisabledTelemetry(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "engine-no-telemetry-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Set environment to disable telemetry
	originalEnabled := os.Getenv("KEVO_TELEMETRY_ENABLED")
	defer os.Setenv("KEVO_TELEMETRY_ENABLED", originalEnabled)
	os.Setenv("KEVO_TELEMETRY_ENABLED", "false")

	// Create engine with disabled telemetry
	engine, err := NewEngineFacade(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Verify telemetry was initialized (should be no-op)
	if engine.telemetry == nil {
		t.Error("Expected telemetry to be initialized (as no-op)")
	}

	// Test that engine operations still work with disabled telemetry
	testKey := []byte("test-key-disabled")
	testValue := []byte("test-value-disabled")

	if err := engine.Put(testKey, testValue); err != nil {
		t.Errorf("Put operation failed with disabled telemetry: %v", err)
	}

	retrievedValue, err := engine.Get(testKey)
	if err != nil {
		t.Errorf("Get operation failed with disabled telemetry: %v", err)
	}

	if string(retrievedValue) != string(testValue) {
		t.Errorf("Expected value '%s', got '%s'", testValue, retrievedValue)
	}
}

func TestEngineFacadeWithCustomTelemetryConfig(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "engine-custom-telemetry-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Set custom telemetry environment variables
	originalServiceName := os.Getenv("KEVO_TELEMETRY_SERVICE_NAME")
	originalServiceVersion := os.Getenv("KEVO_TELEMETRY_SERVICE_VERSION")
	defer func() {
		os.Setenv("KEVO_TELEMETRY_SERVICE_NAME", originalServiceName)
		os.Setenv("KEVO_TELEMETRY_SERVICE_VERSION", originalServiceVersion)
	}()

	os.Setenv("KEVO_TELEMETRY_SERVICE_NAME", "test-engine")
	os.Setenv("KEVO_TELEMETRY_SERVICE_VERSION", "1.0.0-test")

	// Create engine with custom telemetry config
	engine, err := NewEngineFacade(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Verify telemetry was initialized
	if engine.telemetry == nil {
		t.Error("Expected telemetry to be initialized")
	}

	// Test engine operations work with custom config
	testKey := []byte("test-key-custom")
	testValue := []byte("test-value-custom")

	if err := engine.Put(testKey, testValue); err != nil {
		t.Errorf("Put operation failed with custom telemetry: %v", err)
	}
}

func TestEngineFacadeBackwardsCompatibility(t *testing.T) {
	// Test that the legacy NewEngine function still works
	tmpDir, err := os.MkdirTemp("", "engine-legacy-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use legacy constructor
	engine, err := NewEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine with legacy constructor: %v", err)
	}
	defer engine.Close()

	// Verify telemetry was still initialized
	if engine.telemetry == nil {
		t.Error("Expected telemetry to be initialized even with legacy constructor")
	}

	// Test basic operations
	testKey := []byte("legacy-test-key")
	testValue := []byte("legacy-test-value")

	if err := engine.Put(testKey, testValue); err != nil {
		t.Errorf("Put operation failed with legacy constructor: %v", err)
	}

	retrievedValue, err := engine.Get(testKey)
	if err != nil {
		t.Errorf("Get operation failed with legacy constructor: %v", err)
	}

	if string(retrievedValue) != string(testValue) {
		t.Errorf("Expected value '%s', got '%s'", testValue, retrievedValue)
	}
}

func TestEngineFacadeTelemetryShutdown(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "engine-shutdown-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create engine
	engine, err := NewEngineFacade(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Verify telemetry is initialized
	if engine.telemetry == nil {
		t.Error("Expected telemetry to be initialized")
	}

	// Test shutdown - should not fail even with telemetry
	if err := engine.Close(); err != nil {
		t.Errorf("Engine close failed: %v", err)
	}

	// Test double close (should be safe)
	if err := engine.Close(); err != nil {
		t.Errorf("Double close should be safe: %v", err)
	}
}
