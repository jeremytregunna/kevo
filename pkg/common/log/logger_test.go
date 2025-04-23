package log

import (
	"bytes"
	"strings"
	"testing"
)

func TestStandardLogger(t *testing.T) {
	// Create a buffer to capture output
	var buf bytes.Buffer

	// Create a logger with the buffer as output
	logger := NewStandardLogger(
		WithOutput(&buf),
		WithLevel(LevelDebug),
	)

	// Test debug level
	logger.Debug("This is a debug message")
	if !strings.Contains(buf.String(), "[DEBUG]") || !strings.Contains(buf.String(), "This is a debug message") {
		t.Errorf("Debug logging failed, got: %s", buf.String())
	}
	buf.Reset()

	// Test info level
	logger.Info("This is an info message")
	if !strings.Contains(buf.String(), "[INFO]") || !strings.Contains(buf.String(), "This is an info message") {
		t.Errorf("Info logging failed, got: %s", buf.String())
	}
	buf.Reset()

	// Test warn level
	logger.Warn("This is a warning message")
	if !strings.Contains(buf.String(), "[WARN]") || !strings.Contains(buf.String(), "This is a warning message") {
		t.Errorf("Warn logging failed, got: %s", buf.String())
	}
	buf.Reset()

	// Test error level
	logger.Error("This is an error message")
	if !strings.Contains(buf.String(), "[ERROR]") || !strings.Contains(buf.String(), "This is an error message") {
		t.Errorf("Error logging failed, got: %s", buf.String())
	}
	buf.Reset()

	// Test with fields
	loggerWithFields := logger.WithFields(map[string]interface{}{
		"component": "test",
		"count":     123,
	})
	loggerWithFields.Info("Message with fields")
	output := buf.String()
	if !strings.Contains(output, "[INFO]") || 
	   !strings.Contains(output, "Message with fields") ||
	   !strings.Contains(output, "component=test") ||
	   !strings.Contains(output, "count=123") {
		t.Errorf("Logging with fields failed, got: %s", output)
	}
	buf.Reset()

	// Test with a single field
	loggerWithField := logger.WithField("module", "logger")
	loggerWithField.Info("Message with a field")
	output = buf.String()
	if !strings.Contains(output, "[INFO]") || 
	   !strings.Contains(output, "Message with a field") ||
	   !strings.Contains(output, "module=logger") {
		t.Errorf("Logging with a field failed, got: %s", output)
	}
	buf.Reset()

	// Test level filtering
	logger.SetLevel(LevelError)
	logger.Debug("This debug message should not appear")
	logger.Info("This info message should not appear")
	logger.Warn("This warning message should not appear")
	logger.Error("This error message should appear")
	output = buf.String()
	if strings.Contains(output, "should not appear") || 
	   !strings.Contains(output, "This error message should appear") {
		t.Errorf("Level filtering failed, got: %s", output)
	}
	buf.Reset()

	// Test formatted messages
	logger.SetLevel(LevelInfo)
	logger.Info("Formatted %s with %d params", "message", 2)
	if !strings.Contains(buf.String(), "Formatted message with 2 params") {
		t.Errorf("Formatted message failed, got: %s", buf.String())
	}
	buf.Reset()

	// Test GetLevel
	if logger.GetLevel() != LevelInfo {
		t.Errorf("GetLevel failed, expected LevelInfo, got: %v", logger.GetLevel())
	}
}

func TestDefaultLogger(t *testing.T) {
	// Save original default logger
	originalLogger := defaultLogger
	defer func() {
		defaultLogger = originalLogger
	}()

	// Create a buffer to capture output
	var buf bytes.Buffer

	// Set a new default logger
	SetDefaultLogger(NewStandardLogger(
		WithOutput(&buf),
		WithLevel(LevelInfo),
	))

	// Test global functions
	Info("Global info message")
	if !strings.Contains(buf.String(), "[INFO]") || !strings.Contains(buf.String(), "Global info message") {
		t.Errorf("Global info logging failed, got: %s", buf.String())
	}
	buf.Reset()

	// Test global with fields
	WithField("global", true).Info("Global with field")
	output := buf.String()
	if !strings.Contains(output, "[INFO]") || 
	   !strings.Contains(output, "Global with field") ||
	   !strings.Contains(output, "global=true") {
		t.Errorf("Global logging with field failed, got: %s", output)
	}
	buf.Reset()
}