// Package log provides a common logging interface for Kevo components.
package log

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Level represents the logging level
type Level int

const (
	// LevelDebug level for detailed troubleshooting information
	LevelDebug Level = iota
	// LevelInfo level for general operational information
	LevelInfo
	// LevelWarn level for potentially harmful situations
	LevelWarn
	// LevelError level for error events that might still allow the application to continue
	LevelError
	// LevelFatal level for severe error events that will lead the application to abort
	LevelFatal
)

// String returns the string representation of the log level
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	default:
		return fmt.Sprintf("LEVEL(%d)", l)
	}
}

// Logger interface defines the methods for logging at different levels
type Logger interface {
	// Debug logs a debug-level message
	Debug(msg string, args ...interface{})
	// Info logs an info-level message
	Info(msg string, args ...interface{})
	// Warn logs a warning-level message
	Warn(msg string, args ...interface{})
	// Error logs an error-level message
	Error(msg string, args ...interface{})
	// Fatal logs a fatal-level message and then calls os.Exit(1)
	Fatal(msg string, args ...interface{})
	// WithFields returns a new logger with the given fields added to the context
	WithFields(fields map[string]interface{}) Logger
	// WithField returns a new logger with the given field added to the context
	WithField(key string, value interface{}) Logger
	// GetLevel returns the current logging level
	GetLevel() Level
	// SetLevel sets the logging level
	SetLevel(level Level)
}

// StandardLogger implements the Logger interface with a standard output format
type StandardLogger struct {
	mu     sync.Mutex
	level  Level
	out    io.Writer
	fields map[string]interface{}
}

// NewStandardLogger creates a new StandardLogger with the given options
func NewStandardLogger(options ...LoggerOption) *StandardLogger {
	logger := &StandardLogger{
		level:  LevelInfo, // Default level
		out:    os.Stdout,
		fields: make(map[string]interface{}),
	}

	// Apply options
	for _, option := range options {
		option(logger)
	}

	return logger
}

// LoggerOption is a function that configures a StandardLogger
type LoggerOption func(*StandardLogger)

// WithLevel sets the logging level
func WithLevel(level Level) LoggerOption {
	return func(l *StandardLogger) {
		l.level = level
	}
}

// WithOutput sets the output writer
func WithOutput(out io.Writer) LoggerOption {
	return func(l *StandardLogger) {
		l.out = out
	}
}

// WithInitialFields sets initial fields for the logger
func WithInitialFields(fields map[string]interface{}) LoggerOption {
	return func(l *StandardLogger) {
		for k, v := range fields {
			l.fields[k] = v
		}
	}
}

// log logs a message at the specified level
func (l *StandardLogger) log(level Level, msg string, args ...interface{}) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Format the message
	formattedMsg := msg
	if len(args) > 0 {
		formattedMsg = fmt.Sprintf(msg, args...)
	}

	// Format timestamp
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Format fields
	fieldsStr := ""
	if len(l.fields) > 0 {
		for k, v := range l.fields {
			fieldsStr += fmt.Sprintf(" %s=%v", k, v)
		}
	}

	// Write the log entry
	fmt.Fprintf(l.out, "[%s] [%s]%s %s\n", timestamp, level.String(), fieldsStr, formattedMsg)

	// Exit if fatal
	if level == LevelFatal {
		os.Exit(1)
	}
}

// Debug logs a debug-level message
func (l *StandardLogger) Debug(msg string, args ...interface{}) {
	l.log(LevelDebug, msg, args...)
}

// Info logs an info-level message
func (l *StandardLogger) Info(msg string, args ...interface{}) {
	l.log(LevelInfo, msg, args...)
}

// Warn logs a warning-level message
func (l *StandardLogger) Warn(msg string, args ...interface{}) {
	l.log(LevelWarn, msg, args...)
}

// Error logs an error-level message
func (l *StandardLogger) Error(msg string, args ...interface{}) {
	l.log(LevelError, msg, args...)
}

// Fatal logs a fatal-level message and then calls os.Exit(1)
func (l *StandardLogger) Fatal(msg string, args ...interface{}) {
	l.log(LevelFatal, msg, args...)
}

// WithFields returns a new logger with the given fields added to the context
func (l *StandardLogger) WithFields(fields map[string]interface{}) Logger {
	newLogger := &StandardLogger{
		level:  l.level,
		out:    l.out,
		fields: make(map[string]interface{}, len(l.fields)+len(fields)),
	}

	// Copy existing fields
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}

	// Add new fields
	for k, v := range fields {
		newLogger.fields[k] = v
	}

	return newLogger
}

// WithField returns a new logger with the given field added to the context
func (l *StandardLogger) WithField(key string, value interface{}) Logger {
	return l.WithFields(map[string]interface{}{key: value})
}

// GetLevel returns the current logging level
func (l *StandardLogger) GetLevel() Level {
	return l.level
}

// SetLevel sets the logging level
func (l *StandardLogger) SetLevel(level Level) {
	l.level = level
}

// Default logger instance
var defaultLogger = NewStandardLogger()

// SetDefaultLogger sets the default logger instance
func SetDefaultLogger(logger *StandardLogger) {
	defaultLogger = logger
}

// GetDefaultLogger returns the default logger instance
func GetDefaultLogger() *StandardLogger {
	return defaultLogger
}

// These functions use the default logger

// Debug logs a debug-level message to the default logger
func Debug(msg string, args ...interface{}) {
	defaultLogger.Debug(msg, args...)
}

// Info logs an info-level message to the default logger
func Info(msg string, args ...interface{}) {
	defaultLogger.Info(msg, args...)
}

// Warn logs a warning-level message to the default logger
func Warn(msg string, args ...interface{}) {
	defaultLogger.Warn(msg, args...)
}

// Error logs an error-level message to the default logger
func Error(msg string, args ...interface{}) {
	defaultLogger.Error(msg, args...)
}

// Fatal logs a fatal-level message to the default logger and then calls os.Exit(1)
func Fatal(msg string, args ...interface{}) {
	defaultLogger.Fatal(msg, args...)
}

// WithFields returns a new logger with the given fields added to the context
func WithFields(fields map[string]interface{}) Logger {
	return defaultLogger.WithFields(fields)
}

// WithField returns a new logger with the given field added to the context
func WithField(key string, value interface{}) Logger {
	return defaultLogger.WithField(key, value)
}

// SetLevel sets the logging level of the default logger
func SetLevel(level Level) {
	defaultLogger.SetLevel(level)
}
