// ABOUTME: Simple no-op telemetry implementation for testing - ONLY provides disabled telemetry, no business logic mocking
// ABOUTME: Allows testing of real components with telemetry disabled to verify they work without telemetry

package telemetry

// NewForTesting returns a no-op telemetry instance for use in tests.
// This allows testing real components with telemetry completely disabled.
func NewForTesting() Telemetry {
	return NewNoop()
}

// NewDisabled is an alias for NewNoop for testing scenarios where
// telemetry should be explicitly disabled.
func NewDisabled() Telemetry {
	return NewNoop()
}
