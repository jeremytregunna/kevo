package replication

import (
	"errors"
	"testing"
	"time"
)

func TestStateTracker(t *testing.T) {
	// Create a new state tracker
	tracker := NewStateTracker()

	// Test initial state
	if tracker.GetState() != StateConnecting {
		t.Errorf("Expected initial state to be StateConnecting, got %s", tracker.GetState())
	}

	// Test valid state transition
	err := tracker.SetState(StateStreamingEntries)
	if err != nil {
		t.Errorf("Unexpected error for valid transition: %v", err)
	}
	if tracker.GetState() != StateStreamingEntries {
		t.Errorf("Expected state to be StateStreamingEntries, got %s", tracker.GetState())
	}

	// Test invalid state transition
	err = tracker.SetState(StateAcknowledging)
	if err == nil {
		t.Errorf("Expected error for invalid transition, got nil")
	}
	if !errors.Is(err, ErrInvalidStateTransition) {
		t.Errorf("Expected ErrInvalidStateTransition, got %v", err)
	}
	if tracker.GetState() != StateStreamingEntries {
		t.Errorf("State should not change after invalid transition, got %s", tracker.GetState())
	}

	// Test complete valid path
	validPath := []ReplicaState{
		StateApplyingEntries,
		StateFsyncPending,
		StateAcknowledging,
		StateWaitingForData,
		StateStreamingEntries,
		StateApplyingEntries,
		StateFsyncPending,
		StateAcknowledging,
		StateStreamingEntries,
	}

	for i, state := range validPath {
		err := tracker.SetState(state)
		if err != nil {
			t.Errorf("Unexpected error at step %d: %v", i, err)
		}
		if tracker.GetState() != state {
			t.Errorf("Expected state to be %s at step %d, got %s", state, i, tracker.GetState())
		}
	}

	// Test error state transition
	err = tracker.SetError(errors.New("test error"))
	if err != nil {
		t.Errorf("Unexpected error setting error state: %v", err)
	}
	if tracker.GetState() != StateError {
		t.Errorf("Expected state to be StateError, got %s", tracker.GetState())
	}
	if tracker.GetError() == nil {
		t.Errorf("Expected error to be set, got nil")
	}
	if tracker.GetError().Error() != "test error" {
		t.Errorf("Expected error message 'test error', got '%s'", tracker.GetError().Error())
	}

	// Test recovery from error
	err = tracker.SetState(StateConnecting)
	if err != nil {
		t.Errorf("Unexpected error recovering from error state: %v", err)
	}
	if tracker.GetState() != StateConnecting {
		t.Errorf("Expected state to be StateConnecting after recovery, got %s", tracker.GetState())
	}

	// Test transitions tracking
	transitions := tracker.GetTransitions()
	// Count the actual transitions we made
	transitionCount := len(validPath) + 1 // +1 for error state
	if len(transitions) < transitionCount {
		t.Errorf("Expected at least %d transitions, got %d", transitionCount, len(transitions))
	}

	// Test reset
	tracker.ResetState()
	if tracker.GetState() != StateConnecting {
		t.Errorf("Expected state to be StateConnecting after reset, got %s", tracker.GetState())
	}
	if tracker.GetError() != nil {
		t.Errorf("Expected error to be nil after reset, got %v", tracker.GetError())
	}
	if len(tracker.GetTransitions()) != 0 {
		t.Errorf("Expected 0 transitions after reset, got %d", len(tracker.GetTransitions()))
	}
}

func TestStateDuration(t *testing.T) {
	// Create a new state tracker
	tracker := NewStateTracker()

	// Initial state duration should be small
	initialDuration := tracker.GetStateDuration()
	if initialDuration > 100*time.Millisecond {
		t.Errorf("Initial state duration too large: %v", initialDuration)
	}

	// Wait a bit
	time.Sleep(200 * time.Millisecond)

	// Duration should have increased
	afterWaitDuration := tracker.GetStateDuration()
	if afterWaitDuration < 200*time.Millisecond {
		t.Errorf("Duration did not increase as expected: %v", afterWaitDuration)
	}

	// Transition to a new state
	err := tracker.SetState(StateStreamingEntries)
	if err != nil {
		t.Fatalf("Unexpected error transitioning states: %v", err)
	}

	// New state duration should be small again
	newStateDuration := tracker.GetStateDuration()
	if newStateDuration > 100*time.Millisecond {
		t.Errorf("New state duration too large: %v", newStateDuration)
	}
}

func TestStateStringRepresentation(t *testing.T) {
	testCases := []struct {
		state    ReplicaState
		expected string
	}{
		{StateConnecting, "CONNECTING"},
		{StateStreamingEntries, "STREAMING_ENTRIES"},
		{StateApplyingEntries, "APPLYING_ENTRIES"},
		{StateFsyncPending, "FSYNC_PENDING"},
		{StateAcknowledging, "ACKNOWLEDGING"},
		{StateWaitingForData, "WAITING_FOR_DATA"},
		{StateError, "ERROR"},
		{ReplicaState(999), "UNKNOWN(999)"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			if tc.state.String() != tc.expected {
				t.Errorf("Expected state string %s, got %s", tc.expected, tc.state.String())
			}
		})
	}
}

func TestGetStateString(t *testing.T) {
	tracker := NewStateTracker()

	// Test initial state string
	if tracker.GetStateString() != "CONNECTING" {
		t.Errorf("Expected state string CONNECTING, got %s", tracker.GetStateString())
	}

	// Change state and test string
	err := tracker.SetState(StateStreamingEntries)
	if err != nil {
		t.Fatalf("Unexpected error transitioning states: %v", err)
	}

	if tracker.GetStateString() != "STREAMING_ENTRIES" {
		t.Errorf("Expected state string STREAMING_ENTRIES, got %s", tracker.GetStateString())
	}

	// Set error state and test string
	tracker.SetError(errors.New("test error"))
	if tracker.GetStateString() != "ERROR" {
		t.Errorf("Expected state string ERROR, got %s", tracker.GetStateString())
	}
}
