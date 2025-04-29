package replication

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ReplicaState defines the possible states of a replica
type ReplicaState int

const (
	// StateConnecting represents the initial state when establishing a connection to the primary
	StateConnecting ReplicaState = iota

	// StateStreamingEntries represents the state when actively receiving WAL entries
	StateStreamingEntries

	// StateApplyingEntries represents the state when validating and ordering entries
	StateApplyingEntries

	// StateFsyncPending represents the state when buffering writes to durable storage
	StateFsyncPending

	// StateAcknowledging represents the state when sending acknowledgments to the primary
	StateAcknowledging

	// StateWaitingForData represents the state when no entries are available and waiting
	StateWaitingForData

	// StateError represents the state when an error has occurred
	StateError
)

// String returns a string representation of the state
func (s ReplicaState) String() string {
	switch s {
	case StateConnecting:
		return "CONNECTING"
	case StateStreamingEntries:
		return "STREAMING_ENTRIES"
	case StateApplyingEntries:
		return "APPLYING_ENTRIES"
	case StateFsyncPending:
		return "FSYNC_PENDING"
	case StateAcknowledging:
		return "ACKNOWLEDGING"
	case StateWaitingForData:
		return "WAITING_FOR_DATA"
	case StateError:
		return "ERROR"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", s)
	}
}

var (
	// ErrInvalidStateTransition indicates an invalid state transition was attempted
	ErrInvalidStateTransition = errors.New("invalid state transition")
)

// StateTracker manages the state machine for a replica
type StateTracker struct {
	currentState ReplicaState
	lastError    error
	transitions  map[ReplicaState][]ReplicaState
	startTime    time.Time
	transitions1 []StateTransition
	mu           sync.RWMutex
}

// StateTransition represents a transition between states
type StateTransition struct {
	From      ReplicaState
	To        ReplicaState
	Timestamp time.Time
}

// NewStateTracker creates a new state tracker with initial state of StateConnecting
func NewStateTracker() *StateTracker {
	tracker := &StateTracker{
		currentState: StateConnecting,
		transitions:  make(map[ReplicaState][]ReplicaState),
		startTime:    time.Now(),
		transitions1: make([]StateTransition, 0),
	}

	// Define valid state transitions
	tracker.transitions[StateConnecting] = []ReplicaState{
		StateStreamingEntries,
		StateError,
	}

	tracker.transitions[StateStreamingEntries] = []ReplicaState{
		StateApplyingEntries,
		StateWaitingForData,
		StateError,
	}

	tracker.transitions[StateApplyingEntries] = []ReplicaState{
		StateFsyncPending,
		StateError,
	}

	tracker.transitions[StateFsyncPending] = []ReplicaState{
		StateAcknowledging,
		StateError,
	}

	tracker.transitions[StateAcknowledging] = []ReplicaState{
		StateStreamingEntries,
		StateWaitingForData,
		StateError,
	}

	tracker.transitions[StateWaitingForData] = []ReplicaState{
		StateStreamingEntries,
		StateWaitingForData, // Allow staying in waiting state
		StateError,
	}

	tracker.transitions[StateError] = []ReplicaState{
		StateConnecting,
	}

	return tracker
}

// SetState changes the state if the transition is valid
func (t *StateTracker) SetState(newState ReplicaState) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if the transition is valid
	if !t.isValidTransition(t.currentState, newState) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidStateTransition,
			t.currentState.String(), newState.String())
	}

	// Record the transition
	transition := StateTransition{
		From:      t.currentState,
		To:        newState,
		Timestamp: time.Now(),
	}
	t.transitions1 = append(t.transitions1, transition)

	// Change the state
	t.currentState = newState

	return nil
}

// GetState returns the current state
func (t *StateTracker) GetState() ReplicaState {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.currentState
}

// SetError sets the state to StateError and records the error
func (t *StateTracker) SetError(err error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Record the error
	t.lastError = err

	// Always valid to transition to error state from any state
	transition := StateTransition{
		From:      t.currentState,
		To:        StateError,
		Timestamp: time.Now(),
	}
	t.transitions1 = append(t.transitions1, transition)

	// Change the state
	t.currentState = StateError

	return nil
}

// GetError returns the last error
func (t *StateTracker) GetError() error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.lastError
}

// isValidTransition checks if a transition from the current state to the new state is valid
func (t *StateTracker) isValidTransition(fromState, toState ReplicaState) bool {
	validStates, exists := t.transitions[fromState]
	if !exists {
		return false
	}

	for _, validState := range validStates {
		if validState == toState {
			return true
		}
	}

	return false
}

// GetTransitions returns a copy of the recorded state transitions
func (t *StateTracker) GetTransitions() []StateTransition {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Create a copy of the transitions
	result := make([]StateTransition, len(t.transitions1))
	copy(result, t.transitions1)

	return result
}

// GetStateDuration returns the duration the state tracker has been in the current state
func (t *StateTracker) GetStateDuration() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var stateStartTime time.Time

	// Find the last transition to the current state
	for i := len(t.transitions1) - 1; i >= 0; i-- {
		if t.transitions1[i].To == t.currentState {
			stateStartTime = t.transitions1[i].Timestamp
			break
		}
	}

	// If we didn't find a transition (initial state), use the tracker start time
	if stateStartTime.IsZero() {
		stateStartTime = t.startTime
	}

	return time.Since(stateStartTime)
}

// GetStateString returns a string representation of the current state
func (t *StateTracker) GetStateString() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.currentState.String()
}

// ResetState resets the state tracker to its initial state
func (t *StateTracker) ResetState() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.currentState = StateConnecting
	t.lastError = nil
	t.startTime = time.Now()
	t.transitions1 = make([]StateTransition, 0)
}
