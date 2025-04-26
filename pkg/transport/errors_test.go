package transport

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestTemporaryError(t *testing.T) {
	t.Run("Basic error wrapping", func(t *testing.T) {
		baseErr := errors.New("base error")
		tempErr := NewTemporaryError(baseErr, true)

		if !tempErr.IsTemporary() {
			t.Error("Expected IsTemporary() to return true")
		}

		if tempErr.GetRetryAfter() != 0 {
			t.Errorf("Expected GetRetryAfter() to return 0, got %d", tempErr.GetRetryAfter())
		}

		if !strings.Contains(tempErr.Error(), baseErr.Error()) {
			t.Errorf("Expected Error() to contain the base error message")
		}

		if !strings.Contains(tempErr.Error(), "temporary: true") {
			t.Errorf("Expected Error() to indicate temporary status")
		}

		// Test unwrap
		unwrapped := errors.Unwrap(tempErr)
		if unwrapped != baseErr {
			t.Errorf("Expected Unwrap() to return the base error")
		}
	})

	t.Run("Error with retry hint", func(t *testing.T) {
		baseErr := errors.New("connection refused")
		retryAfter := 1000 // 1 second
		tempErr := NewTemporaryErrorWithRetry(baseErr, true, retryAfter)

		if !tempErr.IsTemporary() {
			t.Error("Expected IsTemporary() to return true")
		}

		if tempErr.GetRetryAfter() != retryAfter {
			t.Errorf("Expected GetRetryAfter() to return %d, got %d", retryAfter, tempErr.GetRetryAfter())
		}

		if !strings.Contains(tempErr.Error(), baseErr.Error()) {
			t.Errorf("Expected Error() to contain the base error message")
		}

		if !strings.Contains(tempErr.Error(), "temporary: true") {
			t.Errorf("Expected Error() to indicate temporary status")
		}

		if !strings.Contains(tempErr.Error(), "retry after: 1000ms") {
			t.Errorf("Expected Error() to include retry hint")
		}
	})

	t.Run("Non-temporary error", func(t *testing.T) {
		baseErr := errors.New("permanent error")
		tempErr := NewTemporaryError(baseErr, false)

		if tempErr.IsTemporary() {
			t.Error("Expected IsTemporary() to return false")
		}

		if !strings.Contains(tempErr.Error(), "temporary: false") {
			t.Errorf("Expected Error() to indicate non-temporary status")
		}
	})
}

func TestIsTemporary(t *testing.T) {
	t.Run("With TemporaryError", func(t *testing.T) {
		err := NewTemporaryError(errors.New("test error"), true)
		if !IsTemporary(err) {
			t.Error("Expected IsTemporary() to return true")
		}

		err = NewTemporaryError(errors.New("test error"), false)
		if IsTemporary(err) {
			t.Error("Expected IsTemporary() to return false")
		}
	})

	t.Run("With regular error", func(t *testing.T) {
		err := errors.New("regular error")
		if IsTemporary(err) {
			t.Error("Expected IsTemporary() to return false for regular error")
		}
	})

	t.Run("With wrapped error", func(t *testing.T) {
		tempErr := NewTemporaryError(errors.New("base error"), true)
		wrappedErr := errors.New("wrapper: " + tempErr.Error())
		wrappedTempErr := fmt.Errorf("wrapper: %w", tempErr)

		// Regular wrapping doesn't preserve error type
		if IsTemporary(wrappedErr) {
			t.Error("Expected IsTemporary() to return false for string-wrapped error")
		}

		// fmt.Errorf with %w preserves error type
		if !IsTemporary(wrappedTempErr) {
			t.Error("Expected IsTemporary() to return true for properly wrapped error")
		}
	})
}

func TestGetRetryAfter(t *testing.T) {
	t.Run("With retry hint", func(t *testing.T) {
		retryAfter := 2000 // 2 seconds
		err := NewTemporaryErrorWithRetry(errors.New("test error"), true, retryAfter)

		if GetRetryAfter(err) != retryAfter {
			t.Errorf("Expected GetRetryAfter() to return %d, got %d", retryAfter, GetRetryAfter(err))
		}
	})

	t.Run("Without retry hint", func(t *testing.T) {
		err := NewTemporaryError(errors.New("test error"), true)

		if GetRetryAfter(err) != 0 {
			t.Errorf("Expected GetRetryAfter() to return 0, got %d", GetRetryAfter(err))
		}
	})

	t.Run("With regular error", func(t *testing.T) {
		err := errors.New("regular error")

		if GetRetryAfter(err) != 0 {
			t.Errorf("Expected GetRetryAfter() to return 0, got %d", GetRetryAfter(err))
		}
	})

	t.Run("With wrapped error", func(t *testing.T) {
		retryAfter := 3000 // 3 seconds
		tempErr := NewTemporaryErrorWithRetry(errors.New("base error"), true, retryAfter)
		wrappedTempErr := fmt.Errorf("wrapper: %w", tempErr)

		if GetRetryAfter(wrappedTempErr) != retryAfter {
			t.Errorf("Expected GetRetryAfter() to return %d, got %d", retryAfter, GetRetryAfter(wrappedTempErr))
		}
	})
}
