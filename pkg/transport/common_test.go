package transport

import (
	"errors"
	"testing"
)

func TestBasicRequest(t *testing.T) {
	// Test creating a request
	payload := []byte("test payload")
	req := NewRequest(TypeGet, payload)

	// Test Type method
	if req.Type() != TypeGet {
		t.Errorf("Expected type %s, got %s", TypeGet, req.Type())
	}

	// Test Payload method
	if string(req.Payload()) != string(payload) {
		t.Errorf("Expected payload %s, got %s", string(payload), string(req.Payload()))
	}
}

func TestBasicResponse(t *testing.T) {
	// Test creating a response with no error
	payload := []byte("test response")
	resp := NewResponse(TypeGet, payload, nil)

	// Test Type method
	if resp.Type() != TypeGet {
		t.Errorf("Expected type %s, got %s", TypeGet, resp.Type())
	}

	// Test Payload method
	if string(resp.Payload()) != string(payload) {
		t.Errorf("Expected payload %s, got %s", string(payload), string(resp.Payload()))
	}

	// Test Error method
	if resp.Error() != nil {
		t.Errorf("Expected nil error, got %v", resp.Error())
	}

	// Test creating a response with an error
	testErr := errors.New("test error")
	resp = NewResponse(TypeGet, payload, testErr)

	if resp.Error() != testErr {
		t.Errorf("Expected error %v, got %v", testErr, resp.Error())
	}
}

func TestNewErrorResponse(t *testing.T) {
	// Test creating an error response
	testErr := errors.New("test error")
	resp := NewErrorResponse(testErr)

	// Test Type method
	if resp.Type() != TypeError {
		t.Errorf("Expected type %s, got %s", TypeError, resp.Type())
	}

	// Test Payload method - should contain error message
	if string(resp.Payload()) != testErr.Error() {
		t.Errorf("Expected payload %s, got %s", testErr.Error(), string(resp.Payload()))
	}

	// Test Error method
	if resp.Error() != testErr {
		t.Errorf("Expected error %v, got %v", testErr, resp.Error())
	}

	// Test with nil error
	resp = NewErrorResponse(nil)

	if resp.Type() != TypeError {
		t.Errorf("Expected type %s, got %s", TypeError, resp.Type())
	}

	if len(resp.Payload()) != 0 {
		t.Errorf("Expected empty payload, got %s", string(resp.Payload()))
	}

	if resp.Error() != nil {
		t.Errorf("Expected nil error, got %v", resp.Error())
	}
}
