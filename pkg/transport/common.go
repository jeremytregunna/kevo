package transport

import (
	"errors"
)

// Standard request/response type constants
const (
	TypeGet           = "get"
	TypePut           = "put"
	TypeDelete        = "delete"
	TypeBatchWrite    = "batch_write"
	TypeScan          = "scan"
	TypeBeginTx       = "begin_tx"
	TypeCommitTx      = "commit_tx"
	TypeRollbackTx    = "rollback_tx"
	TypeTxGet         = "tx_get"
	TypeTxPut         = "tx_put"
	TypeTxDelete      = "tx_delete"
	TypeTxScan        = "tx_scan"
	TypeGetStats      = "get_stats"
	TypeCompact       = "compact"
	TypeError         = "error"
)

// Common errors
var (
	ErrInvalidRequest = errors.New("invalid request")
	ErrInvalidPayload = errors.New("invalid payload")
	ErrNotConnected   = errors.New("not connected to server")
	ErrTimeout        = errors.New("operation timed out")
)

// BasicRequest implements the Request interface
type BasicRequest struct {
	RequestType string
	RequestData []byte
}

// Type returns the type of the request
func (r *BasicRequest) Type() string {
	return r.RequestType
}

// Payload returns the payload of the request
func (r *BasicRequest) Payload() []byte {
	return r.RequestData
}

// NewRequest creates a new request with the given type and payload
func NewRequest(requestType string, data []byte) Request {
	return &BasicRequest{
		RequestType: requestType,
		RequestData: data,
	}
}

// BasicResponse implements the Response interface
type BasicResponse struct {
	ResponseType string
	ResponseData []byte
	ResponseErr  error
}

// Type returns the type of the response
func (r *BasicResponse) Type() string {
	return r.ResponseType
}

// Payload returns the payload of the response
func (r *BasicResponse) Payload() []byte {
	return r.ResponseData
}

// Error returns any error associated with the response
func (r *BasicResponse) Error() error {
	return r.ResponseErr
}

// NewResponse creates a new response with the given type, payload, and error
func NewResponse(responseType string, data []byte, err error) Response {
	return &BasicResponse{
		ResponseType: responseType,
		ResponseData: data,
		ResponseErr:  err,
	}
}

// NewErrorResponse creates a new error response
func NewErrorResponse(err error) Response {
	var msg []byte
	if err != nil {
		msg = []byte(err.Error())
	}
	return &BasicResponse{
		ResponseType: TypeError,
		ResponseData: msg,
		ResponseErr:  err,
	}
}