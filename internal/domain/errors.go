package domain

import "fmt"

// ErrorCode is a stable machine-readable API error identifier.
type ErrorCode string

const (
	ErrInvalidRequest    ErrorCode = "invalid_request"
	ErrUnauthorized      ErrorCode = "unauthorized"
	ErrNotFound          ErrorCode = "not_found"
	ErrNamespaceNotFound ErrorCode = "namespace_not_found"
	ErrTableNotFound     ErrorCode = "table_not_found"
	ErrReadOnly          ErrorCode = "read_only"
	ErrConflict          ErrorCode = "conflict"
	ErrValidationFailed  ErrorCode = "validation_failed"
	ErrSQLNotReadOnly    ErrorCode = "sql_not_read_only"
	ErrInternal          ErrorCode = "internal_error"
)

// Error represents a structured service error.
type Error struct {
	Code       ErrorCode
	Message    string
	HTTPStatus int
	Cause      error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause == nil {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// WrapError wraps an internal error in a structured domain error.
func WrapError(code ErrorCode, status int, message string, cause error) *Error {
	return &Error{Code: code, Message: message, HTTPStatus: status, Cause: cause}
}

// NewError creates a structured domain error without inner cause.
func NewError(code ErrorCode, status int, message string) *Error {
	return &Error{Code: code, Message: message, HTTPStatus: status}
}
