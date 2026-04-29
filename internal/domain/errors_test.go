package domain

import (
	"errors"
	"strings"
	"testing"
)

func TestErrorHelpers(t *testing.T) {
	base := errors.New("base")
	err := WrapError(ErrInternal, 500, "failed", base)
	if err.Unwrap() != base {
		t.Fatalf("unwrap mismatch")
	}
	if !strings.Contains(err.Error(), "failed") {
		t.Fatalf("unexpected error string: %s", err.Error())
	}

	errNoCause := NewError(ErrInvalidRequest, 400, "bad request")
	if errNoCause.Unwrap() != nil {
		t.Fatalf("expected nil unwrap")
	}
	if !strings.Contains(errNoCause.Error(), "bad request") {
		t.Fatalf("unexpected error string: %s", errNoCause.Error())
	}

	var nilErr *Error
	if nilErr.Error() != "" {
		t.Fatalf("expected empty nil error string")
	}
	if nilErr.Unwrap() != nil {
		t.Fatalf("expected nil unwrap on nil error")
	}
}
