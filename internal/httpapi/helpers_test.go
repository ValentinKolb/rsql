package httpapi

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ValentinKolb/rsql/internal/domain"
)

func TestWriteServiceErrorAndMethodNotAllowed(t *testing.T) {
	rec := httptest.NewRecorder()
	writeServiceError(rec, domain.NewError(domain.ErrInvalidRequest, 400, "bad"))
	if rec.Code != 400 {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	writeServiceError(rec, errors.New("x"))
	if rec.Code != 500 {
		t.Fatalf("expected 500, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	methodNotAllowed(rec)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestCloneRequestBodyAndExtractMetaNil(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{"a":1}`)))
	req2 := cloneRequestBody(req)
	if req2 == nil {
		t.Fatal("expected cloned request")
	}
	if meta := extractMeta(req2); meta != nil {
		t.Fatalf("expected nil meta, got %s", string(meta))
	}
}
