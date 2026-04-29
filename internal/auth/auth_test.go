package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseBearer(t *testing.T) {
	tests := []struct {
		header string
		ok     bool
		token  string
	}{
		{header: "", ok: false},
		{header: "Basic abc", ok: false},
		{header: "Bearer", ok: false},
		{header: "Bearer token", ok: true, token: "token"},
		{header: "bearer mixedCase", ok: true, token: "mixedCase"},
	}

	for _, tt := range tests {
		t.Run(tt.header, func(t *testing.T) {
			got, ok := ParseBearer(tt.header)
			if ok != tt.ok || got != tt.token {
				t.Fatalf("expected (%q,%v), got (%q,%v)", tt.token, tt.ok, got, ok)
			}
		})
	}
}

func TestMiddleware(t *testing.T) {
	h := Middleware("secret", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	t.Run("missing token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", rec.Code)
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer wrong")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", rec.Code)
		}
	})

	t.Run("valid token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer secret")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204, got %d", rec.Code)
		}
	})
}

func TestMiddlewareMisconfigured(t *testing.T) {
	h := Middleware("", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}
