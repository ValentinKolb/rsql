package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ValentinKolb/rsql/internal/observability"
	"github.com/ValentinKolb/rsql/internal/service"
)

// TestHealthzPublic verifies that /healthz is reachable without auth, as
// documented in docs/api-overview.md. The endpoint is intended for use by
// load balancers and probes that should not be expected to carry the API
// token.
func TestHealthzPublic(t *testing.T) {
	svc := mustNewTestService(t)
	h := NewHandler(Dependencies{Token: "secret", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: observability.NewMetrics(), Service: svc})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("X-Request-ID"); got == "" {
		t.Fatal("expected request id header")
	}
}

// TestV1RequiresAuth verifies that the /v1 surface is gated by the bearer
// token, complementing TestHealthzPublic.
func TestV1RequiresAuth(t *testing.T) {
	svc := mustNewTestService(t)
	h := NewHandler(Dependencies{Token: "secret", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: observability.NewMetrics(), Service: svc})

	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated /v1, got %d", rec.Code)
	}
}

func TestNotImplementedRoute(t *testing.T) {
	svc := mustNewTestService(t)
	h := NewHandler(Dependencies{Token: "secret", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: observability.NewMetrics(), Service: svc})

	req := httptest.NewRequest(http.MethodGet, "/v1/unknown/path", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "not_found") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

// TestMetricsPublic verifies that /metrics is reachable without auth.
// Prometheus scrapers commonly cannot carry custom auth headers; if the
// operator wants the endpoint protected they should restrict it at the
// network layer.
func TestMetricsPublic(t *testing.T) {
	svc := mustNewTestService(t)
	h := NewHandler(Dependencies{Token: "secret", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: observability.NewMetrics(), Service: svc})

	// Seed at least one request so the counter has a value to render.
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/healthz", nil))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "rsql_http_requests_total") {
		t.Fatalf("expected metrics output")
	}
}

func mustNewTestService(t *testing.T) *service.Service {
	t.Helper()
	svc, err := service.New(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}
