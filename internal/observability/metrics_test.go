package observability

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMiddlewareIncrementsCounters(t *testing.T) {
	m := NewMetrics()
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}

	got := testutil.ToFloat64(m.requests.WithLabelValues(http.MethodGet, "/test", "204"))
	if got != 1 {
		t.Fatalf("expected request count 1, got %v", got)
	}
}

func TestMetricsHandler(t *testing.T) {
	m := NewMetrics()

	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/seed", nil))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

	m.Handler().ServeHTTP(rec, req)

	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), "rsql_http_requests_total") {
		t.Fatalf("expected metrics output to contain request counter")
	}

	if m.Registry() == nil {
		t.Fatal("expected registry")
	}
}

func TestMiddlewareNormalizesDynamicV1Paths(t *testing.T) {
	m := NewMetrics()
	h := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/team-a/tables/users/rows/123", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	got := testutil.ToFloat64(m.requests.WithLabelValues(http.MethodGet, "/v1/{ns}/tables/{table}/rows/{id}", "200"))
	if got != 1 {
		t.Fatalf("expected normalized path metric count 1, got %v", got)
	}
}
