package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ValentinKolb/rsql/internal/domain"
	"github.com/ValentinKolb/rsql/internal/observability"
)

func TestAPIMissingNamespaceErrorPaths(t *testing.T) {
	h, _ := newAPIForTest(t)

	tests := []struct {
		method string
		path   string
		body   any
		status int
	}{
		{http.MethodGet, "/v1/missing/tables", nil, http.StatusNotFound},
		{http.MethodPost, "/v1/missing/tables", map[string]any{"type": "table", "name": "t", "columns": []map[string]any{{"name": "v", "type": "text"}}}, http.StatusNotFound},
		{http.MethodGet, "/v1/missing/tables/t", nil, http.StatusNotFound},
		{http.MethodPut, "/v1/missing/tables/t", map[string]any{"rename": "t2"}, http.StatusNotFound},
		{http.MethodDelete, "/v1/missing/tables/t", nil, http.StatusNotFound},
		{http.MethodPost, "/v1/missing/tables/t/indexes", map[string]any{"type": "index", "columns": []string{"v"}}, http.StatusNotFound},
		{http.MethodDelete, "/v1/missing/tables/t/indexes/i", nil, http.StatusNotFound},
		{http.MethodGet, "/v1/missing/tables/t/rows", nil, http.StatusNotFound},
		{http.MethodPost, "/v1/missing/tables/t/rows", map[string]any{"v": "x"}, http.StatusNotFound},
		{http.MethodGet, "/v1/missing/tables/t/rows/1", nil, http.StatusNotFound},
		{http.MethodPut, "/v1/missing/tables/t/rows/1", map[string]any{"v": "x"}, http.StatusNotFound},
		{http.MethodDelete, "/v1/missing/tables/t/rows/1", nil, http.StatusNotFound},
		{http.MethodPatch, "/v1/missing/tables/t/rows", map[string]any{"v": "x"}, http.StatusNotFound},
		{http.MethodDelete, "/v1/missing/tables/t/rows?v=eq.x", nil, http.StatusNotFound},
		{http.MethodPost, "/v1/missing/query", map[string]any{"sql": "SELECT 1", "params": []any{}}, http.StatusNotFound},
		{http.MethodGet, "/v1/missing/changelog", nil, http.StatusNotFound},
		{http.MethodGet, "/v1/missing/stats", nil, http.StatusNotFound},
		{http.MethodGet, "/v1/missing/subscribe", nil, http.StatusNotFound},
	}

	for _, tc := range tests {
		rec := doReq(t, h, tc.method, tc.path, tc.body, nil)
		mustStatus(t, rec, tc.status)
	}
}

func TestAPIMethodNotAllowedBranches(t *testing.T) {
	h, _ := newAPIForTest(t)

	tests := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/ws/subscribe"},
		{http.MethodGet, "/v1/ws/query"},
		{http.MethodDelete, "/v1/namespaces/ws/export"},
		{http.MethodGet, "/v1/namespaces/ws/import"},
		{http.MethodPut, "/v1/ws/tables/t/indexes"},
	}

	for _, tc := range tests {
		rec := doReq(t, h, tc.method, tc.path, nil, nil)
		mustStatus(t, rec, http.StatusMethodNotAllowed)
	}
}

func TestAPIDeleteRowRepresentation(t *testing.T) {
	h, _ := newAPIForTest(t)

	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/namespaces", map[string]any{"name": "ws"}, nil), http.StatusCreated)
	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/ws/tables", map[string]any{
		"type": "table",
		"name": "kunden",
		"columns": []map[string]any{
			{"name": "firma", "type": "text"},
		},
	}, nil), http.StatusCreated)
	ins := doReq(t, h, http.MethodPost, "/v1/ws/tables/kunden/rows", map[string]any{"firma": "X"}, map[string]string{"Prefer": "return=representation"})
	mustStatus(t, ins, http.StatusCreated)
	id := extractIDFromInsert(t, ins.Body.Bytes())

	del := doReq(t, h, http.MethodDelete, "/v1/ws/tables/kunden/rows/"+strconv.Itoa(id), nil, map[string]string{"Prefer": "return=representation"})
	mustStatus(t, del, http.StatusOK)
}

func TestAPIImportValidationBranches(t *testing.T) {
	h, _ := newAPIForTest(t)
	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/namespaces", map[string]any{"name": "ws"}, nil), http.StatusCreated)

	// Missing multipart form.
	req := httptest.NewRequest(http.MethodPost, "/v1/namespaces/ws/import", strings.NewReader("x"))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	mustStatus(t, rec, http.StatusBadRequest)

	// CSV upload without table query parameter.
	var body strings.Builder
	w := multipart.NewWriter(&body)
	fw, err := w.CreateFormFile("file", "rows.csv")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	_, _ = io.WriteString(fw, "firma\nA\n")
	_ = w.Close()

	req = httptest.NewRequest(http.MethodPost, "/v1/namespaces/ws/import", strings.NewReader(body.String()))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	mustStatus(t, rec, http.StatusBadRequest)
}

func TestSubscribeWritesEvent(t *testing.T) {
	_, svc := newAPIForTest(t)
	_, err := svc.CreateNamespace(domain.NamespaceDefinition{
		Name:   "ws",
		Config: domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: true},
	})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if err := svc.CreateTableOrView("ws", domain.TableCreateRequest{
		Type: "table",
		Name: "kunden",
		Columns: []domain.ColumnDefinition{
			{Name: "firma", Type: "text"},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	h := &apiHandler{service: svc, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/v1/ws/subscribe?tables=kunden", nil).WithContext(ctx)
	w := &flushWriter{header: http.Header{}}

	done := make(chan struct{})
	go func() {
		h.subscribe(w, req, "ws")
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	if _, err := svc.InsertRows("ws", "kunden", []map[string]any{{"firma": "A"}}, "", nil); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscribe request did not stop")
	}

	if !strings.Contains(w.body.String(), "event: insert") {
		t.Fatalf("expected insert SSE event, body=%s", w.body.String())
	}
}

func TestSubscribeWithoutFlusher(t *testing.T) {
	_, svc := newAPIForTest(t)
	_, err := svc.CreateNamespace(domain.NamespaceDefinition{
		Name:   "ws",
		Config: domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: true},
	})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	h := &apiHandler{service: svc, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	req := httptest.NewRequest(http.MethodGet, "/v1/ws/subscribe", nil)
	w := &noFlushWriter{header: http.Header{}}
	h.subscribe(w, req.WithContext(context.Background()), "ws")

	if w.status != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.status)
	}
}

func TestReadBodyMapReadError(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/x", io.NopCloser(failingReader{}))
	if _, err := readBodyMap(req); err == nil {
		t.Fatal("expected read body error")
	}
}

func TestRecoveryMiddlewarePanic(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := recoveryMiddleware(logger, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

func TestNewHandlerPanicsWithoutService(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = NewHandler(Dependencies{Token: "secret", Metrics: observability.NewMetrics()})
}

type noFlushWriter struct {
	header http.Header
	status int
	body   strings.Builder
}

func (w *noFlushWriter) Header() http.Header {
	return w.header
}

func (w *noFlushWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func (w *noFlushWriter) WriteHeader(statusCode int) {
	w.status = statusCode
}

type flushWriter struct {
	header http.Header
	status int
	body   strings.Builder
}

func (w *flushWriter) Header() http.Header {
	return w.header
}

func (w *flushWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func (w *flushWriter) WriteHeader(statusCode int) {
	w.status = statusCode
}

func (w *flushWriter) Flush() {}

type failingReader struct{}

func (failingReader) Read(_ []byte) (int, error) {
	return 0, errors.New("read failed")
}
