package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ValentinKolb/rsql/internal/observability"
	"github.com/ValentinKolb/rsql/internal/service"
)

func newAPIForTest(t *testing.T) (http.Handler, *service.Service) {
	t.Helper()
	svc, err := service.New(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	h := NewHandler(Dependencies{
		Token:   "secret",
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: observability.NewMetrics(),
		Service: svc,
	})
	return h, svc
}

func doReq(t *testing.T, h http.Handler, method, path string, body any, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			reader = strings.NewReader(b)
		case []byte:
			reader = bytes.NewReader(b)
		default:
			raw, err := json.Marshal(b)
			if err != nil {
				t.Fatalf("marshal body: %v", err)
			}
			reader = bytes.NewReader(raw)
		}
	}
	if reader == nil {
		reader = http.NoBody
	}

	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Authorization", "Bearer secret")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAPIMainFlow(t *testing.T) {
	h, svc := newAPIForTest(t)

	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/namespaces", map[string]any{
		"name": "ws",
		"config": map[string]any{
			"journal_mode":  "wal",
			"busy_timeout":  5000,
			"query_timeout": 10000,
			"foreign_keys":  true,
		},
	}, nil), http.StatusCreated)

	mustStatus(t, doReq(t, h, http.MethodGet, "/v1/namespaces", nil, nil), http.StatusOK)
	mustStatus(t, doReq(t, h, http.MethodGet, "/v1/namespaces/ws", nil, nil), http.StatusOK)
	mustStatus(t, doReq(t, h, http.MethodPut, "/v1/namespaces/ws", map[string]any{
		"config": map[string]any{"journal_mode": "wal", "busy_timeout": 6000, "query_timeout": 12000, "foreign_keys": true},
	}, nil), http.StatusOK)

	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/ws/tables", map[string]any{
		"type": "table",
		"name": "kunden",
		"columns": []map[string]any{
			{"name": "firma", "type": "text", "not_null": true},
			{"name": "email", "type": "text", "unique": true},
			{"name": "umsatz", "type": "real"},
			{"name": "status", "type": "select", "options": []string{"active", "inactive"}},
		},
	}, nil), http.StatusCreated)

	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/ws/tables/kunden/indexes", map[string]any{"type": "index", "columns": []string{"firma"}}, nil), http.StatusCreated)

	insertRec := doReq(t, h, http.MethodPost, "/v1/ws/tables/kunden/rows", map[string]any{
		"firma":  "Muller",
		"email":  "a@b.com",
		"umsatz": 100.0,
		"status": "active",
	}, map[string]string{"Prefer": "return=representation"})
	mustStatus(t, insertRec, http.StatusCreated)

	id := extractIDFromInsert(t, insertRec.Body.Bytes())

	mustStatus(t, doReq(t, h, http.MethodGet, "/v1/ws/tables", nil, nil), http.StatusOK)
	mustStatus(t, doReq(t, h, http.MethodGet, "/v1/ws/tables/kunden", nil, nil), http.StatusOK)
	mustStatus(t, doReq(t, h, http.MethodGet, "/v1/ws/tables/kunden/rows?status=eq.active&limit=10&offset=0", nil, nil), http.StatusOK)
	mustStatus(t, doReq(t, h, http.MethodGet, "/v1/ws/tables/kunden/rows/"+strconv.Itoa(id), nil, nil), http.StatusOK)
	mustStatus(t, doReq(t, h, http.MethodPut, "/v1/ws/tables/kunden/rows/"+strconv.Itoa(id), map[string]any{"umsatz": map[string]any{"$increment": 10}}, nil), http.StatusOK)
	mustStatus(t, doReq(t, h, http.MethodPatch, "/v1/ws/tables/kunden/rows?status=eq.active", map[string]any{"status": "inactive"}, nil), http.StatusOK)
	mustStatus(t, doReq(t, h, http.MethodDelete, "/v1/ws/tables/kunden/rows?status=eq.inactive", nil, nil), http.StatusNoContent)

	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/ws/query", map[string]any{"sql": "SELECT COUNT(*) AS c FROM kunden", "params": []any{}}, nil), http.StatusOK)
	mustStatus(t, doReq(t, h, http.MethodGet, "/v1/ws/changelog?limit=50&offset=0", nil, nil), http.StatusOK)
	mustStatus(t, doReq(t, h, http.MethodGet, "/v1/ws/stats", nil, nil), http.StatusOK)

	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/ws/tables", map[string]any{"type": "view", "name": "v_kunden", "sql": "SELECT firma FROM kunden"}, nil), http.StatusCreated)
	mustStatus(t, doReq(t, h, http.MethodPut, "/v1/ws/tables/v_kunden", map[string]any{"sql": "SELECT firma FROM kunden"}, nil), http.StatusOK)
	mustStatus(t, doReq(t, h, http.MethodDelete, "/v1/ws/tables/v_kunden", map[string]any{"_meta": map[string]any{"u": "1"}}, nil), http.StatusNoContent)
	mustStatus(t, doReq(t, h, http.MethodDelete, "/v1/ws/tables/kunden/indexes/idx_kunden_firma", map[string]any{"_meta": map[string]any{"u": "1"}}, nil), http.StatusNoContent)
	mustStatus(t, doReq(t, h, http.MethodDelete, "/v1/ws/tables/kunden", map[string]any{"_meta": map[string]any{"u": "1"}}, nil), http.StatusNoContent)

	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/namespaces/ws/duplicate", map[string]any{"name": "ws_copy"}, nil), http.StatusCreated)
	exportRec := doReq(t, h, http.MethodGet, "/v1/namespaces/ws/export", nil, nil)
	mustStatus(t, exportRec, http.StatusOK)
	wsPath, err := svc.BuildImportPath("ws")
	if err != nil {
		t.Fatalf("build ws path: %v", err)
	}
	exportDir := filepath.Join(filepath.Dir(wsPath), "exports")
	entries, err := os.ReadDir(exportDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read exports dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected export temp files to be cleaned up, got %d files", len(entries))
	}

	var form bytes.Buffer
	mw := multipart.NewWriter(&form)
	fw, err := mw.CreateFormFile("file", "db.db")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	_, _ = fw.Write(exportRec.Body.Bytes())
	_ = mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/v1/namespaces/ws_copy/import", &form)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	mustStatus(t, rec, http.StatusOK)

	var csvForm bytes.Buffer
	csvMw := multipart.NewWriter(&csvForm)
	csvFw, err := csvMw.CreateFormFile("file", "rows.csv")
	if err != nil {
		t.Fatalf("create csv form file: %v", err)
	}
	_, _ = csvFw.Write([]byte("firma,email,status\nCSV GmbH,csv@t.com,active\n"))
	_ = csvMw.Close()

	req = httptest.NewRequest(http.MethodPost, "/v1/namespaces/ws_copy/import?table=kunden", &csvForm)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", csvMw.FormDataContentType())
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// CSV import requires the target table to exist. ws_copy was duplicated
	// after kunden was dropped, so we expect a clean 404.
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected csv import status: %d body=%s", rec.Code, rec.Body.String())
	}

	ctx, cancel := context.WithCancel(context.Background())
	subReq := httptest.NewRequest(http.MethodGet, "/v1/ws_copy/subscribe?tables=kunden", nil).WithContext(ctx)
	subReq.Header.Set("Authorization", "Bearer secret")
	subRec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(subRec, subReq)
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscribe did not stop after cancel")
	}

	mustStatus(t, doReq(t, h, http.MethodDelete, "/v1/namespaces/ws_copy", nil, nil), http.StatusNoContent)
	mustStatus(t, doReq(t, h, http.MethodDelete, "/v1/namespaces/ws", nil, nil), http.StatusNoContent)
}

func TestAPIErrorAndHelperBranches(t *testing.T) {
	h, _ := newAPIForTest(t)

	mustStatus(t, doReq(t, h, http.MethodGet, "/v1/unknown", nil, nil), http.StatusNotFound)

	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	mustStatus(t, rec, http.StatusUnauthorized)

	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/namespaces", "{bad", map[string]string{"Content-Type": "application/json"}), http.StatusBadRequest)

	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/namespaces", map[string]any{"name": "ws"}, nil), http.StatusCreated)
	mustStatus(t, doReq(t, h, http.MethodPatch, "/v1/namespaces/ws", nil, nil), http.StatusMethodNotAllowed)

	req = httptest.NewRequest(http.MethodPost, "/v1/ws/query", strings.NewReader(`{"sql":"SELECT 1"}`))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "text/plain")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	mustStatus(t, rec, http.StatusBadRequest)

	mustStatus(t, doReq(t, h, http.MethodGet, "/v1/ws/tables/unknown/rows/abc", nil, nil), http.StatusBadRequest)

	if parseInt("x", 9) != 9 || parseInt("5", 9) != 5 {
		t.Fatalf("parseInt helper mismatch")
	}

	payload := []byte(`{"_meta":{"u":"1"},"x":1}`)
	r := httptest.NewRequest(http.MethodPost, "/x", bytes.NewReader(payload))
	r2 := cloneRequestBody(r)
	if extractMeta(r2) == nil {
		t.Fatalf("expected meta extraction")
	}
	// Legacy unprefixed key must no longer be picked up.
	r = httptest.NewRequest(http.MethodPost, "/x", bytes.NewReader([]byte(`{"meta":{"u":"1"},"x":1}`)))
	r2 = cloneRequestBody(r)
	if extractMeta(r2) != nil {
		t.Fatalf("legacy meta key must not be extracted")
	}

	if _, err := readBodyMap(httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("{bad"))); err == nil {
		t.Fatal("expected readBodyMap parse error")
	}

	var body map[string]any
	req = httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"a":1}`))
	req.Header.Set("Content-Type", "application/json")
	if err := decodeJSONBody(req, &body); err != nil {
		t.Fatalf("decodeJSONBody valid: %v", err)
	}
	req = httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"a":1}`))
	req.Header.Set("Content-Type", "text/plain")
	if err := decodeJSONBody(req, &body); err == nil {
		t.Fatal("expected decodeJSONBody content-type error")
	}
}

func mustStatus(t *testing.T, rec *httptest.ResponseRecorder, expected int) {
	t.Helper()
	if rec.Code != expected {
		t.Fatalf("unexpected status: got=%d expected=%d body=%s", rec.Code, expected, rec.Body.String())
	}
}

// TestCSVExportEndpoint exercises the four documented response paths of
// the streaming CSV export route.
func TestCSVExportEndpoint(t *testing.T) {
	h, _ := newAPIForTest(t)

	// Setup: namespace + table + 3 rows.
	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/namespaces", map[string]any{"name": "exp"}, nil), http.StatusCreated)
	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/exp/tables", map[string]any{
		"type": "table", "name": "items",
		"columns": []map[string]any{
			{"name": "label", "type": "text"},
			{"name": "score", "type": "integer"},
			{"name": "active", "type": "boolean"},
		},
	}, nil), http.StatusCreated)
	mustStatus(t, doReq(t, h, http.MethodPost, "/v1/exp/tables/items/rows", map[string]any{
		"rows": []map[string]any{
			{"label": "a", "score": 1, "active": true},
			{"label": "b", "score": 2, "active": false},
			{"label": "c", "score": 3, "active": true},
		},
	}, nil), http.StatusCreated)

	// 200 happy path
	rec := doReq(t, h, http.MethodGet, "/v1/exp/tables/items/export?format=csv&select=label,score,active&order=score.asc", nil, nil)
	mustStatus(t, rec, http.StatusOK)
	if ct := rec.Header().Get("Content-Type"); ct != "text/csv; charset=utf-8" {
		t.Fatalf("content-type: %q", ct)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, `filename="items.csv"`) {
		t.Fatalf("content-disposition: %q", cd)
	}
	body := rec.Body.String()
	wantBody := "label,score,active\na,1,true\nb,2,false\nc,3,true\n"
	if body != wantBody {
		t.Fatalf("body mismatch:\nwant=%q\ngot =%q", wantBody, body)
	}

	// 400 missing format
	rec = doReq(t, h, http.MethodGet, "/v1/exp/tables/items/export", nil, nil)
	mustStatus(t, rec, http.StatusBadRequest)
	if !strings.Contains(rec.Body.String(), "format query parameter") {
		t.Fatalf("body: %s", rec.Body.String())
	}

	// 400 unsupported format
	rec = doReq(t, h, http.MethodGet, "/v1/exp/tables/items/export?format=xml", nil, nil)
	mustStatus(t, rec, http.StatusBadRequest)
	if !strings.Contains(rec.Body.String(), "unsupported export format") {
		t.Fatalf("body: %s", rec.Body.String())
	}

	// 404 missing table — the pre-flight returns JSON, status is set
	// before any CSV bytes are sent.
	rec = doReq(t, h, http.MethodGet, "/v1/exp/tables/missing/export?format=csv", nil, nil)
	mustStatus(t, rec, http.StatusNotFound)
	if !strings.Contains(rec.Body.String(), "not_found") {
		t.Fatalf("body: %s", rec.Body.String())
	}

	// 404 missing namespace
	rec = doReq(t, h, http.MethodGet, "/v1/missing/tables/items/export?format=csv", nil, nil)
	mustStatus(t, rec, http.StatusNotFound)

	// BOM prefix when ?bom=true
	rec = doReq(t, h, http.MethodGet, "/v1/exp/tables/items/export?format=csv&bom=true&select=label", nil, nil)
	mustStatus(t, rec, http.StatusOK)
	got := rec.Body.Bytes()
	if len(got) < 3 || got[0] != 0xEF || got[1] != 0xBB || got[2] != 0xBF {
		t.Fatalf("expected UTF-8 BOM prefix, got first 3 bytes: % X", got[:3])
	}

	// Method other than GET → 405
	rec = doReq(t, h, http.MethodPost, "/v1/exp/tables/items/export?format=csv", nil, nil)
	mustStatus(t, rec, http.StatusMethodNotAllowed)
}

func extractIDFromInsert(t *testing.T, body []byte) int {
	t.Helper()
	var out map[string][]map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal insert body: %v", err)
	}
	if len(out["data"]) == 0 {
		t.Fatalf("insert response missing data: %s", string(body))
	}
	return int(out["data"][0]["id"].(float64))
}
