package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ValentinKolb/rsql/internal/auth"
	"github.com/ValentinKolb/rsql/internal/domain"
	"github.com/ValentinKolb/rsql/internal/observability"
	"github.com/ValentinKolb/rsql/internal/service"
	"github.com/ValentinKolb/rsql/internal/store/sqlite"
)

// Dependencies provides required collaborators for HTTP handler creation.
type Dependencies struct {
	Token   string
	Metrics *observability.Metrics
	Logger  *slog.Logger
	Service *service.Service
}

type ctxKey string

const requestIDKey ctxKey = "request-id"

// NewHandler builds the root HTTP handler with middleware and route registration.
func NewHandler(dep Dependencies) http.Handler {
	if dep.Metrics == nil {
		dep.Metrics = observability.NewMetrics()
	}
	if dep.Logger == nil {
		dep.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if dep.Service == nil {
		panic("service dependency is required")
	}

	h := &apiHandler{service: dep.Service, logger: dep.Logger}

	mux := http.NewServeMux()

	// Public operational endpoints — no auth.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /metrics", dep.Metrics.Handler())

	// API surface — bearer token required.
	mux.Handle("/v1/", auth.Middleware(dep.Token, http.HandlerFunc(h.handleV1)))

	chain := http.Handler(mux)
	chain = dep.Metrics.Middleware(chain)
	chain = loggingMiddleware(dep.Logger, chain)
	chain = requestIDMiddleware(chain)
	chain = recoveryMiddleware(dep.Logger, chain)

	return chain
}

type apiHandler struct {
	service *service.Service
	logger  *slog.Logger
}

func (h *apiHandler) handleV1(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	path = strings.Trim(path, "/")
	if path == "" {
		writeError(w, http.StatusNotFound, "not_found", "Endpoint not found")
		return
	}
	parts := strings.Split(path, "/")

	if parts[0] == "namespaces" {
		h.handleNamespaces(w, r, parts)
		return
	}

	h.handleNamespaceScoped(w, r, parts)
}

func (h *apiHandler) handleNamespaces(w http.ResponseWriter, r *http.Request, parts []string) {
	switch len(parts) {
	case 1:
		switch r.Method {
		case http.MethodGet:
			h.listNamespaces(w, r)
		case http.MethodPost:
			h.createNamespace(w, r)
		default:
			methodNotAllowed(w)
		}
	case 2:
		ns := parts[1]
		switch r.Method {
		case http.MethodGet:
			h.getNamespace(w, r, ns)
		case http.MethodPut:
			h.updateNamespace(w, r, ns)
		case http.MethodDelete:
			h.deleteNamespace(w, r, ns)
		default:
			methodNotAllowed(w)
		}
	case 3:
		ns := parts[1]
		action := parts[2]
		switch action {
		case "duplicate":
			if r.Method != http.MethodPost {
				methodNotAllowed(w)
				return
			}
			h.duplicateNamespace(w, r, ns)
		case "export":
			if r.Method != http.MethodGet {
				methodNotAllowed(w)
				return
			}
			h.exportNamespace(w, r, ns)
		case "import":
			if r.Method != http.MethodPost {
				methodNotAllowed(w)
				return
			}
			h.importNamespace(w, r, ns)
		default:
			writeError(w, http.StatusNotFound, "not_found", "Endpoint not found")
		}
	default:
		writeError(w, http.StatusNotFound, "not_found", "Endpoint not found")
	}
}

func (h *apiHandler) handleNamespaceScoped(w http.ResponseWriter, r *http.Request, parts []string) {
	ns := parts[0]
	if len(parts) == 2 {
		switch parts[1] {
		case "query":
			if r.Method != http.MethodPost {
				methodNotAllowed(w)
				return
			}
			h.query(w, r, ns)
			return
		case "subscribe":
			if r.Method != http.MethodGet {
				methodNotAllowed(w)
				return
			}
			h.subscribe(w, r, ns)
			return
		case "changelog":
			if r.Method != http.MethodGet {
				methodNotAllowed(w)
				return
			}
			h.changelog(w, r, ns)
			return
		case "stats":
			if r.Method != http.MethodGet {
				methodNotAllowed(w)
				return
			}
			h.stats(w, r, ns)
			return
		case "tables":
			switch r.Method {
			case http.MethodGet:
				h.listTables(w, r, ns)
			case http.MethodPost:
				h.createTable(w, r, ns)
			default:
				methodNotAllowed(w)
			}
			return
		}
	}

	if len(parts) >= 3 && parts[1] == "tables" {
		table := parts[2]

		if len(parts) == 3 {
			switch r.Method {
			case http.MethodGet:
				h.getTable(w, r, ns, table)
			case http.MethodPut:
				h.updateTable(w, r, ns, table)
			case http.MethodDelete:
				h.deleteTable(w, r, ns, table)
			default:
				methodNotAllowed(w)
			}
			return
		}

		if len(parts) == 4 {
			switch parts[3] {
			case "indexes":
				if r.Method != http.MethodPost {
					methodNotAllowed(w)
					return
				}
				h.createIndex(w, r, ns, table)
				return
			case "rows":
				switch r.Method {
				case http.MethodGet:
					h.listRows(w, r, ns, table)
				case http.MethodPost:
					h.insertRows(w, r, ns, table)
				case http.MethodPatch:
					h.bulkUpdateRows(w, r, ns, table)
				case http.MethodDelete:
					h.bulkDeleteRows(w, r, ns, table)
				default:
					methodNotAllowed(w)
				}
				return
			}
		}

		if len(parts) == 5 {
			switch parts[3] {
			case "indexes":
				if r.Method != http.MethodDelete {
					methodNotAllowed(w)
					return
				}
				h.deleteIndex(w, r, ns, table, parts[4])
				return
			case "rows":
				id, err := service.ParseID(parts[4])
				if err != nil {
					writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
					return
				}
				switch r.Method {
				case http.MethodGet:
					h.getRow(w, r, ns, table, id)
				case http.MethodPut:
					h.updateRow(w, r, ns, table, id)
				case http.MethodDelete:
					h.deleteRow(w, r, ns, table, id)
				default:
					methodNotAllowed(w)
				}
				return
			}
		}
	}

	writeError(w, http.StatusNotFound, "not_found", "Endpoint not found")
}

func (h *apiHandler) createNamespace(w http.ResponseWriter, r *http.Request) {
	var req domain.NamespaceDefinition
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}
	if req.Config.JournalMode == "" {
		req.Config.JournalMode = "wal"
	}
	if req.Config.BusyTimeout == 0 {
		req.Config.BusyTimeout = 5000
	}
	if req.Config.QueryTimeout == 0 {
		req.Config.QueryTimeout = 10000
	}
	if !req.Config.ForeignKeys {
		req.Config.ForeignKeys = true
	}

	result, err := h.service.CreateNamespace(req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (h *apiHandler) listNamespaces(w http.ResponseWriter, _ *http.Request) {
	result, err := h.service.ListNamespaces()
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *apiHandler) getNamespace(w http.ResponseWriter, _ *http.Request, ns string) {
	result, err := h.service.GetNamespace(ns)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *apiHandler) updateNamespace(w http.ResponseWriter, r *http.Request, ns string) {
	var req struct {
		Config domain.NamespaceConfig `json:"config"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	result, err := h.service.UpdateNamespaceConfig(ns, req.Config)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *apiHandler) deleteNamespace(w http.ResponseWriter, _ *http.Request, ns string) {
	if err := h.service.DeleteNamespace(ns); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *apiHandler) duplicateNamespace(w http.ResponseWriter, r *http.Request, source string) {
	var req struct {
		Name string `json:"name"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}
	result, err := h.service.DuplicateNamespace(source, req.Name)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (h *apiHandler) exportNamespace(w http.ResponseWriter, r *http.Request, ns string) {
	path, err := h.service.ExportNamespace(ns)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	defer func() {
		_ = os.Remove(path)
	}()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.db"`, ns))
	http.ServeFile(w, r, path)
}

func (h *apiHandler) importNamespace(w http.ResponseWriter, r *http.Request, ns string) {
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid multipart form")
		return
	}
	f, fh, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "missing form file 'file'")
		return
	}
	defer f.Close()

	table := r.URL.Query().Get("table")
	ct := fh.Header.Get("Content-Type")
	ext := strings.ToLower(filepath.Ext(fh.Filename))
	if ext == ".csv" || strings.Contains(ct, "csv") {
		if table == "" {
			writeError(w, http.StatusBadRequest, "invalid_request", "table query parameter required for csv import")
			return
		}
		result, err := h.service.ImportNamespaceCSV(ns, table, f, nil)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}

	if err := h.service.ImportNamespaceDB(ns, f); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"imported": true, "type": "db"})
}

func (h *apiHandler) listTables(w http.ResponseWriter, _ *http.Request, ns string) {
	result, err := h.service.TablesList(ns)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *apiHandler) createTable(w http.ResponseWriter, r *http.Request, ns string) {
	var req domain.TableCreateRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := h.service.CreateTableOrView(ns, req); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"created": req.Name, "type": req.Type})
}

func (h *apiHandler) getTable(w http.ResponseWriter, _ *http.Request, ns, table string) {
	result, err := h.service.GetTable(ns, table)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *apiHandler) updateTable(w http.ResponseWriter, r *http.Request, ns, table string) {
	var req domain.TableUpdateRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := h.service.UpdateTableOrView(ns, table, req); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"updated": true})
}

func (h *apiHandler) deleteTable(w http.ResponseWriter, r *http.Request, ns, table string) {
	meta := extractMeta(r)
	if err := h.service.DeleteTableOrView(ns, table, meta); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *apiHandler) createIndex(w http.ResponseWriter, r *http.Request, ns, table string) {
	var req domain.IndexCreateRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := h.service.CreateIndex(ns, table, req); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"created": true})
}

func (h *apiHandler) deleteIndex(w http.ResponseWriter, r *http.Request, ns, table, idx string) {
	meta := extractMeta(r)
	if err := h.service.DeleteIndex(ns, table, idx, meta); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *apiHandler) listRows(w http.ResponseWriter, r *http.Request, ns, table string) {
	result, err := h.service.ListRows(ns, table, r.URL.Query())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *apiHandler) insertRows(w http.ResponseWriter, r *http.Request, ns, table string) {
	body, err := readBodyMap(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	rows, meta, err := service.ParseRowsPayload(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	prefer := r.Header.Get("Prefer")
	result, err := h.service.InsertRows(ns, table, rows, prefer, meta)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (h *apiHandler) getRow(w http.ResponseWriter, _ *http.Request, ns, table string, id int64) {
	result, err := h.service.GetRow(ns, table, id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *apiHandler) updateRow(w http.ResponseWriter, r *http.Request, ns, table string, id int64) {
	body, err := readBodyMap(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	payload, meta, err := service.ParseUpdatePayload(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	prefer := r.Header.Get("Prefer")
	result, err := h.service.UpdateRow(ns, table, id, payload, prefer, meta)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *apiHandler) deleteRow(w http.ResponseWriter, r *http.Request, ns, table string, id int64) {
	prefer := r.Header.Get("Prefer")
	result, err := h.service.DeleteRow(ns, table, id, prefer, extractMeta(r))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if prefer == "return=representation" {
		writeJSON(w, http.StatusOK, result)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *apiHandler) bulkUpdateRows(w http.ResponseWriter, r *http.Request, ns, table string) {
	body, err := readBodyMap(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	payload, meta, err := service.ParseUpdatePayload(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	prefer := r.Header.Get("Prefer")
	result, err := h.service.BulkUpdate(ns, table, r.URL.Query(), payload, prefer, meta)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *apiHandler) bulkDeleteRows(w http.ResponseWriter, r *http.Request, ns, table string) {
	prefer := r.Header.Get("Prefer")
	result, err := h.service.BulkDelete(ns, table, r.URL.Query(), prefer, extractMeta(r))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if prefer == "return=representation" {
		writeJSON(w, http.StatusOK, result)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *apiHandler) query(w http.ResponseWriter, r *http.Request, ns string) {
	var req sqlite.QueryRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	result, err := h.service.Query(ns, req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *apiHandler) subscribe(w http.ResponseWriter, r *http.Request, ns string) {
	tablesParam := r.URL.Query().Get("tables")
	var tables []string
	if tablesParam != "" {
		tables = strings.Split(tablesParam, ",")
	}

	id, ch, err := h.service.Subscribe(ns, tables)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	defer h.service.Unsubscribe(ns, id)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal_error", "streaming not supported")
		return
	}

	// Flush headers immediately so clients can start consuming the stream
	// before the first data event arrives.
	_, _ = io.WriteString(w, ": connected\n\n")
	flusher.Flush()

	keepAlive := time.NewTicker(25 * time.Second)
	defer keepAlive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepAlive.C:
			_, _ = io.WriteString(w, ": keep-alive\n\n")
			flusher.Flush()
		case event, ok := <-ch:
			if !ok {
				return
			}
			payload, err := json.Marshal(event)
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(w, "event: %s\n", event.Action)
			_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}
	}
}

func (h *apiHandler) changelog(w http.ResponseWriter, r *http.Request, ns string) {
	limit := parseInt(r.URL.Query().Get("limit"), 50)
	offset := parseInt(r.URL.Query().Get("offset"), 0)
	table := r.URL.Query().Get("table")

	result, err := h.service.Changelog(ns, table, limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *apiHandler) stats(w http.ResponseWriter, _ *http.Request, ns string) {
	result, err := h.service.Stats(ns)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = fmt.Sprintf("req-%d", time.Now().UnixNano())
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

func loggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		logger.Info("http_request",
			slog.String("request_id", requestIDFromContext(r.Context())),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Duration("duration", time.Since(start)),
		)
	})
}

func recoveryMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Error("panic_recovered",
					slog.String("request_id", requestIDFromContext(r.Context())),
					slog.Any("panic", recovered),
				)
				writeError(w, http.StatusInternalServerError, "internal_error", "Internal server error")
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func requestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{
		"error":   code,
		"message": message,
	})
}

func writeServiceError(w http.ResponseWriter, err error) {
	var derr *domain.Error
	if errors.As(err, &derr) {
		writeError(w, derr.HTTPStatus, string(derr.Code), derr.Message)
		return
	}
	writeError(w, http.StatusInternalServerError, string(domain.ErrInternal), "Internal server error")
}

func methodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed")
}

func decodeJSONBody(r *http.Request, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mediaType, _, _ := mime.ParseMediaType(ct)
		if mediaType != "application/json" && !strings.HasSuffix(mediaType, "+json") {
			return fmt.Errorf("content-type must be application/json")
		}
	}
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid json body")
	}
	return nil
}

func readBodyMap(r *http.Request) (map[string]json.RawMessage, error) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read request body")
	}
	return service.ParseJSONMap(raw)
}

func extractMeta(r *http.Request) json.RawMessage {
	body, err := readBodyMap(cloneRequestBody(r))
	if err != nil {
		return nil
	}
	if raw, ok := body["meta"]; ok {
		return raw
	}
	return nil
}

func cloneRequestBody(r *http.Request) *http.Request {
	if r.Body == nil {
		return r
	}
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		return r
	}
	r.Body = io.NopCloser(bytes.NewReader(buf))
	r2 := r.Clone(r.Context())
	r2.Body = io.NopCloser(bytes.NewReader(buf))
	return r2
}

func parseInt(v string, def int) int {
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
