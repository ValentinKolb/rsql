package service

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ValentinKolb/rsql/internal/domain"
	"github.com/ValentinKolb/rsql/internal/namespace"
	"github.com/ValentinKolb/rsql/internal/sse"
	"github.com/ValentinKolb/rsql/internal/store/control"
	"github.com/ValentinKolb/rsql/internal/store/sqlite"
)

var namespaceNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

// Service orchestrates all rsql operations.
type Service struct {
	dataDir  string
	registry *control.Registry
	ns       *namespace.Manager
	broker   *sse.Broker
}

// New creates a service instance.
func New(dataDir string, idleTimeout time.Duration) (*Service, error) {
	reg, err := control.Open(dataDir)
	if err != nil {
		return nil, err
	}
	return &Service{
		dataDir:  dataDir,
		registry: reg,
		ns:       namespace.NewManager(namespace.Config{IdleTimeout: idleTimeout}),
		broker:   sse.NewBroker(),
	}, nil
}

// Close closes all service resources.
func (s *Service) Close() error {
	var errs []error
	if s.ns != nil {
		errs = append(errs, s.ns.Close())
	}
	if s.registry != nil {
		errs = append(errs, s.registry.Close())
	}
	return errors.Join(errs...)
}

func validateNamespaceName(name string) error {
	if !namespaceNameRe.MatchString(name) {
		return fmt.Errorf("invalid namespace name %q", name)
	}
	return nil
}

// CreateNamespace creates a namespace DB and persists config.
func (s *Service) CreateNamespace(req domain.NamespaceDefinition) (map[string]any, error) {
	if err := validateNamespaceName(req.Name); err != nil {
		return nil, domain.NewError(domain.ErrInvalidRequest, 400, err.Error())
	}
	path := control.PathFor(s.dataDir, req.Name)
	if err := s.registry.Create(req.Name, path); err != nil {
		if errors.Is(err, control.ErrAlreadyExists) {
			return nil, domain.WrapError(domain.ErrConflict, 409, "namespace already exists", err)
		}
		return nil, domain.WrapError(domain.ErrInternal, 500, "failed to create namespace record", err)
	}

	err := s.ns.WithWrite(req.Name, path, func(db *sql.DB) error {
		if err := sqlite.EnsureInternalSchema(db); err != nil {
			return err
		}
		if err := sqlite.PutMeta(db, "namespace_config", "self", req.Config); err != nil {
			return err
		}
		if err := sqlite.ApplyNamespaceConfig(db, req.Config); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, domain.WrapError(domain.ErrInternal, 500, "failed to initialize namespace", err)
	}

	return map[string]any{"name": req.Name, "config": req.Config}, nil
}

// ListNamespaces lists all active namespaces.
func (s *Service) ListNamespaces() ([]map[string]any, error) {
	recs, err := s.registry.List()
	if err != nil {
		return nil, domain.WrapError(domain.ErrInternal, 500, "failed to list namespaces", err)
	}
	out := make([]map[string]any, 0, len(recs))
	for _, r := range recs {
		cfg, _ := s.GetNamespaceConfig(r.Name)
		out = append(out, map[string]any{
			"name":       r.Name,
			"created_at": r.CreatedAt,
			"config":     cfg,
		})
	}
	return out, nil
}

// GetNamespace returns namespace info.
func (s *Service) GetNamespace(name string) (map[string]any, error) {
	rec, err := s.registry.Get(name)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.NewError(domain.ErrNamespaceNotFound, 404, fmt.Sprintf("Namespace '%s' does not exist", name))
		}
		return nil, domain.WrapError(domain.ErrInternal, 500, "failed to read namespace", err)
	}
	cfg, err := s.GetNamespaceConfig(name)
	if err != nil {
		return nil, err
	}
	return map[string]any{"name": rec.Name, "db_path": rec.DBPath, "created_at": rec.CreatedAt, "config": cfg}, nil
}

// UpdateNamespaceConfig updates and applies namespace config.
func (s *Service) UpdateNamespaceConfig(name string, cfg domain.NamespaceConfig) (map[string]any, error) {
	rec, err := s.registry.Get(name)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.NewError(domain.ErrNamespaceNotFound, 404, fmt.Sprintf("Namespace '%s' does not exist", name))
		}
		return nil, domain.WrapError(domain.ErrInternal, 500, "failed to read namespace", err)
	}

	err = s.ns.WithWrite(name, rec.DBPath, func(db *sql.DB) error {
		if err := sqlite.EnsureInternalSchema(db); err != nil {
			return err
		}
		if err := sqlite.PutMeta(db, "namespace_config", "self", cfg); err != nil {
			return err
		}
		if err := sqlite.ApplyNamespaceConfig(db, cfg); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, domain.WrapError(domain.ErrInternal, 500, "failed to update namespace config", err)
	}

	return map[string]any{"name": name, "config": cfg}, nil
}

// DeleteNamespace deletes namespace DB and lookup record.
func (s *Service) DeleteNamespace(name string) error {
	rec, err := s.registry.Get(name)
	if err != nil {
		if err == sql.ErrNoRows {
			return domain.NewError(domain.ErrNamespaceNotFound, 404, fmt.Sprintf("Namespace '%s' does not exist", name))
		}
		return domain.WrapError(domain.ErrInternal, 500, "failed to read namespace", err)
	}

	if err := s.ns.CloseHandle(name); err != nil {
		return domain.WrapError(domain.ErrInternal, 500, "failed to close namespace handle", err)
	}
	if err := os.Remove(rec.DBPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return domain.WrapError(domain.ErrInternal, 500, "failed to remove namespace database", err)
	}
	if err := s.registry.Delete(name); err != nil {
		if err == sql.ErrNoRows {
			return domain.NewError(domain.ErrNamespaceNotFound, 404, fmt.Sprintf("Namespace '%s' does not exist", name))
		}
		return domain.WrapError(domain.ErrInternal, 500, "failed to delete namespace", err)
	}
	return nil
}

// DuplicateNamespace duplicates namespace DB into a new namespace.
func (s *Service) DuplicateNamespace(source, target string) (map[string]any, error) {
	if err := validateNamespaceName(target); err != nil {
		return nil, domain.NewError(domain.ErrInvalidRequest, 400, err.Error())
	}
	src, err := s.registry.Get(source)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.NewError(domain.ErrNamespaceNotFound, 404, fmt.Sprintf("Namespace '%s' does not exist", source))
		}
		return nil, domain.WrapError(domain.ErrInternal, 500, "failed to load source namespace", err)
	}
	targetPath := control.PathFor(s.dataDir, target)
	if err := s.registry.Create(target, targetPath); err != nil {
		return nil, domain.WrapError(domain.ErrConflict, 409, "target namespace already exists", err)
	}

	if err := s.ns.WithRead(source, src.DBPath, func(db *sql.DB) error {
		_, err := db.Exec(`VACUUM INTO ?`, targetPath)
		return err
	}); err != nil {
		_ = s.registry.Delete(target)
		return nil, domain.WrapError(domain.ErrInternal, 500, "failed to duplicate namespace", err)
	}
	return map[string]any{"source": source, "target": target}, nil
}

// ExportNamespace returns namespace DB path for file download.
func (s *Service) ExportNamespace(name string) (string, error) {
	rec, err := s.registry.Get(name)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", domain.NewError(domain.ErrNamespaceNotFound, 404, fmt.Sprintf("Namespace '%s' does not exist", name))
		}
		return "", domain.WrapError(domain.ErrInternal, 500, "failed to read namespace", err)
	}
	exportDir := filepath.Join(s.dataDir, "exports")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		return "", domain.WrapError(domain.ErrInternal, 500, "failed to create export directory", err)
	}
	exportPath := filepath.Join(exportDir, fmt.Sprintf("%s-%d.db", name, time.Now().UTC().UnixNano()))
	_ = os.Remove(exportPath)

	err = s.ns.WithRead(name, rec.DBPath, func(db *sql.DB) error {
		_, err := db.Exec(`VACUUM INTO ?`, exportPath)
		return err
	})
	if err != nil {
		// VACUUM may have left a partial file; remove it so failed exports
		// do not accumulate on disk.
		_ = os.Remove(exportPath)
		return "", domain.WrapError(domain.ErrInternal, 500, "failed to export namespace snapshot", err)
	}
	return exportPath, nil
}

// ImportNamespaceDB replaces namespace DB with uploaded database content.
func (s *Service) ImportNamespaceDB(name string, src io.Reader) error {
	rec, err := s.registry.Get(name)
	if err != nil {
		if err == sql.ErrNoRows {
			return domain.NewError(domain.ErrNamespaceNotFound, 404, fmt.Sprintf("Namespace '%s' does not exist", name))
		}
		return domain.WrapError(domain.ErrInternal, 500, "failed to read namespace", err)
	}

	tmp := rec.DBPath + ".import.tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return domain.WrapError(domain.ErrInternal, 500, "failed to create import temp file", err)
	}
	if _, err := io.Copy(f, src); err != nil {
		f.Close()
		return domain.WrapError(domain.ErrInternal, 500, "failed to write import temp file", err)
	}
	if err := f.Close(); err != nil {
		return domain.WrapError(domain.ErrInternal, 500, "failed to close import temp file", err)
	}

	if err := s.ns.CloseHandle(name); err != nil {
		return domain.WrapError(domain.ErrInternal, 500, "failed to close namespace handle", err)
	}
	if err := os.Rename(tmp, rec.DBPath); err != nil {
		return domain.WrapError(domain.ErrInternal, 500, "failed to replace namespace database", err)
	}
	return nil
}

// ImportNamespaceCSV imports CSV rows into a table.
func (s *Service) ImportNamespaceCSV(name, table string, src io.Reader, meta json.RawMessage) (map[string]any, error) {
	rec, err := s.registry.Get(name)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.NewError(domain.ErrNamespaceNotFound, 404, fmt.Sprintf("Namespace '%s' does not exist", name))
		}
		return nil, domain.WrapError(domain.ErrInternal, 500, "failed to read namespace", err)
	}

	reader := csv.NewReader(src)
	headers, err := reader.Read()
	if err != nil {
		return nil, domain.NewError(domain.ErrInvalidRequest, 400, "invalid csv header")
	}

	rows := make([]map[string]any, 0, 128)
	for {
		recRow, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, domain.NewError(domain.ErrInvalidRequest, 400, "invalid csv row")
		}
		row := make(map[string]any, len(headers))
		for i, h := range headers {
			if i < len(recRow) {
				row[h] = recRow[i]
			}
		}
		rows = append(rows, row)
	}

	var inserted int
	var ids []any
	err = s.ns.WithWrite(name, rec.DBPath, func(db *sql.DB) error {
		if err := sqlite.EnsureInternalSchema(db); err != nil {
			return err
		}
		insertedRows, insertedIDs, err := sqlite.InsertRows(db, table, rows, "")
		if err != nil {
			return err
		}
		inserted = len(insertedRows)
		if len(insertedIDs) > 0 {
			ids = append([]any(nil), insertedIDs...)
		}
		return nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	if len(ids) > 0 {
		s.publishBulk(name, table, "bulk_insert", ids, meta)
	}

	return map[string]any{"inserted": inserted}, nil
}

// TablesList lists tables/views in namespace.
func (s *Service) TablesList(ns string) ([]map[string]any, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	err = s.ns.WithRead(ns, path, func(db *sql.DB) error {
		if err := sqlite.EnsureInternalSchema(db); err != nil {
			return err
		}
		rows, err := sqlite.ListTables(db)
		if err != nil {
			return err
		}
		out = rows
		return nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return out, nil
}

func (s *Service) pathForNamespace(name string) (string, error) {
	rec, err := s.registry.Get(name)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", domain.NewError(domain.ErrNamespaceNotFound, 404, fmt.Sprintf("Namespace '%s' does not exist", name))
		}
		return "", domain.WrapError(domain.ErrInternal, 500, "failed to resolve namespace", err)
	}
	return rec.DBPath, nil
}

func (s *Service) GetNamespaceConfig(name string) (domain.NamespaceConfig, error) {
	path, err := s.pathForNamespace(name)
	if err != nil {
		return domain.NamespaceConfig{}, err
	}

	var cfg domain.NamespaceConfig
	err = s.ns.WithRead(name, path, func(db *sql.DB) error {
		if err := sqlite.EnsureInternalSchema(db); err != nil {
			return err
		}
		ok, err := sqlite.GetMeta(db, "namespace_config", "self", &cfg)
		if err != nil {
			return err
		}
		if !ok {
			cfg = domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, ForeignKeys: true, QueryTimeout: 10000}
		}
		return sqlite.ApplyNamespaceConfig(db, cfg)
	})
	if err != nil {
		return domain.NamespaceConfig{}, mapErr(err)
	}
	return cfg, nil
}

// CreateTableOrView creates a table/view and emits schema event.
func (s *Service) CreateTableOrView(ns string, req domain.TableCreateRequest) error {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return err
	}
	err = s.ns.WithWrite(ns, path, func(db *sql.DB) error {
		if err := sqlite.EnsureInternalSchema(db); err != nil {
			return err
		}
		if err := sqlite.CreateTableOrView(db, req); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return mapErr(err)
	}
	s.publishSchema(ns, req.Name, map[string]any{"type": "create", "request": req}, req.Meta)
	return nil
}

// GetTable returns table/view schema details.
func (s *Service) GetTable(ns, name string) (map[string]any, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	err = s.ns.WithRead(ns, path, func(db *sql.DB) error {
		row, err := sqlite.GetTable(db, name)
		if err != nil {
			return err
		}
		out = row
		return nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return out, nil
}

// UpdateTableOrView updates table/view schema and emits schema event.
func (s *Service) UpdateTableOrView(ns, name string, req domain.TableUpdateRequest) error {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return err
	}
	err = s.ns.WithWrite(ns, path, func(db *sql.DB) error {
		return sqlite.UpdateTableOrView(db, name, req)
	})
	if err != nil {
		return mapErr(err)
	}
	target := name
	if req.Rename != "" {
		target = req.Rename
	}
	s.publishSchema(ns, target, map[string]any{"type": "update", "request": req}, req.Meta)
	return nil
}

// DeleteTableOrView deletes table/view and emits schema event.
func (s *Service) DeleteTableOrView(ns, name string, meta json.RawMessage) error {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return err
	}
	err = s.ns.WithWrite(ns, path, func(db *sql.DB) error {
		return sqlite.DeleteTableOrView(db, name, meta)
	})
	if err != nil {
		return mapErr(err)
	}
	s.publishSchema(ns, name, map[string]any{"type": "delete"}, meta)
	return nil
}

// CreateIndex creates an index.
func (s *Service) CreateIndex(ns, table string, req domain.IndexCreateRequest) error {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return err
	}
	err = s.ns.WithWrite(ns, path, func(db *sql.DB) error {
		return sqlite.CreateIndex(db, table, req)
	})
	if err != nil {
		return mapErr(err)
	}
	s.publishSchema(ns, table, map[string]any{"type": "create_index", "request": req}, req.Meta)
	return nil
}

// DeleteIndex deletes an index.
func (s *Service) DeleteIndex(ns, table, idx string, meta json.RawMessage) error {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return err
	}
	err = s.ns.WithWrite(ns, path, func(db *sql.DB) error {
		return sqlite.DeleteIndex(db, table, idx, meta)
	})
	if err != nil {
		return mapErr(err)
	}
	s.publishSchema(ns, table, map[string]any{"type": "delete_index", "index": idx}, meta)
	return nil
}

// ListRows lists rows.
func (s *Service) ListRows(ns, table string, query map[string][]string) (any, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}
	var out any
	err = s.ns.WithRead(ns, path, func(db *sql.DB) error {
		rows, err := sqlite.ListRows(db, table, query)
		if err != nil {
			return err
		}
		out = rows
		return nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return out, nil
}

// AssertTableOrViewExists fails with a typed namespace/not-found error when
// the namespace or the table/view does not exist. Used by streaming
// endpoints to pre-flight existence before sending HTTP headers.
func (s *Service) AssertTableOrViewExists(ns, table string) error {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return err
	}
	err = s.ns.WithRead(ns, path, func(db *sql.DB) error {
		if !sqlite.TableOrViewExists(db, table) {
			return sql.ErrNoRows
		}
		return nil
	})
	if err != nil {
		return mapErr(err)
	}
	return nil
}

// ExportTableCSV streams a table's rows as CSV-encoded bytes to w.
//
// The query map mirrors the shape used by ListRows so HTTP callers can pass
// r.URL.Query() unchanged. Reads run under the namespace read lock; this
// function does not publish SSE or changelog events because read paths are
// not observable in the existing rsql model.
func (s *Service) ExportTableCSV(ns, table string, query map[string][]string, w io.Writer) error {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return err
	}
	err = s.ns.WithRead(ns, path, func(db *sql.DB) error {
		return sqlite.StreamCSV(db, table, query, w)
	})
	if err != nil {
		return mapErr(err)
	}
	return nil
}

// GetRow fetches a row by id.
func (s *Service) GetRow(ns, table string, id any) (map[string]any, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	err = s.ns.WithRead(ns, path, func(db *sql.DB) error {
		row, err := sqlite.GetRowByID(db, table, id)
		if err != nil {
			return err
		}
		out = row
		return nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return out, nil
}

// InsertRows inserts one or many rows.
func (s *Service) InsertRows(ns, table string, rows []map[string]any, prefer string, meta json.RawMessage) (any, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	var ids []any
	err = s.ns.WithWrite(ns, path, func(db *sql.DB) error {
		isView, err := sqlite.IsView(db, table)
		if err != nil {
			return err
		}
		if isView {
			return domain.NewError(domain.ErrReadOnly, 405, fmt.Sprintf("Cannot insert into view '%s'. Views are read-only.", table))
		}
		inserted, insertedIDs, err := sqlite.InsertRows(db, table, rows, prefer)
		if err != nil {
			return err
		}
		out = inserted
		ids = insertedIDs
		return nil
	})
	if err != nil {
		return nil, mapErr(err)
	}

	if len(rows) > 1 {
		s.publishBulk(ns, table, "bulk_insert", ids, meta)
	} else if len(out) > 0 {
		s.publishRow(ns, table, "insert", out[0], meta)
	}

	if prefer == "return=representation" {
		return map[string]any{"data": out}, nil
	}
	return map[string]any{"inserted": len(out)}, nil
}

// UpdateRow updates one row.
func (s *Service) UpdateRow(ns, table string, id any, payload map[string]any, prefer string, meta json.RawMessage) (any, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	err = s.ns.WithWrite(ns, path, func(db *sql.DB) error {
		isView, err := sqlite.IsView(db, table)
		if err != nil {
			return err
		}
		if isView {
			return domain.NewError(domain.ErrReadOnly, 405, fmt.Sprintf("Cannot update view '%s'. Views are read-only.", table))
		}
		rows, err = sqlite.UpdateRowByID(db, table, id, payload, prefer)
		return err
	})
	if err != nil {
		return nil, mapErr(err)
	}

	if len(rows) > 0 {
		s.publishRow(ns, table, "update", rows[0], meta)
	}
	if prefer == "return=representation" {
		return map[string]any{"data": rows}, nil
	}
	return map[string]any{"updated": 1}, nil
}

// DeleteRow deletes one row.
func (s *Service) DeleteRow(ns, table string, id any, prefer string, meta json.RawMessage) (any, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	err = s.ns.WithWrite(ns, path, func(db *sql.DB) error {
		isView, err := sqlite.IsView(db, table)
		if err != nil {
			return err
		}
		if isView {
			return domain.NewError(domain.ErrReadOnly, 405, fmt.Sprintf("Cannot delete from view '%s'. Views are read-only.", table))
		}
		rows, err = sqlite.DeleteRowByID(db, table, id, prefer)
		return err
	})
	if err != nil {
		return nil, mapErr(err)
	}

	if len(rows) > 0 {
		s.publishRow(ns, table, "delete", rows[0], meta)
	}
	if prefer == "return=representation" {
		return map[string]any{"data": rows}, nil
	}
	return map[string]any{"deleted": 1}, nil
}

// BulkUpdate updates rows by filter.
func (s *Service) BulkUpdate(ns, table string, query map[string][]string, payload map[string]any, prefer string, meta json.RawMessage) (any, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}

	where, args, err := sqlite.BuildWhereForBulk(query)
	if err != nil {
		return nil, domain.NewError(domain.ErrInvalidRequest, 400, err.Error())
	}

	var ids []any
	var rows []map[string]any
	err = s.ns.WithWrite(ns, path, func(db *sql.DB) error {
		isView, err := sqlite.IsView(db, table)
		if err != nil {
			return err
		}
		if isView {
			return domain.NewError(domain.ErrReadOnly, 405, fmt.Sprintf("Cannot update view '%s'. Views are read-only.", table))
		}
		ids, rows, err = sqlite.BulkUpdateRows(db, table, payload, where, args, prefer)
		return err
	})
	if err != nil {
		return nil, mapErr(err)
	}
	if len(ids) > 0 {
		s.publishBulk(ns, table, "bulk_update", ids, meta)
	}
	if prefer == "return=representation" {
		return map[string]any{"data": rows}, nil
	}
	return map[string]any{"updated": len(ids)}, nil
}

// BulkDelete deletes rows by filter.
func (s *Service) BulkDelete(ns, table string, query map[string][]string, prefer string, meta json.RawMessage) (any, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}

	where, args, err := sqlite.BuildWhereForBulk(query)
	if err != nil {
		return nil, domain.NewError(domain.ErrInvalidRequest, 400, err.Error())
	}

	var ids []any
	var rows []map[string]any
	err = s.ns.WithWrite(ns, path, func(db *sql.DB) error {
		isView, err := sqlite.IsView(db, table)
		if err != nil {
			return err
		}
		if isView {
			return domain.NewError(domain.ErrReadOnly, 405, fmt.Sprintf("Cannot delete from view '%s'. Views are read-only.", table))
		}
		ids, rows, err = sqlite.BulkDeleteRows(db, table, where, args, prefer)
		return err
	})
	if err != nil {
		return nil, mapErr(err)
	}
	if len(ids) > 0 {
		s.publishBulk(ns, table, "bulk_delete", ids, meta)
	}
	if prefer == "return=representation" {
		return map[string]any{"data": rows}, nil
	}
	return map[string]any{"deleted": len(ids)}, nil
}

// Query executes read-only SQL statements.
func (s *Service) Query(ns string, req sqlite.QueryRequest) (any, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}
	var out any
	err = s.ns.WithRead(ns, path, func(db *sql.DB) error {
		result, err := sqlite.ExecuteReadOnly(db, req)
		if err != nil {
			return err
		}
		out = result
		return nil
	})
	if err != nil {
		if strings.Contains(err.Error(), "sql_not_read_only") {
			return nil, domain.NewError(domain.ErrSQLNotReadOnly, 400, "Only read-only SELECT queries are allowed")
		}
		return nil, mapErr(err)
	}
	return out, nil
}

// Changelog returns schema changelog entries.
func (s *Service) Changelog(ns, table string, limit, offset int) ([]sqlite.ChangelogEntry, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}
	var out []sqlite.ChangelogEntry
	err = s.ns.WithRead(ns, path, func(db *sql.DB) error {
		entries, err := sqlite.ListChangelog(db, table, limit, offset)
		if err != nil {
			return err
		}
		out = entries
		return nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return out, nil
}

// Stats returns namespace statistics.
func (s *Service) Stats(ns string) (map[string]any, error) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return nil, err
	}
	cfg, err := s.GetNamespaceConfig(ns)
	if err != nil {
		return nil, err
	}
	cfgMap := map[string]any{"journal_mode": cfg.JournalMode, "busy_timeout": cfg.BusyTimeout, "max_db_size": cfg.MaxDBSize, "query_timeout": cfg.QueryTimeout, "foreign_keys": cfg.ForeignKeys, "read_only": cfg.ReadOnly}

	var out map[string]any
	err = s.ns.WithRead(ns, path, func(db *sql.DB) error {
		stats, err := sqlite.Stats(db, path, cfgMap)
		if err != nil {
			return err
		}
		out = stats
		return nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return out, nil
}

// Subscribe registers SSE subscriptions.
func (s *Service) Subscribe(ns string, tables []string) (uint64, <-chan domain.SSEEvent, error) {
	if _, err := s.pathForNamespace(ns); err != nil {
		return 0, nil, err
	}
	id, ch := s.broker.Subscribe(ns, tables)
	return id, ch, nil
}

// Unsubscribe removes SSE subscription.
func (s *Service) Unsubscribe(ns string, id uint64) {
	s.broker.Unsubscribe(ns, id)
}

func (s *Service) publishRow(ns, table, action string, row map[string]any, meta json.RawMessage) {
	ev := domain.SSEEvent{
		Namespace: ns,
		Table:     table,
		Action:    action,
		Row:       row,
		Meta:      meta,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	s.broker.Publish(ns, ev)
	s.publishViewInvalidations(ns, table, action)
}

func (s *Service) publishBulk(ns, table, action string, ids []any, meta json.RawMessage) {
	ev := domain.SSEEvent{
		Namespace: ns,
		Table:     table,
		Action:    action,
		RowCount:  len(ids),
		RowIDs:    ids,
		Meta:      meta,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	s.broker.Publish(ns, ev)
	s.publishViewInvalidations(ns, table, action)
}

func (s *Service) publishSchema(ns, table string, detail map[string]any, meta json.RawMessage) {
	ev := domain.SSEEvent{
		Namespace: ns,
		Table:     table,
		Action:    "schema_change",
		Detail:    detail,
		Meta:      meta,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	s.broker.Publish(ns, ev)
}

func (s *Service) publishViewInvalidations(ns, sourceTable, sourceAction string) {
	path, err := s.pathForNamespace(ns)
	if err != nil {
		return
	}
	_ = s.ns.WithRead(ns, path, func(db *sql.DB) error {
		tables, err := sqlite.ListTables(db)
		if err != nil {
			return err
		}
		for _, t := range tables {
			if t["type"] != "view" {
				continue
			}
			sources, _ := t["source_tables"].([]string)
			for _, st := range sources {
				if st == sourceTable {
					s.broker.Publish(ns, domain.SSEEvent{
						Namespace:    ns,
						Table:        t["name"].(string),
						Action:       "source_changed",
						SourceTable:  sourceTable,
						SourceAction: sourceAction,
						Timestamp:    time.Now().UTC().Format(time.RFC3339),
					})
					break
				}
			}
		}
		return nil
	})
}

func mapErr(err error) error {
	var derr *domain.Error
	if errors.As(err, &derr) {
		return derr
	}
	if errors.Is(err, sql.ErrNoRows) {
		return domain.NewError(domain.ErrNotFound, 404, "resource not found")
	}
	msg := err.Error()
	if strings.Contains(msg, "validation_failed") {
		return domain.NewError(domain.ErrValidationFailed, 400, msg)
	}
	if strings.Contains(msg, "table names with _ prefix") {
		return domain.NewError(domain.ErrValidationFailed, 400, msg)
	}
	if strings.Contains(msg, "view") && strings.Contains(msg, "read-only") {
		return domain.NewError(domain.ErrReadOnly, 405, msg)
	}
	if strings.Contains(msg, "invalid") {
		return domain.NewError(domain.ErrInvalidRequest, 400, msg)
	}
	if strings.Contains(strings.ToLower(msg), "constraint") {
		return domain.NewError(domain.ErrConflict, 409, msg)
	}
	return domain.WrapError(domain.ErrInternal, 500, "internal server error", err)
}

// BuildImportPath returns absolute import path target for namespace utility commands.
func (s *Service) BuildImportPath(name string) (string, error) {
	if err := validateNamespaceName(name); err != nil {
		return "", err
	}
	path := control.PathFor(s.dataDir, name)
	return filepath.Abs(path)
}

// WithNamespaceRead runs a raw read callback (used by handlers for advanced needs).
func (s *Service) WithNamespaceRead(name string, fn func(*sql.DB) error) error {
	path, err := s.pathForNamespace(name)
	if err != nil {
		return err
	}
	if err := s.ns.WithRead(name, path, fn); err != nil {
		return mapErr(err)
	}
	return nil
}

// WithNamespaceWrite runs a raw write callback (used by handlers for advanced needs).
func (s *Service) WithNamespaceWrite(name string, fn func(*sql.DB) error) error {
	path, err := s.pathForNamespace(name)
	if err != nil {
		return err
	}
	if err := s.ns.WithWrite(name, path, fn); err != nil {
		return mapErr(err)
	}
	return nil
}

// NormalizeMeta extracts optional meta from arbitrary payload map.
func NormalizeMeta(payload map[string]json.RawMessage) json.RawMessage {
	if raw, ok := payload["meta"]; ok {
		return raw
	}
	return nil
}

// ParseRowsPayload normalizes POST row payloads.
func ParseRowsPayload(body map[string]json.RawMessage) ([]map[string]any, json.RawMessage, error) {
	meta := NormalizeMeta(body)

	if rawRows, ok := body["rows"]; ok {
		var rows []map[string]any
		if err := json.Unmarshal(rawRows, &rows); err != nil {
			return nil, nil, fmt.Errorf("invalid rows payload")
		}
		return rows, meta, nil
	}

	row := make(map[string]any)
	for k, raw := range body {
		if k == "meta" {
			continue
		}
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, nil, fmt.Errorf("invalid field %s", k)
		}
		row[k] = v
	}
	if len(row) == 0 {
		return nil, nil, fmt.Errorf("empty row payload")
	}
	return []map[string]any{row}, meta, nil
}

// ParseJSONMap decodes raw body into map.
func ParseJSONMap(raw []byte) (map[string]json.RawMessage, error) {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("invalid json body")
	}
	if body == nil {
		body = map[string]json.RawMessage{}
	}
	return body, nil
}

// ParseUpdatePayload decodes update payload and extracts meta.
func ParseUpdatePayload(body map[string]json.RawMessage) (map[string]any, json.RawMessage, error) {
	meta := NormalizeMeta(body)
	out := make(map[string]any)
	for k, raw := range body {
		if k == "meta" {
			continue
		}
		var v any
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			return nil, nil, fmt.Errorf("invalid field %s", k)
		}
		out[k] = normalizeJSONNumbers(v)
	}
	if len(out) == 0 {
		return nil, nil, fmt.Errorf("empty update payload")
	}
	return out, meta, nil
}

func normalizeJSONNumbers(v any) any {
	switch t := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, v2 := range t {
			m[k] = normalizeJSONNumbers(v2)
		}
		return m
	case []any:
		arr := make([]any, len(t))
		for i := range t {
			arr[i] = normalizeJSONNumbers(t[i])
		}
		return arr
	case json.Number:
		if strings.Contains(t.String(), ".") {
			f, err := t.Float64()
			if err == nil {
				return f
			}
			return t.String()
		}
		i, err := t.Int64()
		if err == nil {
			return i
		}
		f, err := t.Float64()
		if err == nil {
			return f
		}
		return t.String()
	default:
		return v
	}
}

// ParseID converts path id to integer id.
func ParseID(idStr string) (int64, error) {
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid row id")
	}
	return id, nil
}
