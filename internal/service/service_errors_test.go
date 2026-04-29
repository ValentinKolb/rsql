package service

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ValentinKolb/rsql/internal/domain"
	"github.com/ValentinKolb/rsql/internal/store/sqlite"
)

func TestServiceNamespaceErrorPaths(t *testing.T) {
	svc := newTestService(t)

	_, err := svc.CreateNamespace(domain.NamespaceDefinition{Name: "bad space"})
	assertDomainCode(t, err, domain.ErrInvalidRequest)

	_, err = svc.CreateNamespace(domain.NamespaceDefinition{
		Name:   "ws",
		Config: domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: true},
	})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	_, err = svc.CreateNamespace(domain.NamespaceDefinition{
		Name:   "ws",
		Config: domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: true},
	})
	if err != nil {
		t.Fatalf("create existing namespace should be idempotent with current registry semantics: %v", err)
	}

	_, err = svc.GetNamespace("missing")
	assertDomainCode(t, err, domain.ErrNamespaceNotFound)
	_, err = svc.UpdateNamespaceConfig("missing", domain.NamespaceConfig{})
	assertDomainCode(t, err, domain.ErrNamespaceNotFound)
	err = svc.DeleteNamespace("missing")
	assertDomainCode(t, err, domain.ErrNamespaceNotFound)
	_, err = svc.ExportNamespace("missing")
	assertDomainCode(t, err, domain.ErrNamespaceNotFound)
	err = svc.ImportNamespaceDB("missing", bytes.NewReader([]byte("x")))
	assertDomainCode(t, err, domain.ErrNamespaceNotFound)

	_, err = svc.DuplicateNamespace("missing", "copy")
	assertDomainCode(t, err, domain.ErrNamespaceNotFound)
	_, err = svc.DuplicateNamespace("ws", "bad space")
	assertDomainCode(t, err, domain.ErrInvalidRequest)

	_, err = svc.CreateNamespace(domain.NamespaceDefinition{
		Name:   "copy",
		Config: domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: true},
	})
	if err != nil {
		t.Fatalf("create copy namespace: %v", err)
	}
	_, _ = svc.DuplicateNamespace("ws", "copy")
}

func TestServiceReadOnlyAndSQLErrorPaths(t *testing.T) {
	svc := newTestService(t)
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
	if err := svc.CreateTableOrView("ws", domain.TableCreateRequest{
		Type: "view",
		Name: "v_kunden",
		SQL:  "SELECT firma FROM kunden",
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}

	_, err = svc.InsertRows("ws", "v_kunden", []map[string]any{{"firma": "x"}}, "", nil)
	assertDomainCode(t, err, domain.ErrReadOnly)
	_, err = svc.UpdateRow("ws", "v_kunden", int64(1), map[string]any{"firma": "x"}, "", nil)
	assertDomainCode(t, err, domain.ErrReadOnly)
	_, err = svc.DeleteRow("ws", "v_kunden", int64(1), "", nil)
	assertDomainCode(t, err, domain.ErrReadOnly)
	_, err = svc.BulkUpdate("ws", "v_kunden", map[string][]string{"id": {"eq.1"}}, map[string]any{"firma": "x"}, "", nil)
	assertDomainCode(t, err, domain.ErrReadOnly)
	_, err = svc.BulkDelete("ws", "v_kunden", map[string][]string{"id": {"eq.1"}}, "", nil)
	assertDomainCode(t, err, domain.ErrReadOnly)

	_, err = svc.Query("ws", sqlite.QueryRequest{SQL: "DELETE FROM kunden", Params: nil})
	assertDomainCode(t, err, domain.ErrSQLNotReadOnly)

	_, _, err = svc.Subscribe("missing", nil)
	assertDomainCode(t, err, domain.ErrNamespaceNotFound)
}

func TestServiceImportCSVErrorPaths(t *testing.T) {
	svc := newTestService(t)
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

	_, err = svc.ImportNamespaceCSV("missing", "kunden", strings.NewReader("firma\nA\n"), nil)
	assertDomainCode(t, err, domain.ErrNamespaceNotFound)

	_, err = svc.ImportNamespaceCSV("ws", "kunden", strings.NewReader(""), nil)
	assertDomainCode(t, err, domain.ErrInvalidRequest)

	_, err = svc.ImportNamespaceCSV("ws", "kunden", strings.NewReader("firma\n\"broken\n"), nil)
	assertDomainCode(t, err, domain.ErrInvalidRequest)
}

func TestServiceWithNamespaceCallbackErrors(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.CreateNamespace(domain.NamespaceDefinition{
		Name:   "ws",
		Config: domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: true},
	})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	if err = svc.WithNamespaceRead("ws", func(*sql.DB) error { return nil }); err != nil {
		t.Fatalf("with namespace read success: %v", err)
	}

	readErr := svc.WithNamespaceRead("ws", func(_ *sql.DB) error {
		return errors.New("invalid custom read")
	})
	assertDomainCode(t, readErr, domain.ErrInvalidRequest)

	writeErr := svc.WithNamespaceWrite("ws", func(_ *sql.DB) error {
		return errors.New("constraint failed")
	})
	assertDomainCode(t, writeErr, domain.ErrConflict)
}

func TestNormalizeJSONNumbersFallbackString(t *testing.T) {
	n := normalizeJSONNumbers(json.Number("not-a-number"))
	if _, ok := n.(string); !ok {
		t.Fatalf("expected fallback string type, got %T", n)
	}
}

func TestServiceNewError(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := New(file, 0); err == nil {
		t.Fatal("expected new service error")
	}
}

func assertDomainCode(t *testing.T, err error, code domain.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected domain error %s", code)
	}
	var derr *domain.Error
	if !errors.As(err, &derr) {
		t.Fatalf("expected domain error, got %T %v", err, err)
	}
	if derr.Code != code {
		t.Fatalf("expected code %s, got %s (%v)", code, derr.Code, err)
	}
}
