package control

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestRegistryCRUD(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("open registry: %v", err)
	}
	defer r.Close()

	p := PathFor(dir, "workspace")
	if filepath.Base(p) != "workspace.db" {
		t.Fatalf("unexpected path: %s", p)
	}

	if err := r.Create("workspace", p); err != nil {
		t.Fatalf("create namespace record: %v", err)
	}

	rec, err := r.Get("workspace")
	if err != nil {
		t.Fatalf("get namespace record: %v", err)
	}
	if rec.Name != "workspace" {
		t.Fatalf("unexpected record: %#v", rec)
	}

	list, err := r.List()
	if err != nil {
		t.Fatalf("list namespace records: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("unexpected list len: %d", len(list))
	}

	if err := r.Delete("workspace"); err != nil {
		t.Fatalf("delete namespace record: %v", err)
	}
	if err := r.Delete("workspace"); err == nil {
		t.Fatal("expected sql.ErrNoRows for second delete")
	}

	if _, err := r.Get("workspace"); err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestRegistryOpenError(t *testing.T) {
	file := filepath.Join(t.TempDir(), "as-file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := Open(file); err == nil {
		t.Fatal("expected open error when data dir is a file")
	}
}

func TestRegistryCloseNil(t *testing.T) {
	var r *Registry
	if err := r.Close(); err != nil {
		t.Fatalf("close nil registry: %v", err)
	}
	r = &Registry{}
	if err := r.Close(); err != nil {
		t.Fatalf("close empty registry: %v", err)
	}
}

func TestRegistryInitSchemaError(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("open registry: %v", err)
	}
	defer r.Close()

	_, err = r.db.Exec(`DROP TABLE namespaces`)
	if err != nil {
		t.Fatalf("drop namespaces: %v", err)
	}
	if err := r.Delete("x"); err == nil {
		t.Fatal("expected error after dropping table")
	}

	if _, err := r.Get("x"); err == nil {
		t.Fatal("expected get error after dropping table")
	}
	if _, err := r.List(); err == nil {
		t.Fatal("expected list error after dropping table")
	}
	if err := r.Create("x", "y"); err == nil {
		t.Fatal("expected create error after dropping table")
	}

	// restore schema for clean close.
	if err := initSchema(r.db); err != nil {
		t.Fatalf("re-init schema: %v", err)
	}
}

func TestRegistryDeleteNoRowsType(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("open registry: %v", err)
	}
	defer r.Close()

	err = r.Delete("missing")
	if err == nil {
		t.Fatal("expected delete error for missing namespace")
	}
	if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}
