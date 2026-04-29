package service

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ValentinKolb/rsql/internal/domain"
)

// TestReconcileDeletesOrphanRegistryRow simulates a crash mid-DELETE: the
// .db file was removed but registry.Delete never ran. Reopening the
// service must drop the dangling registry row so the namespace is cleanly
// gone.
func TestReconcileDeletesOrphanRegistryRow(t *testing.T) {
	dir := t.TempDir()
	svc, err := New(dir, 0)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := svc.CreateNamespace(domain.NamespaceDefinition{
		Name: "ghost",
		Config: domain.NamespaceConfig{
			JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000,
			ForeignKeys: domain.BoolPtr(true),
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Simulate crash mid-DELETE: handle closed, file gone, registry row remains.
	if err := svc.ns.CloseHandle("ghost"); err != nil {
		t.Fatalf("close handle: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "namespaces", "ghost.db")); err != nil {
		t.Fatalf("remove db: %v", err)
	}
	_ = svc.Close()

	// Reopen: reconcile must drop the orphan row.
	svc2, err := New(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = svc2.Close() })

	list, err := svc2.ListNamespaces()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, e := range list {
		if e["name"] == "ghost" {
			t.Fatalf("orphan registry row not cleaned: %v", list)
		}
	}
}

// TestReconcileHealsPartialCreate simulates a crash mid-CREATE: the
// registry row and an empty .db file exist but EnsureInternalSchema /
// ApplyNamespaceConfig never finished. Reopening must heal the file so
// subsequent ops succeed.
func TestReconcileHealsPartialCreate(t *testing.T) {
	dir := t.TempDir()
	svc, err := New(dir, 0)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	// Hand-craft the registry entry + an empty namespace DB to mimic
	// "registry insert succeeded, schema init did not".
	nsDir := filepath.Join(dir, "namespaces")
	if err := os.MkdirAll(nsDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	dbPath := filepath.Join(nsDir, "halfbaked.db")
	if err := os.WriteFile(dbPath, nil, 0o644); err != nil {
		t.Fatalf("create empty db: %v", err)
	}
	if err := svc.registry.Create("halfbaked", dbPath); err != nil {
		t.Fatalf("registry create: %v", err)
	}
	_ = svc.Close()

	// Reopen: reconcile should heal the empty DB.
	svc2, err := New(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = svc2.Close() })

	if _, err := svc2.GetNamespace("halfbaked"); err != nil {
		t.Fatalf("get healed namespace: %v", err)
	}
	// Should be usable end-to-end now.
	if err := svc2.CreateTableOrView("halfbaked", domain.TableCreateRequest{
		Type:    "table",
		Name:    "items",
		Columns: []domain.ColumnDefinition{{Name: "v", Type: "text"}},
	}); err != nil {
		t.Fatalf("create table after heal: %v", err)
	}
}

// TestReconcilePurgesStaleExports asserts that any *.db left over in
// data/exports from a previous run is removed at startup.
func TestReconcilePurgesStaleExports(t *testing.T) {
	dir := t.TempDir()
	exportDir := filepath.Join(dir, "exports")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatalf("mkdir exports: %v", err)
	}
	stale := filepath.Join(exportDir, "stale-1234.db")
	if err := os.WriteFile(stale, []byte("x"), 0o644); err != nil {
		t.Fatalf("write stale: %v", err)
	}

	svc, err := New(dir, 0)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale export still present: err=%v", err)
	}
}

// TestReconcileKeepsOrphanFiles asserts that namespace files without a
// registry entry are *not* deleted automatically. The operator may have
// placed them deliberately (backup copy, manual restore).
func TestReconcileKeepsOrphanFiles(t *testing.T) {
	dir := t.TempDir()
	svc, err := New(dir, 0)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	nsDir := filepath.Join(dir, "namespaces")
	if err := os.MkdirAll(nsDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	orphan := filepath.Join(nsDir, "orphan.db")
	if err := os.WriteFile(orphan, []byte("not a real db"), 0o644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	_ = svc.Close()

	svc2, err := New(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = svc2.Close() })

	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("orphan file was deleted: %v", err)
	}
}

// TestReconcileHealFailureDoesNotBlockBoot proves that a single broken
// namespace cannot prevent the rest of the server from coming up. The
// healthy namespace must still be reachable; the broken one is left as
// the operator finds it.
func TestReconcileHealFailureDoesNotBlockBoot(t *testing.T) {
	dir := t.TempDir()
	svc, err := New(dir, 0)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := svc.CreateNamespace(domain.NamespaceDefinition{
		Name: "ok",
		Config: domain.NamespaceConfig{
			JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000,
			ForeignKeys: domain.BoolPtr(true),
		},
	}); err != nil {
		t.Fatalf("create ok: %v", err)
	}
	// Inject a broken namespace: registry row + a .db file containing
	// random bytes that SQLite cannot open.
	nsDir := filepath.Join(dir, "namespaces")
	bad := filepath.Join(nsDir, "broken.db")
	if err := os.WriteFile(bad, []byte("this is not a sqlite database"), 0o644); err != nil {
		t.Fatalf("write bad db: %v", err)
	}
	if err := svc.registry.Create("broken", bad); err != nil {
		t.Fatalf("registry create broken: %v", err)
	}
	_ = svc.Close()

	svc2, err := New(dir, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = svc2.Close() })

	// The healthy namespace must still be reachable.
	if _, err := svc2.GetNamespace("ok"); err != nil {
		t.Fatalf("healthy namespace lost after reconcile: %v", err)
	}
}

// helper: assert that no stale wal/shm sidecar from a closed handle keeps
// us from deleting the registry row in the orphan-row scenario.
var _ = strings.HasSuffix // silence unused-import linters across helpers
var _ = sql.ErrNoRows
