package service

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ValentinKolb/rsql/internal/domain"
	"github.com/ValentinKolb/rsql/internal/store/sqlite"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	svc, err := New(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func TestNamespaceLifecycle(t *testing.T) {
	svc := newTestService(t)

	cfg := domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: true}
	created, err := svc.CreateNamespace(domain.NamespaceDefinition{Name: "workspace1", Config: cfg})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if created["name"] != "workspace1" {
		t.Fatalf("unexpected namespace name: %v", created["name"])
	}

	list, err := svc.ListNamespaces()
	if err != nil {
		t.Fatalf("list namespaces: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 namespace, got %d", len(list))
	}

	info, err := svc.GetNamespace("workspace1")
	if err != nil {
		t.Fatalf("get namespace: %v", err)
	}
	if info["name"] != "workspace1" {
		t.Fatalf("unexpected get namespace result: %v", info)
	}

	updatedCfg := domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 6000, QueryTimeout: 15000, ForeignKeys: true, ReadOnly: false}
	if _, err := svc.UpdateNamespaceConfig("workspace1", updatedCfg); err != nil {
		t.Fatalf("update namespace config: %v", err)
	}

	readCfg, err := svc.GetNamespaceConfig("workspace1")
	if err != nil {
		t.Fatalf("get namespace config: %v", err)
	}
	if readCfg.BusyTimeout != 6000 {
		t.Fatalf("expected busy_timeout 6000, got %d", readCfg.BusyTimeout)
	}

	if err := svc.DeleteNamespace("workspace1"); err != nil {
		t.Fatalf("delete namespace: %v", err)
	}

	if _, err := svc.GetNamespace("workspace1"); err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestSchemaRowsQueryAndStats(t *testing.T) {
	svc := newTestService(t)

	_, err := svc.CreateNamespace(domain.NamespaceDefinition{Name: "work", Config: domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: true}})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	createReq := domain.TableCreateRequest{
		Type: "table",
		Name: "kunden",
		Columns: []domain.ColumnDefinition{
			{Name: "firma", Type: "text", NotNull: true},
			{Name: "umsatz", Type: "real", Min: floatPtr(0)},
			{Name: "email", Type: "text", Unique: true},
			{Name: "status", Type: "select", Options: []string{"active", "inactive"}},
			{Name: "bezahlt", Type: "boolean", Default: false},
		},
	}
	if err := svc.CreateTableOrView("work", createReq); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tables, err := svc.TablesList("work")
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if len(tables) != 1 || tables[0]["name"] != "kunden" {
		t.Fatalf("unexpected tables: %#v", tables)
	}

	insertRes, err := svc.InsertRows("work", "kunden", []map[string]any{{
		"firma":  "Muller GmbH",
		"umsatz": 1200.5,
		"email":  "info@mueller.de",
		"status": "active",
	}}, "return=representation", json.RawMessage(`{"user_id":"test"}`))
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}
	insertMap := insertRes.(map[string]any)
	if len(insertMap["data"].([]map[string]any)) != 1 {
		t.Fatalf("unexpected insert data: %#v", insertMap)
	}

	_, err = svc.InsertRows("work", "kunden", []map[string]any{{
		"firma":  "Schmidt AG",
		"umsatz": 2200.0,
		"email":  "info@schmidt.de",
		"status": "inactive",
	}, {
		"firma":  "Weber KG",
		"umsatz": 500.0,
		"email":  "info@weber.de",
		"status": "active",
	}}, "", nil)
	if err != nil {
		t.Fatalf("bulk insert: %v", err)
	}

	rowsRes, err := svc.ListRows("work", "kunden", map[string][]string{"status": {"eq.active"}, "limit": {"10"}, "offset": {"0"}})
	if err != nil {
		t.Fatalf("list rows: %v", err)
	}
	rowsWrapped := rowsRes.(domain.ListResponse[map[string]any])
	if rowsWrapped.Meta.FilterCount != 2 {
		t.Fatalf("unexpected filter_count: %d", rowsWrapped.Meta.FilterCount)
	}

	row1, err := svc.GetRow("work", "kunden", int64(1))
	if err != nil {
		t.Fatalf("get row: %v", err)
	}
	if row1["firma"] != "Muller GmbH" {
		t.Fatalf("unexpected row data: %#v", row1)
	}

	if _, err := svc.UpdateRow("work", "kunden", int64(1), map[string]any{"umsatz": map[string]any{"$increment": 200.0}}, "", nil); err != nil {
		t.Fatalf("update row: %v", err)
	}

	queryRes, err := svc.Query("work", sqlite.QueryRequest{SQL: "SELECT COUNT(*) AS c FROM kunden", Params: []any{}})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	queryMap := queryRes.(map[string]any)
	if len(queryMap["data"].([]map[string]any)) != 1 {
		t.Fatalf("unexpected query result: %#v", queryMap)
	}

	stats, err := svc.Stats("work")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats["table_count"].(int) != 1 {
		t.Fatalf("unexpected table_count in stats: %#v", stats)
	}

	changelog, err := svc.Changelog("work", "kunden", 50, 0)
	if err != nil {
		t.Fatalf("changelog: %v", err)
	}
	if len(changelog) == 0 {
		t.Fatal("expected changelog entries")
	}

	if _, err := svc.DeleteRow("work", "kunden", int64(2), "", nil); err != nil {
		t.Fatalf("delete row: %v", err)
	}
}

func TestSSEAndDupExportImport(t *testing.T) {
	svc := newTestService(t)

	_, err := svc.CreateNamespace(domain.NamespaceDefinition{Name: "src", Config: domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: true}})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if err := svc.CreateTableOrView("src", domain.TableCreateRequest{Type: "table", Name: "items", Columns: []domain.ColumnDefinition{{Name: "name", Type: "text", NotNull: true}}}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	subID, ch, err := svc.Subscribe("src", []string{"items"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer svc.Unsubscribe("src", subID)

	if _, err := svc.InsertRows("src", "items", []map[string]any{{"name": "A"}}, "", json.RawMessage(`{"source":"test"}`)); err != nil {
		t.Fatalf("insert for sse: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Action != "insert" || ev.Table != "items" {
			t.Fatalf("unexpected event: %#v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for sse event")
	}

	dup, err := svc.DuplicateNamespace("src", "copy")
	if err != nil {
		t.Fatalf("duplicate namespace: %v", err)
	}
	if dup["target"] != "copy" {
		t.Fatalf("unexpected duplicate result: %#v", dup)
	}

	exportPath, err := svc.ExportNamespace("src")
	if err != nil {
		t.Fatalf("export namespace: %v", err)
	}
	content, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("read exported db: %v", err)
	}

	if err := svc.ImportNamespaceDB("copy", bytes.NewReader(content)); err != nil {
		t.Fatalf("import db: %v", err)
	}

	rows, err := svc.ListRows("copy", "items", map[string][]string{"limit": {"10"}, "offset": {"0"}})
	if err != nil {
		t.Fatalf("list copied rows: %v", err)
	}
	if rows.(domain.ListResponse[map[string]any]).Meta.TotalCount == 0 {
		t.Fatal("expected copied data after import")
	}
}

func TestSSEViewInvalidationEvent(t *testing.T) {
	svc := newTestService(t)

	_, err := svc.CreateNamespace(domain.NamespaceDefinition{Name: "ws", Config: domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: true}})
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
		t.Fatalf("create source table: %v", err)
	}
	if err := svc.CreateTableOrView("ws", domain.TableCreateRequest{
		Type: "view",
		Name: "v_kunden",
		SQL:  "SELECT firma FROM kunden",
	}); err != nil {
		t.Fatalf("create dependent view: %v", err)
	}

	subID, ch, err := svc.Subscribe("ws", []string{"v_kunden"})
	if err != nil {
		t.Fatalf("subscribe view: %v", err)
	}
	defer svc.Unsubscribe("ws", subID)

	if _, err := svc.InsertRows("ws", "kunden", []map[string]any{{"firma": "A"}}, "", nil); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Action != "source_changed" || ev.Table != "v_kunden" {
			t.Fatalf("unexpected invalidation event: %#v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for view invalidation event")
	}
}

func TestImportNamespaceCSVSuccess(t *testing.T) {
	svc := newTestService(t)

	_, err := svc.CreateNamespace(domain.NamespaceDefinition{Name: "csvok", Config: domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: true}})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if err := svc.CreateTableOrView("csvok", domain.TableCreateRequest{
		Type: "table",
		Name: "items",
		Columns: []domain.ColumnDefinition{
			{Name: "name", Type: "text", NotNull: true},
			{Name: "state", Type: "select", Options: []string{"open", "done"}},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err = svc.ImportNamespaceCSV("csvok", "items", strings.NewReader("name,state\nCSV Item,open\n"), nil)
	if err != nil {
		t.Fatalf("csv import should succeed, got: %v", err)
	}
}

func floatPtr(v float64) *float64 { return &v }
