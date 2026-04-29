package service

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ValentinKolb/rsql/internal/domain"
)

func TestServiceHelpersAndMapErr(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.CreateNamespace(domain.NamespaceDefinition{Name: "ws", Config: domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 5000, QueryTimeout: 10000, ForeignKeys: domain.BoolPtr(true)}})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	path, err := svc.BuildImportPath("ws")
	if err != nil || !strings.HasSuffix(path, "ws.db") {
		t.Fatalf("build import path: path=%s err=%v", path, err)
	}
	if _, err := svc.BuildImportPath("bad space"); err == nil {
		t.Fatal("expected invalid namespace name")
	}

	if err := svc.WithNamespaceWrite("ws", func(db *sql.DB) error {
		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS x (id INTEGER PRIMARY KEY, v TEXT)`)
		return err
	}); err != nil {
		t.Fatalf("with namespace write: %v", err)
	}
	if err := svc.WithNamespaceRead("ws", func(db *sql.DB) error {
		var c int
		return db.QueryRow(`SELECT COUNT(*) FROM x`).Scan(&c)
	}); err != nil {
		t.Fatalf("with namespace read: %v", err)
	}
	if err := svc.WithNamespaceRead("missing", func(*sql.DB) error { return nil }); err == nil {
		t.Fatal("expected missing namespace error")
	}
	if err := svc.WithNamespaceWrite("missing", func(*sql.DB) error { return nil }); err == nil {
		t.Fatal("expected missing namespace error")
	}

	if got := NormalizeMeta(map[string]json.RawMessage{"_meta": json.RawMessage(`{"a":1}`)}); got == nil {
		t.Fatal("expected normalize meta")
	}
	if got := NormalizeMeta(map[string]json.RawMessage{}); got != nil {
		t.Fatal("expected nil normalize meta")
	}
	// Confirm the legacy unprefixed key is no longer treated as audit-meta
	// — it is just a regular row field now.
	if got := NormalizeMeta(map[string]json.RawMessage{"meta": json.RawMessage(`{"a":1}`)}); got != nil {
		t.Fatal("expected nil normalize meta for legacy unprefixed key")
	}

	rows, meta, err := ParseRowsPayload(map[string]json.RawMessage{
		"rows":  json.RawMessage(`[{"a":1}]`),
		"_meta": json.RawMessage(`{"u":"1"}`),
	})
	if err != nil || len(rows) != 1 || meta == nil {
		t.Fatalf("parse rows payload bulk: rows=%v meta=%s err=%v", rows, string(meta), err)
	}
	rows, _, err = ParseRowsPayload(map[string]json.RawMessage{"a": json.RawMessage(`1`)})
	if err != nil || len(rows) != 1 {
		t.Fatalf("parse rows payload single: %v %v", rows, err)
	}
	if _, _, err := ParseRowsPayload(map[string]json.RawMessage{"a": json.RawMessage(`{`)}); err == nil {
		t.Fatal("expected invalid single-field payload")
	}
	if _, _, err := ParseRowsPayload(map[string]json.RawMessage{"rows": json.RawMessage(`bad`)}); err == nil {
		t.Fatal("expected invalid rows payload")
	}
	if _, _, err := ParseRowsPayload(map[string]json.RawMessage{}); err == nil {
		t.Fatal("expected empty row payload")
	}

	if _, err := ParseJSONMap([]byte(`{"a":1}`)); err != nil {
		t.Fatalf("parse json map valid: %v", err)
	}
	body, err := ParseJSONMap([]byte(`null`))
	if err != nil {
		t.Fatalf("parse json map null: %v", err)
	}
	if body == nil || len(body) != 0 {
		t.Fatalf("expected empty map from null body: %#v", body)
	}
	if _, err := ParseJSONMap([]byte(`bad`)); err == nil {
		t.Fatal("expected parse json map error")
	}

	upd, meta, err := ParseUpdatePayload(map[string]json.RawMessage{
		"a":     json.RawMessage(`1`),
		"_meta": json.RawMessage(`{"u":"1"}`),
	})
	if err != nil || len(upd) != 1 || meta == nil {
		t.Fatalf("parse update payload: %v %v %v", upd, string(meta), err)
	}
	if _, _, err := ParseUpdatePayload(map[string]json.RawMessage{}); err == nil {
		t.Fatal("expected empty update payload")
	}
	if _, _, err := ParseUpdatePayload(map[string]json.RawMessage{"a": json.RawMessage(`bad`)}); err == nil {
		t.Fatal("expected invalid update field")
	}

	n := normalizeJSONNumbers(map[string]any{"a": json.Number("1"), "b": []any{json.Number("1.2")}}).(map[string]any)
	if _, ok := n["a"].(int64); !ok {
		t.Fatalf("expected int64 normalized number")
	}
	arr := n["b"].([]any)
	if _, ok := arr[0].(float64); !ok {
		t.Fatalf("expected float64 normalized number")
	}

	if _, err := ParseID("abc"); err == nil {
		t.Fatal("expected invalid id")
	}
	if _, err := ParseID("0"); err == nil {
		t.Fatal("expected invalid id zero")
	}
	if id, err := ParseID("42"); err != nil || id != 42 {
		t.Fatalf("parse id: id=%d err=%v", id, err)
	}

	cases := []error{
		domain.NewError(domain.ErrInvalidRequest, 400, "x"),
		sql.ErrNoRows,
		errors.New("validation_failed: x"),
		errors.New("table names with _ prefix are reserved"),
		errors.New("view is read-only"),
		errors.New("invalid field"),
		errors.New("constraint failed"),
		errors.New("other"),
	}
	for _, in := range cases {
		if mapErr(in) == nil {
			t.Fatalf("mapErr returned nil for %v", in)
		}
	}
}
