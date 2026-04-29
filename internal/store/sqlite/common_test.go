package sqlite

import (
	"encoding/json"
	"testing"

	"github.com/ValentinKolb/rsql/internal/domain"
)

func TestCommonHelpersAndMeta(t *testing.T) {
	db, _ := openTestDB(t)

	cfg := domain.NamespaceConfig{JournalMode: "wal", BusyTimeout: 1000, MaxDBSize: 1 << 20, QueryTimeout: 1000, ForeignKeys: domain.BoolPtr(true), ReadOnly: false}
	if err := ApplyNamespaceConfig(db, cfg); err != nil {
		t.Fatalf("apply namespace config: %v", err)
	}

	if boolInt(true) != 1 || boolInt(false) != 0 {
		t.Fatalf("unexpected boolInt")
	}
	if quotePragmaValue("w'al") != "'wal'" {
		t.Fatalf("unexpected quoted pragma")
	}
	if err := validateIdentifier("valid_1"); err != nil {
		t.Fatalf("validate identifier: %v", err)
	}
	if err := validateIdentifier("1invalid"); err == nil {
		t.Fatal("expected invalid identifier error")
	}
	if q := quotedIdentifier(`a"b`); q != `"a""b"` {
		t.Fatalf("unexpected quoted identifier: %s", q)
	}

	meta := map[string]any{"icon": "user"}
	if err := PutMeta(db, "table_meta", "kunden", meta); err != nil {
		t.Fatalf("put meta: %v", err)
	}
	var got map[string]any
	ok, err := GetMeta(db, "table_meta", "kunden", &got)
	if err != nil || !ok {
		t.Fatalf("get meta: ok=%v err=%v", ok, err)
	}
	if got["icon"] != "user" {
		t.Fatalf("unexpected meta: %#v", got)
	}

	ok, err = GetMeta(db, "table_meta", "missing", &got)
	if err != nil || ok {
		t.Fatalf("expected missing meta")
	}

	if err := DeleteMeta(db, "table_meta", "kunden"); err != nil {
		t.Fatalf("delete meta: %v", err)
	}

	if err := AppendSchemaLog(db, "create", "kunden", map[string]any{"name": "x"}, json.RawMessage(`{"u":"1"}`)); err != nil {
		t.Fatalf("append schema log: %v", err)
	}
	if nullableString(nil) != nil {
		t.Fatalf("expected nil nullableString")
	}
	if nullableString([]byte("x")).(string) != "x" {
		t.Fatalf("expected string nullableString")
	}
}
