package sqlite

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ValentinKolb/rsql/internal/domain"
)

func TestSchemaLifecycleAndIndexes(t *testing.T) {
	db, _ := openTestDB(t)

	createTable := domain.TableCreateRequest{
		Type: "table",
		Name: "kunden",
		Columns: []domain.ColumnDefinition{
			{Name: "firma", Type: "text", NotNull: true, Index: true},
			{Name: "email", Type: "text", Unique: true},
			{Name: "umsatz", Type: "real", Min: floatPtr(0)},
			{Name: "status", Type: "select", Options: []string{"active", "inactive"}},
			{Name: "gesamt", Type: "real", Formula: "umsatz * 2"},
		},
		Metadata: json.RawMessage(`{"icon":"users"}`),
	}
	if err := CreateTableOrView(db, createTable); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if err := CreateTableOrView(db, domain.TableCreateRequest{Type: "table", Name: "_hidden", Columns: []domain.ColumnDefinition{{Name: "x", Type: "text"}}}); err == nil {
		t.Fatal("expected reserved name error")
	}
	if err := CreateTableOrView(db, domain.TableCreateRequest{Type: "table", Name: "bad", Columns: []domain.ColumnDefinition{{Name: "x", Type: "unknown"}}}); err == nil {
		t.Fatal("expected bad type error")
	}

	if err := CreateTableOrView(db, domain.TableCreateRequest{Type: "view", Name: "top_kunden", SQL: "SELECT firma, umsatz FROM kunden"}); err != nil {
		t.Fatalf("create view: %v", err)
	}
	if err := CreateTableOrView(db, domain.TableCreateRequest{Type: "view", Name: "bad_view"}); err == nil {
		t.Fatal("expected empty view sql error")
	}
	if err := CreateTableOrView(db, domain.TableCreateRequest{Type: "x", Name: "invalid"}); err == nil {
		t.Fatal("expected invalid object type")
	}

	list, err := ListTables(db)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("unexpected object count: %d (%#v)", len(list), list)
	}

	tableObj, err := GetTable(db, "kunden")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if tableObj["name"] != "kunden" {
		t.Fatalf("unexpected table object: %#v", tableObj)
	}

	viewObj, err := GetTable(db, "top_kunden")
	if err != nil {
		t.Fatalf("get view: %v", err)
	}
	if viewObj["type"] != "view" {
		t.Fatalf("expected view type")
	}
	if _, err := GetTable(db, "missing"); err == nil {
		t.Fatal("expected table not found")
	}

	upd := domain.TableUpdateRequest{
		Rename: "kunden_new",
		AddColumns: []domain.ColumnDefinition{
			{Name: "telefon", Type: "text"},
		},
		RenameColumns: map[string]string{"firma": "company"},
	}
	if err := UpdateTableOrView(db, "kunden", upd); err != nil {
		t.Fatalf("update table: %v", err)
	}
	if err := UpdateTableOrView(db, "top_kunden", domain.TableUpdateRequest{SQL: "SELECT company, umsatz FROM kunden_new"}); err != nil {
		t.Fatalf("update view: %v", err)
	}
	if err := UpdateTableOrView(db, "top_kunden", domain.TableUpdateRequest{}); err == nil {
		t.Fatal("expected missing sql on view update")
	}

	if err := CreateIndex(db, "kunden_new", domain.IndexCreateRequest{Type: "index", Columns: []string{"telefon"}}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := CreateIndex(db, "kunden_new", domain.IndexCreateRequest{Type: "unique", Columns: []string{"email"}}); err != nil {
		t.Fatalf("create unique index: %v", err)
	}
	if err := CreateIndex(db, "kunden_new", domain.IndexCreateRequest{Type: "fts", Columns: []string{"company", "email"}}); err != nil {
		t.Fatalf("create fts index: %v", err)
	}
	if err := CreateIndex(db, "kunden_new", domain.IndexCreateRequest{Type: "unknown", Columns: []string{"x"}}); err == nil {
		t.Fatal("expected invalid index type")
	}

	idxs, err := tableIndexes(db, "kunden_new")
	if err != nil {
		t.Fatalf("table indexes: %v", err)
	}
	if len(idxs) == 0 {
		t.Fatal("expected indexes")
	}

	if err := DeleteIndex(db, "kunden_new", "idx_kunden_new_telefon", nil); err != nil {
		t.Fatalf("delete index: %v", err)
	}
	if err := DeleteIndex(db, "kunden_new", "_fts_kunden_new", nil); err != nil {
		t.Fatalf("delete fts index: %v", err)
	}

	if extract := extractSourceTables("SELECT * FROM a JOIN b ON a.id=b.id"); len(extract) != 2 {
		t.Fatalf("unexpected source tables: %#v", extract)
	}

	if err := DeleteTableOrView(db, "top_kunden", nil); err != nil {
		t.Fatalf("delete view: %v", err)
	}
	if err := DeleteTableOrView(db, "kunden_new", nil); err != nil {
		t.Fatalf("delete table: %v", err)
	}
	if err := DeleteTableOrView(db, "missing", nil); err == nil {
		t.Fatal("expected delete missing error")
	}
}

func TestSchemaHelpersAndTxMeta(t *testing.T) {
	db, _ := openTestDB(t)

	if got, err := storageType("text"); err != nil || got != "TEXT" {
		t.Fatalf("unexpected storage type: %q %v", got, err)
	}
	if _, err := storageType("nope"); err == nil {
		t.Fatal("expected unsupported storage type")
	}

	if sqlLiteral(nil) != "NULL" || sqlLiteral(true) != "1" || sqlLiteral(false) != "0" {
		t.Fatal("unexpected bool/nil sql literal")
	}
	if sqlLiteral("a'b") != "'a''b'" {
		t.Fatalf("unexpected string sql literal")
	}

	if prefixedList("new.", []string{"a", "b"}) != `new."a",new."b"` {
		t.Fatalf("unexpected prefixed list")
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := putMetaTx(tx, "x", "y", map[string]any{"a": 1}); err != nil {
		t.Fatalf("put meta tx: %v", err)
	}
	var m map[string]any
	ok, err := getMetaTx(tx, "x", "y", &m)
	if err != nil || !ok {
		t.Fatalf("get meta tx: ok=%v err=%v", ok, err)
	}
	if err := deleteMetaTx(tx, "x", "y"); err != nil {
		t.Fatalf("delete meta tx: %v", err)
	}
	if err := appendSchemaLogTx(tx, "act", "tbl", map[string]any{"k": "v"}, json.RawMessage(`{"u":"1"}`)); err != nil {
		t.Fatalf("append schema log tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	if nowUTC() == "" {
		t.Fatal("expected nowUTC")
	}

	if n, err := columnCount(db, "missing"); err != nil || n != 0 {
		t.Fatalf("expected missing table column count 0 without error, got n=%d err=%v", n, err)
	}
	if _, err := countRows(db, "missing"); err == nil {
		t.Fatal("expected missing table error")
	}
	if cols, err := indexColumns(db, "missing_idx"); err != nil || len(cols) != 0 {
		t.Fatalf("expected missing index cols empty without error, got cols=%v err=%v", cols, err)
	}
}

func TestGeneratedFormulaValidation(t *testing.T) {
	db, _ := openTestDB(t)

	if err := CreateTableOrView(db, domain.TableCreateRequest{
		Type: "table",
		Name: "safe_formula",
		Columns: []domain.ColumnDefinition{
			{Name: "a", Type: "real"},
			{Name: "b", Type: "real"},
			{Name: "sum", Type: "real", Formula: "a + b * 2"},
		},
	}); err != nil {
		t.Fatalf("expected safe formula to be accepted: %v", err)
	}

	if err := CreateTableOrView(db, domain.TableCreateRequest{
		Type: "table",
		Name: "unsafe_formula",
		Columns: []domain.ColumnDefinition{
			{Name: "a", Type: "real"},
			{Name: "evil", Type: "real", Formula: "1) VIRTUAL; DROP TABLE safe_formula; --"},
		},
	}); err == nil {
		t.Fatal("expected unsafe formula to be rejected")
	}

	if err := CreateTableOrView(db, domain.TableCreateRequest{
		Type: "table",
		Name: "formula_update",
		Columns: []domain.ColumnDefinition{
			{Name: "a", Type: "real"},
		},
	}); err != nil {
		t.Fatalf("create formula_update table: %v", err)
	}

	if err := UpdateTableOrView(db, "formula_update", domain.TableUpdateRequest{
		AddColumns: []domain.ColumnDefinition{
			{Name: "bad", Type: "real", Formula: "a; DROP TABLE formula_update;"},
		},
	}); err == nil {
		t.Fatal("expected unsafe update formula to be rejected")
	}
}

// TestValidateGeneratedFormulaRegression locks down the formula validator
// against bypass primitives reported in past code reviews. Generated columns
// are concatenated into DDL strings (SQLite does not bind DDL parameters),
// so this validator is the only line of defence.
func TestValidateGeneratedFormulaRegression(t *testing.T) {
	rejected := []struct {
		name    string
		formula string
	}{
		// Statement terminators / multi-statement smuggling.
		{"semicolon", "1) VIRTUAL; DROP TABLE t"},
		{"semicolon_only", "a + b;"},
		{"semicolon_in_subexpr", "(a;b)"},

		// Comments.
		{"line_comment", "a + b -- DROP"},
		{"block_comment_open", "a /* hidden"},
		{"block_comment_full", "/* DROP */ 1"},

		// Quoting (would let an attacker introduce identifiers/literals).
		{"single_quote", "a + 'x'"},
		{"double_quote", `a + "evil"`},
		{"backtick", "a + `evil`"},

		// SQL keywords that must never appear in a formula.
		{"select", "SELECT 1"},
		{"select_subquery", "(SELECT 1)"},
		{"from", "a FROM t"},
		{"where", "a WHERE 1"},
		{"drop", "DROP TABLE t"},
		{"delete", "DELETE FROM t"},
		{"update", "UPDATE t SET a=1"},
		{"insert", "INSERT INTO t VALUES (1)"},
		{"alter", "ALTER TABLE t"},
		{"create", "CREATE TABLE t"},
		{"pragma", "PRAGMA writable_schema"},
		{"attach", "ATTACH DATABASE 'x'"},
		{"detach", "DETACH DATABASE main"},
		{"vacuum", "VACUUM"},
		{"reindex", "REINDEX t"},
		{"truncate", "TRUNCATE t"},
		{"trigger", "TRIGGER t"},
		{"view", "VIEW t"},
		{"index", "INDEX t"},
		{"union", "1 UNION SELECT 1"},
		{"with", "WITH x AS (SELECT 1) SELECT 1"},

		// Empty / whitespace.
		{"empty", ""},
		{"whitespace", "   "},
	}
	for _, tc := range rejected {
		t.Run("reject_"+tc.name, func(t *testing.T) {
			if err := validateGeneratedFormula(tc.formula); err == nil {
				t.Fatalf("validateGeneratedFormula accepted forbidden input: %q", tc.formula)
			}
		})
	}

	accepted := []string{
		"a + b",
		"(price * quantity) / 100",
		"a * 2 + b",
		"a - b",
		"a % 2",
		"a > 0",
		"(a + b) <= 100",
		"a & b",
		"a | b",
	}
	for _, f := range accepted {
		t.Run("accept_"+f, func(t *testing.T) {
			if err := validateGeneratedFormula(f); err != nil {
				t.Fatalf("validateGeneratedFormula rejected valid input %q: %v", f, err)
			}
		})
	}
}

// TestColumnNameReservation locks down the contract: rsql-managed column
// names cannot be created, renamed to/from, or dropped by user input.
func TestColumnNameReservation(t *testing.T) {
	db, _ := openTestDB(t)
	if err := CreateTableOrView(db, domain.TableCreateRequest{
		Type:    "table",
		Name:    "items",
		Columns: []domain.ColumnDefinition{{Name: "label", Type: "text"}},
	}); err != nil {
		t.Fatalf("create base table: %v", err)
	}

	rejectedNames := []string{"id", "created_at", "updated_at", "_meta", "_secret", "_anything"}

	t.Run("create_with_reserved_column", func(t *testing.T) {
		for _, name := range rejectedNames {
			err := CreateTableOrView(db, domain.TableCreateRequest{
				Type: "table", Name: "doomed_" + strings.ReplaceAll(name, "_", ""),
				Columns: []domain.ColumnDefinition{{Name: name, Type: "text"}},
			})
			if err == nil {
				t.Fatalf("create with reserved column %q must fail", name)
			}
		}
	})

	t.Run("add_column_reserved", func(t *testing.T) {
		for _, name := range rejectedNames {
			err := UpdateTableOrView(db, "items", domain.TableUpdateRequest{
				AddColumns: []domain.ColumnDefinition{{Name: name, Type: "text"}},
			})
			if err == nil {
				t.Fatalf("add column %q must be rejected", name)
			}
		}
	})

	t.Run("rename_column_to_reserved", func(t *testing.T) {
		for _, name := range rejectedNames {
			err := UpdateTableOrView(db, "items", domain.TableUpdateRequest{
				RenameColumns: map[string]string{"label": name},
			})
			if err == nil {
				t.Fatalf("rename to reserved %q must be rejected", name)
			}
		}
	})

	t.Run("rename_reserved_column", func(t *testing.T) {
		for _, name := range []string{"id", "created_at", "updated_at"} {
			err := UpdateTableOrView(db, "items", domain.TableUpdateRequest{
				RenameColumns: map[string]string{name: "user_" + name},
			})
			if err == nil {
				t.Fatalf("renaming reserved column %q must be rejected", name)
			}
		}
	})

	t.Run("drop_reserved_column", func(t *testing.T) {
		for _, name := range []string{"id", "created_at", "updated_at"} {
			err := UpdateTableOrView(db, "items", domain.TableUpdateRequest{
				DropColumns: []string{name},
			})
			if err == nil {
				t.Fatalf("dropping reserved column %q must be rejected", name)
			}
		}
	})

	// And the positive case: a user-defined column called "meta" must work
	// now that the audit-meta passthrough lives under "_meta".
	t.Run("user_column_meta_works", func(t *testing.T) {
		if err := CreateTableOrView(db, domain.TableCreateRequest{
			Type: "table", Name: "with_meta",
			Columns: []domain.ColumnDefinition{
				{Name: "name", Type: "text"},
				{Name: "meta", Type: "json"},
			},
		}); err != nil {
			t.Fatalf("create table with meta column: %v", err)
		}
		_, _, err := InsertRows(db, "with_meta", []map[string]any{
			{"name": "x", "meta": map[string]any{"k": "v"}},
		}, "")
		if err != nil {
			t.Fatalf("insert into meta column: %v", err)
		}
		row, err := GetRowByID(db, "with_meta", int64(1))
		if err != nil {
			t.Fatalf("read back: %v", err)
		}
		got, ok := row["meta"].(map[string]any)
		if !ok || got["k"] != "v" {
			t.Fatalf("meta round-trip failed: %#v", row["meta"])
		}
	})
}

func floatPtr(v float64) *float64 { return &v }

var _ = sql.ErrNoRows
