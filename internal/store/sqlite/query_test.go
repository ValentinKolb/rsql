package sqlite

import "testing"

func TestQueryReadOnlyExecution(t *testing.T) {
	db, _ := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO t(v) VALUES ('a'),('b')`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	res, err := ExecuteReadOnly(db, QueryRequest{SQL: "SELECT id, v FROM t ORDER BY id", Params: []any{}})
	if err != nil {
		t.Fatalf("execute read-only query: %v", err)
	}
	data := res.(map[string]any)["data"].([]map[string]any)
	if len(data) != 2 {
		t.Fatalf("unexpected query data len: %d", len(data))
	}

	batch, err := ExecuteReadOnly(db, QueryRequest{Statements: []QueryStatement{
		{SQL: "SELECT COUNT(*) AS c FROM t", Params: []any{}},
		{SQL: "SELECT v FROM t WHERE id = ?", Params: []any{1}},
	}})
	if err != nil {
		t.Fatalf("execute batch query: %v", err)
	}
	if len(batch.(map[string]any)["results"].([]map[string]any)) != 2 {
		t.Fatalf("unexpected batch result")
	}

	if _, err := ExecuteReadOnly(db, QueryRequest{SQL: "UPDATE t SET v='x'"}); err == nil {
		t.Fatal("expected write statement rejection")
	}
	if _, err := ExecuteReadOnly(db, QueryRequest{SQL: "SELECT * FROM _meta"}); err == nil {
		t.Fatal("expected internal table rejection")
	}
	if _, err := ExecuteReadOnly(db, QueryRequest{SQL: "SELECT 1; DROP TABLE t"}); err == nil {
		t.Fatal("expected multi-statement rejection")
	}
	if _, err := ExecuteReadOnly(db, QueryRequest{SQL: "SELECT 1 -- comment"}); err == nil {
		t.Fatal("expected comment-based query rejection")
	}

	if isReadOnlyQuery("") {
		t.Fatalf("empty query should not be read-only")
	}
	if !isReadOnlyQuery("select 1") {
		t.Fatalf("select query should be read-only")
	}
	if isReadOnlyQuery("delete from t") {
		t.Fatalf("delete query should not be read-only")
	}
	if isReadOnlyQuery("select 1;drop table t") {
		t.Fatalf("multi statement query should not be read-only")
	}
	if isReadOnlyQuery("select 1 -- test") {
		t.Fatalf("comment query should not be read-only")
	}

	if !referencesInternalObject("select * from _x") {
		t.Fatalf("expected internal object detection")
	}
	if referencesInternalObject("select * from customers") {
		t.Fatalf("unexpected internal object detection")
	}

	if normalizeSQL(" \nselect\t1 ") != "SELECT 1" {
		t.Fatalf("unexpected normalized sql")
	}
}

// TestReadOnlyInjectionRegression covers bypass attempts that have been
// reported in past code reviews. The intent is to lock the validator's
// rejection behaviour against future refactors.
func TestReadOnlyInjectionRegression(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		// Multi-statement bypass via semicolon, with various spacing.
		{"semicolon_no_space", "SELECT 1;DROP TABLE users"},
		{"semicolon_with_space", "SELECT 1 ; DROP TABLE users"},
		{"semicolon_newline", "SELECT 1\nDROP TABLE users"},
		{"trailing_semicolon", "SELECT 1;"},

		// Comment-based bypass.
		{"line_comment_after", "SELECT 1 -- DROP"},
		{"line_comment_inline", "SELECT 1 --;DROP TABLE t"},
		{"block_comment_open", "SELECT 1 /* hidden"},
		{"block_comment_full", "SELECT /* DROP */ 1"},

		// Forbidden top-level statements.
		{"insert", "INSERT INTO t VALUES (1)"},
		{"update", "UPDATE t SET v=1"},
		{"delete", "DELETE FROM t"},
		{"create", "CREATE TABLE x(id INT)"},
		{"alter", "ALTER TABLE t ADD COLUMN y INT"},
		{"drop", "DROP TABLE t"},
		{"replace", "REPLACE INTO t VALUES (1)"},
		{"pragma", "PRAGMA writable_schema = ON"},
		{"attach", "ATTACH DATABASE 'evil.db' AS evil"},
		{"detach", "DETACH DATABASE main"},
		{"vacuum", "VACUUM"},
		{"reindex", "REINDEX t"},
		{"truncate", "TRUNCATE t"},

		// Forbidden write keyword nested in a SELECT.
		{"select_with_insert", "SELECT id FROM t WHERE id IN (INSERT INTO u VALUES (1) RETURNING id)"},

		// Empty / whitespace-only.
		{"empty", ""},
		{"whitespace", "   \n\t  "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if isReadOnlyQuery(tc.sql) {
				t.Fatalf("isReadOnlyQuery accepted forbidden input: %q", tc.sql)
			}
		})
	}

	// Internal-object access is enforced by a separate guard in the read-only
	// pipeline (`referencesInternalObject`) — verify that contract here.
	internalCases := []string{
		"SELECT * FROM _meta",
		"SELECT * FROM _fts_users",
		"SELECT _hidden FROM t",
	}
	for _, q := range internalCases {
		t.Run("internal_"+q, func(t *testing.T) {
			if !referencesInternalObject(q) {
				t.Fatalf("referencesInternalObject missed: %q", q)
			}
		})
	}
}

// TestReadOnlyAcceptsValid documents the queries that must continue to work.
func TestReadOnlyAcceptsValid(t *testing.T) {
	cases := []string{
		"SELECT 1",
		"select 1",
		"SELECT id, name FROM users",
		"WITH x AS (SELECT 1) SELECT * FROM x",
		"SELECT COUNT(*) FROM t WHERE v > 10",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			if !isReadOnlyQuery(q) {
				t.Fatalf("isReadOnlyQuery rejected valid input: %q", q)
			}
		})
	}
}
