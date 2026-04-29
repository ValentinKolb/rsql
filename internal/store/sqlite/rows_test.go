package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ValentinKolb/rsql/internal/domain"
)

func setupRowsTable(t *testing.T) *sql.DB {
	t.Helper()
	db, _ := openTestDB(t)
	if err := CreateTableOrView(db, domain.TableCreateRequest{
		Type: "table",
		Name: "kunden",
		Columns: []domain.ColumnDefinition{
			{Name: "firma", Type: "text", NotNull: true, MaxLength: 100},
			{Name: "email", Type: "text", Unique: true, Pattern: `^[^@]+@[^@]+\.[^@]+$`},
			{Name: "umsatz", Type: "real", Min: fp(0), Max: fp(1000000)},
			{Name: "status", Type: "select", Options: []string{"active", "inactive"}},
			{Name: "bezahlt", Type: "boolean", Default: false},
			{Name: "tags", Type: "json"},
			{Name: "geburtstag", Type: "date"},
			{Name: "updated_custom", Type: "datetime"},
		},
	}); err != nil {
		t.Fatalf("create rows table: %v", err)
	}
	if err := CreateIndex(db, "kunden", domain.IndexCreateRequest{Type: "fts", Columns: []string{"firma", "email"}}); err != nil {
		t.Fatalf("create fts: %v", err)
	}
	return db
}

func TestRowsCRUDAndBulk(t *testing.T) {
	db := setupRowsTable(t)

	if isView, err := IsView(db, "kunden"); err != nil || isView {
		t.Fatalf("expected kunden to be table")
	}
	if _, err := IsView(db, "missing"); err == nil {
		t.Fatal("expected missing object error")
	}

	ins, ids, err := InsertRows(db, "kunden", []map[string]any{{
		"firma":      "Mueller",
		"email":      "a@b.com",
		"umsatz":     100.5,
		"status":     "active",
		"bezahlt":    true,
		"tags":       map[string]any{"k": "v"},
		"geburtstag": "2000-01-01",
	}}, "")
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}
	if len(ins) != 1 || len(ids) != 1 {
		t.Fatalf("unexpected insert output")
	}

	_, _, err = InsertRows(db, "kunden", []map[string]any{{
		"firma":  "Mueller2",
		"email":  "a@b.com",
		"status": "active",
	}}, "resolution=ignore-duplicates")
	if err != nil {
		t.Fatalf("ignore duplicate insert should not fail: %v", err)
	}

	_, _, err = InsertRows(db, "kunden", []map[string]any{{
		"firma":  "Mueller Updated",
		"email":  "a@b.com",
		"status": "inactive",
	}}, "resolution=merge-duplicates")
	if err != nil {
		t.Fatalf("merge duplicate insert: %v", err)
	}

	if _, _, err := InsertRows(db, "kunden", nil, ""); err == nil {
		t.Fatal("expected insert no rows error")
	}

	listRes, err := ListRows(db, "kunden", map[string][]string{
		"status": {"eq.inactive"},
		"select": {"firma,umsatz"},
		"order":  {"umsatz.desc"},
		"limit":  {"10"},
		"offset": {"0"},
	})
	if err != nil {
		t.Fatalf("list rows: %v", err)
	}
	if listRes.(domain.ListResponse[map[string]any]).Meta.TotalCount == 0 {
		t.Fatal("expected rows in list")
	}
	clamped, err := ListRows(db, "kunden", map[string][]string{
		"limit": {"999999999"},
	})
	if err != nil {
		t.Fatalf("list rows with huge limit: %v", err)
	}
	if clamped.(domain.ListResponse[map[string]any]).Meta.Limit != maxListLimit {
		t.Fatalf("expected clamped limit %d, got %d", maxListLimit, clamped.(domain.ListResponse[map[string]any]).Meta.Limit)
	}

	aggRes, err := ListRows(db, "kunden", map[string][]string{
		"select": {"status,count(),umsatz.sum()"},
		"order":  {"umsatz.sum().desc"},
	})
	if err != nil {
		t.Fatalf("list aggregate: %v", err)
	}
	if len(aggRes.(map[string]any)["data"].([]map[string]any)) == 0 {
		t.Fatal("expected aggregate data")
	}

	searchRes, err := ListRows(db, "kunden", map[string][]string{"search": {"Mueller"}})
	if err != nil {
		t.Fatalf("list search: %v", err)
	}
	if searchRes.(domain.ListResponse[map[string]any]).Meta.FilterCount == 0 {
		t.Fatal("expected search match")
	}

	row, err := GetRowByID(db, "kunden", ids[0])
	if err != nil {
		t.Fatalf("get row by id: %v", err)
	}
	if row["email"] != "a@b.com" {
		t.Fatalf("unexpected row: %#v", row)
	}
	if _, err := GetRowByID(db, "kunden", int64(99999)); err == nil {
		t.Fatal("expected no row")
	}

	updated, err := UpdateRowByID(db, "kunden", ids[0], map[string]any{
		"umsatz":  map[string]any{"$increment": 50.0},
		"bezahlt": map[string]any{"$toggle": true},
		"tags":    map[string]any{"$append": "new"},
	}, "return=representation")
	if err != nil {
		t.Fatalf("update row by id: %v", err)
	}
	if len(updated) != 1 {
		t.Fatalf("expected updated row representation")
	}

	// insert extra for bulk operations
	if _, _, err := InsertRows(db, "kunden", []map[string]any{{
		"firma":  "Bulk1",
		"email":  "bulk1@test.com",
		"status": "active",
	}, {
		"firma":  "Bulk2",
		"email":  "bulk2@test.com",
		"status": "active",
	}}, ""); err != nil {
		t.Fatalf("insert for bulk: %v", err)
	}

	where, whereArgs, err := BuildWhereForBulk(map[string][]string{"status": {"eq.active"}})
	if err != nil {
		t.Fatalf("build where for bulk: %v", err)
	}
	bulkIDs, bulkRows, err := BulkUpdateRows(db, "kunden", map[string]any{"status": "inactive"}, where, whereArgs, "return=representation")
	if err != nil {
		t.Fatalf("bulk update rows: %v", err)
	}
	if len(bulkIDs) == 0 || len(bulkRows) == 0 {
		t.Fatalf("expected bulk update effect")
	}

	delWhere, delArgs, err := BuildWhereForBulk(map[string][]string{"status": {"eq.inactive"}})
	if err != nil {
		t.Fatalf("build where for delete: %v", err)
	}
	bulkDelIDs, bulkDeletedRows, err := BulkDeleteRows(db, "kunden", delWhere, delArgs, "return=representation")
	if err != nil {
		t.Fatalf("bulk delete rows: %v", err)
	}
	_ = bulkDeletedRows
	if len(bulkDelIDs) == 0 {
		t.Fatalf("expected bulk delete effect")
	}

	newRows, newIDs, err := InsertRows(db, "kunden", []map[string]any{{"firma": "DeleteMe", "email": "delete@test.com", "status": "active"}}, "")
	if err != nil || len(newRows) != 1 || len(newIDs) != 1 {
		t.Fatalf("insert row for single delete: rows=%v ids=%v err=%v", newRows, newIDs, err)
	}
	if _, err := DeleteRowByID(db, "kunden", newIDs[0], "return=representation"); err != nil {
		t.Fatalf("delete row by id: %v", err)
	}
	if _, err := DeleteRowByID(db, "kunden", int64(999), ""); err == nil {
		t.Fatal("expected delete missing row error")
	}
}

func TestInsertRowsConflictAfterDeleteDoesNotReturnNotFound(t *testing.T) {
	db := setupRowsTable(t)

	if _, _, err := InsertRows(db, "kunden", []map[string]any{{
		"firma":  "Open",
		"email":  "open@test.com",
		"status": "active",
	}}, ""); err != nil {
		t.Fatalf("insert open row: %v", err)
	}

	if _, _, err := InsertRows(db, "kunden", []map[string]any{{
		"firma":  "Done",
		"email":  "done@test.com",
		"status": "inactive",
	}}, ""); err != nil {
		t.Fatalf("insert done row: %v", err)
	}

	where, args, err := BuildWhereForBulk(map[string][]string{
		"status": {"eq.inactive"},
	})
	if err != nil {
		t.Fatalf("build bulk delete where: %v", err)
	}
	if _, _, err := BulkDeleteRows(db, "kunden", where, args, ""); err != nil {
		t.Fatalf("bulk delete done row: %v", err)
	}

	if _, _, err := InsertRows(db, "kunden", []map[string]any{
		{
			"firma":  "Open Duplicate",
			"email":  "open@test.com",
			"status": "active",
		},
		{
			"firma":  "New Ignore",
			"email":  "new-ignore@test.com",
			"status": "active",
		},
	}, "resolution=ignore-duplicates"); err != nil {
		t.Fatalf("ignore duplicates after delete should not fail: %v", err)
	}

	if _, _, err := InsertRows(db, "kunden", []map[string]any{
		{
			"firma":  "Open Merge",
			"email":  "open@test.com",
			"status": "inactive",
		},
		{
			"firma":  "New Merge",
			"email":  "new-merge@test.com",
			"status": "active",
		},
	}, "resolution=merge-duplicates"); err != nil {
		t.Fatalf("merge duplicates after delete should not fail: %v", err)
	}
}

func TestRowsFilterAndHelpers(t *testing.T) {
	db := setupRowsTable(t)
	_, _, _ = InsertRows(db, "kunden", []map[string]any{{"firma": "A", "email": "a@test.com", "status": "active"}}, "")

	if _, _, err := parseFilterToken(`"x"`, "bad"); err == nil {
		t.Fatal("expected invalid filter token")
	}
	if _, _, err := parseFilterToken(`"x"`, "in.bad"); err == nil {
		t.Fatal("expected invalid in syntax")
	}
	if _, _, err := parseFilterToken(`"x"`, "is.bad"); err == nil {
		t.Fatal("expected invalid is syntax")
	}
	if _, _, err := parseFilterToken(`"x"`, "unknown.1"); err == nil {
		t.Fatal("expected unknown operator")
	}
	if expr, args, err := parseFilterToken(`"x"`, "not.eq.1"); err != nil || expr == "" || len(args) != 1 {
		t.Fatalf("unexpected not.eq parse: %s %#v %v", expr, args, err)
	}

	if _, _, err := parseLogicalExpr("or=(a.eq.1,b.eq.2)"); err != nil {
		t.Fatalf("parse or expr: %v", err)
	}
	if _, _, err := parseLogicalExpr("and=(a.eq.1,b.eq.2)"); err != nil {
		t.Fatalf("parse and expr: %v", err)
	}
	if _, _, err := parseLogicalExpr("not.a.eq.1"); err != nil {
		t.Fatalf("parse not expr: %v", err)
	}
	if _, _, err := parseLogicalExpr("a.eq.1"); err != nil {
		t.Fatalf("parse condition expr: %v", err)
	}
	if _, _, err := parseLogicalExpr("bad"); err == nil {
		t.Fatal("expected parse logical error")
	}

	if _, _, err := parseConditionExpr("bad"); err == nil {
		t.Fatal("expected parse condition error")
	}

	if val := parseFilterValue("true"); val != 1 {
		t.Fatalf("unexpected parse bool true")
	}
	if val := parseFilterValue("false"); val != 0 {
		t.Fatalf("unexpected parse bool false")
	}
	if val := parseFilterValue("12"); val != int64(12) {
		t.Fatalf("unexpected parse int: %#v", val)
	}
	if val := parseFilterValue("12.5"); val != 12.5 {
		t.Fatalf("unexpected parse float: %#v", val)
	}
	if val := parseFilterValue("text"); val != "text" {
		t.Fatalf("unexpected parse string")
	}

	if v := parseIntOrDefault("x", 7); v != 7 {
		t.Fatalf("unexpected parseIntOrDefault fallback")
	}
	if v := parseIntOrDefault("5", 7); v != 5 {
		t.Fatalf("unexpected parseIntOrDefault value")
	}
	if got := splitCSV("a,(b,c),d"); len(got) != 3 {
		t.Fatalf("unexpected splitCSV: %#v", got)
	}

	if _, _, err := buildSearchClause(db, "kunden", "A"); err != nil {
		t.Fatalf("build search fts: %v", err)
	}
	if _, _, err := buildSearchClause(db, "missing", "x"); err != nil {
		t.Fatalf("buildSearchClause missing table should gracefully fallback: %v", err)
	}

	if _, _, _, err := buildSelectClause("invalid("); err == nil {
		t.Fatal("expected invalid select clause")
	}
	if _, err := buildOrderClause("invalid("); err == nil {
		t.Fatal("expected invalid order clause")
	}

	if _, _, err := BuildWhereForBulk(map[string][]string{"bad": {"bad"}}); err == nil {
		t.Fatal("expected invalid bulk where")
	}
	if _, _, err := buildWhereClause(map[string][]string{"x": {"eq.1"}, "or": {"or=(x.eq.1,y.eq.2)"}}); err != nil {
		t.Fatalf("build where clause: %v", err)
	}
}

func TestRowsValidationAndAtomicBranches(t *testing.T) {
	db := setupRowsTable(t)
	schema, err := TableSchemaForValidation(db, "kunden")
	if err != nil {
		t.Fatalf("table schema for validation: %v", err)
	}
	if schema.Name != "kunden" {
		t.Fatalf("unexpected schema name")
	}

	schemaMissing, err := TableSchemaForValidation(db, "missing")
	if err != nil {
		t.Fatalf("missing table schema should not error: %v", err)
	}
	if len(schemaMissing.Columns) != 0 {
		t.Fatalf("expected missing table to have 0 columns, got %#v", schemaMissing.Columns)
	}
	if mapStorageToLogical("INTEGER") != "integer" || mapStorageToLogical("REAL") != "real" || mapStorageToLogical("BLOB") != "blob" || mapStorageToLogical("X") != "text" {
		t.Fatalf("unexpected mapStorageToLogical")
	}

	columns := map[string]domain.ColumnDefinition{
		"i":  {Name: "i", Type: "integer", NotNull: true, Min: fp(0), Max: fp(10)},
		"r":  {Name: "r", Type: "real", Min: fp(0), Max: fp(10)},
		"b":  {Name: "b", Type: "boolean"},
		"d":  {Name: "d", Type: "date"},
		"dt": {Name: "dt", Type: "datetime"},
		"j":  {Name: "j", Type: "json"},
		"s":  {Name: "s", Type: "select", Options: []string{"a", "b"}},
		"t":  {Name: "t", Type: "text", MaxLength: 3, Pattern: `^[a-z]+$`},
		"f":  {Name: "f", Type: "text", Formula: "1+1"},
	}

	if _, err := sanitizeAndValidateRow(map[string]any{"f": "x"}, columns, true); err == nil {
		t.Fatal("expected formula read-only error")
	}
	if _, err := sanitizeAndValidateRow(map[string]any{}, columns, true); err == nil {
		t.Fatal("expected missing required fields")
	}

	validRow := map[string]any{"i": 1.0, "r": 1.5, "b": true, "d": "2020-01-01", "dt": "2020-01-01T00:00:00Z", "j": map[string]any{"x": 1}, "s": "a", "t": "abc"}
	if _, err := sanitizeAndValidateRow(validRow, columns, false); err != nil {
		t.Fatalf("sanitize valid row: %v", err)
	}

	invalids := []map[string]any{
		{"i": 1.2},
		{"i": -1.0},
		{"r": -1.0},
		{"b": 3.0},
		{"d": "01-01-2020"},
		{"dt": "bad"},
		{"s": "x"},
		{"t": "ABCD"},
		{"t": "ABC"},
	}
	for _, row := range invalids {
		_, err := sanitizeAndValidateRow(row, columns, false)
		if err == nil {
			t.Fatalf("expected validation error for %#v", row)
		}
	}

	if _, err := toFloat("x"); err == nil {
		t.Fatal("expected toFloat error")
	}
	if _, err := toFloat(json.Number("1.2")); err != nil {
		t.Fatalf("toFloat json number: %v", err)
	}

	if _, _, err := buildAtomicOp("c", domain.ColumnDefinition{Name: "c", Type: "text"}, "$increment", 1); err == nil {
		t.Fatal("expected invalid increment")
	}
	if _, _, err := buildAtomicOp("c", domain.ColumnDefinition{Name: "c", Type: "real"}, "$increment", 1); err != nil {
		t.Fatalf("valid increment: %v", err)
	}
	if _, _, err := buildAtomicOp("c", domain.ColumnDefinition{Name: "c", Type: "text"}, "$multiply", 2); err == nil {
		t.Fatal("expected invalid multiply")
	}
	if _, _, err := buildAtomicOp("c", domain.ColumnDefinition{Name: "c", Type: "real"}, "$multiply", 2); err != nil {
		t.Fatalf("valid multiply: %v", err)
	}
	if _, _, err := buildAtomicOp("c", domain.ColumnDefinition{Name: "c", Type: "text"}, "$append", "x"); err == nil {
		t.Fatal("expected invalid append")
	}
	if _, _, err := buildAtomicOp("c", domain.ColumnDefinition{Name: "c", Type: "json"}, "$append", "x"); err != nil {
		t.Fatalf("valid append: %v", err)
	}
	if _, _, err := buildAtomicOp("c", domain.ColumnDefinition{Name: "c", Type: "json"}, "$remove", 1); err == nil {
		t.Fatal("expected remove path error")
	}
	if _, _, err := buildAtomicOp("c", domain.ColumnDefinition{Name: "c", Type: "json"}, "$remove", "$.x"); err != nil {
		t.Fatalf("valid remove: %v", err)
	}
	if _, _, err := buildAtomicOp("c", domain.ColumnDefinition{Name: "c", Type: "text"}, "$toggle", true); err == nil {
		t.Fatal("expected invalid toggle")
	}
	if _, _, err := buildAtomicOp("c", domain.ColumnDefinition{Name: "c", Type: "boolean"}, "$toggle", true); err != nil {
		t.Fatalf("valid toggle: %v", err)
	}
	if _, _, err := buildAtomicOp("c", domain.ColumnDefinition{Name: "c", Type: "boolean"}, "$x", true); err == nil {
		t.Fatal("expected invalid atomic op")
	}

	if normalizeDBValue([]byte("x")).(string) != "x" {
		t.Fatalf("unexpected normalize db value")
	}

	_, _, err = buildInsertStatement("kunden", map[string]any{}, "", nil)
	if err == nil {
		t.Fatal("expected no writable fields error")
	}

	insertedRows, insertedIDs, err := InsertRows(db, "kunden", []map[string]any{{"firma": "Upd", "email": "upd@test.com", "status": "active"}}, "")
	if err != nil || len(insertedRows) != 1 || len(insertedIDs) != 1 {
		t.Fatalf("insert for updateRows branch: rows=%v ids=%v err=%v", insertedRows, insertedIDs, err)
	}
	if _, _, _, err := updateRows(db, "kunden", map[string]any{"x": 1}, "id = ?", []any{insertedIDs[0]}); err == nil {
		t.Fatal("expected updateRows error for no update set")
	}

	if _, _, err := deleteRows(db, "kunden", "id = ?", []any{999}); err != nil {
		t.Fatalf("deleteRows with no matches should not fail: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if _, err := selectIDsTx(tx, "kunden", "", nil); err != nil {
		t.Fatalf("selectIDsTx: %v", err)
	}
	if rows, err := selectRowsByIDsTx(tx, "kunden", nil); err != nil || rows != nil {
		t.Fatalf("selectRowsByIDsTx nil ids: rows=%#v err=%v", rows, err)
	}

	if _, _, _, err := buildSelectClause(""); err != nil {
		t.Fatalf("empty select clause should be ok: %v", err)
	}
	if _, err := buildOrderClause(""); err != nil {
		t.Fatalf("empty order clause should be ok: %v", err)
	}
}

func TestConflictTargetNoUnique(t *testing.T) {
	db, _ := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if _, err := conflictTarget(tx, "t"); err == nil {
		t.Fatal("expected no unique index error")
	}
}

func fp(v float64) *float64 { return &v }

func TestBuildInsertStatementUnknownPrefer(t *testing.T) {
	db := setupRowsTable(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	stmt, args, err := buildInsertStatement("kunden", map[string]any{"firma": "x", "email": "x@test.com", "status": "active"}, "unknown", tx)
	if err != nil {
		t.Fatalf("build insert statement: %v", err)
	}
	if stmt == "" || len(args) == 0 {
		t.Fatalf("unexpected statement/args")
	}
}

func TestParseLogicalListErrorBranch(t *testing.T) {
	if _, _, err := parseLogicalList("bad", "OR"); err == nil {
		t.Fatal("expected parse logical list error")
	}
}

func TestBulkReturnRepresentationLargeSet(t *testing.T) {
	db := setupRowsTable(t)

	const total = 1200
	rows := make([]map[string]any, 0, total)
	for i := 0; i < total; i++ {
		rows = append(rows, map[string]any{
			"firma":  fmt.Sprintf("Bulk %d", i),
			"email":  fmt.Sprintf("bulk-%d@test.com", i),
			"status": "active",
		})
	}

	inserted, ids, err := InsertRows(db, "kunden", rows, "")
	if err != nil {
		t.Fatalf("insert large bulk set: %v", err)
	}
	if len(inserted) != total || len(ids) != total {
		t.Fatalf("unexpected inserted count: rows=%d ids=%d", len(inserted), len(ids))
	}

	where, args, err := BuildWhereForBulk(map[string][]string{
		"status": {"eq.active"},
	})
	if err != nil {
		t.Fatalf("build where for bulk update: %v", err)
	}

	updatedIDs, updatedRows, err := BulkUpdateRows(db, "kunden", map[string]any{"status": "inactive"}, where, args, "return=representation")
	if err != nil {
		t.Fatalf("bulk update large set: %v", err)
	}
	if len(updatedIDs) != total || len(updatedRows) != total {
		t.Fatalf("unexpected bulk update representation size: ids=%d rows=%d", len(updatedIDs), len(updatedRows))
	}

	delWhere, delArgs, err := BuildWhereForBulk(map[string][]string{
		"status": {"eq.inactive"},
	})
	if err != nil {
		t.Fatalf("build where for bulk delete: %v", err)
	}
	deletedIDs, deletedRows, err := BulkDeleteRows(db, "kunden", delWhere, delArgs, "return=representation")
	if err != nil {
		t.Fatalf("bulk delete large set: %v", err)
	}
	if len(deletedIDs) != total || len(deletedRows) != total {
		t.Fatalf("unexpected bulk delete representation size: ids=%d rows=%d", len(deletedIDs), len(deletedRows))
	}
}
