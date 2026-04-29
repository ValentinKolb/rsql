package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ValentinKolb/rsql/internal/domain"
)

var sourceTableRe = regexp.MustCompile(`(?i)\b(?:from|join)\s+([A-Za-z][A-Za-z0-9_]*)`)
var internalIdentRe = regexp.MustCompile(`^[_A-Za-z][_A-Za-z0-9]*$`)
var formulaAllowedRe = regexp.MustCompile(`^[A-Za-z0-9_+\-*/%(),.<>=!&| \t\r\n]+$`)
var formulaBlockedKeywordRe = regexp.MustCompile(`(?i)\b(SELECT|FROM|WHERE|DROP|ALTER|CREATE|DELETE|UPDATE|INSERT|PRAGMA|ATTACH|DETACH|VACUUM|REINDEX|TRUNCATE|TRIGGER|TABLE|VIEW|INDEX|UNION|WITH)\b`)

// ListTables lists non-internal tables and views.
func ListTables(db *sql.DB) ([]map[string]any, error) {
	rows, err := db.Query(`SELECT name, type, sql FROM sqlite_master WHERE type IN ('table','view') AND name NOT GLOB '_*' AND name != 'sqlite_sequence' ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list sqlite_master: %w", err)
	}
	defer rows.Close()

	var out []map[string]any
	for rows.Next() {
		var name, typ string
		var sqlText sql.NullString
		if err := rows.Scan(&name, &typ, &sqlText); err != nil {
			return nil, fmt.Errorf("scan sqlite_master row: %w", err)
		}
		entry := map[string]any{"name": name, "type": typ}

		countCols, err := columnCount(db, name)
		if err != nil {
			return nil, err
		}
		entry["column_count"] = countCols

		if typ == "table" {
			rowCount, err := countRows(db, name)
			if err != nil {
				return nil, err
			}
			entry["row_count"] = rowCount
		} else {
			if sqlText.Valid {
				entry["sql"] = sqlText.String
				entry["source_tables"] = extractSourceTables(sqlText.String)
			}
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sqlite_master rows: %w", err)
	}

	return out, nil
}

func columnCount(db *sql.DB, table string) (int, error) {
	if err := validateIdentifier(table); err != nil {
		return 0, err
	}
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_xinfo(%s)", quotedIdentifier(table)))
	if err != nil {
		return 0, fmt.Errorf("table_xinfo: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	return count, rows.Err()
}

func countRows(db *sql.DB, table string) (int, error) {
	if err := validateIdentifier(table); err != nil {
		return 0, err
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + quotedIdentifier(table)).Scan(&n); err != nil {
		return 0, fmt.Errorf("count rows: %w", err)
	}
	return n, nil
}

// CreateTableOrView creates a table or view.
func CreateTableOrView(db *sql.DB, req domain.TableCreateRequest) error {
	if err := validateIdentifier(req.Name); err != nil {
		return err
	}
	if strings.HasPrefix(req.Name, "_") {
		return fmt.Errorf("table names with _ prefix are reserved")
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin create table/view: %w", err)
	}
	defer tx.Rollback()

	schema := TableSchema{Name: req.Name, Type: req.Type, Metadata: req.Metadata}

	switch req.Type {
	case "table":
		colsSQL, colsMeta, err := buildCreateColumns(req.Columns)
		if err != nil {
			return err
		}
		schema.Columns = colsMeta
		createSQL := fmt.Sprintf(`CREATE TABLE %s (%s)`, quotedIdentifier(req.Name), strings.Join(colsSQL, ", "))
		if _, err := tx.Exec(createSQL); err != nil {
			return fmt.Errorf("create table: %w", err)
		}
		if _, err := tx.Exec(`CREATE INDEX ` + quotedIdentifier("idx_"+req.Name+"_created_at") + ` ON ` + quotedIdentifier(req.Name) + `(created_at)`); err != nil {
			return fmt.Errorf("create created_at index: %w", err)
		}
		for _, c := range colsMeta {
			if c.Unique {
				idx := "ux_" + req.Name + "_" + c.Name
				if _, err := tx.Exec(`CREATE UNIQUE INDEX ` + quotedIdentifier(idx) + ` ON ` + quotedIdentifier(req.Name) + `(` + quotedIdentifier(c.Name) + `)`); err != nil {
					return fmt.Errorf("create unique index: %w", err)
				}
			}
			if c.Index {
				idx := "idx_" + req.Name + "_" + c.Name
				if _, err := tx.Exec(`CREATE INDEX ` + quotedIdentifier(idx) + ` ON ` + quotedIdentifier(req.Name) + `(` + quotedIdentifier(c.Name) + `)`); err != nil {
					return fmt.Errorf("create index: %w", err)
				}
			}
		}
	case "view":
		if strings.TrimSpace(req.SQL) == "" {
			return fmt.Errorf("view sql must not be empty")
		}
		schema.SQL = req.SQL
		schema.SourceTables = extractSourceTables(req.SQL)
		createSQL := fmt.Sprintf(`CREATE VIEW %s AS %s`, quotedIdentifier(req.Name), req.SQL)
		if _, err := tx.Exec(createSQL); err != nil {
			return fmt.Errorf("create view: %w", err)
		}
	default:
		return fmt.Errorf("type must be table or view")
	}

	if err := putMetaTx(tx, "table_schema", req.Name, schema); err != nil {
		return err
	}
	if len(req.Metadata) > 0 {
		if err := putMetaTx(tx, "table_meta", req.Name, json.RawMessage(req.Metadata)); err != nil {
			return err
		}
	}
	if err := appendSchemaLogTx(tx, "create", req.Name, req, req.Meta); err != nil {
		return err
	}

	return tx.Commit()
}

func buildCreateColumns(cols []domain.ColumnDefinition) ([]string, []domain.ColumnDefinition, error) {
	sqlCols := []string{
		`id INTEGER PRIMARY KEY AUTOINCREMENT`,
		`created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))`,
		`updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))`,
	}
	metaCols := []domain.ColumnDefinition{
		{Name: "id", Type: "integer", PrimaryKey: true, ReadOnly: true},
		{Name: "created_at", Type: "datetime", Auto: true},
		{Name: "updated_at", Type: "datetime", Auto: true},
	}

	for _, c := range cols {
		if err := validateIdentifier(c.Name); err != nil {
			return nil, nil, err
		}
		storage, err := storageType(c.Type)
		if err != nil {
			return nil, nil, err
		}
		colSQL := quotedIdentifier(c.Name) + " " + storage
		if c.Formula != "" {
			if err := validateGeneratedFormula(c.Formula); err != nil {
				return nil, nil, err
			}
			colSQL = quotedIdentifier(c.Name) + " " + storage + " GENERATED ALWAYS AS (" + c.Formula + ") VIRTUAL"
			c.ReadOnly = true
		}
		if c.NotNull && c.Formula == "" {
			colSQL += " NOT NULL"
		}
		if c.Default != nil && c.Formula == "" {
			colSQL += " DEFAULT " + sqlLiteral(c.Default)
		}
		sqlCols = append(sqlCols, colSQL)
		metaCols = append(metaCols, c)
	}

	return sqlCols, metaCols, nil
}

func storageType(logical string) (string, error) {
	switch strings.ToLower(logical) {
	case "text", "date", "datetime", "json", "select":
		return "TEXT", nil
	case "integer", "boolean":
		return "INTEGER", nil
	case "real":
		return "REAL", nil
	case "blob":
		return "BLOB", nil
	default:
		return "", fmt.Errorf("unsupported column type %q", logical)
	}
}

func sqlLiteral(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case bool:
		if t {
			return "1"
		}
		return "0"
	case int, int64, float64, float32:
		return fmt.Sprintf("%v", t)
	case string:
		return "'" + strings.ReplaceAll(t, "'", "''") + "'"
	default:
		b, _ := json.Marshal(t)
		return "'" + strings.ReplaceAll(string(b), "'", "''") + "'"
	}
}

// GetTable returns schema details for a table or view.
func GetTable(db *sql.DB, name string) (map[string]any, error) {
	if err := validateIdentifier(name); err != nil {
		return nil, err
	}

	var typ, sqlText string
	row := db.QueryRow(`SELECT type, COALESCE(sql,'') FROM sqlite_master WHERE name=? AND type IN ('table','view')`, name)
	if err := row.Scan(&typ, &sqlText); err != nil {
		if err == sql.ErrNoRows {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("read object type: %w", err)
	}

	out := map[string]any{"name": name, "type": typ}

	columns, err := tableColumns(db, name)
	if err != nil {
		return nil, err
	}
	out["columns"] = columns

	indexes, err := tableIndexes(db, name)
	if err != nil {
		return nil, err
	}
	out["indexes"] = indexes

	if typ == "view" {
		out["sql"] = sqlText
		out["source_tables"] = extractSourceTables(sqlText)
	}

	var metadata json.RawMessage
	ok, err := GetMeta(db, "table_meta", name, &metadata)
	if err != nil {
		return nil, err
	}
	if ok {
		out["metadata"] = metadata
	}

	return out, nil
}

func tableColumns(db *sql.DB, table string) ([]map[string]any, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_xinfo(%s)", quotedIdentifier(table)))
	if err != nil {
		return nil, fmt.Errorf("table_xinfo: %w", err)
	}
	defer rows.Close()

	var out []map[string]any
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		var hidden int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk, &hidden); err != nil {
			return nil, fmt.Errorf("scan table_xinfo: %w", err)
		}
		entry := map[string]any{
			"name":        name,
			"type":        strings.ToLower(typ),
			"not_null":    notnull == 1,
			"primary_key": pk == 1,
		}
		if dflt.Valid {
			entry["default"] = dflt.String
		}
		if hidden > 0 {
			entry["read_only"] = true
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table_xinfo: %w", err)
	}
	return out, nil
}

func tableIndexes(db *sql.DB, table string) ([]IndexSchema, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA index_list(%s)", quotedIdentifier(table)))
	if err != nil {
		return nil, fmt.Errorf("index_list: %w", err)
	}
	defer rows.Close()

	var out []IndexSchema
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return nil, fmt.Errorf("scan index_list: %w", err)
		}
		cols, err := indexColumns(db, name)
		if err != nil {
			return nil, err
		}
		typ := "index"
		if unique == 1 {
			typ = "unique"
		}
		if strings.HasPrefix(name, "_fts_") {
			typ = "fts"
		}
		_ = origin
		_ = partial
		out = append(out, IndexSchema{Name: name, Type: typ, Columns: cols})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate index_list: %w", err)
	}

	// Include synthetic FTS entries even if not returned by index_list.
	ftsName := "_fts_" + table
	ftsRows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, ftsName)
	if err == nil {
		defer ftsRows.Close()
		for ftsRows.Next() {
			var name string
			if err := ftsRows.Scan(&name); err == nil {
				out = append(out, IndexSchema{Name: name, Type: "fts", Columns: nil})
			}
		}
	}

	return out, nil
}

func indexColumns(db *sql.DB, idx string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA index_info(%s)", quotedIdentifier(idx)))
	if err != nil {
		return nil, fmt.Errorf("index_info: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var seqno, cid int
		var name string
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, fmt.Errorf("scan index_info: %w", err)
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

// UpdateTableOrView applies schema mutations.
func UpdateTableOrView(db *sql.DB, name string, req domain.TableUpdateRequest) error {
	if err := validateIdentifier(name); err != nil {
		return err
	}

	obj, err := GetTable(db, name)
	if err != nil {
		return err
	}
	typ := obj["type"].(string)

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin update table/view: %w", err)
	}
	defer tx.Rollback()

	if typ == "view" {
		if strings.TrimSpace(req.SQL) == "" {
			return fmt.Errorf("view update requires sql")
		}
		if _, err := tx.Exec(`DROP VIEW ` + quotedIdentifier(name)); err != nil {
			return fmt.Errorf("drop old view: %w", err)
		}
		if _, err := tx.Exec(`CREATE VIEW ` + quotedIdentifier(name) + ` AS ` + req.SQL); err != nil {
			return fmt.Errorf("create updated view: %w", err)
		}

		schema := TableSchema{Name: name, Type: "view", SQL: req.SQL, SourceTables: extractSourceTables(req.SQL), Metadata: req.Metadata}
		if err := putMetaTx(tx, "table_schema", name, schema); err != nil {
			return err
		}
		if len(req.Metadata) > 0 {
			if err := putMetaTx(tx, "table_meta", name, json.RawMessage(req.Metadata)); err != nil {
				return err
			}
		}
		if err := appendSchemaLogTx(tx, "update_view", name, req, req.Meta); err != nil {
			return err
		}
		return tx.Commit()
	}

	newName := name
	if req.Rename != "" {
		if err := validateIdentifier(req.Rename); err != nil {
			return err
		}
		if _, err := tx.Exec(`ALTER TABLE ` + quotedIdentifier(name) + ` RENAME TO ` + quotedIdentifier(req.Rename)); err != nil {
			return fmt.Errorf("rename table: %w", err)
		}
		newName = req.Rename
	}

	for oldCol, newCol := range req.RenameColumns {
		if err := validateIdentifier(oldCol); err != nil {
			return err
		}
		if err := validateIdentifier(newCol); err != nil {
			return err
		}
		if _, err := tx.Exec(`ALTER TABLE ` + quotedIdentifier(newName) + ` RENAME COLUMN ` + quotedIdentifier(oldCol) + ` TO ` + quotedIdentifier(newCol)); err != nil {
			return fmt.Errorf("rename column: %w", err)
		}
	}

	for _, col := range req.AddColumns {
		if err := validateIdentifier(col.Name); err != nil {
			return err
		}
		storage, err := storageType(col.Type)
		if err != nil {
			return err
		}
		stmt := `ALTER TABLE ` + quotedIdentifier(newName) + ` ADD COLUMN ` + quotedIdentifier(col.Name) + ` ` + storage
		if col.Formula != "" {
			if err := validateGeneratedFormula(col.Formula); err != nil {
				return err
			}
			stmt = `ALTER TABLE ` + quotedIdentifier(newName) + ` ADD COLUMN ` + quotedIdentifier(col.Name) + ` ` + storage + ` GENERATED ALWAYS AS (` + col.Formula + `) VIRTUAL`
		} else {
			if col.NotNull {
				stmt += ` NOT NULL`
			}
			if col.Default != nil {
				stmt += ` DEFAULT ` + sqlLiteral(col.Default)
			}
		}
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("add column: %w", err)
		}
	}

	for _, col := range req.DropColumns {
		if err := validateIdentifier(col); err != nil {
			return err
		}
		if _, err := tx.Exec(`ALTER TABLE ` + quotedIdentifier(newName) + ` DROP COLUMN ` + quotedIdentifier(col)); err != nil {
			return fmt.Errorf("drop column: %w", err)
		}
	}

	if len(req.Metadata) > 0 {
		if err := putMetaTx(tx, "table_meta", newName, json.RawMessage(req.Metadata)); err != nil {
			return err
		}
	}
	if req.Rename != "" {
		var schema TableSchema
		if ok, _ := getMetaTx(tx, "table_schema", name, &schema); ok {
			schema.Name = req.Rename
			if err := putMetaTx(tx, "table_schema", req.Rename, schema); err != nil {
				return err
			}
			if err := deleteMetaTx(tx, "table_schema", name); err != nil {
				return err
			}
		}
	}

	if err := appendSchemaLogTx(tx, "update_table", newName, req, req.Meta); err != nil {
		return err
	}

	return tx.Commit()
}

// DeleteTableOrView drops an object and its metadata.
func DeleteTableOrView(db *sql.DB, name string, meta json.RawMessage) error {
	if err := validateIdentifier(name); err != nil {
		return err
	}
	obj, err := GetTable(db, name)
	if err != nil {
		return err
	}
	typ := obj["type"].(string)

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin delete table/view: %w", err)
	}
	defer tx.Rollback()

	stmt := "DROP TABLE "
	if typ == "view" {
		stmt = "DROP VIEW "
	}
	if _, err := tx.Exec(stmt + quotedIdentifier(name)); err != nil {
		return fmt.Errorf("drop object: %w", err)
	}
	_ = deleteMetaTx(tx, "table_schema", name)
	_ = deleteMetaTx(tx, "table_meta", name)
	if err := appendSchemaLogTx(tx, "delete", name, map[string]string{"type": typ}, meta); err != nil {
		return err
	}

	return tx.Commit()
}

// CreateIndex creates index, unique index, or FTS index.
func CreateIndex(db *sql.DB, table string, req domain.IndexCreateRequest) error {
	if err := validateIdentifier(table); err != nil {
		return err
	}
	for _, col := range req.Columns {
		if err := validateIdentifier(col); err != nil {
			return err
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin create index: %w", err)
	}
	defer tx.Rollback()

	switch req.Type {
	case "index", "unique":
		name := req.Name
		if name == "" {
			prefix := "idx_"
			if req.Type == "unique" {
				prefix = "ux_"
			}
			name = prefix + table + "_" + strings.Join(req.Columns, "_")
		}
		if err := validateIdentifier(name); err != nil {
			return err
		}
		q := "CREATE "
		if req.Type == "unique" {
			q += "UNIQUE "
		}
		q += `INDEX ` + quotedIdentifier(name) + ` ON ` + quotedIdentifier(table) + `(`
		for i, col := range req.Columns {
			if i > 0 {
				q += ","
			}
			q += quotedIdentifier(col)
		}
		q += `)`
		if _, err := tx.Exec(q); err != nil {
			return fmt.Errorf("create index: %w", err)
		}
	case "fts":
		name := req.Name
		if name == "" {
			name = "_fts_" + table
		}
		if !internalIdentRe.MatchString(name) {
			return fmt.Errorf("invalid identifier %q", name)
		}
		colList := strings.Join(req.Columns, ",")
		if _, err := tx.Exec(`CREATE VIRTUAL TABLE ` + quotedIdentifier(name) + ` USING fts5(` + colList + `, content=` + sqlLiteral(table) + `, content_rowid='id')`); err != nil {
			return fmt.Errorf("create fts table: %w", err)
		}
		if _, err := tx.Exec(`INSERT INTO ` + quotedIdentifier(name) + ` (rowid, ` + colList + `) SELECT id, ` + colList + ` FROM ` + quotedIdentifier(table)); err != nil {
			return fmt.Errorf("seed fts table: %w", err)
		}
		if _, err := tx.Exec(`CREATE TRIGGER ` + quotedIdentifier(name+"_ai") + ` AFTER INSERT ON ` + quotedIdentifier(table) + ` BEGIN INSERT INTO ` + quotedIdentifier(name) + ` (rowid, ` + colList + `) VALUES (new.id, ` + prefixedList("new.", req.Columns) + `); END;`); err != nil {
			return fmt.Errorf("create fts insert trigger: %w", err)
		}
		if _, err := tx.Exec(`CREATE TRIGGER ` + quotedIdentifier(name+"_ad") + ` AFTER DELETE ON ` + quotedIdentifier(table) + ` BEGIN INSERT INTO ` + quotedIdentifier(name) + `(` + quotedIdentifier(name) + `, rowid, ` + colList + `) VALUES('delete', old.id, ` + prefixedList("old.", req.Columns) + `); END;`); err != nil {
			return fmt.Errorf("create fts delete trigger: %w", err)
		}
		if _, err := tx.Exec(`CREATE TRIGGER ` + quotedIdentifier(name+"_au") + ` AFTER UPDATE ON ` + quotedIdentifier(table) + ` BEGIN INSERT INTO ` + quotedIdentifier(name) + `(` + quotedIdentifier(name) + `, rowid, ` + colList + `) VALUES('delete', old.id, ` + prefixedList("old.", req.Columns) + `); INSERT INTO ` + quotedIdentifier(name) + `(rowid, ` + colList + `) VALUES(new.id, ` + prefixedList("new.", req.Columns) + `); END;`); err != nil {
			return fmt.Errorf("create fts update trigger: %w", err)
		}
	default:
		return fmt.Errorf("unsupported index type %q", req.Type)
	}

	if err := appendSchemaLogTx(tx, "create_index", table, req, req.Meta); err != nil {
		return err
	}

	return tx.Commit()
}

func prefixedList(prefix string, cols []string) string {
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		parts = append(parts, prefix+quotedIdentifier(c))
	}
	return strings.Join(parts, ",")
}

func validateGeneratedFormula(formula string) error {
	expr := strings.TrimSpace(formula)
	if expr == "" {
		return fmt.Errorf("formula must not be empty")
	}
	// DDL is assembled as string; reject obvious injection primitives.
	if strings.Contains(expr, ";") || strings.Contains(expr, "--") || strings.Contains(expr, "/*") || strings.Contains(expr, "*/") {
		return fmt.Errorf("formula contains forbidden SQL tokens")
	}
	if strings.ContainsAny(expr, "'\"`") {
		return fmt.Errorf("formula contains unsupported quoting")
	}
	if !formulaAllowedRe.MatchString(expr) {
		return fmt.Errorf("formula contains unsupported characters")
	}
	if formulaBlockedKeywordRe.MatchString(expr) {
		return fmt.Errorf("formula contains forbidden SQL keywords")
	}
	return nil
}

// DeleteIndex deletes index or FTS helper objects.
func DeleteIndex(db *sql.DB, table, idx string, meta json.RawMessage) error {
	if err := validateIdentifier(table); err != nil {
		return err
	}
	if !internalIdentRe.MatchString(idx) {
		return fmt.Errorf("invalid identifier %q", idx)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin delete index: %w", err)
	}
	defer tx.Rollback()

	if strings.HasPrefix(idx, "_fts_") {
		_, _ = tx.Exec(`DROP TRIGGER IF EXISTS ` + quotedIdentifier(idx+"_ai"))
		_, _ = tx.Exec(`DROP TRIGGER IF EXISTS ` + quotedIdentifier(idx+"_ad"))
		_, _ = tx.Exec(`DROP TRIGGER IF EXISTS ` + quotedIdentifier(idx+"_au"))
		if _, err := tx.Exec(`DROP TABLE IF EXISTS ` + quotedIdentifier(idx)); err != nil {
			return fmt.Errorf("drop fts table: %w", err)
		}
	} else {
		if _, err := tx.Exec(`DROP INDEX IF EXISTS ` + quotedIdentifier(idx)); err != nil {
			return fmt.Errorf("drop index: %w", err)
		}
	}

	if err := appendSchemaLogTx(tx, "delete_index", table, map[string]any{"index": idx}, meta); err != nil {
		return err
	}
	return tx.Commit()
}

func extractSourceTables(sqlText string) []string {
	matches := sourceTableRe.FindAllStringSubmatch(sqlText, -1)
	uniq := make(map[string]struct{})
	for _, m := range matches {
		if len(m) > 1 {
			uniq[m[1]] = struct{}{}
		}
	}
	out := make([]string, 0, len(uniq))
	for t := range uniq {
		out = append(out, t)
	}
	return out
}

func putMetaTx(tx *sql.Tx, entityType, entityName string, data any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	_, err = tx.Exec(`
INSERT INTO _meta (entity_type, entity_name, metadata_json)
VALUES (?, ?, ?)
ON CONFLICT(entity_type, entity_name) DO UPDATE SET metadata_json=excluded.metadata_json
`, entityType, entityName, string(b))
	if err != nil {
		return fmt.Errorf("upsert meta: %w", err)
	}
	return nil
}

func getMetaTx(tx *sql.Tx, entityType, entityName string, dst any) (bool, error) {
	row := tx.QueryRow(`SELECT metadata_json FROM _meta WHERE entity_type=? AND entity_name=?`, entityType, entityName)
	var raw string
	if err := row.Scan(&raw); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("read meta: %w", err)
	}
	if err := json.Unmarshal([]byte(raw), dst); err != nil {
		return false, fmt.Errorf("unmarshal meta: %w", err)
	}
	return true, nil
}

func deleteMetaTx(tx *sql.Tx, entityType, entityName string) error {
	_, err := tx.Exec(`DELETE FROM _meta WHERE entity_type=? AND entity_name=?`, entityType, entityName)
	if err != nil {
		return fmt.Errorf("delete meta: %w", err)
	}
	return nil
}

func appendSchemaLogTx(tx *sql.Tx, action, table string, detail, meta any) error {
	detailRaw, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("marshal detail: %w", err)
	}
	var metaRaw []byte
	if meta != nil {
		metaRaw, err = json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshal meta: %w", err)
		}
	}
	_, err = tx.Exec(`INSERT INTO _schema_log (timestamp, action, table_name, detail_json, meta_json) VALUES (?, ?, ?, ?, ?)`,
		nowUTC(), action, table, string(detailRaw), nullableString(metaRaw))
	if err != nil {
		return fmt.Errorf("insert schema log: %w", err)
	}
	return nil
}

func nowUTC() string {
	return time.Now().UTC().Format(time.RFC3339)
}
