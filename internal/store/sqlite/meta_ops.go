package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
)

// ChangelogEntry is one schema changelog item.
type ChangelogEntry struct {
	ID        int             `json:"id"`
	Timestamp string          `json:"timestamp"`
	Action    string          `json:"action"`
	Table     string          `json:"table"`
	Detail    json.RawMessage `json:"detail"`
	Meta      json.RawMessage `json:"meta,omitempty"`
}

// ListChangelog lists schema changelog entries.
func ListChangelog(db *sql.DB, tableFilter string, limit, offset int) ([]ChangelogEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	query := `SELECT id, timestamp, action, table_name, detail_json, COALESCE(meta_json,'null') FROM _schema_log`
	args := make([]any, 0, 3)
	if tableFilter != "" {
		query += ` WHERE table_name = ?`
		args = append(args, tableFilter)
	}
	query += ` ORDER BY id DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query changelog: %w", err)
	}
	defer rows.Close()

	var out []ChangelogEntry
	for rows.Next() {
		var e ChangelogEntry
		var detailRaw string
		var metaRaw string
		if err := rows.Scan(&e.ID, &e.Timestamp, &e.Action, &e.Table, &detailRaw, &metaRaw); err != nil {
			return nil, fmt.Errorf("scan changelog entry: %w", err)
		}
		e.Detail = json.RawMessage(detailRaw)
		e.Meta = json.RawMessage(metaRaw)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate changelog: %w", err)
	}
	return out, nil
}

// Stats builds namespace statistics.
func Stats(db *sql.DB, dbPath string, cfg map[string]any) (map[string]any, error) {
	fileInfo, err := os.Stat(dbPath)
	if err != nil {
		return nil, fmt.Errorf("stat db file: %w", err)
	}

	rows, err := db.Query(`SELECT name, type, sql FROM sqlite_master WHERE type IN ('table','view') AND name NOT GLOB '_*' AND name != 'sqlite_sequence'`)
	if err != nil {
		return nil, fmt.Errorf("list objects for stats: %w", err)
	}
	defer rows.Close()

	tables := map[string]any{}
	tableCount := 0
	viewCount := 0
	totalRows := 0

	for rows.Next() {
		var name, typ, sqlText string
		if err := rows.Scan(&name, &typ, &sqlText); err != nil {
			return nil, fmt.Errorf("scan object for stats: %w", err)
		}

		obj := map[string]any{"type": typ}
		colCount, err := columnCount(db, name)
		if err != nil {
			return nil, err
		}
		obj["column_count"] = colCount

		if typ == "table" {
			tableCount++
			count, err := countRows(db, name)
			if err != nil {
				return nil, err
			}
			obj["row_count"] = count
			totalRows += count
		} else {
			viewCount++
			obj["source_tables"] = extractSourceTables(sqlText)
		}
		tables[name] = obj
	}

	return map[string]any{
		"file_size":   fileInfo.Size(),
		"table_count": tableCount,
		"view_count":  viewCount,
		"total_rows":  totalRows,
		"config":      cfg,
		"tables":      tables,
	}, nil
}
