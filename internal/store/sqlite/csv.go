package sqlite

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// StreamCSV streams a table or view's rows as CSV-encoded bytes to w.
//
// The endpoint mirrors ListRows: same filter grammar (eq/gte/like/is.null/...),
// same logical operators (or=, and=), same select/order/search semantics, even
// aggregate selects (count(), sum(), …) pass through unchanged because the
// header row is taken from the actual SELECT result column names.
//
// The only intentional difference: when the caller does not provide `limit`
// nor `offset`, no LIMIT/OFFSET clause is emitted at all and the export is
// unbounded. When `limit` is set it is clamped at maxListLimit just like
// ListRows so a single client cannot pin a runner with `limit=10^9`.
//
// Type encoding:
//   - boolean (stored as INTEGER 0/1) → "true" / "false"
//   - json (stored as TEXT)           → JSON-marshalled inline
//   - NULL                            → empty cell
//   - everything else                 → fmt.Sprint of the raw scan value
//
// The function flushes the underlying csv.Writer every csvFlushChunk rows so
// memory stays bounded for multi-million-row exports.
func StreamCSV(db *sql.DB, table string, query map[string][]string, w io.Writer) error {
	if err := validateIdentifier(table); err != nil {
		return err
	}
	if !tableOrViewExists(db, table) {
		return sql.ErrNoRows
	}

	sel := firstOrDefault(query, "select", "*")
	order := firstOrDefault(query, "order", "id.asc")
	search := firstOrDefault(query, "search", "")

	whereSQL, args, err := buildWhereClause(query)
	if err != nil {
		return err
	}
	if search != "" {
		searchSQL, searchArgs, err := buildSearchClause(db, table, search)
		if err != nil {
			return err
		}
		if whereSQL == "" {
			whereSQL = searchSQL
		} else {
			whereSQL = "(" + whereSQL + ") AND (" + searchSQL + ")"
		}
		args = append(args, searchArgs...)
	}

	selectSQL, aggMode, groupBy, err := buildSelectClause(sel)
	if err != nil {
		return err
	}
	orderSQL, err := buildOrderClause(order)
	if err != nil {
		return err
	}

	stmt := `SELECT ` + selectSQL + ` FROM ` + quotedIdentifier(table)
	if whereSQL != "" {
		stmt += ` WHERE ` + whereSQL
	}
	if aggMode && groupBy != "" {
		stmt += ` GROUP BY ` + groupBy
	}
	if orderSQL != "" {
		stmt += ` ORDER BY ` + orderSQL
	}

	// limit/offset are honoured if provided; absent → unbounded export.
	limitSet, limitVal := lookupQueryInt(query, "limit")
	offsetSet, offsetVal := lookupQueryInt(query, "offset")
	if limitSet {
		if limitVal <= 0 {
			limitVal = defaultListLimit
		}
		if limitVal > maxListLimit {
			limitVal = maxListLimit
		}
		stmt += ` LIMIT ?`
		args = append(args, limitVal)
		if offsetSet {
			if offsetVal < 0 {
				offsetVal = 0
			}
			stmt += ` OFFSET ?`
			args = append(args, offsetVal)
		}
	} else if offsetSet {
		// Offset without limit makes no sense in standard SQL; pair it with
		// the maximum cap so the user gets predictable behaviour instead of
		// a SQL error.
		if offsetVal < 0 {
			offsetVal = 0
		}
		stmt += ` LIMIT ? OFFSET ?`
		args = append(args, maxListLimit, offsetVal)
	}

	rows, err := db.Query(stmt, args...)
	if err != nil {
		return fmt.Errorf("stream csv query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("rows columns: %w", err)
	}

	// Look up the column→type map once. Aggregate output, view columns and
	// expression aliases will not be present; that's fine — the encoder
	// falls back to the raw value for unknown columns.
	colTypes := columnTypeMap(db, table)

	writer := csv.NewWriter(w)
	if err := writer.Write(cols); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	record := make([]string, len(cols))
	scanDest := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		for i, col := range cols {
			record[i] = encodeCSVCell(scanDest[i], colTypes[col])
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
		rowCount++
		if rowCount%csvFlushChunk == 0 {
			writer.Flush()
			if err := writer.Error(); err != nil {
				return fmt.Errorf("flush csv: %w", err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}
	writer.Flush()
	return writer.Error()
}

const csvFlushChunk = 1000

// columnTypeMap returns the logical type per column name for the given table.
// Returns an empty map (not nil) when the table has no stored schema (views,
// internal/diagnostic tables, …); the encoder will then take the default
// path for every cell.
func columnTypeMap(db *sql.DB, table string) map[string]string {
	out := make(map[string]string)
	schema, err := TableSchemaForValidation(db, table)
	if err != nil {
		return out
	}
	for _, c := range schema.Columns {
		out[c.Name] = c.Type
	}
	return out
}

// encodeCSVCell renders one DB value as a CSV cell string, applying the same
// type-aware decoding decisions as decodeRowsWithSchema for read paths.
func encodeCSVCell(v any, colType string) string {
	if v == nil {
		return ""
	}
	switch colType {
	case "boolean":
		switch n := v.(type) {
		case int64:
			return strconvBool(n != 0)
		case int:
			return strconvBool(n != 0)
		case float64:
			return strconvBool(n != 0)
		case bool:
			return strconvBool(n)
		}
	case "json":
		// Stored as TEXT; emit as JSON if it parses, otherwise raw.
		if s, ok := v.(string); ok {
			var parsed any
			if err := json.Unmarshal([]byte(s), &parsed); err == nil {
				if b, err := json.Marshal(parsed); err == nil {
					return string(b)
				}
			}
			return s
		}
	}
	switch t := v.(type) {
	case []byte:
		return string(t)
	case string:
		return t
	default:
		// Numbers and the like.
		s := fmt.Sprint(t)
		// Trim Go's "+Inf"/"NaN" surprises by leaving them as-is — RFC 4180
		// has no opinion. The csv.Writer handles quoting.
		return s
	}
}

func strconvBool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// lookupQueryInt returns (true, value) when the key is present and parsable
// as an int, (false, 0) otherwise. Used so the export can tell "no limit
// given" from "limit=0".
func lookupQueryInt(query map[string][]string, key string) (bool, int) {
	values, ok := query[key]
	if !ok || len(values) == 0 || strings.TrimSpace(values[0]) == "" {
		return false, 0
	}
	n := parseIntOrDefault(values[0], -1)
	if n < 0 {
		return false, 0
	}
	return true, n
}
