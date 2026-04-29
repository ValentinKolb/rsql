package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ValentinKolb/rsql/internal/domain"
)

var emailLikeWildcard = strings.NewReplacer("*", "%")

const (
	defaultListLimit = 100
	maxListLimit     = 10000
	maxSQLVariables  = 900
)

// IsView reports whether object is a view.
func IsView(db *sql.DB, name string) (bool, error) {
	var typ string
	if err := db.QueryRow(`SELECT type FROM sqlite_master WHERE name=? AND type IN ('table','view')`, name).Scan(&typ); err != nil {
		if err == sql.ErrNoRows {
			return false, sql.ErrNoRows
		}
		return false, fmt.Errorf("lookup object type: %w", err)
	}
	return typ == "view", nil
}

// TableSchemaForValidation returns schema metadata for a table.
func TableSchemaForValidation(db *sql.DB, table string) (TableSchema, error) {
	var schema TableSchema
	ok, err := GetMeta(db, "table_schema", table, &schema)
	if err != nil {
		return TableSchema{}, err
	}
	if ok {
		return schema, nil
	}

	cols, err := tableColumns(db, table)
	if err != nil {
		return TableSchema{}, err
	}
	schema = TableSchema{Name: table, Type: "table"}
	for _, c := range cols {
		typ, _ := c["type"].(string)
		schema.Columns = append(schema.Columns, domain.ColumnDefinition{
			Name:    c["name"].(string),
			Type:    mapStorageToLogical(typ),
			NotNull: c["not_null"].(bool),
		})
	}
	return schema, nil
}

func mapStorageToLogical(typ string) string {
	t := strings.ToUpper(typ)
	switch t {
	case "INTEGER":
		return "integer"
	case "REAL":
		return "real"
	case "BLOB":
		return "blob"
	default:
		return "text"
	}
}

// ListRows lists rows with filters and pagination metadata.
func ListRows(db *sql.DB, table string, query map[string][]string) (any, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, err
	}

	sel := firstOrDefault(query, "select", "*")
	order := firstOrDefault(query, "order", "id.asc")
	limit := parseIntOrDefault(firstOrDefault(query, "limit", "100"), defaultListLimit)
	offset := parseIntOrDefault(firstOrDefault(query, "offset", "0"), 0)
	search := firstOrDefault(query, "search", "")
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	if offset < 0 {
		offset = 0
	}

	whereSQL, args, err := buildWhereClause(query)
	if err != nil {
		return nil, err
	}
	if search != "" {
		searchSQL, searchArgs, err := buildSearchClause(db, table, search)
		if err != nil {
			return nil, err
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
		return nil, err
	}

	orderSQL, err := buildOrderClause(order)
	if err != nil {
		return nil, err
	}

	baseSQL := `SELECT ` + selectSQL + ` FROM ` + quotedIdentifier(table)
	if whereSQL != "" {
		baseSQL += ` WHERE ` + whereSQL
	}
	if aggMode && groupBy != "" {
		baseSQL += ` GROUP BY ` + groupBy
	}
	if orderSQL != "" {
		baseSQL += ` ORDER BY ` + orderSQL
	}
	if !aggMode {
		baseSQL += ` LIMIT ? OFFSET ?`
		args = append(args, limit, offset)
	}

	rows, err := db.Query(baseSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("list rows query: %w", err)
	}
	defer rows.Close()

	data, err := scanRows(rows)
	if err != nil {
		return nil, err
	}

	if aggMode {
		return map[string]any{"data": data}, nil
	}

	var total int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + quotedIdentifier(table)).Scan(&total); err != nil {
		return nil, fmt.Errorf("total count: %w", err)
	}

	countSQL := `SELECT COUNT(*) FROM ` + quotedIdentifier(table)
	if whereSQL != "" {
		countSQL += ` WHERE ` + whereSQL
	}
	var filtered int
	if err := db.QueryRow(countSQL, args[:len(args)-2]...).Scan(&filtered); err != nil {
		return nil, fmt.Errorf("filter count: %w", err)
	}

	return domain.ListResponse[map[string]any]{
		Data: data,
		Meta: domain.ListMeta{
			TotalCount:  total,
			FilterCount: filtered,
			Limit:       limit,
			Offset:      offset,
		},
	}, nil
}

func buildSearchClause(db *sql.DB, table, search string) (string, []any, error) {
	ftsName := "_fts_" + table
	var exists int
	_ = db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`, ftsName).Scan(&exists)
	if exists == 1 {
		return `id IN (SELECT rowid FROM ` + quotedIdentifier(ftsName) + ` WHERE ` + quotedIdentifier(ftsName) + ` MATCH ?)`, []any{search}, nil
	}

	schema, err := TableSchemaForValidation(db, table)
	if err != nil {
		return "", nil, err
	}
	var textCols []string
	for _, col := range schema.Columns {
		if col.Type == "text" || col.Type == "select" || col.Type == "date" || col.Type == "datetime" {
			textCols = append(textCols, col.Name)
		}
	}
	if len(textCols) == 0 {
		return "1=0", nil, nil
	}

	parts := make([]string, 0, len(textCols))
	args := make([]any, 0, len(textCols))
	like := "%" + search + "%"
	for _, c := range textCols {
		parts = append(parts, quotedIdentifier(c)+` LIKE ?`)
		args = append(args, like)
	}
	return strings.Join(parts, " OR "), args, nil
}

func buildSelectClause(sel string) (sqlExpr string, aggMode bool, groupBy string, err error) {
	if sel == "*" || strings.TrimSpace(sel) == "" {
		return "*", false, "", nil
	}

	items := splitCSV(sel)
	if len(items) == 0 {
		return "*", false, "", nil
	}

	var exprs []string
	var groupCols []string
	for _, raw := range items {
		item := strings.TrimSpace(raw)
		if item == "count()" {
			aggMode = true
			exprs = append(exprs, `COUNT(*) AS "count"`)
			continue
		}
		if m := regexp.MustCompile(`^([A-Za-z][A-Za-z0-9_]*)\.(count|sum|avg|min|max)\(\)$`).FindStringSubmatch(item); len(m) == 3 {
			aggMode = true
			col := m[1]
			if err := validateIdentifier(col); err != nil {
				return "", false, "", err
			}
			fn := strings.ToUpper(m[2])
			exprs = append(exprs, fn+`(`+quotedIdentifier(col)+`) AS "`+m[2]+`"`)
			continue
		}
		if err := validateIdentifier(item); err != nil {
			return "", false, "", err
		}
		exprs = append(exprs, quotedIdentifier(item))
		groupCols = append(groupCols, quotedIdentifier(item))
	}

	if aggMode && len(groupCols) > 0 {
		groupBy = strings.Join(groupCols, ",")
	}
	return strings.Join(exprs, ","), aggMode, groupBy, nil
}

func buildOrderClause(order string) (string, error) {
	if strings.TrimSpace(order) == "" {
		return "", nil
	}
	parts := splitCSV(order)
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		dir := "ASC"
		if strings.HasSuffix(p, ".desc") {
			dir = "DESC"
			p = strings.TrimSuffix(p, ".desc")
		} else if strings.HasSuffix(p, ".asc") {
			p = strings.TrimSuffix(p, ".asc")
		}

		if m := regexp.MustCompile(`^([A-Za-z][A-Za-z0-9_]*)\.(count|sum|avg|min|max)\(\)$`).FindStringSubmatch(p); len(m) == 3 {
			out = append(out, strings.ToUpper(m[2])+`(`+quotedIdentifier(m[1])+`) `+dir)
			continue
		}

		if err := validateIdentifier(p); err != nil {
			return "", err
		}
		out = append(out, quotedIdentifier(p)+` `+dir)
	}
	return strings.Join(out, ","), nil
}

var reservedQuery = map[string]struct{}{
	"select": {}, "order": {}, "limit": {}, "offset": {}, "or": {}, "and": {}, "search": {},
}

// BuildWhereForBulk builds SQL WHERE clause for bulk update/delete operations.
func BuildWhereForBulk(query map[string][]string) (string, []any, error) {
	clone := make(map[string][]string, len(query))
	for k, v := range query {
		clone[k] = v
	}
	delete(clone, "select")
	delete(clone, "order")
	delete(clone, "limit")
	delete(clone, "offset")
	delete(clone, "search")
	return buildWhereClause(clone)
}

func buildWhereClause(query map[string][]string) (string, []any, error) {
	parts := make([]string, 0)
	args := make([]any, 0)

	for key, values := range query {
		if _, ok := reservedQuery[key]; ok {
			continue
		}
		if len(values) == 0 {
			continue
		}
		if err := validateIdentifier(key); err != nil {
			return "", nil, err
		}
		frag, fragArgs, err := parseFilterToken(quotedIdentifier(key), values[0])
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, frag)
		args = append(args, fragArgs...)
	}

	if raw := firstOrDefault(query, "or", ""); raw != "" {
		frag, fragArgs, err := parseLogicalExpr("or=(" + stripLogicalWrap(raw, "or") + ")")
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, "("+frag+")")
		args = append(args, fragArgs...)
	}
	if raw := firstOrDefault(query, "and", ""); raw != "" {
		frag, fragArgs, err := parseLogicalExpr("and=(" + stripLogicalWrap(raw, "and") + ")")
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, "("+frag+")")
		args = append(args, fragArgs...)
	}

	if len(parts) == 0 {
		return "", nil, nil
	}
	return strings.Join(parts, " AND "), args, nil
}

// stripLogicalWrap normalizes the value of an `or` / `and` query parameter
// by removing an optional `<op>=` prefix and the outermost matching parens.
// Both shapes must be accepted because:
//   - HTTP query parsing yields the bare value `(a.eq.1,b.eq.2)`
//   - test fixtures and some clients pass the prefixed form `or=(a.eq.1,…)`
func stripLogicalWrap(raw, op string) string {
	v := strings.TrimSpace(raw)
	v = strings.TrimPrefix(v, op+"=")
	if strings.HasPrefix(v, "(") && strings.HasSuffix(v, ")") {
		v = v[1 : len(v)-1]
	}
	return v
}

func parseLogicalExpr(expr string) (string, []any, error) {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "or=(") && strings.HasSuffix(expr, ")") {
		inner := expr[len("or=(") : len(expr)-1]
		return parseLogicalList(inner, "OR")
	}
	if strings.HasPrefix(expr, "and=(") && strings.HasSuffix(expr, ")") {
		inner := expr[len("and=(") : len(expr)-1]
		return parseLogicalList(inner, "AND")
	}
	if strings.HasPrefix(expr, "not.") {
		frag, args, err := parseLogicalExpr(expr[len("not."):])
		if err != nil {
			return "", nil, err
		}
		return "NOT (" + frag + ")", args, nil
	}
	return parseConditionExpr(expr)
}

func parseLogicalList(inner, op string) (string, []any, error) {
	tokens := splitCSV(inner)
	parts := make([]string, 0, len(tokens))
	args := make([]any, 0)
	for _, t := range tokens {
		frag, fragArgs, err := parseLogicalExpr(strings.TrimSpace(t))
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, "("+frag+")")
		args = append(args, fragArgs...)
	}
	return strings.Join(parts, " "+op+" "), args, nil
}

func parseConditionExpr(expr string) (string, []any, error) {
	parts := strings.SplitN(expr, ".", 3)
	if len(parts) < 3 {
		return "", nil, fmt.Errorf("invalid condition expression %q", expr)
	}
	col := parts[0]
	if err := validateIdentifier(col); err != nil {
		return "", nil, err
	}
	op := parts[1]
	val := parts[2]
	return parseFilterToken(quotedIdentifier(col), op+"."+val)
}

func parseFilterToken(colExpr, token string) (string, []any, error) {
	not := false
	if strings.HasPrefix(token, "not.") {
		not = true
		token = strings.TrimPrefix(token, "not.")
	}
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("invalid filter token %q", token)
	}
	op, val := parts[0], parts[1]

	wrapNot := func(expr string) string {
		if not {
			return "NOT (" + expr + ")"
		}
		return expr
	}

	switch op {
	case "eq":
		return wrapNot(colExpr + " = ?"), []any{parseFilterValue(val)}, nil
	case "neq":
		return wrapNot(colExpr + " != ?"), []any{parseFilterValue(val)}, nil
	case "gt":
		return wrapNot(colExpr + " > ?"), []any{parseFilterValue(val)}, nil
	case "gte":
		return wrapNot(colExpr + " >= ?"), []any{parseFilterValue(val)}, nil
	case "lt":
		return wrapNot(colExpr + " < ?"), []any{parseFilterValue(val)}, nil
	case "lte":
		return wrapNot(colExpr + " <= ?"), []any{parseFilterValue(val)}, nil
	case "like":
		return wrapNot(colExpr + " LIKE ?"), []any{emailLikeWildcard.Replace(val)}, nil
	case "ilike":
		return wrapNot(colExpr + " LIKE ? COLLATE NOCASE"), []any{emailLikeWildcard.Replace(val)}, nil
	case "in":
		if !strings.HasPrefix(val, "(") || !strings.HasSuffix(val, ")") {
			return "", nil, fmt.Errorf("invalid in() value %q", val)
		}
		inner := strings.TrimSuffix(strings.TrimPrefix(val, "("), ")")
		items := splitCSV(inner)
		if len(items) == 0 {
			return "", nil, fmt.Errorf("empty in list")
		}
		place := make([]string, len(items))
		args := make([]any, len(items))
		for i, item := range items {
			place[i] = "?"
			args[i] = parseFilterValue(strings.TrimSpace(item))
		}
		return wrapNot(colExpr + " IN (" + strings.Join(place, ",") + ")"), args, nil
	case "is":
		switch strings.ToLower(val) {
		case "null":
			return wrapNot(colExpr + " IS NULL"), nil, nil
		case "true":
			return wrapNot(colExpr + " IS TRUE"), nil, nil
		case "false":
			return wrapNot(colExpr + " IS FALSE"), nil, nil
		default:
			return "", nil, fmt.Errorf("invalid is value %q", val)
		}
	default:
		return "", nil, fmt.Errorf("unsupported filter operator %q", op)
	}
}

func parseFilterValue(v string) any {
	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	if v == "true" {
		return 1
	}
	if v == "false" {
		return 0
	}
	if v == "null" {
		return nil
	}
	return v
}

func splitCSV(v string) []string {
	var out []string
	level := 0
	start := 0
	for i, r := range v {
		switch r {
		case '(':
			level++
		case ')':
			if level > 0 {
				level--
			}
		case ',':
			if level == 0 {
				out = append(out, strings.TrimSpace(v[start:i]))
				start = i + 1
			}
		}
	}
	if start <= len(v) {
		out = append(out, strings.TrimSpace(v[start:]))
	}
	filtered := out[:0]
	for _, item := range out {
		if item != "" {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func firstOrDefault(query map[string][]string, key, def string) string {
	v := query[key]
	if len(v) == 0 || strings.TrimSpace(v[0]) == "" {
		return def
	}
	return v[0]
}

func parseIntOrDefault(v string, def int) int {
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func scanRows(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("rows columns: %w", err)
	}

	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = normalizeDBValue(vals[i])
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}
	return out, nil
}

func normalizeDBValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return t
	}
}

// GetRowByID fetches a row by id.
func GetRowByID(db *sql.DB, table string, id any) (map[string]any, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	rows, err := db.Query(`SELECT * FROM `+quotedIdentifier(table)+` WHERE id = ? LIMIT 1`, id)
	if err != nil {
		return nil, fmt.Errorf("query row by id: %w", err)
	}
	defer rows.Close()
	list, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, sql.ErrNoRows
	}
	return list[0], nil
}

// InsertRows inserts one or many rows.
func InsertRows(db *sql.DB, table string, rowsIn []map[string]any, prefer string) ([]map[string]any, []any, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, nil, err
	}
	if len(rowsIn) == 0 {
		return nil, nil, fmt.Errorf("no rows to insert")
	}

	schema, err := TableSchemaForValidation(db, table)
	if err != nil {
		return nil, nil, err
	}
	colMap := make(map[string]domain.ColumnDefinition, len(schema.Columns))
	for _, c := range schema.Columns {
		colMap[c.Name] = c
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, nil, fmt.Errorf("begin insert: %w", err)
	}
	defer tx.Rollback()

	ids := make([]any, 0, len(rowsIn))

	for _, input := range rowsIn {
		clean, err := sanitizeAndValidateRow(input, colMap, true)
		if err != nil {
			return nil, nil, err
		}

		clean["updated_at"] = time.Now().UTC().Format(time.RFC3339)
		if _, ok := clean["created_at"]; !ok {
			clean["created_at"] = time.Now().UTC().Format(time.RFC3339)
		}

		stmt, args, err := buildInsertStatement(table, clean, prefer, tx)
		if err != nil {
			return nil, nil, err
		}
		stmt += ` RETURNING id`

		var id int64
		if err := tx.QueryRow(stmt, args...).Scan(&id); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// ON CONFLICT DO NOTHING path.
				continue
			}
			return nil, nil, fmt.Errorf("insert row: %w", err)
		}

		if id == 0 {
			continue
		}
		ids = append(ids, id)
	}

	inserted, err := selectRowsByIDsTx(tx, table, ids)
	if err != nil {
		return nil, nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit insert: %w", err)
	}
	return inserted, ids, nil
}

func sanitizeAndValidateRow(row map[string]any, columns map[string]domain.ColumnDefinition, forInsert bool) (map[string]any, error) {
	clean := make(map[string]any, len(row))
	for key, val := range row {
		if key == "meta" {
			continue
		}
		if key == "id" || key == "created_at" || key == "updated_at" {
			continue
		}
		col, ok := columns[key]
		if !ok {
			continue
		}
		if col.Formula != "" {
			return nil, fmt.Errorf("column %s is read-only formula", key)
		}
		converted, err := validateValue(col, val)
		if err != nil {
			return nil, err
		}
		clean[key] = converted
	}

	if forInsert {
		for name, col := range columns {
			if _, exists := clean[name]; exists {
				continue
			}
			if col.Auto {
				if col.Type == "date" {
					clean[name] = time.Now().UTC().Format("2006-01-02")
				} else {
					clean[name] = time.Now().UTC().Format(time.RFC3339)
				}
			}
			if col.NotNull && col.Default == nil && !col.Auto && col.Name != "id" && col.Name != "created_at" && col.Name != "updated_at" {
				if _, ok := clean[name]; !ok {
					return nil, fmt.Errorf("validation_failed: missing required column %s", name)
				}
			}
		}
	}

	return clean, nil
}

func validateValue(col domain.ColumnDefinition, val any) (any, error) {
	if val == nil {
		if col.NotNull {
			return nil, fmt.Errorf("validation_failed: %s cannot be null", col.Name)
		}
		return nil, nil
	}

	switch col.Type {
	case "integer":
		switch v := val.(type) {
		case float64:
			if float64(int64(v)) != v {
				return nil, fmt.Errorf("validation_failed: %s expects integer", col.Name)
			}
			if col.Min != nil && v < *col.Min {
				return nil, fmt.Errorf("validation_failed: %s below minimum", col.Name)
			}
			if col.Max != nil && v > *col.Max {
				return nil, fmt.Errorf("validation_failed: %s above maximum", col.Name)
			}
			return int64(v), nil
		case int, int64:
			return v, nil
		case json.Number:
			i, err := v.Int64()
			if err != nil {
				return nil, fmt.Errorf("validation_failed: %s expects integer", col.Name)
			}
			return i, nil
		default:
			return nil, fmt.Errorf("validation_failed: %s expects integer", col.Name)
		}
	case "real":
		f, err := toFloat(val)
		if err != nil {
			return nil, fmt.Errorf("validation_failed: %s expects number", col.Name)
		}
		if col.Min != nil && f < *col.Min {
			return nil, fmt.Errorf("validation_failed: %s below minimum", col.Name)
		}
		if col.Max != nil && f > *col.Max {
			return nil, fmt.Errorf("validation_failed: %s above maximum", col.Name)
		}
		return f, nil
	case "boolean":
		switch v := val.(type) {
		case bool:
			if v {
				return 1, nil
			}
			return 0, nil
		case float64:
			if v == 0 || v == 1 {
				return int(v), nil
			}
		}
		return nil, fmt.Errorf("validation_failed: %s expects boolean", col.Name)
	case "date":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("validation_failed: %s expects date string", col.Name)
		}
		if _, err := time.Parse("2006-01-02", s); err != nil {
			return nil, fmt.Errorf("validation_failed: %s invalid date", col.Name)
		}
		return s, nil
	case "datetime":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("validation_failed: %s expects datetime string", col.Name)
		}
		if _, err := time.Parse(time.RFC3339, s); err != nil {
			return nil, fmt.Errorf("validation_failed: %s invalid datetime", col.Name)
		}
		return s, nil
	case "json":
		b, err := json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("validation_failed: %s invalid json", col.Name)
		}
		return string(b), nil
	case "select":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("validation_failed: %s expects string", col.Name)
		}
		for _, opt := range col.Options {
			if s == opt {
				return s, nil
			}
		}
		return nil, fmt.Errorf("validation_failed: value '%s' not in options for %s", s, col.Name)
	case "text", "blob":
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("validation_failed: %s expects string", col.Name)
		}
		if col.MaxLength > 0 && len([]rune(s)) > col.MaxLength {
			return nil, fmt.Errorf("validation_failed: %s exceeds max_length", col.Name)
		}
		if col.Pattern != "" {
			re, err := regexp.Compile(col.Pattern)
			if err != nil {
				return nil, fmt.Errorf("invalid pattern on %s", col.Name)
			}
			if !re.MatchString(s) {
				return nil, fmt.Errorf("validation_failed: value for %s does not match pattern", col.Name)
			}
		}
		return s, nil
	default:
		return val, nil
	}
}

func toFloat(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case json.Number:
		return n.Float64()
	default:
		return 0, fmt.Errorf("not a number")
	}
}

func buildInsertStatement(table string, row map[string]any, prefer string, tx *sql.Tx) (string, []any, error) {
	cols := make([]string, 0, len(row))
	place := make([]string, 0, len(row))
	args := make([]any, 0, len(row))
	for col, v := range row {
		if err := validateIdentifier(col); err != nil {
			return "", nil, err
		}
		cols = append(cols, quotedIdentifier(col))
		place = append(place, "?")
		args = append(args, v)
	}
	if len(cols) == 0 {
		return "", nil, fmt.Errorf("no writable fields")
	}

	stmt := `INSERT INTO ` + quotedIdentifier(table) + ` (` + strings.Join(cols, ",") + `) VALUES (` + strings.Join(place, ",") + `)`
	switch prefer {
	case "resolution=ignore-duplicates":
		stmt += ` ON CONFLICT DO NOTHING`
	case "resolution=merge-duplicates":
		target, err := conflictTarget(tx, table)
		if err != nil {
			return "", nil, err
		}
		var updates []string
		for _, c := range cols {
			if c == quotedIdentifier("id") {
				continue
			}
			updates = append(updates, c+`=excluded.`+c)
		}
		stmt += ` ON CONFLICT (` + target + `) DO UPDATE SET ` + strings.Join(updates, ",")
	}
	return stmt, args, nil
}

func conflictTarget(tx *sql.Tx, table string) (string, error) {
	rows, err := tx.Query(fmt.Sprintf("PRAGMA index_list(%s)", quotedIdentifier(table)))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return "", err
		}
		if unique != 1 {
			continue
		}
		infoRows, err := tx.Query(fmt.Sprintf("PRAGMA index_info(%s)", quotedIdentifier(name)))
		if err != nil {
			return "", err
		}
		cols := make([]string, 0, 4)
		for infoRows.Next() {
			var seqno, cid int
			var colName string
			if err := infoRows.Scan(&seqno, &cid, &colName); err != nil {
				infoRows.Close()
				return "", err
			}
			cols = append(cols, colName)
		}
		if err := infoRows.Close(); err != nil {
			return "", err
		}
		if len(cols) > 0 {
			quoted := make([]string, len(cols))
			for i, c := range cols {
				quoted[i] = quotedIdentifier(c)
			}
			return strings.Join(quoted, ","), nil
		}
		_ = origin
		_ = partial
	}
	return "", fmt.Errorf("no unique index available for upsert")
}

func getRowByIDTx(tx *sql.Tx, table string, id any) (map[string]any, error) {
	rows, err := tx.Query(`SELECT * FROM `+quotedIdentifier(table)+` WHERE id = ? LIMIT 1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	data, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, sql.ErrNoRows
	}
	return data[0], nil
}

// UpdateRowByID updates a single row by id.
func UpdateRowByID(db *sql.DB, table string, id any, payload map[string]any, prefer string) ([]map[string]any, error) {
	ids, _, rows, err := updateRows(db, table, payload, `id = ?`, []any{id})
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, sql.ErrNoRows
	}
	if prefer == "return=representation" {
		return rows, nil
	}
	return nil, nil
}

// DeleteRowByID deletes one row.
func DeleteRowByID(db *sql.DB, table string, id any, prefer string) ([]map[string]any, error) {
	ids, rows, err := deleteRows(db, table, `id = ?`, []any{id})
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, sql.ErrNoRows
	}
	if prefer == "return=representation" {
		return rows, nil
	}
	return nil, nil
}

// BulkUpdateRows updates filtered rows.
func BulkUpdateRows(db *sql.DB, table string, payload map[string]any, where string, whereArgs []any, prefer string) ([]any, []map[string]any, error) {
	ids, _, rows, err := updateRows(db, table, payload, where, whereArgs)
	if err != nil {
		return nil, nil, err
	}
	if prefer == "return=representation" {
		return ids, rows, nil
	}
	return ids, nil, nil
}

// BulkDeleteRows deletes filtered rows.
func BulkDeleteRows(db *sql.DB, table string, where string, whereArgs []any, prefer string) ([]any, []map[string]any, error) {
	ids, rows, err := deleteRows(db, table, where, whereArgs)
	if err != nil {
		return nil, nil, err
	}
	if prefer == "return=representation" {
		return ids, rows, nil
	}
	return ids, nil, nil
}

func updateRows(db *sql.DB, table string, payload map[string]any, where string, whereArgs []any) ([]any, int, []map[string]any, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, 0, nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, 0, nil, err
	}
	defer tx.Rollback()

	ids, err := selectIDsTx(tx, table, where, whereArgs)
	if err != nil {
		return nil, 0, nil, err
	}
	if len(ids) == 0 {
		return nil, 0, nil, nil
	}

	schema, err := TableSchemaForValidation(db, table)
	if err != nil {
		return nil, 0, nil, err
	}
	colMap := make(map[string]domain.ColumnDefinition, len(schema.Columns))
	for _, c := range schema.Columns {
		colMap[c.Name] = c
	}

	set, args, err := buildUpdateSet(payload, colMap)
	if err != nil {
		return nil, 0, nil, err
	}
	set = append(set, `updated_at = ?`)
	args = append(args, time.Now().UTC().Format(time.RFC3339))
	args = append(args, whereArgs...)

	stmt := `UPDATE ` + quotedIdentifier(table) + ` SET ` + strings.Join(set, ",")
	if where != "" {
		stmt += ` WHERE ` + where
	}

	res, err := tx.Exec(stmt, args...)
	if err != nil {
		return nil, 0, nil, err
	}
	affected, _ := res.RowsAffected()

	rows, err := selectRowsByIDsTx(tx, table, ids)
	if err != nil {
		return nil, 0, nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, 0, nil, err
	}

	return ids, int(affected), rows, nil
}

func buildUpdateSet(payload map[string]any, columns map[string]domain.ColumnDefinition) ([]string, []any, error) {
	set := make([]string, 0, len(payload))
	args := make([]any, 0, len(payload))

	for key, val := range payload {
		if key == "meta" || key == "id" || key == "created_at" || key == "updated_at" {
			continue
		}
		if err := validateIdentifier(key); err != nil {
			return nil, nil, err
		}
		col, ok := columns[key]
		if !ok {
			continue
		}
		if opMap, ok := val.(map[string]any); ok {
			if len(opMap) == 1 {
				for op, arg := range opMap {
					expr, exprArgs, err := buildAtomicOp(key, col, op, arg)
					if err != nil {
						return nil, nil, err
					}
					set = append(set, expr)
					args = append(args, exprArgs...)
				}
				continue
			}
		}
		converted, err := validateValue(col, val)
		if err != nil {
			return nil, nil, err
		}
		set = append(set, quotedIdentifier(key)+` = ?`)
		args = append(args, converted)
	}
	if len(set) == 0 {
		return nil, nil, fmt.Errorf("no updatable fields")
	}
	return set, args, nil
}

func buildAtomicOp(colName string, col domain.ColumnDefinition, op string, arg any) (string, []any, error) {
	colExpr := quotedIdentifier(colName)
	switch op {
	case "$increment":
		if col.Type != "integer" && col.Type != "real" {
			return "", nil, fmt.Errorf("validation_failed: $increment invalid for %s", colName)
		}
		return colExpr + ` = ` + colExpr + ` + ?`, []any{arg}, nil
	case "$multiply":
		if col.Type != "integer" && col.Type != "real" {
			return "", nil, fmt.Errorf("validation_failed: $multiply invalid for %s", colName)
		}
		return colExpr + ` = ` + colExpr + ` * ?`, []any{arg}, nil
	case "$append":
		if col.Type != "json" {
			return "", nil, fmt.Errorf("validation_failed: $append invalid for %s", colName)
		}
		b, _ := json.Marshal(arg)
		return colExpr + ` = json_insert(COALESCE(` + colExpr + `,'[]'), '$[#]', json(?))`, []any{string(b)}, nil
	case "$remove":
		if col.Type != "json" {
			return "", nil, fmt.Errorf("validation_failed: $remove invalid for %s", colName)
		}
		path, ok := arg.(string)
		if !ok {
			return "", nil, fmt.Errorf("validation_failed: $remove expects json path")
		}
		return colExpr + ` = json_remove(` + colExpr + `, ?)`, []any{path}, nil
	case "$toggle":
		if col.Type != "boolean" {
			return "", nil, fmt.Errorf("validation_failed: $toggle invalid for %s", colName)
		}
		return colExpr + ` = CASE WHEN ` + colExpr + ` = 1 THEN 0 ELSE 1 END`, nil, nil
	default:
		return "", nil, fmt.Errorf("unsupported atomic operator %s", op)
	}
}

func deleteRows(db *sql.DB, table, where string, whereArgs []any) ([]any, []map[string]any, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	ids, err := selectIDsTx(tx, table, where, whereArgs)
	if err != nil {
		return nil, nil, err
	}
	if len(ids) == 0 {
		return nil, nil, nil
	}
	rows, err := selectRowsByIDsTx(tx, table, ids)
	if err != nil {
		return nil, nil, err
	}

	stmt := `DELETE FROM ` + quotedIdentifier(table)
	if where != "" {
		stmt += ` WHERE ` + where
	}
	if _, err := tx.Exec(stmt, whereArgs...); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	return ids, rows, nil
}

func selectIDsTx(tx *sql.Tx, table, where string, args []any) ([]any, error) {
	stmt := `SELECT id FROM ` + quotedIdentifier(table)
	if where != "" {
		stmt += ` WHERE ` + where
	}
	rows, err := tx.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []any
	for rows.Next() {
		var id any
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func selectRowsByIDsTx(tx *sql.Tx, table string, ids []any) ([]map[string]any, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rowsByID := make(map[string]map[string]any, len(ids))

	for i := 0; i < len(ids); i += maxSQLVariables {
		end := i + maxSQLVariables
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[i:end]

		ph := make([]string, len(chunk))
		for j := range chunk {
			ph[j] = "?"
		}
		stmt := `SELECT * FROM ` + quotedIdentifier(table) + ` WHERE id IN (` + strings.Join(ph, ",") + `)`
		rows, err := tx.Query(stmt, chunk...)
		if err != nil {
			return nil, err
		}
		scanned, err := scanRows(rows)
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
		for _, row := range scanned {
			id, ok := row["id"]
			if !ok {
				continue
			}
			rowsByID[fmt.Sprint(id)] = row
		}
	}

	ordered := make([]map[string]any, 0, len(rowsByID))
	for _, id := range ids {
		if row, ok := rowsByID[fmt.Sprint(id)]; ok {
			ordered = append(ordered, row)
		}
	}
	return ordered, nil
}
