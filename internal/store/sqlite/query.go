package sqlite

import (
	"database/sql"
	"fmt"
	"strings"
)

var forbiddenSQLKeywords = map[string]struct{}{
	"INSERT":   {},
	"UPDATE":   {},
	"DELETE":   {},
	"CREATE":   {},
	"ALTER":    {},
	"DROP":     {},
	"REPLACE":  {},
	"PRAGMA":   {},
	"ATTACH":   {},
	"DETACH":   {},
	"VACUUM":   {},
	"REINDEX":  {},
	"TRUNCATE": {},
}

// QueryStatement defines one query statement.
type QueryStatement struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params"`
}

// QueryRequest defines a single or batch read-only query request.
type QueryRequest struct {
	SQL        string           `json:"sql"`
	Params     []any            `json:"params"`
	Statements []QueryStatement `json:"statements"`
}

// ExecuteReadOnly executes read-only SQL statements.
func ExecuteReadOnly(db *sql.DB, req QueryRequest) (any, error) {
	if len(req.Statements) > 0 {
		out := make([]map[string]any, 0, len(req.Statements))
		for _, st := range req.Statements {
			rows, err := executeReadOnlyStatement(db, st.SQL, st.Params)
			if err != nil {
				return nil, err
			}
			out = append(out, map[string]any{"data": rows})
		}
		return map[string]any{"results": out}, nil
	}

	rows, err := executeReadOnlyStatement(db, req.SQL, req.Params)
	if err != nil {
		return nil, err
	}
	return map[string]any{"data": rows}, nil
}

func executeReadOnlyStatement(db *sql.DB, query string, params []any) ([]map[string]any, error) {
	if !isReadOnlyQuery(query) {
		return nil, fmt.Errorf("sql_not_read_only")
	}
	if referencesInternalObject(query) {
		return nil, fmt.Errorf("sql_not_read_only")
	}

	rows, err := db.Query(query, params...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func isReadOnlyQuery(q string) bool {
	raw := strings.TrimSpace(q)
	if raw == "" {
		return false
	}
	// Block multi-statement and comment-based bypasses.
	if strings.Contains(raw, ";") || strings.Contains(raw, "--") || strings.Contains(raw, "/*") || strings.Contains(raw, "*/") {
		return false
	}

	normalized := normalizeSQL(raw)
	if normalized == "" {
		return false
	}
	tokens := strings.Fields(normalized)
	if len(tokens) == 0 {
		return false
	}
	if tokens[0] != "SELECT" && tokens[0] != "WITH" {
		return false
	}
	for _, token := range tokens {
		if _, blocked := forbiddenSQLKeywords[token]; blocked {
			return false
		}
	}
	return true
}

func referencesInternalObject(q string) bool {
	n := normalizeSQL(q)
	parts := strings.Fields(n)
	for _, p := range parts {
		p = strings.Trim(p, `",;()`) // punctuation
		if strings.HasPrefix(p, "_") {
			return true
		}
	}
	return false
}

func normalizeSQL(q string) string {
	q = strings.TrimSpace(q)
	q = strings.NewReplacer(
		"\n", " ",
		"\t", " ",
		"\r", " ",
		"(", " ( ",
		")", " ) ",
		",", " , ",
	).Replace(q)
	q = " " + strings.ToUpper(q) + " "
	for strings.Contains(q, "  ") {
		q = strings.ReplaceAll(q, "  ", " ")
	}
	return strings.TrimSpace(q)
}
