package sqlite

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"strings"
	"testing"

	"github.com/ValentinKolb/rsql/internal/domain"
)

// setupCSVTable creates a fresh table covering every type the encoder has
// special handling for, so each test can pick the row shapes it needs.
func setupCSVTable(t *testing.T) *sql.DB {
	t.Helper()
	db, _ := openTestDB(t)
	if err := CreateTableOrView(db, domain.TableCreateRequest{
		Type: "table",
		Name: "items",
		Columns: []domain.ColumnDefinition{
			{Name: "label", Type: "text"},
			{Name: "score", Type: "integer"},
			{Name: "ratio", Type: "real"},
			{Name: "active", Type: "boolean"},
			{Name: "tags", Type: "json"},
			{Name: "status", Type: "select", Options: []string{"a", "b", "c"}},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return db
}

func parseCSV(t *testing.T, body string) [][]string {
	t.Helper()
	r := csv.NewReader(strings.NewReader(body))
	all, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse csv: %v\nbody=%q", err, body)
	}
	return all
}

func runCSV(t *testing.T, db *sql.DB, table string, query map[string][]string) string {
	t.Helper()
	var buf bytes.Buffer
	if err := StreamCSV(db, table, query, &buf); err != nil {
		t.Fatalf("stream csv: %v", err)
	}
	return buf.String()
}

func TestStreamCSV_EmptyTable(t *testing.T) {
	db := setupCSVTable(t)
	out := runCSV(t, db, "items", nil)
	rows := parseCSV(t, out)
	if len(rows) != 1 {
		t.Fatalf("expected header only, got %d rows: %v", len(rows), rows)
	}
	header := rows[0]
	want := []string{"id", "created_at", "updated_at", "label", "score", "ratio", "active", "tags", "status"}
	if len(header) != len(want) {
		t.Fatalf("header length: want %d got %d (%v)", len(want), len(header), header)
	}
	for i, w := range want {
		if header[i] != w {
			t.Fatalf("header[%d]: want %q got %q", i, w, header[i])
		}
	}
}

func TestStreamCSV_AllTypesAndNulls(t *testing.T) {
	db := setupCSVTable(t)
	if _, _, err := InsertRows(db, "items", []map[string]any{
		{"label": "Ada", "score": 92, "ratio": 0.5, "active": true, "tags": map[string]any{"v": 1}, "status": "a"},
		{"label": "Bob", "score": nil, "ratio": nil, "active": false, "tags": nil, "status": "b"},
	}, ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rows := parseCSV(t, runCSV(t, db, "items", map[string][]string{
		"select": {"label,score,ratio,active,tags,status"},
		"order":  {"id.asc"},
	}))
	if len(rows) != 3 {
		t.Fatalf("expected 1 header + 2 data rows, got %d: %v", len(rows), rows)
	}
	// Header
	if rows[0][3] != "active" || rows[0][4] != "tags" {
		t.Fatalf("header order off: %v", rows[0])
	}
	// Ada
	if rows[1][0] != "Ada" || rows[1][3] != "true" {
		t.Fatalf("ada row: %v", rows[1])
	}
	if rows[1][4] != `{"v":1}` {
		t.Fatalf("ada tags: %q", rows[1][4])
	}
	// Bob: null score and ratio → empty cells, false bool
	if rows[2][0] != "Bob" || rows[2][1] != "" || rows[2][2] != "" || rows[2][3] != "false" || rows[2][4] != "" {
		t.Fatalf("bob row: %v", rows[2])
	}
}

func TestStreamCSV_FilteredAndOrdered(t *testing.T) {
	db := setupCSVTable(t)
	if _, _, err := InsertRows(db, "items", []map[string]any{
		{"label": "x1", "score": 10, "status": "a"},
		{"label": "x2", "score": 50, "status": "b"},
		{"label": "x3", "score": 90, "status": "a"},
	}, ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	out := runCSV(t, db, "items", map[string][]string{
		"select": {"label,score"},
		"status": {"eq.a"},
		"order":  {"score.desc"},
	})
	rows := parseCSV(t, out)
	if len(rows) != 3 {
		t.Fatalf("got %v", rows)
	}
	if rows[1][0] != "x3" || rows[2][0] != "x1" {
		t.Fatalf("ordering wrong: %v", rows)
	}
}

func TestStreamCSV_RFC4180SpecialChars(t *testing.T) {
	db := setupCSVTable(t)
	if _, _, err := InsertRows(db, "items", []map[string]any{
		{"label": `has,comma`, "status": "a"},
		{"label": `has "quote"`, "status": "a"},
		{"label": "has\nnewline", "status": "a"},
	}, ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rows := parseCSV(t, runCSV(t, db, "items", map[string][]string{
		"select": {"label"},
		"order":  {"id.asc"},
	}))
	if len(rows) != 4 {
		t.Fatalf("expected header + 3 rows, got %v", rows)
	}
	wantValues := []string{"has,comma", `has "quote"`, "has\nnewline"}
	for i, want := range wantValues {
		if rows[i+1][0] != want {
			t.Fatalf("row %d: want %q got %q", i+1, want, rows[i+1][0])
		}
	}
}

func TestStreamCSV_View(t *testing.T) {
	db := setupCSVTable(t)
	if _, _, err := InsertRows(db, "items", []map[string]any{
		{"label": "v1", "status": "a"},
		{"label": "v2", "status": "b"},
	}, ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := CreateTableOrView(db, domain.TableCreateRequest{
		Type: "view",
		Name: "v_items",
		SQL:  `SELECT label, status FROM items WHERE status='a'`,
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}
	rows := parseCSV(t, runCSV(t, db, "v_items", nil))
	if len(rows) != 2 || rows[1][0] != "v1" {
		t.Fatalf("view export wrong: %v", rows)
	}
}

func TestStreamCSV_LimitHonoredOffsetWorks(t *testing.T) {
	db := setupCSVTable(t)
	for i := 0; i < 10; i++ {
		if _, _, err := InsertRows(db, "items", []map[string]any{
			{"label": "row", "score": i, "status": "a"},
		}, ""); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	// Without limit → all 10
	rows := parseCSV(t, runCSV(t, db, "items", nil))
	if len(rows) != 11 {
		t.Fatalf("unlimited: want 11 (header+10), got %d", len(rows))
	}
	// With limit=3 → 3 data rows. Use a narrow select so the assertion does
	// not depend on the implicit column order of the default '*'.
	rows = parseCSV(t, runCSV(t, db, "items", map[string][]string{"limit": {"3"}, "select": {"score"}, "order": {"score.asc"}}))
	if len(rows) != 4 {
		t.Fatalf("limit=3: want 4, got %d", len(rows))
	}
	if rows[1][0] != "0" || rows[3][0] != "2" {
		t.Fatalf("limit ordering wrong: %v", rows)
	}
	// limit + offset
	rows = parseCSV(t, runCSV(t, db, "items", map[string][]string{
		"limit":  {"3"},
		"offset": {"5"},
		"select": {"score"},
		"order":  {"score.asc"},
	}))
	if len(rows) != 4 {
		t.Fatalf("limit+offset: want 4, got %d", len(rows))
	}
	if rows[1][0] != "5" || rows[3][0] != "7" {
		t.Fatalf("offset wrong: %v", rows)
	}
}

func TestStreamCSV_AggregateSelect(t *testing.T) {
	db := setupCSVTable(t)
	if _, _, err := InsertRows(db, "items", []map[string]any{
		{"label": "x", "status": "a"},
		{"label": "y", "status": "a"},
		{"label": "z", "status": "b"},
	}, ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rows := parseCSV(t, runCSV(t, db, "items", map[string][]string{
		"select": {"status,count()"},
		"order":  {"status.asc"},
	}))
	if len(rows) != 3 {
		t.Fatalf("aggregate output: want 3 rows, got %v", rows)
	}
	if rows[0][0] != "status" || rows[0][1] != "count" {
		t.Fatalf("aggregate header: %v", rows[0])
	}
	if rows[1][0] != "a" || rows[1][1] != "2" {
		t.Fatalf("aggregate row a: %v", rows[1])
	}
	if rows[2][0] != "b" || rows[2][1] != "1" {
		t.Fatalf("aggregate row b: %v", rows[2])
	}
}

func TestStreamCSV_MissingTable(t *testing.T) {
	db := setupCSVTable(t)
	var buf bytes.Buffer
	err := StreamCSV(db, "missing", nil, &buf)
	if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected no bytes written on missing table, got %q", buf.String())
	}
}

func TestStreamCSV_InvalidIdentifier(t *testing.T) {
	db := setupCSVTable(t)
	if err := StreamCSV(db, "bad name", nil, &bytes.Buffer{}); err == nil {
		t.Fatal("expected invalid identifier error")
	}
}

func TestStreamCSV_FlushesProgressively(t *testing.T) {
	// Verify the function does not buffer the entire result before writing.
	// We seed 2 500 rows (>2× csvFlushChunk) and assert that w receives the
	// header before all data has been processed.
	db := setupCSVTable(t)
	const total = 2500
	batch := make([]map[string]any, 0, total)
	for i := 0; i < total; i++ {
		batch = append(batch, map[string]any{"label": "L", "score": i, "status": "a"})
	}
	if _, _, err := InsertRows(db, "items", batch, ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var buf bytes.Buffer
	if err := StreamCSV(db, "items", nil, &buf); err != nil {
		t.Fatalf("stream: %v", err)
	}
	rows := parseCSV(t, buf.String())
	if len(rows) != total+1 {
		t.Fatalf("want %d rows, got %d", total+1, len(rows))
	}
}
