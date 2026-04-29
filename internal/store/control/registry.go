package control

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// NamespaceRecord stores namespace lookup metadata.
type NamespaceRecord struct {
	Name      string
	DBPath    string
	CreatedAt string
}

// Registry persists namespace lookup state in control.db.
type Registry struct {
	db *sql.DB
}

// Open opens or initializes the control registry database.
func Open(dataDir string) (*Registry, error) {
	if err := os.MkdirAll(filepath.Join(dataDir, "namespaces"), 0o755); err != nil {
		return nil, fmt.Errorf("create namespaces directory: %w", err)
	}

	path := filepath.Join(dataDir, "control.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open control db: %w", err)
	}

	if err := initSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Registry{db: db}, nil
}

func initSchema(db *sql.DB) error {
	query := `
CREATE TABLE IF NOT EXISTS namespaces (
	name TEXT PRIMARY KEY,
	db_path TEXT NOT NULL,
	created_at TEXT NOT NULL,
	deleted_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_namespaces_deleted_at ON namespaces(deleted_at);
`
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("init control schema: %w", err)
	}
	return nil
}

// Close closes the control registry DB handle.
func (r *Registry) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

// ErrAlreadyExists is returned by Create when the name is already taken
// by an active (non-deleted) namespace.
var ErrAlreadyExists = fmt.Errorf("namespace already exists")

// Create inserts a namespace lookup entry. If the name belonged to a
// previously soft-deleted namespace, the entry is restored. If the name
// is currently active, ErrAlreadyExists is returned.
func (r *Registry) Create(name, dbPath string) error {
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("begin create: %w", err)
	}
	defer tx.Rollback()

	var deletedAt sql.NullString
	row := tx.QueryRow(`SELECT deleted_at FROM namespaces WHERE name=?`, name)
	switch err := row.Scan(&deletedAt); err {
	case nil:
		// Row exists. Active rows must not be silently overwritten;
		// soft-deleted rows can be restored.
		if !deletedAt.Valid {
			return ErrAlreadyExists
		}
		if _, err := tx.Exec(`UPDATE namespaces SET db_path=?, created_at=?, deleted_at=NULL WHERE name=?`,
			dbPath, time.Now().UTC().Format(time.RFC3339), name); err != nil {
			return fmt.Errorf("restore namespace record: %w", err)
		}
	case sql.ErrNoRows:
		if _, err := tx.Exec(`INSERT INTO namespaces (name, db_path, created_at, deleted_at) VALUES (?, ?, ?, NULL)`,
			name, dbPath, time.Now().UTC().Format(time.RFC3339)); err != nil {
			return fmt.Errorf("insert namespace record: %w", err)
		}
	default:
		return fmt.Errorf("lookup namespace: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit create: %w", err)
	}
	return nil
}

// Delete soft-deletes a namespace lookup entry.
func (r *Registry) Delete(name string) error {
	res, err := r.db.Exec(`UPDATE namespaces SET deleted_at=? WHERE name=? AND deleted_at IS NULL`, time.Now().UTC().Format(time.RFC3339), name)
	if err != nil {
		return fmt.Errorf("delete namespace record: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// Get returns a namespace record.
func (r *Registry) Get(name string) (NamespaceRecord, error) {
	var rec NamespaceRecord
	row := r.db.QueryRow(`SELECT name, db_path, created_at FROM namespaces WHERE name=? AND deleted_at IS NULL`, name)
	if err := row.Scan(&rec.Name, &rec.DBPath, &rec.CreatedAt); err != nil {
		return NamespaceRecord{}, err
	}
	return rec, nil
}

// List returns all active namespace records.
func (r *Registry) List() ([]NamespaceRecord, error) {
	rows, err := r.db.Query(`SELECT name, db_path, created_at FROM namespaces WHERE deleted_at IS NULL ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	defer rows.Close()

	var records []NamespaceRecord
	for rows.Next() {
		var rec NamespaceRecord
		if err := rows.Scan(&rec.Name, &rec.DBPath, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan namespace: %w", err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate namespaces: %w", err)
	}

	return records, nil
}

// PathFor computes the canonical namespace DB path.
func PathFor(dataDir, name string) string {
	return filepath.Join(dataDir, "namespaces", name+".db")
}
