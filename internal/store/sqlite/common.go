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

var identRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

// EnsureInternalSchema creates internal tables used by rsql in a namespace DB.
func EnsureInternalSchema(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS _meta (
	entity_type TEXT NOT NULL,
	entity_name TEXT NOT NULL,
	metadata_json TEXT NOT NULL,
	PRIMARY KEY (entity_type, entity_name)
);
CREATE TABLE IF NOT EXISTS _schema_log (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	timestamp TEXT NOT NULL,
	action TEXT NOT NULL,
	table_name TEXT NOT NULL,
	detail_json TEXT NOT NULL,
	meta_json TEXT
);
`)
	if err != nil {
		return fmt.Errorf("ensure internal schema: %w", err)
	}
	return nil
}

// ApplyNamespaceConfig applies namespace PRAGMAs.
func ApplyNamespaceConfig(db *sql.DB, cfg domain.NamespaceConfig) error {
	if cfg.JournalMode != "" {
		if _, err := db.Exec("PRAGMA journal_mode = " + quotePragmaValue(cfg.JournalMode)); err != nil {
			return fmt.Errorf("apply journal_mode: %w", err)
		}
	}
	if cfg.BusyTimeout > 0 {
		if _, err := db.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d", cfg.BusyTimeout)); err != nil {
			return fmt.Errorf("apply busy_timeout: %w", err)
		}
	}
	if cfg.QueryTimeout > 0 {
		// Stored for service-level timeouts, no pragma equivalent needed here.
	}
	if cfg.MaxDBSize > 0 {
		pageSize := int64(4096)
		maxPages := cfg.MaxDBSize / pageSize
		if maxPages > 0 {
			if _, err := db.Exec(fmt.Sprintf("PRAGMA max_page_count = %d", maxPages)); err != nil {
				return fmt.Errorf("apply max_db_size: %w", err)
			}
		}
	}
	if _, err := db.Exec(fmt.Sprintf("PRAGMA foreign_keys = %d", boolInt(cfg.ForeignKeysOrDefault()))); err != nil {
		return fmt.Errorf("apply foreign_keys: %w", err)
	}
	if cfg.ReadOnly {
		if _, err := db.Exec("PRAGMA query_only = 1"); err != nil {
			return fmt.Errorf("apply read_only: %w", err)
		}
	} else {
		if _, err := db.Exec("PRAGMA query_only = 0"); err != nil {
			return fmt.Errorf("apply read_only: %w", err)
		}
	}
	return nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func quotePragmaValue(v string) string {
	v = strings.ReplaceAll(v, "'", "")
	return "'" + v + "'"
}

func validateIdentifier(name string) error {
	if !identRe.MatchString(name) {
		return fmt.Errorf("invalid identifier %q", name)
	}
	return nil
}

func quotedIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// PutMeta upserts metadata for an entity.
func PutMeta(db *sql.DB, entityType, entityName string, data any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	_, err = db.Exec(`
INSERT INTO _meta (entity_type, entity_name, metadata_json)
VALUES (?, ?, ?)
ON CONFLICT(entity_type, entity_name) DO UPDATE SET metadata_json=excluded.metadata_json
`, entityType, entityName, string(b))
	if err != nil {
		return fmt.Errorf("upsert meta: %w", err)
	}
	return nil
}

// GetMeta unmarshals metadata into dst and returns false if key is missing.
func GetMeta(db *sql.DB, entityType, entityName string, dst any) (bool, error) {
	row := db.QueryRow(`SELECT metadata_json FROM _meta WHERE entity_type=? AND entity_name=?`, entityType, entityName)
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

// DeleteMeta removes metadata by key.
func DeleteMeta(db *sql.DB, entityType, entityName string) error {
	_, err := db.Exec(`DELETE FROM _meta WHERE entity_type=? AND entity_name=?`, entityType, entityName)
	if err != nil {
		return fmt.Errorf("delete meta: %w", err)
	}
	return nil
}

// AppendSchemaLog writes a changelog entry.
func AppendSchemaLog(db *sql.DB, action, table string, detail, meta any) error {
	detailRaw, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("marshal changelog detail: %w", err)
	}
	var metaRaw []byte
	if meta != nil {
		metaRaw, err = json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshal changelog meta: %w", err)
		}
	}
	_, err = db.Exec(`
INSERT INTO _schema_log (timestamp, action, table_name, detail_json, meta_json)
VALUES (?, ?, ?, ?, ?)
`, time.Now().UTC().Format(time.RFC3339), action, table, string(detailRaw), nullableString(metaRaw))
	if err != nil {
		return fmt.Errorf("insert schema_log: %w", err)
	}
	return nil
}

func nullableString(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}
