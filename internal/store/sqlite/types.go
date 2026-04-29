package sqlite

import (
	"encoding/json"

	"github.com/ValentinKolb/rsql/internal/domain"
)

// TableSchema stores table/view metadata for validation and API contracts.
type TableSchema struct {
	Name         string                    `json:"name"`
	Type         string                    `json:"type"`
	SQL          string                    `json:"sql,omitempty"`
	Columns      []domain.ColumnDefinition `json:"columns,omitempty"`
	Metadata     json.RawMessage           `json:"metadata,omitempty"`
	SourceTables []string                  `json:"source_tables,omitempty"`
}

// IndexSchema describes an index entry.
type IndexSchema struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Columns []string `json:"columns"`
}
