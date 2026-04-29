package domain

import "encoding/json"

// NamespaceConfig configures a namespace database runtime.
type NamespaceConfig struct {
	JournalMode  string `json:"journal_mode"`
	BusyTimeout  int    `json:"busy_timeout"`
	MaxDBSize    int64  `json:"max_db_size"`
	QueryTimeout int    `json:"query_timeout"`
	ForeignKeys  bool   `json:"foreign_keys"`
	ReadOnly     bool   `json:"read_only"`
}

// NamespaceDefinition describes a namespace.
type NamespaceDefinition struct {
	Name   string          `json:"name"`
	Config NamespaceConfig `json:"config"`
}

// ColumnDefinition describes a table column.
type ColumnDefinition struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	NotNull    bool            `json:"not_null,omitempty"`
	Unique     bool            `json:"unique,omitempty"`
	Default    any             `json:"default,omitempty"`
	Index      bool            `json:"index,omitempty"`
	Pattern    string          `json:"pattern,omitempty"`
	MaxLength  int             `json:"max_length,omitempty"`
	Min        *float64        `json:"min,omitempty"`
	Max        *float64        `json:"max,omitempty"`
	Auto       bool            `json:"auto,omitempty"`
	Options    []string        `json:"options,omitempty"`
	Formula    string          `json:"formula,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
	PrimaryKey bool            `json:"primary_key,omitempty"`
	ReadOnly   bool            `json:"read_only,omitempty"`
}

// TableCreateRequest creates either a table or a view.
type TableCreateRequest struct {
	Type     string             `json:"type"`
	Name     string             `json:"name"`
	Metadata json.RawMessage    `json:"metadata,omitempty"`
	Columns  []ColumnDefinition `json:"columns,omitempty"`
	SQL      string             `json:"sql,omitempty"`
	// Meta is the audit-meta passthrough. The wire key is `_meta` so it
	// cannot collide with a user-defined column named `meta`.
	Meta json.RawMessage `json:"_meta,omitempty"`
}

// IndexCreateRequest defines a table index creation.
type IndexCreateRequest struct {
	Type    string          `json:"type"`
	Name    string          `json:"name,omitempty"`
	Columns []string        `json:"columns"`
	Meta    json.RawMessage `json:"_meta,omitempty"`
}

// TableUpdateRequest updates table schema or view SQL.
type TableUpdateRequest struct {
	Rename        string             `json:"rename,omitempty"`
	AddColumns    []ColumnDefinition `json:"add_columns,omitempty"`
	DropColumns   []string           `json:"drop_columns,omitempty"`
	RenameColumns map[string]string  `json:"rename_columns,omitempty"`
	SQL           string             `json:"sql,omitempty"`
	Metadata      json.RawMessage    `json:"metadata,omitempty"`
	Meta          json.RawMessage    `json:"_meta,omitempty"`
}

// ListMeta is the metadata payload for list responses.
type ListMeta struct {
	TotalCount  int `json:"total_count"`
	FilterCount int `json:"filter_count"`
	Limit       int `json:"limit"`
	Offset      int `json:"offset"`
}

// ListResponse wraps data plus list metadata.
type ListResponse[T any] struct {
	Data []T      `json:"data"`
	Meta ListMeta `json:"meta"`
}

// SSEEvent represents a change-stream event.
type SSEEvent struct {
	Namespace    string          `json:"namespace,omitempty"`
	Table        string          `json:"table"`
	Action       string          `json:"action"`
	SourceTable  string          `json:"source_table,omitempty"`
	SourceAction string          `json:"source_action,omitempty"`
	Row          map[string]any  `json:"row,omitempty"`
	RowCount     int             `json:"row_count,omitempty"`
	RowIDs       []any           `json:"row_ids,omitempty"`
	Detail       map[string]any  `json:"detail,omitempty"`
	Meta         json.RawMessage `json:"_meta,omitempty"`
	Timestamp    string          `json:"timestamp"`
}
