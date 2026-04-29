# API Overview

Base URL: `http://<host>:<port>/v1`

## Authentication

All `/v1/*` endpoints require bearer token auth:

```http
Authorization: Bearer <token>
```

Public operational endpoints:

- `GET /healthz`
- `GET /metrics`

## Conventions

### Error envelope

```json
{
  "error": "invalid_request",
  "message": "..."
}
```

### `meta` passthrough

Write operations can include an optional `meta` object. It is forwarded to changelog and SSE events.

### `Prefer` header

Supported values:

- `Prefer: return=representation`
- `Prefer: resolution=merge-duplicates`
- `Prefer: resolution=ignore-duplicates`

## Endpoint Groups

### Namespaces

- `POST /v1/namespaces`
- `GET /v1/namespaces`
- `GET /v1/namespaces/:ns`
- `PUT /v1/namespaces/:ns`
- `DELETE /v1/namespaces/:ns`
- `POST /v1/namespaces/:ns/duplicate`
- `GET /v1/namespaces/:ns/export`
- `POST /v1/namespaces/:ns/import` (`multipart/form-data`)
  - DB import: upload `.db`
  - CSV import: upload file and set `?table=<table_name>`

### Schema (tables/views)

- `GET /v1/:ns/tables`
- `POST /v1/:ns/tables`
- `GET /v1/:ns/tables/:table`
- `PUT /v1/:ns/tables/:table`
- `DELETE /v1/:ns/tables/:table`
- `POST /v1/:ns/tables/:table/indexes`
- `DELETE /v1/:ns/tables/:table/indexes/:index`

### Rows

- `GET /v1/:ns/tables/:table/rows`
- `POST /v1/:ns/tables/:table/rows`
- `PATCH /v1/:ns/tables/:table/rows`
- `DELETE /v1/:ns/tables/:table/rows`
- `GET /v1/:ns/tables/:table/rows/:id`
- `PUT /v1/:ns/tables/:table/rows/:id`
- `DELETE /v1/:ns/tables/:table/rows/:id`

### Query / Realtime / Observability

- `POST /v1/:ns/query` (read-only SQL)
- `GET /v1/:ns/subscribe` (SSE, optional `?tables=t1,t2`)
- `GET /v1/:ns/changelog?table=<name>&limit=50&offset=0`
- `GET /v1/:ns/stats`

## Filtering and Pagination

Row list supports:

- `select=*` (default)
- `order=id.asc` (default)
- `limit=100` (default)
- `offset=0` (default)
- `search=<term>` (FTS if available, otherwise LIKE fallback)

Filter operators (`column=<op>.<value>`):

- `eq`, `neq`, `gt`, `gte`, `lt`, `lte`
- `like`, `ilike`
- `in.(a,b,c)`
- `is.null`, `is.true`, `is.false`
- negation: `not.<op>.<value>`

Logical expressions:

- `or=(status.eq.active,priority.gte.3)`
- `and=(status.eq.active,city.ilike.Berlin)`

Example:

```http
GET /v1/demo/tables/contacts/rows?status=eq.active&score=gte.80&order=score.desc&limit=20
```

## Read-only SQL Endpoint

`POST /v1/:ns/query` supports:

- Single statement: `{ "sql": "SELECT ...", "params": [] }`
- Batch mode: `{ "statements": [{"sql":"SELECT ...","params":[]}] }`

Rules:

- Only read-only statements (`SELECT`/`WITH`) are allowed
- Internal objects (prefixed with `_`) are blocked

