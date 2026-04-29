# rsql

A multi-tenant SQLite HTTP server written in Go.

Each namespace is one isolated SQLite database file, exposed over a REST API for schema operations, row CRUD, read-only SQL queries, SSE change streams, and import/export.

## Features

- Namespace lifecycle: create, list, update config, delete, duplicate
- Schema: tables and views, indexes including FTS5
- Rows: list, get, insert, update, delete, bulk update/delete
- Filter grammar on row lists (`status=eq.active`, `or=(...)`, `like`, `is.null`, …)
- Read-only SQL endpoint (`SELECT` / `WITH`, single or batch)
- SSE change stream per namespace
- Import / export as `.db` files; CSV import per table
- Bearer-token auth, Prometheus metrics, optional pprof

## Run

```bash
go run ./cmd/rsql serve \
  --listen=127.0.0.1:8080 \
  --api-token=dev-token \
  --data-dir=./data
```

```bash
curl -s http://127.0.0.1:8080/healthz

curl -s -X POST http://127.0.0.1:8080/v1/namespaces \
  -H 'Authorization: Bearer dev-token' \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo"}'
```

Configuration is read from CLI flags, then `RSQL_*` environment variables, then defaults. Print effective config:

```bash
go run ./cmd/rsql config print --format=json --api-token=dev-token
```

## TypeScript Client

The repo includes a Bun/TypeScript client in [`client/`](./client). It is not published to npm; use it as a workspace dependency or copy `client/src/` into your project.

```ts
import { createRsqlClient } from "./client/src";

const db = createRsqlClient({
  url: "http://127.0.0.1:8080",
  token: "dev-token",
});

await db.namespaces.list();
```

## Documentation

- [Quickstart](./docs/quickstart.md)
- [API Overview](./docs/api-overview.md)
- [Development Guide](./docs/development.md)
- [Client](./client/README.md)
- [Performance Framework](./client/perf/README.md)
- [Security](./SECURITY.md)

## Status

Pre-1.0. The HTTP API is stabilising; breaking changes between minor versions remain possible until v1.0.

## License

MIT — see [LICENSE](./LICENSE).
