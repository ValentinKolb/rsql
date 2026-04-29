# Development Guide

## Project Layout

- `cmd/rsql`: CLI entrypoint
- `internal/cli`: Cobra commands (`serve`, `version`, `config print`)
- `internal/config`: Viper-based config loading and validation
- `internal/app`: server lifecycle and graceful shutdown
- `internal/httpapi`: HTTP routing and middleware
- `internal/service`: business orchestration
- `internal/store/control`: namespace registry
- `internal/store/sqlite`: SQLite schema/query/row operations
- `internal/namespace`: DB handle lifecycle management
- `internal/sse`: SSE broker
- `internal/observability`: Prometheus metrics
- `client/`: TypeScript/Bun client
- `client/perf/`: performance framework (`fast` / `deep`)

## Local Workflow

### Run server

```bash
go run ./cmd/rsql serve --api-token=dev-token
```

### Print effective config

```bash
go run ./cmd/rsql config print --format=json --api-token=dev-token
```

### Build binary

```bash
go build -o bin/rsql ./cmd/rsql
```

## Testing

### Go tests

```bash
go test ./...
go test -race ./...
```

### Client tests

```bash
cd client
bun test
bunx tsc --noEmit
```

## Performance Benchmarks

```bash
cd client
bun run perf:fast
bun run perf:deep
bun run perf:report --run=<run-id-or-path>
```

Quick smoke run:

```bash
bun run perf:fast --quick=true
```

Performance docs:

- [client/perf/README.md](../client/perf/README.md)

## Configuration Model

Config precedence:

1. CLI flags
2. Environment variables (`RSQL_*`)
3. Defaults

Useful env keys:

- `RSQL_LISTEN`
- `RSQL_API_TOKEN`
- `RSQL_DATA_DIR`
- `RSQL_LOG_LEVEL`
- `RSQL_QUERY_TIMEOUT_MS`
- `RSQL_NAMESPACE_IDLE_TIMEOUT_MS`
- `RSQL_MAX_OPEN_NAMESPACES`
- `RSQL_PPROF_ENABLED`
- `RSQL_PPROF_LISTEN`

## Notes

- All core packages include `doc.go` summaries for package intent.
- Keep service-layer logic in `internal/service`; keep `internal/httpapi` focused on transport and validation.
