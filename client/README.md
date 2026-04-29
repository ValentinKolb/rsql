# rsql Client

Type-safe Bun/TypeScript client for `rsql`.

## Install (local workspace)

```bash
cd client
bun install
```

## Usage

```ts
import { rsql, createRsqlClient } from "./src";

const explicit = createRsqlClient({
  url: "http://127.0.0.1:8080",
  token: "secret",
});

const ns = explicit.ns("crm");
await ns.tables.list();

// Lazy default client, reads env on first property access only
// Required env: RSQL_URL, RSQL_API_TOKEN
await rsql.namespaces.list();
```

## Test

```bash
cd client
bun test
```

## Performance Benchmarks

```bash
cd client
bun run perf:fast
bun run perf:deep
bun run perf:report --run=<run-id-or-path>
```

Detailed benchmark docs and scenario list:
`client/perf/README.md`
