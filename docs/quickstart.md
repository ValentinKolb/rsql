# Quickstart

## 1. Start the Server

```bash
go run ./cmd/rsql serve \
  --listen=127.0.0.1:8080 \
  --api-token=dev-token \
  --data-dir=./data
```

Check:

```bash
curl -s http://127.0.0.1:8080/healthz
curl -s http://127.0.0.1:8080/metrics | head
```

## 2. Create Namespace and Table

```bash
# create namespace
curl -s -X POST http://127.0.0.1:8080/v1/namespaces \
  -H 'Authorization: Bearer dev-token' \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo"}'

# create table
curl -s -X POST http://127.0.0.1:8080/v1/demo/tables \
  -H 'Authorization: Bearer dev-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "type":"table",
    "name":"contacts",
    "columns":[
      {"name":"name","type":"text","not_null":true},
      {"name":"email","type":"text","unique":true},
      {"name":"status","type":"select","options":["active","inactive"],"index":true}
    ]
  }'
```

## 3. Insert and Read Rows

```bash
# insert row
curl -s -X POST http://127.0.0.1:8080/v1/demo/tables/contacts/rows \
  -H 'Authorization: Bearer dev-token' \
  -H 'Content-Type: application/json' \
  -d '{"name":"Ada","email":"ada@example.com","status":"active"}'

# list rows with pagination and sorting
curl -s 'http://127.0.0.1:8080/v1/demo/tables/contacts/rows?order=id.desc&limit=20&offset=0' \
  -H 'Authorization: Bearer dev-token'
```

## 4. Run Read-only SQL

```bash
curl -s -X POST http://127.0.0.1:8080/v1/demo/query \
  -H 'Authorization: Bearer dev-token' \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT status, COUNT(*) AS c FROM contacts GROUP BY status"}'
```

## 5. Use the TypeScript Client

```bash
cd client
bun install
```

```ts
import { createRsqlClient } from "./src";

const db = createRsqlClient({
  url: "http://127.0.0.1:8080",
  token: "dev-token",
});

await db.namespaces.create({ name: "demo2" });
await db.ns("demo").table("contacts").rows.list({ status: "eq.active", limit: 10 });
```

## 6. Stop / Cleanup

```bash
# stop server with Ctrl+C
# optional cleanup
rm -rf ./data
```
