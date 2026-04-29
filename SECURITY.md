# Security

## Threat model

`rsql` is designed to run as an internal service behind an authentication and authorization layer (e.g. an application backend that maps end users to namespaces). It does not implement user authentication, role-based access control, or per-row permissions. Authentication is a single bearer token shared between the calling service and `rsql`.

This means:

- `rsql` trusts every authenticated caller equally.
- The calling service is responsible for mapping its users to namespaces and enforcing per-user access rules before forwarding requests.
- Direct exposure of `rsql` to untrusted clients is out of scope.

## Read-only SQL endpoint

The `POST /v1/:ns/query` endpoint accepts free-form SQL but enforces a static, conservative validator:

- Only `SELECT` and `WITH` statements are accepted.
- Multi-statement input (`;`) and SQL comments (`--`, `/* */`) are rejected outright.
- DDL/DML keywords (`INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, `DROP`, `REPLACE`, `PRAGMA`, `ATTACH`, `DETACH`, `VACUUM`, `REINDEX`, `TRUNCATE`) are rejected anywhere in the statement.
- References to internal objects (identifiers prefixed with `_`) are rejected.

The validator is intentionally a static check, not a full SQL parser. It is paired with regression tests covering known bypass primitives. Operators should not rely on this endpoint as the sole boundary against untrusted SQL.

## Generated-column formulas

Generated columns (`column.formula`) are concatenated into SQLite DDL because SQLite does not bind DDL parameters. The formula validator enforces:

- A strict allow-list of characters (alphanumerics, arithmetic and comparison operators, parentheses, commas, dots).
- Rejection of all SQL keywords, semicolons, comments, and quoting.

Regression tests in `internal/store/sqlite/schema_test.go` cover known bypass attempts.

## Operational constraints

### Single-process per data directory

A given `--data-dir` is owned by exactly one running `rsql` process. The server takes no cross-process file lock and does not arbitrate concurrent writers across instances. Running two `rsql` processes against the same directory can corrupt the control registry or namespace databases under load.

For HA / horizontal scaling, run separate data directories per process and shard namespaces at the calling service layer. Backups are plain `.db` file copies, so an offline copy plus restart on a new host is the supported failover.

### Crash recovery

`service.New` runs a single reconcile pass against the on-disk state at startup; see [docs/development.md § Crash recovery](docs/development.md#crash-recovery). The two known lifecycle crash windows (mid-`CreateNamespace`, mid-`DeleteNamespace`) heal on the next boot. SSE event delivery is at-most-once: subscribers that fall behind their channel buffer drop events permanently for that connection.

## Reporting a vulnerability

Please open a [GitHub Security Advisory](https://github.com/ValentinKolb/rsql/security/advisories/new) for any suspected vulnerability. Do not open a public issue. A response is typically provided within seven days.

Please include:

- A reproducible test case or proof of concept.
- The version or commit hash you tested against.
- The expected and observed behavior.
