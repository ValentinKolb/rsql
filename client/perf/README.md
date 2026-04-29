# rsql Performance Framework

This directory contains the real-world performance benchmark runner for `rsql`.

## Profiles

- `fast`: Manual baseline run with full scenario coverage and a target runtime below 15 minutes.
- `deep`: Full bottleneck run with larger scale and optional pprof capture.

## Commands

```bash
bun run perf:fast
bun run perf:deep
bun run perf:report --run=<run-id-or-path>
```

Optional arguments:

- `--quick=true`: short smoke run for local validation.
- `--scenarios=S1,S2,...`: run a subset of scenarios.

Examples:

```bash
bun run perf:fast --quick=true
bun run perf:deep --scenarios=S5,S8,S11
bun run perf:report --run=2026-03-02T19-00-00-000Z-fast-abc123
```

## Artifacts per run

Generated under `client/perf/runs/<run-id>/`:

- `run-manifest.json`
- `raw-metrics.jsonl`
- `scenario-summary.json`
- `telemetry.json`
- `bottlenecks.md`
- `profiles/*.pprof` (when pprof capture is enabled and triggered)

## Scenario coverage

The runner implements S1-S12:

- Namespace control plane
- Schema lifecycle
- Point reads
- Filter/pagination/search (FTS and LIKE fallback)
- Write collaboration
- Bulk and upsert
- SQL query workloads
- SSE realtime
- Import/export
- Stats/changelog under load
- Multi-tenant noisy-neighbor isolation
- Cold vs warm behavior
