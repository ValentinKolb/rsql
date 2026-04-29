import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createRsqlClient } from "../src";
import { startServer, type RunningServer } from "./helpers/server";

let server: RunningServer;

beforeAll(async () => {
  server = await startServer();
});

afterAll(async () => {
  await server.stop();
});

describe("rsql client performance", () => {
  test("bulk insert and read workload", async () => {
    const client = createRsqlClient({ url: server.url, token: server.token });
    const ns = testRunId("perf");

    await ok(client.namespaces.create({ name: ns }));
    const db = client.ns(ns);

    await ok(
      db.tables.create({
        type: "table",
        name: "metrics",
        columns: [
          { name: "name", type: "text", not_null: true, index: true },
          { name: "value", type: "real" },
          { name: "state", type: "select", options: ["hot", "cold"] },
        ],
      }),
    );

    const table = db.table<{ id?: number; name: string; value: number; state: string }>("metrics");

    const totalRows = 2000;
    const batchSize = 200;

    const tWriteStart = performance.now();
    for (let i = 0; i < totalRows; i += batchSize) {
      const rows = Array.from({ length: batchSize }, (_, idx) => {
        const n = i + idx;
        return {
          name: `row-${n}`,
          value: n % 100,
          state: n % 2 === 0 ? "hot" : "cold",
        };
      });
      await ok(table.rows.insert(rows));
    }
    const tWriteMs = performance.now() - tWriteStart;

    const listDurations: number[] = [];
    for (let i = 0; i < 120; i++) {
      const start = performance.now();
      await ok(table.rows.list({ state: "eq.hot", limit: 50, offset: (i * 7) % 200 }));
      listDurations.push(performance.now() - start);
    }

    const getDurations: number[] = [];
    for (let i = 0; i < 120; i++) {
      const id = 1 + ((i * 13) % totalRows);
      const start = performance.now();
      await ok(table.rows.get(id));
      getDurations.push(performance.now() - start);
    }

    const queryDurations: number[] = [];
    for (let i = 0; i < 80; i++) {
      const start = performance.now();
      await ok(db.query.run({ sql: "SELECT COUNT(*) AS c FROM metrics WHERE state = ?", params: ["hot"] }));
      queryDurations.push(performance.now() - start);
    }

    const summary = {
      totalRows,
      batchSize,
      writeTotalMs: round(tWriteMs),
      writesPerSecond: round((totalRows / tWriteMs) * 1000),
      listLatencyMs: latencySummary(listDurations),
      getLatencyMs: latencySummary(getDurations),
      queryLatencyMs: latencySummary(queryDurations),
    };

    // Real-world local numbers from this test run.
    console.log("RSQL_PERF_SUMMARY", JSON.stringify(summary));

    expect(summary.writesPerSecond).toBeGreaterThan(0);
    expect(summary.listLatencyMs.p95).toBeGreaterThan(0);
    expect(summary.getLatencyMs.p95).toBeGreaterThan(0);
    expect(summary.queryLatencyMs.p95).toBeGreaterThan(0);

    await ok(client.namespaces.delete(ns));
  }, 120_000);


  test("parallel namespaces throughput and fanout query latency", async () => {
    const client = createRsqlClient({ url: server.url, token: server.token });
    const runId = testRunId("perf_par");

    const namespaceCount = 6;
    const workersPerNamespace = 4;
    const rowsPerWorker = 120;
    const namespaces = Array.from({ length: namespaceCount }, (_, i) => `${runId}_${i}`);

    for (const name of namespaces) {
      await ok(client.namespaces.create({ name }));
    }

    await Promise.all(
      namespaces.map((name) =>
        ok(
          client.ns(name).tables.create({
            type: "table",
            name: "metrics",
            columns: [
              { name: "label", type: "text", not_null: true, index: true },
              { name: "state", type: "select", options: ["hot", "cold"], index: true },
              { name: "worker", type: "integer", not_null: true },
            ],
          }),
        ),
      ),
    );

    const totalRows = namespaceCount * workersPerNamespace * rowsPerWorker;

    const writeStart = performance.now();
    await Promise.all(
      namespaces.flatMap((name) => {
        const table = client.ns(name).table<{ id?: number; label: string; state: string; worker: number }>("metrics");
        return Array.from({ length: workersPerNamespace }, (_, worker) => {
          const rows = Array.from({ length: rowsPerWorker }, (_, idx) => ({
            label: `${name}-${worker}-${idx}`,
            state: idx % 2 === 0 ? "hot" : "cold",
            worker,
          }));
          return ok(table.rows.insert(rows));
        });
      }),
    );
    const writeMs = performance.now() - writeStart;

    const fanoutDurations: number[] = [];
    for (let i = 0; i < 40; i++) {
      const t0 = performance.now();
      await Promise.all(
        namespaces.map((name) =>
          ok(client.ns(name).query.run({ sql: "SELECT COUNT(*) AS c FROM metrics WHERE state = ?", params: ["hot"] })),
        ),
      );
      fanoutDurations.push(performance.now() - t0);
    }

    const summary = {
      namespaces: namespaceCount,
      workersPerNamespace,
      rowsPerWorker,
      totalRows,
      writeTotalMs: round(writeMs),
      writeRowsPerSecond: round((totalRows / writeMs) * 1000),
      fanoutLatencyMs: latencySummary(fanoutDurations),
      approxPerQueryMs: round(latencySummary(fanoutDurations).avg / namespaceCount),
    };

    console.log("RSQL_PARALLEL_PERF_SUMMARY", JSON.stringify(summary));

    expect(summary.writeRowsPerSecond).toBeGreaterThan(0);
    expect(summary.fanoutLatencyMs.p95).toBeGreaterThan(0);

    for (const name of namespaces) {
      await ok(client.namespaces.delete(name));
    }
  }, 120_000);

  test("single namespace high-contention write throughput", async () => {
    const client = createRsqlClient({ url: server.url, token: server.token });
    const ns = testRunId("perf_cont");

    await ok(client.namespaces.create({ name: ns }));
    const db = client.ns(ns);

    await ok(
      db.tables.create({
        type: "table",
        name: "events",
        columns: [
          { name: "worker", type: "integer", not_null: true, index: true },
          { name: "payload", type: "text", not_null: true },
        ],
      }),
    );

    const table = db.table<{ id?: number; worker: number; payload: string }>("events");

    const workers = 12;
    const rowsPerWorker = 180;
    const totalRows = workers * rowsPerWorker;

    const start = performance.now();
    await Promise.all(
      Array.from({ length: workers }, (_, worker) => {
        const rows = Array.from({ length: rowsPerWorker }, (_, idx) => ({
          worker,
          payload: `w${worker}-row${idx}`,
        }));
        return ok(table.rows.insert(rows));
      }),
    );
    const writeMs = performance.now() - start;

    const countRes = await ok(db.query.run({ sql: "SELECT COUNT(*) AS c FROM events", params: [] }));
    const count = extractCount(countRes, "c");

    const summary = {
      workers,
      rowsPerWorker,
      totalRows,
      persistedRows: count,
      writeTotalMs: round(writeMs),
      writeRowsPerSecond: round((totalRows / writeMs) * 1000),
    };

    console.log("RSQL_CONTENTION_PERF_SUMMARY", JSON.stringify(summary));

    expect(count).toBe(totalRows);
    expect(summary.writeRowsPerSecond).toBeGreaterThan(0);

    await ok(client.namespaces.delete(ns));
  }, 120_000);
});

const latencySummary = (samples: number[]) => {
  const sorted = [...samples].sort((a, b) => a - b);
  return {
    min: round(sorted[0] ?? 0),
    p50: round(percentile(sorted, 0.5)),
    p95: round(percentile(sorted, 0.95)),
    max: round(sorted[sorted.length - 1] ?? 0),
    avg: round(sorted.reduce((sum, x) => sum + x, 0) / Math.max(sorted.length, 1)),
  };
};

const percentile = (sorted: number[], p: number): number => {
  if (sorted.length === 0) {
    return 0;
  }
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * p)));
  return sorted[idx];
};



const extractCount = (result: unknown, key: string): number => {
  const rows = (result as { data?: Array<Record<string, unknown>> }).data;
  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error(`missing query rows for count key '${key}'`);
  }
  const raw = rows[0]?.[key];
  if (typeof raw === "number") {
    return raw;
  }
  if (typeof raw === "string") {
    const parsed = Number(raw);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  throw new Error(`cannot parse count key '${key}' from value ${String(raw)}`);
};
const round = (n: number): number => Math.round(n * 100) / 100;
const testRunId = (prefix: string): string => {
  const rand = Math.random().toString(36).slice(2, 10);
  return `${prefix}_${Date.now()}_${rand}`;
};

async function ok<T>(promise: Promise<{ ok: true; data: T } | { ok: false; status: number; error: { error: string; message: string } }>): Promise<T> {
  const result = await promise;
  if (!result.ok) {
    throw new Error(`request failed: ${result.status} ${result.error.error} ${result.error.message}`);
  }
  return result.data;
}
