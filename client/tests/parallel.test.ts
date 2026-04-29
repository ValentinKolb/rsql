import { describe, expect, test } from "bun:test";
import { createRsqlClient, type RsqlResult, type SSEEvent } from "../src";
import { startServer, type RunningServer } from "./helpers/server";

describe("rsql parallel + isolation", () => {
  test("parallel namespaces stay isolated under concurrent writes", async () => {
    await withServer(async (running) => {
      const client = createRsqlClient({ url: running.url, token: running.token });
      const runId = testRunId("iso");
      const namespaceCount = 6;
      const workersPerNamespace = 4;
      const rowsPerWorker = 50;
      const namespaces = Array.from({ length: namespaceCount }, (_, i) => `${runId}_${i}`);

      for (const name of namespaces) {
        await expectOk(client.namespaces.create({ name }));
      }

      await Promise.all(
        namespaces.map((name) =>
          expectOk(
            client.ns(name).tables.create({
              type: "table",
              name: "items",
              columns: [
                { name: "label", type: "text", not_null: true, index: true },
                { name: "worker", type: "integer", not_null: true },
              ],
            }),
          ),
        ),
      );

      await Promise.all(
        namespaces.flatMap((name) => {
          const table = client.ns(name).table<{ id?: number; label: string; worker: number }>("items");
          return Array.from({ length: workersPerNamespace }, (_, worker) => {
            const rows = Array.from({ length: rowsPerWorker }, (_, idx) => ({
              label: `${name}-w${worker}-r${idx}`,
              worker,
            }));
            return expectOk(table.rows.insert(rows));
          });
        }),
      );

      const expected = workersPerNamespace * rowsPerWorker;

      for (const name of namespaces) {
        const db = client.ns(name);
        const countRes = await expectOk(db.query.run({ sql: "SELECT COUNT(*) AS c FROM items", params: [] }));
        const count = extractCount(countRes, "c");
        expect(count).toBe(expected);

        const sample = await expectOk(
          db.table<{ id?: number; label: string; worker: number }>("items").rows.list({ limit: 5, offset: 0 }),
        );
        if ("data" in sample && Array.isArray(sample.data)) {
          for (const row of sample.data) {
            const label = String(row.label ?? "");
            expect(label.startsWith(name)).toBe(true);
          }
        }
      }

      for (const name of namespaces) {
        await expectOk(client.namespaces.delete(name));
      }
    });
  }, 120_000);

  test("single namespace supports concurrent bucketed updates and deletes", async () => {
    await withServer(async (running) => {
      const client = createRsqlClient({ url: running.url, token: running.token });
      const ns = testRunId("cont");

      await expectOk(client.namespaces.create({ name: ns }));
      const db = client.ns(ns);
      await expectOk(
        db.tables.create({
          type: "table",
          name: "jobs",
          columns: [
            { name: "name", type: "text", not_null: true },
            { name: "bucket", type: "integer", not_null: true, index: true },
            { name: "state", type: "select", options: ["pending", "done"], index: true },
          ],
        }),
      );

      const jobs = db.table<{ id?: number; name: string; bucket: number; state: string }>("jobs");
      const bucketCount = 10;
      const rowsPerBucket = 80;

      for (let bucket = 0; bucket < bucketCount; bucket++) {
        const rows = Array.from({ length: rowsPerBucket }, (_, i) => ({
          name: `job-${bucket}-${i}`,
          bucket,
          state: "pending",
        }));
        await expectOk(jobs.rows.insert(rows));
      }

      await Promise.all(
        Array.from({ length: bucketCount }, (_, bucket) =>
          expectOk(jobs.rows.bulkUpdate({ bucket: `eq.${bucket}` }, { state: "done" })),
        ),
      );

      const updatedCountRes = await expectOk(db.query.run({ sql: "SELECT COUNT(*) AS c FROM jobs WHERE state = ?", params: ["done"] }));
      expect(extractCount(updatedCountRes, "c")).toBe(bucketCount * rowsPerBucket);

      const bucketsToDelete = [0, 1, 2, 3, 4];
      await Promise.all(bucketsToDelete.map((bucket) => expectOk(jobs.rows.bulkDelete({ bucket: `eq.${bucket}` }))));

      const remainingRes = await expectOk(db.query.run({ sql: "SELECT COUNT(*) AS c FROM jobs", params: [] }));
      expect(extractCount(remainingRes, "c")).toBe((bucketCount - bucketsToDelete.length) * rowsPerBucket);

      await expectOk(client.namespaces.delete(ns));
    });
  }, 120_000);

  test("sse subscriptions are isolated across namespaces", async () => {
    await withServer(async (running) => {
      const client = createRsqlClient({ url: running.url, token: running.token });
      const nsA = testRunId("ssea");
      const nsB = testRunId("sseb");

      await expectOk(client.namespaces.create({ name: nsA }));
      await expectOk(client.namespaces.create({ name: nsB }));

      const dbA = client.ns(nsA);
      const dbB = client.ns(nsB);

      await expectOk(
        dbA.tables.create({
          type: "table",
          name: "events",
          columns: [{ name: "name", type: "text", not_null: true }],
        }),
      );
      await expectOk(
        dbB.tables.create({
          type: "table",
          name: "events",
          columns: [{ name: "name", type: "text", not_null: true }],
        }),
      );

      const subA = await dbA.events.subscribe({ tables: ["events"] });
      const subB = await dbB.events.subscribe({ tables: ["events"] });
      if (!subA.ok || !subB.ok) {
        throw new Error("failed to subscribe for SSE isolation test");
      }

      await Promise.all([
        expectOk(dbA.table<{ id?: number; name: string }>("events").rows.insert({ name: "A1" })),
        expectOk(dbB.table<{ id?: number; name: string }>("events").rows.insert({ name: "B1" })),
      ]);

      const [evA, evB] = await Promise.all([nextEvent(subA.data.stream, 4000), nextEvent(subB.data.stream, 4000)]);

      expect(evA.namespace).toBe(nsA);
      expect(evA.table).toBe("events");
      expect(evA.action).toBe("insert");

      expect(evB.namespace).toBe(nsB);
      expect(evB.table).toBe("events");
      expect(evB.action).toBe("insert");

      subA.data.close();
      subB.data.close();

      await expectOk(client.namespaces.delete(nsA));
      await expectOk(client.namespaces.delete(nsB));
    });
  }, 120_000);

  test("unauthorized token is rejected deterministically", async () => {
    await withServer(async (running) => {
      const bad = createRsqlClient({ url: running.url, token: "wrong-token" });
      const res = await bad.namespaces.list();
      expect(res.ok).toBe(false);
      if (!res.ok) {
        expect(res.status).toBe(401);
        expect(res.error.error).toBe("unauthorized");
      }
    });
  }, 60_000);
});

async function expectOk<T>(resultPromise: Promise<RsqlResult<T>>): Promise<T> {
  const result = await resultPromise;
  if (!result.ok) {
    throw new Error(`request failed: ${result.status} ${result.error.error} ${result.error.message}`);
  }
  return result.data;
}

const nextEvent = async (stream: AsyncIterable<SSEEvent>, timeoutMs: number): Promise<SSEEvent> => {
  return new Promise<SSEEvent>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`timed out waiting for SSE event after ${timeoutMs}ms`));
    }, timeoutMs);

    (async () => {
      try {
        for await (const event of stream) {
          clearTimeout(timer);
          resolve(event);
          return;
        }
        clearTimeout(timer);
        reject(new Error("SSE stream ended without event"));
      } catch (err) {
        clearTimeout(timer);
        reject(err);
      }
    })();
  });
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

const withServer = async (run: (server: RunningServer) => Promise<void>): Promise<void> => {
  const server = await startServer();
  try {
    await run(server);
  } finally {
    await server.stop();
  }
};

const testRunId = (prefix: string): string => {
  const rand = Math.random().toString(36).slice(2, 10);
  return `${prefix}_${Date.now()}_${rand}`;
};
