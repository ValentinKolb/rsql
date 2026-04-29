import { describe, expect, test } from "bun:test";
import { createRsqlClient, type RsqlResult, type SSEEvent } from "../src";
import { startServer, type RunningServer } from "./helpers/server";

describe("rsql client e2e", () => {
  test("namespace + schema + row + query lifecycle", async () => {
    await withServer(async (running) => {
      const client = createRsqlClient({ url: running.url, token: running.token });
      const ns = `e2e_main_${Date.now()}`;

      await expectOk(client.namespaces.create({
        name: ns,
        config: {
          journal_mode: "wal",
          busy_timeout: 5000,
          query_timeout: 10000,
          foreign_keys: true,
          read_only: false,
        },
      }));

      const nsList = await expectOk(client.namespaces.list());
      expect(nsList.some((entry) => entry.name === ns)).toBe(true);

      await expectOk(
        client.namespaces.update(ns, {
          journal_mode: "wal",
          busy_timeout: 7000,
          query_timeout: 12000,
          foreign_keys: true,
          read_only: false,
        }),
      );

      const workspace = client.ns(ns);

      await expectOk(
        workspace.tables.create({
          type: "table",
          name: "customers",
          columns: [
            { name: "name", type: "text", not_null: true },
            { name: "email", type: "text", unique: true },
            { name: "status", type: "select", options: ["active", "inactive"] },
            { name: "score", type: "real", min: 0 },
          ],
        }),
      );

      const customers = workspace.table<{ id?: number; name: string; email: string; status: string; score: number }>("customers");

      await expectOk(customers.indexes.create({ type: "index", columns: ["name"] }));

      const insertedSingle = await expectOk(
        customers.rows.insert(
          { name: "Alice", email: "alice@example.com", status: "active", score: 10 },
          { prefer: "return=representation" },
        ),
      );
      expect("data" in insertedSingle).toBe(true);

      await expectOk(
        customers.rows.insert(
          [
            { name: "Bob", email: "bob@example.com", status: "inactive", score: 5 },
            { name: "Cara", email: "cara@example.com", status: "active", score: 8 },
          ],
          { meta: { user_id: "e2e" } },
        ),
      );

      const listed = await expectOk(customers.rows.list({ status: "eq.active", limit: 10, offset: 0 }));
      if ("meta" in listed) {
        expect(listed.meta.total_count).toBeGreaterThanOrEqual(3);
        expect(listed.meta.filter_count).toBeGreaterThanOrEqual(2);
      }

      const first = await expectOk(customers.rows.get(1));
      expect(typeof first.name).toBe("string");

      await expectOk(
        customers.rows.update(1, { score: { $increment: 2 } }, { prefer: "return=representation", meta: { source: "e2e" } }),
      );

      await expectOk(customers.rows.bulkUpdate({ status: "eq.active" }, { status: "inactive" }, { meta: { bulk: true } }));

      const deleted = await expectOk(
        customers.rows.bulkDelete({ status: "eq.inactive" }, { prefer: "return=representation", meta: { bulk: true } }),
      );
      if (deleted && typeof deleted === "object" && "data" in deleted) {
        expect(deleted.data.length).toBeGreaterThan(0);
      }

      await expectOk(
        workspace.tables.create({
          type: "view",
          name: "v_customers",
          sql: "SELECT name, email FROM customers",
        }),
      );

      const view = workspace.table("v_customers");
      const viewInsert = await view.rows.insert({ name: "X", email: "x@example.com" } as Record<string, unknown>);
      expect(viewInsert.ok).toBe(false);
      if (!viewInsert.ok) {
        expect(viewInsert.status).toBe(405);
      }

      const q = (await expectOk(workspace.query.run({ sql: "SELECT COUNT(*) AS c FROM customers", params: [] }))) as {
        data?: unknown[];
      };
      expect(Array.isArray(q.data)).toBe(true);

      const batch = (await expectOk(
        workspace.query.batch([
          { sql: "SELECT 1 AS a", params: [] },
          { sql: "SELECT COUNT(*) AS c FROM customers", params: [] },
        ]),
      )) as {
        results?: unknown[];
      };
      expect(Array.isArray(batch.results)).toBe(true);

      const changelog = await expectOk(workspace.changelog.list({ limit: 50, offset: 0 }));
      expect(changelog.length).toBeGreaterThan(0);

      const stats = await expectOk(workspace.stats.get());
      expect(typeof stats.table_count).toBe("number");

      await expectOk(view.schema.delete({ cleanup: true }));
      await expectOk(customers.indexes.delete("idx_customers_name"));
      await expectOk(customers.schema.delete({ cleanup: true }));
      await expectOk(client.namespaces.delete(ns));
    });
  }, 60_000);

  test("duplicate + export", async () => {
    await withServer(async (running) => {
      const client = createRsqlClient({ url: running.url, token: running.token });
      const source = `e2e_src_${Date.now()}`;
      const copy = `e2e_copy_${Date.now()}`;

      await expectOk(client.namespaces.create({ name: source }));
      const src = client.ns(source);

      await expectOk(
        src.tables.create({
          type: "table",
          name: "items",
          columns: [
            { name: "name", type: "text", not_null: true },
            { name: "state", type: "select", options: ["open", "done"] },
          ],
        }),
      );

      const items = src.table<{ id?: number; name: string; state: string }>("items");
      await expectOk(items.rows.insert({ name: "One", state: "open" }));

      await expectOk(client.namespaces.duplicate(source, copy));

      const exported = await expectOk(client.namespaces.exportDb(source));
      expect(exported.byteLength).toBeGreaterThan(0);

      const copiedRows = await expectOk(
        client
          .ns(copy)
          .table<{ id?: number; name: string; state: string }>("items")
          .rows.list({ limit: 100, offset: 0 }),
      );
      if ("meta" in copiedRows) {
        expect(copiedRows.meta.total_count).toBeGreaterThanOrEqual(1);
      }

      await expectOk(client.namespaces.delete(copy));
      await expectOk(client.namespaces.delete(source));
    });
  }, 60_000);

  test("csv import into fresh namespace", async () => {
    await withServer(async (running) => {
      const client = createRsqlClient({ url: running.url, token: running.token });
      const ns = `e2e_csv_${Date.now()}`;

      await expectOk(client.namespaces.create({ name: ns }));
      const db = client.ns(ns);
      await expectOk(
        db.tables.create({
          type: "table",
          name: "items",
          columns: [
            { name: "name", type: "text", not_null: true },
            { name: "state", type: "select", options: ["open", "done"] },
          ],
        }),
      );
      await expectOk(
        client.namespaces.importCsv(ns, "items", {
          filename: "items.csv",
          content: "name,state\nCSV Item,open\n",
          contentType: "text/csv",
        }),
      );

      const rows = await expectOk(
        db.table<{ id?: number; name: string; state: string }>("items").rows.list({ limit: 100, offset: 0 }),
      );
      if ("meta" in rows) {
        expect(rows.meta.total_count).toBeGreaterThanOrEqual(1);
      }

      await expectOk(client.namespaces.delete(ns));
    });
  }, 60_000);

  test("sse stream delivers row events", async () => {
    await withServer(async (running) => {
      const client = createRsqlClient({ url: running.url, token: running.token });
      const ns = `e2e_sse_${Date.now()}`;

      await expectOk(client.namespaces.create({ name: ns }));
      const workspace = client.ns(ns);

      await expectOk(
        workspace.tables.create({
          type: "table",
          name: "events",
          columns: [{ name: "name", type: "text" }],
        }),
      );

      const subRes = await workspace.events.subscribe({ tables: ["events"] });
      expect(subRes.ok).toBe(true);
      if (!subRes.ok) {
        throw new Error("failed to subscribe");
      }

      const events = workspace.table<{ id?: number; name: string }>("events");
      await expectOk(events.rows.insert({ name: "hello" }));

      const event = await nextEvent(subRes.data.stream, 4000);
      expect(event.table).toBe("events");
      expect(event.action).toBe("insert");

      subRes.data.close();
      await expectOk(client.namespaces.delete(ns));
    });
  }, 60_000);

  test("csv export with full filter parity", async () => {
    await withServer(async (running) => {
      const client = createRsqlClient({ url: running.url, token: running.token });
      const ns = `e2e_csv_${Date.now()}`;

      await expectOk(
        client.namespaces.create({
          name: ns,
          config: {
            journal_mode: "wal",
            busy_timeout: 5000,
            query_timeout: 10000,
            foreign_keys: true,
            read_only: false,
          },
        }),
      );
      const workspace = client.ns(ns);
      await expectOk(
        workspace.tables.create({
          type: "table",
          name: "items",
          columns: [
            { name: "label", type: "text" },
            { name: "score", type: "integer" },
            { name: "status", type: "select", options: ["a", "b", "c"] },
            { name: "active", type: "boolean" },
          ],
        }),
      );
      await expectOk(
        workspace.table("items").rows.insert([
          { label: "Alpha", score: 10, status: "a", active: true },
          { label: "Bravo", score: 50, status: "b", active: false },
          { label: "Charlie, Junior", score: 90, status: "a", active: true },
          { label: 'has "quote"', score: 5, status: "c", active: false },
        ]),
      );

      // Basic full export — every row, every column.
      const fullRes = await workspace.table("items").export({ format: "csv" });
      if (!fullRes.ok) throw new Error(`export failed: ${fullRes.error.message}`);
      expect(fullRes.data.headers.get("content-type")).toBe("text/csv; charset=utf-8");
      expect(fullRes.data.headers.get("content-disposition")).toContain('filename="items.csv"');
      const fullText = await fullRes.data.text();
      const fullLines = fullText.trim().split("\n");
      expect(fullLines.length).toBe(5); // 1 header + 4 rows

      // Filtered export with the same filter grammar as rows.list.
      const filtered = await workspace.table("items").export({
        format: "csv",
        query: { status: "eq.a", select: "label,score", order: "score.desc" },
      });
      if (!filtered.ok) throw new Error(`filtered export failed: ${filtered.error.message}`);
      const filteredText = await filtered.data.text();
      const filteredLines = filteredText.trim().split("\n");
      expect(filteredLines).toEqual(["label,score", `"Charlie, Junior",90`, "Alpha,10"]);

      // Special characters round-trip cleanly through RFC-4180 quoting.
      const specialRes = await workspace.table("items").export({
        format: "csv",
        query: { select: "label", order: "id.asc" },
      });
      if (!specialRes.ok) throw new Error(`special export failed: ${specialRes.error.message}`);
      const specialText = await specialRes.data.text();
      // The "Charlie, Junior" cell must be quoted; the `"quote"` cell must
      // double its inner quotes.
      expect(specialText).toContain(`"Charlie, Junior"`);
      expect(specialText).toContain(`"has ""quote"""`);

      // BOM prefix when requested.
      const bomRes = await workspace.table("items").export({
        format: "csv",
        query: { select: "label", limit: 1 },
        bom: true,
      });
      if (!bomRes.ok) throw new Error(`bom export failed: ${bomRes.error.message}`);
      const bomBytes = new Uint8Array(await bomRes.data.arrayBuffer());
      expect(bomBytes[0]).toBe(0xef);
      expect(bomBytes[1]).toBe(0xbb);
      expect(bomBytes[2]).toBe(0xbf);

      // Missing format → 400 with structured error body.
      const noFormat = await client.ns(ns).table("items").export({} as never);
      expect(noFormat.ok).toBe(false);
      if (!noFormat.ok) {
        expect(noFormat.status).toBe(400);
      }

      // Aggregate select passes through unchanged.
      const agg = await workspace.table("items").export({
        format: "csv",
        query: { select: "status,count()", order: "status.asc" },
      });
      if (!agg.ok) throw new Error(`agg export failed: ${agg.error.message}`);
      const aggText = await agg.data.text();
      expect(aggText.split("\n")[0]).toBe("status,count");
      expect(aggText).toContain("a,2");
      expect(aggText).toContain("b,1");
      expect(aggText).toContain("c,1");

      await expectOk(client.namespaces.delete(ns));
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

const withServer = async (run: (server: RunningServer) => Promise<void>): Promise<void> => {
  const server = await startServer();
  try {
    await run(server);
  } finally {
    await server.stop();
  }
};
