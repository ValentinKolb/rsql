import type { RsqlResult, SSEEvent } from "../src";
import {
  buildContactsCSV,
  makeContacts,
  makeLineItems,
  makeMetrics,
  makeOrders,
  makeTickets,
} from "./factory";
import type { ScenarioContext, ScenarioDefinition, ScenarioPhaseContext } from "./types";

const runPhaseLoop = async (
  phaseCtx: ScenarioPhaseContext,
  scenarioId: string,
  scenarioName: string,
  durationMs: number,
  tick: (ctx: ScenarioPhaseContext, seq: number) => Promise<void>,
): Promise<void> => {
  const end = Date.now() + durationMs;
  let seq = 0;
  while (Date.now() < end) {
    await tick(phaseCtx, seq);
    seq++;
  }
  phaseCtx.note(scenarioId, scenarioName, `phase=${phaseCtx.phase} repeat=${phaseCtx.repeat} ticks=${seq}`);
};

const scenarioNamespace = (runId: string, scenarioId: string, suffix: string): string =>
  `${scenarioId.toLowerCase()}_${runId}_${suffix}`.replace(/[^a-zA-Z0-9_-]/g, "_");

const parseInserted = (result: unknown): number => {
  if (typeof result === "object" && result !== null && "inserted" in result) {
    const value = (result as { inserted?: unknown }).inserted;
    if (typeof value === "number") {
      return value;
    }
  }
  if (typeof result === "object" && result !== null && "data" in result) {
    const data = (result as { data?: unknown[] }).data;
    if (Array.isArray(data)) {
      return data.length;
    }
  }
  return 0;
};

const percentile = (values: number[], q: number): number => {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * q)));
  return sorted[idx] ?? 0;
};

const asRows = <T extends object>(rows: T[]): Array<Record<string, unknown>> => {
  return rows.map((row) => ({ ...row }) as Record<string, unknown>);
};

const withNamespace = async <T>(
  ctx: ScenarioPhaseContext,
  scenarioId: string,
  scenarioName: string,
  ns: string,
  run: () => Promise<T>,
): Promise<T> => {
  await ctx.op(scenarioId, scenarioName, "namespace.create", () => ctx.client.namespaces.create({ name: ns }), { namespace: ns });
  try {
    return await run();
  } finally {
    await ctx.op(scenarioId, scenarioName, "namespace.delete", () => ctx.client.namespaces.delete(ns), { namespace: ns });
  }
};

const seedCRM = async (
  ctx: ScenarioPhaseContext,
  scenarioId: string,
  scenarioName: string,
  ns: string,
  rows: number,
): Promise<{ contacts: string[] }> => {
  const db = ctx.client.ns(ns);

  await ctx.op(
    scenarioId,
    scenarioName,
    "tables.create.contacts",
    () =>
      db.tables.create({
        type: "table",
        name: "contacts",
        columns: [
          { name: "external_id", type: "text", unique: true, not_null: true },
          { name: "name", type: "text", not_null: true, index: true },
          { name: "email", type: "text", not_null: true, unique: true },
          { name: "status", type: "select", options: ["active", "inactive"], index: true },
          { name: "city", type: "text", index: true },
          { name: "segment", type: "select", options: ["enterprise", "smb", "startup"], index: true },
          { name: "score", type: "integer", index: true },
          { name: "notes", type: "text" },
        ],
      }),
    { namespace: ns, table: "contacts" },
  );

  await ctx.op(
    scenarioId,
    scenarioName,
    "index.create.contacts_fts",
    () => db.table("contacts").indexes.create({ type: "fts", columns: ["name", "email", "notes"] }),
    { namespace: ns, table: "contacts" },
  );

  const contacts = makeContacts(ctx.random, rows, `${ns}-crm`);
  const extIDs: string[] = [];

  for (let i = 0; i < contacts.length; i += ctx.profile.rows.batchSize) {
    const batch = contacts.slice(i, i + ctx.profile.rows.batchSize);
    batch.forEach((entry) => extIDs.push(entry.external_id));

    await ctx.op(
      scenarioId,
      scenarioName,
      "rows.insert.contacts",
      () => db.table("contacts").rows.insert(asRows(batch)),
      { namespace: ns, table: "contacts" },
    );
  }

  return { contacts: extIDs };
};

const seedOrders = async (
  ctx: ScenarioPhaseContext,
  scenarioId: string,
  scenarioName: string,
  ns: string,
  contactIDs: string[],
  rows: number,
): Promise<void> => {
  const db = ctx.client.ns(ns);

  await ctx.op(
    scenarioId,
    scenarioName,
    "tables.create.orders",
    () =>
      db.tables.create({
        type: "table",
        name: "orders",
        columns: [
          { name: "contact_external_id", type: "text", not_null: true, index: true },
          { name: "total", type: "real", not_null: true },
          { name: "currency", type: "select", options: ["EUR", "USD"], not_null: true },
          { name: "state", type: "select", options: ["draft", "paid", "cancelled"], index: true },
        ],
      }),
    { namespace: ns, table: "orders" },
  );

  const orderRows = makeOrders(ctx.random, contactIDs, rows, ns);
  for (let i = 0; i < orderRows.length; i += ctx.profile.rows.batchSize) {
    const batch = orderRows.slice(i, i + ctx.profile.rows.batchSize);
    await ctx.op(
      scenarioId,
      scenarioName,
      "rows.insert.orders",
      () => db.table("orders").rows.insert(asRows(batch)),
      { namespace: ns, table: "orders" },
    );
  }

  const lineItems = makeLineItems(
    ctx.random,
    orderRows.map((_, i) => `${ns}-ord-${i}`),
    Math.max(2000, Math.floor(rows * 1.5)),
  );

  await ctx.op(
    scenarioId,
    scenarioName,
    "tables.create.line_items",
    () =>
      db.tables.create({
        type: "table",
        name: "line_items",
        columns: [
          { name: "order_id_ref", type: "text", not_null: true, index: true },
          { name: "sku", type: "text", index: true },
          { name: "quantity", type: "integer", not_null: true },
          { name: "price", type: "real", not_null: true },
        ],
      }),
    { namespace: ns, table: "line_items" },
  );

  for (let i = 0; i < lineItems.length; i += ctx.profile.rows.batchSize) {
    const batch = lineItems.slice(i, i + ctx.profile.rows.batchSize);
    await ctx.op(
      scenarioId,
      scenarioName,
      "rows.insert.line_items",
      () => db.table("line_items").rows.insert(asRows(batch)),
      { namespace: ns, table: "line_items" },
    );
  }
};

const seedTicketsAndMetrics = async (
  ctx: ScenarioPhaseContext,
  scenarioId: string,
  scenarioName: string,
  ns: string,
  contactIDs: string[],
): Promise<void> => {
  const db = ctx.client.ns(ns);

  await ctx.op(
    scenarioId,
    scenarioName,
    "tables.create.tickets",
    () =>
      db.tables.create({
        type: "table",
        name: "tickets",
        columns: [
          { name: "contact_external_id", type: "text", not_null: true, index: true },
          { name: "title", type: "text", not_null: true },
          { name: "priority", type: "select", options: ["low", "medium", "high"], index: true },
          { name: "state", type: "select", options: ["open", "in_progress", "closed"], index: true },
        ],
      }),
    { namespace: ns, table: "tickets" },
  );

  await ctx.op(
    scenarioId,
    scenarioName,
    "tables.create.metrics",
    () =>
      db.tables.create({
        type: "table",
        name: "metrics",
        columns: [
          { name: "ts_bucket", type: "datetime", index: true },
          { name: "kind", type: "select", options: ["latency", "errors", "throughput"], index: true },
          { name: "value", type: "real", not_null: true },
          { name: "source", type: "text", index: true },
        ],
      }),
    { namespace: ns, table: "metrics" },
  );

  const tickets = makeTickets(ctx.random, contactIDs, Math.max(5000, Math.floor(contactIDs.length * 0.2)));
  const metrics = makeMetrics(ctx.random, Math.max(8000, Math.floor(contactIDs.length * 0.5)), ns);

  for (let i = 0; i < tickets.length; i += ctx.profile.rows.batchSize) {
    const batch = tickets.slice(i, i + ctx.profile.rows.batchSize);
    await ctx.op(
      scenarioId,
      scenarioName,
      "rows.insert.tickets",
      () => db.table("tickets").rows.insert(asRows(batch)),
      { namespace: ns, table: "tickets" },
    );
  }

  for (let i = 0; i < metrics.length; i += ctx.profile.rows.batchSize) {
    const batch = metrics.slice(i, i + ctx.profile.rows.batchSize);
    await ctx.op(
      scenarioId,
      scenarioName,
      "rows.insert.metrics",
      () => db.table("metrics").rows.insert(asRows(batch)),
      { namespace: ns, table: "metrics" },
    );
  }
};

const nextEvent = async (stream: AsyncIterable<SSEEvent>, timeoutMs: number): Promise<SSEEvent> => {
  return new Promise<SSEEvent>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`timeout waiting for sse event after ${timeoutMs}ms`));
    }, timeoutMs);

    (async () => {
      try {
        for await (const event of stream) {
          clearTimeout(timer);
          resolve(event);
          return;
        }
        clearTimeout(timer);
        reject(new Error("sse stream ended"));
      } catch (err) {
        clearTimeout(timer);
        reject(err);
      }
    })();
  });
};

const scenarioS1: ScenarioDefinition = {
  id: "S1",
  name: "Namespace Control Plane",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      const warm = ctx.withPhase("warmup", repeat);
      const measure = ctx.withPhase("measure", repeat);

      await runPhaseLoop(warm, this.id, this.name, ctx.profile.timing.warmupMs, async (phaseCtx, seq) => {
        const ns = scenarioNamespace(ctx.runId, this.id, `w_${repeat}_${seq}`);
        await withNamespace(phaseCtx, this.id, this.name, ns, async () => {
          await phaseCtx.op(
            this.id,
            this.name,
            "tables.create",
            () =>
              phaseCtx.client.ns(ns).tables.create({
                type: "table",
                name: "items",
                columns: [{ name: "name", type: "text", not_null: true }],
              }),
            { namespace: ns, table: "items" },
          );
          await phaseCtx.op(
            this.id,
            this.name,
            "rows.insert",
            () => phaseCtx.client.ns(ns).table("items").rows.insert({ name: `item-${seq}` }),
            { namespace: ns, table: "items" },
          );

          const copy = `${ns}_copy`;
          await phaseCtx.op(this.id, this.name, "namespace.duplicate", () => phaseCtx.client.namespaces.duplicate(ns, copy), { namespace: ns });
          await phaseCtx.op(this.id, this.name, "namespace.list", () => phaseCtx.client.namespaces.list());
          await phaseCtx.op(this.id, this.name, "namespace.get", () => phaseCtx.client.namespaces.get(ns), { namespace: ns });
          await phaseCtx.op(
            this.id,
            this.name,
            "namespace.update",
            () =>
              phaseCtx.client.namespaces.update(ns, {
                journal_mode: "wal",
                busy_timeout: 5000,
                query_timeout: 10000,
                foreign_keys: true,
                read_only: false,
              }),
            { namespace: ns },
          );
          await phaseCtx.op(this.id, this.name, "namespace.delete.copy", () => phaseCtx.client.namespaces.delete(copy), { namespace: copy });
        });
      });

      await runPhaseLoop(measure, this.id, this.name, ctx.profile.timing.measureMs, async (phaseCtx, seq) => {
        const ns = scenarioNamespace(ctx.runId, this.id, `m_${repeat}_${seq}`);
        await withNamespace(phaseCtx, this.id, this.name, ns, async () => {
          const copy = `${ns}_copy`;
          await phaseCtx.op(this.id, this.name, "namespace.duplicate", () => phaseCtx.client.namespaces.duplicate(ns, copy), { namespace: ns });
          await phaseCtx.op(this.id, this.name, "namespace.list", () => phaseCtx.client.namespaces.list());
          await phaseCtx.op(this.id, this.name, "namespace.get", () => phaseCtx.client.namespaces.get(ns), { namespace: ns });
          await phaseCtx.op(this.id, this.name, "namespace.delete.copy", () => phaseCtx.client.namespaces.delete(copy), { namespace: copy });
        });
      });
    }
  },
};

const scenarioS2: ScenarioDefinition = {
  id: "S2",
  name: "Schema Lifecycle",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      const phaseCtx = ctx.withPhase("setup", repeat);
      const ns = scenarioNamespace(ctx.runId, this.id, `repeat_${repeat}`);
      await withNamespace(phaseCtx, this.id, this.name, ns, async () => {
        const warm = ctx.withPhase("warmup", repeat);
        const measure = ctx.withPhase("measure", repeat);

        await runPhaseLoop(warm, this.id, this.name, ctx.profile.timing.warmupMs, async (pc, seq) => {
          const t = `tbl_w_${seq}`;
          await pc.op(
            this.id,
            this.name,
            "tables.create.table",
            () =>
              pc.client.ns(ns).tables.create({
                type: "table",
                name: t,
                columns: [
                  { name: "name", type: "text", not_null: true },
                  { name: "email", type: "text" },
                  { name: "notes", type: "text" },
                ],
              }),
            { namespace: ns, table: t },
          );
          await pc.op(this.id, this.name, "indexes.create.fts", () => pc.client.ns(ns).table(t).indexes.create({ type: "fts", columns: ["name", "notes"] }), {
            namespace: ns,
            table: t,
          });
          await pc.op(
            this.id,
            this.name,
            "tables.update",
            () => pc.client.ns(ns).tables.update(t, { add_columns: [{ name: "segment", type: "text" }] }),
            { namespace: ns, table: t },
          );
          const view = `v_${t}`;
          await pc.op(this.id, this.name, "tables.create.view", () => pc.client.ns(ns).tables.create({ type: "view", name: view, sql: `SELECT name,email FROM ${t}` }), {
            namespace: ns,
            table: view,
          });
          await pc.op(this.id, this.name, "tables.delete.view", () => pc.client.ns(ns).tables.delete(view), { namespace: ns, table: view });
          await pc.op(this.id, this.name, "tables.delete.table", () => pc.client.ns(ns).tables.delete(t), { namespace: ns, table: t });
        });

        await runPhaseLoop(measure, this.id, this.name, ctx.profile.timing.measureMs, async (pc, seq) => {
          const t = `tbl_m_${seq}`;
          await pc.op(
            this.id,
            this.name,
            "tables.create.table",
            () =>
              pc.client.ns(ns).tables.create({
                type: "table",
                name: t,
                columns: [
                  { name: "name", type: "text", not_null: true },
                  { name: "notes", type: "text" },
                ],
              }),
            { namespace: ns, table: t },
          );
          await pc.op(this.id, this.name, "indexes.create", () => pc.client.ns(ns).table(t).indexes.create({ type: "index", columns: ["name"] }), {
            namespace: ns,
            table: t,
          });
          await pc.op(this.id, this.name, "indexes.create.fts", () => pc.client.ns(ns).table(t).indexes.create({ type: "fts", columns: ["name", "notes"] }), {
            namespace: ns,
            table: t,
          });
          await pc.op(this.id, this.name, "tables.delete.table", () => pc.client.ns(ns).tables.delete(t), { namespace: ns, table: t });
        });
      });
    }
  },
};

const scenarioS3: ScenarioDefinition = {
  id: "S3",
  name: "Point Reads",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      const setup = ctx.withPhase("setup", repeat);
      const ns = scenarioNamespace(ctx.runId, this.id, `repeat_${repeat}`);

      await withNamespace(setup, this.id, this.name, ns, async () => {
        const rows = Math.max(2000, ctx.profile.rows.basePerNamespace);
        await seedCRM(setup, this.id, this.name, ns, rows);

        const warm = ctx.withPhase("warmup", repeat);
        const measure = ctx.withPhase("measure", repeat);

        await runPhaseLoop(warm, this.id, this.name, ctx.profile.timing.warmupMs, async (pc) => {
          const id = pc.random.int(1, rows);
          await pc.op(this.id, this.name, "rows.get.random", () => pc.client.ns(ns).table("contacts").rows.get(id), { namespace: ns, table: "contacts" });
        });

        await runPhaseLoop(measure, this.id, this.name, ctx.profile.timing.measureMs, async (pc) => {
          const hot = Math.max(100, Math.floor(rows * 0.01));
          const hotId = 1 + pc.random.zipfIndex(hot, 1.2);
          const randomID = pc.random.int(1, rows);

          await pc.op(this.id, this.name, "rows.get.hot", () => pc.client.ns(ns).table("contacts").rows.get(hotId), { namespace: ns, table: "contacts" });
          await pc.op(this.id, this.name, "rows.get.random", () => pc.client.ns(ns).table("contacts").rows.get(randomID), { namespace: ns, table: "contacts" });
        });
      });
    }
  },
};

const scenarioS4: ScenarioDefinition = {
  id: "S4",
  name: "Filter Pagination Search",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      const setup = ctx.withPhase("setup", repeat);
      const ns = scenarioNamespace(ctx.runId, this.id, `repeat_${repeat}`);

      await withNamespace(setup, this.id, this.name, ns, async () => {
        const rows = Math.max(2000, Math.floor(ctx.profile.rows.basePerNamespace * 0.75));
        await seedCRM(setup, this.id, this.name, ns, rows);

        await setup.op(
          this.id,
          this.name,
          "tables.create.contacts_like",
          () =>
            setup.client.ns(ns).tables.create({
              type: "table",
              name: "contacts_like",
              columns: [
                { name: "name", type: "text", not_null: true },
                { name: "notes", type: "text" },
              ],
            }),
          { namespace: ns, table: "contacts_like" },
        );

        const sampleLike = makeContacts(setup.random, Math.max(2000, Math.floor(rows * 0.2)), `${ns}-like`).map((entry) => ({
          name: entry.name,
          notes: entry.notes,
        }));

        for (let i = 0; i < sampleLike.length; i += setup.profile.rows.batchSize) {
          await setup.op(this.id, this.name, "rows.insert.contacts_like", () => setup.client.ns(ns).table("contacts_like").rows.insert(sampleLike.slice(i, i + setup.profile.rows.batchSize)), {
            namespace: ns,
            table: "contacts_like",
          });
        }

        const warm = ctx.withPhase("warmup", repeat);
        const measure = ctx.withPhase("measure", repeat);

        await runPhaseLoop(warm, this.id, this.name, ctx.profile.timing.warmupMs, async (pc, seq) => {
          const offset = (seq * 31) % 500;
          await pc.op(this.id, this.name, "rows.list.filtered", () => pc.client.ns(ns).table("contacts").rows.list({ status: "eq.active", limit: 100, offset, order: "score.desc" }), {
            namespace: ns,
            table: "contacts",
          });
          await pc.op(this.id, this.name, "rows.list.search.fts", () => pc.client.ns(ns).table("contacts").rows.list({ search: "enterprise", limit: 50, offset: 0 }), {
            namespace: ns,
            table: "contacts",
          });
        });

        await runPhaseLoop(measure, this.id, this.name, ctx.profile.timing.measureMs, async (pc, seq) => {
          const offset = (seq * 17) % 1000;
          await pc.op(this.id, this.name, "rows.list.filtered", () => pc.client.ns(ns).table("contacts").rows.list({
            status: "eq.active",
            segment: "eq.enterprise",
            city: "eq.Berlin",
            limit: 100,
            offset,
            order: "score.desc",
          }), { namespace: ns, table: "contacts" });

          await pc.op(this.id, this.name, "rows.list.search.fts", () => pc.client.ns(ns).table("contacts").rows.list({ search: "spreadsheet", limit: 50, offset: 0 }), {
            namespace: ns,
            table: "contacts",
          });

          await pc.op(this.id, this.name, "rows.list.search.like_fallback", () => pc.client.ns(ns).table("contacts_like").rows.list({ search: "support", limit: 50, offset: 0 }), {
            namespace: ns,
            table: "contacts_like",
          });
        });
      });
    }
  },
};

const scenarioS5: ScenarioDefinition = {
  id: "S5",
  name: "Write Collaboration",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      const setup = ctx.withPhase("setup", repeat);
      const ns = scenarioNamespace(ctx.runId, this.id, `repeat_${repeat}`);
      await withNamespace(setup, this.id, this.name, ns, async () => {
        await setup.op(
          this.id,
          this.name,
          "tables.create.events",
          () =>
            setup.client.ns(ns).tables.create({
              type: "table",
              name: "events",
              columns: [
                { name: "worker", type: "integer", index: true },
                { name: "state", type: "select", options: ["new", "done"], index: true },
                { name: "payload", type: "text" },
              ],
            }),
          { namespace: ns, table: "events" },
        );

        const warm = ctx.withPhase("warmup", repeat);
        const measure = ctx.withPhase("measure", repeat);

        const runBurst = async (pc: ScenarioPhaseContext, label: string): Promise<void> => {
          const concurrency = ctx.profile.noisyNeighbor.writerConcurrency;
          await Promise.all(
            Array.from({ length: concurrency }, async (_, worker) => {
              const inserted = await pc.op(
                this.id,
                this.name,
                `${label}.rows.insert`,
                () =>
                  pc.client
                    .ns(ns)
                    .table("events")
                    .rows.insert(
                      Array.from({ length: 20 }, (_, i) => ({
                        worker,
                        state: "new",
                        payload: `w${worker}-i${i}-r${repeat}`,
                      })),
                    ),
                { namespace: ns, table: "events" },
              );

              const insertedCount = parseInserted(inserted);
              if (insertedCount > 0) {
                await pc.op(this.id, this.name, `${label}.rows.bulk_update`, () => pc.client.ns(ns).table("events").rows.bulkUpdate({ worker: `eq.${worker}` }, { state: "done" }), {
                  namespace: ns,
                  table: "events",
                });
                if (pc.random.bool(0.2)) {
                  await pc.op(this.id, this.name, `${label}.rows.bulk_delete`, () => pc.client.ns(ns).table("events").rows.bulkDelete({ worker: `eq.${worker}` }), {
                    namespace: ns,
                    table: "events",
                  });
                }
              }
            }),
          );
        };

        await runPhaseLoop(warm, this.id, this.name, ctx.profile.timing.warmupMs, async (pc) => runBurst(pc, "warmup"));
        await runPhaseLoop(measure, this.id, this.name, ctx.profile.timing.measureMs, async (pc) => runBurst(pc, "measure"));
      });
    }
  },
};

const scenarioS6: ScenarioDefinition = {
  id: "S6",
  name: "Bulk and Upsert",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      const setup = ctx.withPhase("setup", repeat);
      const ns = scenarioNamespace(ctx.runId, this.id, `repeat_${repeat}`);
      await withNamespace(setup, this.id, this.name, ns, async () => {
        await setup.op(
          this.id,
          this.name,
          "tables.create.bulk_items",
          () =>
            setup.client.ns(ns).tables.create({
              type: "table",
              name: "bulk_items",
              columns: [
                { name: "external_id", type: "text", unique: true, not_null: true },
                { name: "state", type: "select", options: ["open", "done"], index: true },
                { name: "value", type: "real" },
              ],
            }),
          { namespace: ns, table: "bulk_items" },
        );

        const seedRows = Array.from({ length: 5000 }, (_, i) => ({
          external_id: `bulk-${i}`,
          state: i % 2 === 0 ? "open" : "done",
          value: i,
        }));

        for (let i = 0; i < seedRows.length; i += setup.profile.rows.batchSize) {
          await setup.op(this.id, this.name, "rows.insert.seed", () => setup.client.ns(ns).table("bulk_items").rows.insert(seedRows.slice(i, i + setup.profile.rows.batchSize)), {
            namespace: ns,
            table: "bulk_items",
          });
        }

        const warm = ctx.withPhase("warmup", repeat);
        const measure = ctx.withPhase("measure", repeat);

        const runOps = async (pc: ScenarioPhaseContext): Promise<void> => {
          const ensureBulkTable = async (): Promise<void> => {
            const exists = await pc.client.ns(ns).tables.get("bulk_items");
            if (exists.ok) {
              return;
            }
            if (exists.status !== 404) {
              throw new Error(`bulk_items lookup failed: ${exists.status} ${exists.error.error}`);
            }

            await pc.op(
              this.id,
              this.name,
              "tables.recreate.bulk_items",
              () =>
                pc.client.ns(ns).tables.create({
                  type: "table",
                  name: "bulk_items",
                  columns: [
                    { name: "external_id", type: "text", unique: true, not_null: true },
                    { name: "state", type: "select", options: ["open", "done"], index: true },
                    { name: "value", type: "real" },
                  ],
                }),
              { namespace: ns, table: "bulk_items" },
            );
          };

          await ensureBulkTable();

          const payload = Array.from({ length: 1000 }, (_, i) => ({
            external_id: `bulk-${pc.random.int(0, 6000)}`,
            state: pc.random.bool(0.5) ? "open" : "done",
            value: pc.random.int(1, 10000),
          }));

          await pc.op(
            this.id,
            this.name,
            "rows.insert.ignore_duplicates",
            async () => {
              let res = await pc.client.ns(ns).table("bulk_items").rows.insert(payload, { prefer: "resolution=ignore-duplicates" });
              if (!res.ok && res.status === 404) {
                await ensureBulkTable();
                res = await pc.client.ns(ns).table("bulk_items").rows.insert(payload, { prefer: "resolution=ignore-duplicates" });
              }
              return res;
            },
            {
              namespace: ns,
              table: "bulk_items",
              expectError: true,
            },
          );

          await pc.op(
            this.id,
            this.name,
            "rows.insert.merge_duplicates",
            async () => {
              let res = await pc.client.ns(ns).table("bulk_items").rows.insert(payload, { prefer: "resolution=merge-duplicates" });
              if (!res.ok && res.status === 404) {
                await ensureBulkTable();
                res = await pc.client.ns(ns).table("bulk_items").rows.insert(payload, { prefer: "resolution=merge-duplicates" });
              }
              return res;
            },
            {
              namespace: ns,
              table: "bulk_items",
              expectError: true,
            },
          );

          await pc.op(this.id, this.name, "rows.bulk_update", () => pc.client.ns(ns).table("bulk_items").rows.bulkUpdate({ state: "eq.open" }, { value: { $increment: 1 } }), {
            namespace: ns,
            table: "bulk_items",
            expectError: true,
          });

          await pc.op(this.id, this.name, "rows.bulk_delete", () => pc.client.ns(ns).table("bulk_items").rows.bulkDelete({ state: "eq.done" }), {
            namespace: ns,
            table: "bulk_items",
            expectError: true,
          });
        };

        await runPhaseLoop(warm, this.id, this.name, ctx.profile.timing.warmupMs, runOps);
        await runPhaseLoop(measure, this.id, this.name, ctx.profile.timing.measureMs, runOps);
      });
    }
  },
};

const scenarioS7: ScenarioDefinition = {
  id: "S7",
  name: "SQL Query Workloads",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      const setup = ctx.withPhase("setup", repeat);
      const ns = scenarioNamespace(ctx.runId, this.id, `repeat_${repeat}`);
      await withNamespace(setup, this.id, this.name, ns, async () => {
        const rows = Math.max(3000, Math.floor(ctx.profile.rows.basePerNamespace * 0.25));
        const { contacts } = await seedCRM(setup, this.id, this.name, ns, rows);
        await seedOrders(setup, this.id, this.name, ns, contacts, rows);

        const warm = ctx.withPhase("warmup", repeat);
        const measure = ctx.withPhase("measure", repeat);

        const runSQL = async (pc: ScenarioPhaseContext): Promise<void> => {
          await pc.op(this.id, this.name, "query.simple_select", () => pc.client.ns(ns).query.run({ sql: "SELECT COUNT(*) AS c FROM contacts WHERE status = ?", params: ["active"] }), {
            namespace: ns,
          });

          await pc.op(
            this.id,
            this.name,
            "query.join_agg",
            () =>
              pc.client.ns(ns).query.run({
                sql: `
                  SELECT c.segment AS segment, COUNT(*) AS order_count, ROUND(AVG(o.total), 2) AS avg_total
                  FROM contacts c
                  JOIN orders o ON o.contact_external_id = c.external_id
                  WHERE o.state = ?
                  GROUP BY c.segment
                  ORDER BY order_count DESC
                `,
                params: ["paid"],
              }),
            { namespace: ns },
          );

          await pc.op(
            this.id,
            this.name,
            "query.batch",
            () =>
              pc.client.ns(ns).query.batch([
                { sql: "SELECT COUNT(*) AS contacts FROM contacts", params: [] },
                { sql: "SELECT COUNT(*) AS orders FROM orders", params: [] },
                { sql: "SELECT COUNT(*) AS open_orders FROM orders WHERE state = ?", params: ["draft"] },
              ]),
            { namespace: ns },
          );
        };

        await runPhaseLoop(warm, this.id, this.name, ctx.profile.timing.warmupMs, runSQL);
        await runPhaseLoop(measure, this.id, this.name, ctx.profile.timing.measureMs, runSQL);
      });
    }
  },
};

const scenarioS8: ScenarioDefinition = {
  id: "S8",
  name: "SSE Realtime",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      for (const subscriberCount of ctx.profile.sse.subscribers) {
        const setup = ctx.withPhase("setup", repeat);
        const ns = scenarioNamespace(ctx.runId, this.id, `repeat_${repeat}_${subscriberCount}`);
        await withNamespace(setup, this.id, this.name, ns, async () => {
          await setup.op(
            this.id,
            this.name,
            "tables.create.events",
            () =>
              setup.client.ns(ns).tables.create({
                type: "table",
                name: "events",
                columns: [
                  { name: "message", type: "text", not_null: true },
                  { name: "topic", type: "text", index: true },
                ],
              }),
            { namespace: ns, table: "events" },
          );

          const subscriptions: Array<{ close: () => void; stream: AsyncIterable<SSEEvent> }> = [];
          for (let i = 0; i < subscriberCount; i++) {
            const sub = await setup.op(this.id, this.name, "events.subscribe", () => setup.client.ns(ns).events.subscribe({ tables: ["events"] }), { namespace: ns });
            subscriptions.push(sub);
          }

          try {
            const warm = ctx.withPhase("warmup", repeat);
            const measure = ctx.withPhase("measure", repeat);

            const runSSEPhase = async (pc: ScenarioPhaseContext, durationMs: number): Promise<void> => {
              const lags: number[] = [];
              let failedSubscribers = 0;

              await runPhaseLoop(pc, this.id, this.name, durationMs, async (phaseCtx) => {
                const writeStarted = performance.now();
                await phaseCtx.op(
                  this.id,
                  this.name,
                  "rows.insert.event",
                  () => phaseCtx.client.ns(ns).table("events").rows.insert({ message: `msg-${Date.now()}`, topic: "perf" }),
                  {
                    namespace: ns,
                    table: "events",
                  },
                );

                const maxWaiters = Math.min(subscriptions.length, 100);
                const waits = subscriptions.slice(0, maxWaiters).map((sub) => nextEvent(sub.stream, 3000));
                const results = await Promise.allSettled(waits);
                lags.push(performance.now() - writeStarted);
                failedSubscribers += results.filter((entry) => entry.status === "rejected").length;
              });

              if (lags.length === 0) {
                return;
              }

              const p50 = percentile(lags, 0.5);
              const p95 = percentile(lags, 0.95);
              const max = Math.max(...lags);
              pc.note(
                this.id,
                this.name,
                `subscriber_count=${subscriberCount} phase=${pc.phase} fanout_lag_ms_p50=${p50.toFixed(2)} fanout_lag_ms_p95=${p95.toFixed(2)} fanout_lag_ms_max=${max.toFixed(2)} failed_subscribers_total=${failedSubscribers} operations=${lags.length}`,
              );
            };

            await runSSEPhase(warm, Math.min(ctx.profile.timing.warmupMs, 5000));
            await runSSEPhase(measure, Math.min(ctx.profile.timing.measureMs, 15_000));
          } finally {
            for (const sub of subscriptions) {
              sub.close();
            }
          }
        });
      }
    }
  },
};

const scenarioS9: ScenarioDefinition = {
  id: "S9",
  name: "Import Export",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      const setup = ctx.withPhase("setup", repeat);
      const source = scenarioNamespace(ctx.runId, this.id, `src_${repeat}`);
      await withNamespace(setup, this.id, this.name, source, async () => {
        await setup.op(
          this.id,
          this.name,
          "tables.create.contacts",
          () =>
            setup.client.ns(source).tables.create({
              type: "table",
              name: "contacts",
              columns: [
                { name: "external_id", type: "text", unique: true },
                { name: "name", type: "text", not_null: true },
                { name: "email", type: "text", not_null: true },
                { name: "status", type: "select", options: ["active", "inactive"] },
                { name: "city", type: "text" },
                { name: "segment", type: "text" },
                { name: "score", type: "text" },
                { name: "notes", type: "text" },
              ],
            }),
          { namespace: source, table: "contacts" },
        );

        const seedRows = Math.max(1500, Math.floor(setup.profile.rows.basePerNamespace * 0.2));
        const seed = makeContacts(setup.random, seedRows, `${source}-exp`).map((row) => ({
          ...row,
          score: String(row.score),
        }));
        for (let i = 0; i < seed.length; i += setup.profile.rows.batchSize) {
          await setup.op(
            this.id,
            this.name,
            "rows.insert.seed",
            () => setup.client.ns(source).table("contacts").rows.insert(asRows(seed.slice(i, i + setup.profile.rows.batchSize))),
            {
              namespace: source,
              table: "contacts",
            },
          );
        }

        const warm = ctx.withPhase("warmup", repeat);
        const measure = ctx.withPhase("measure", repeat);

        const runTransfer = async (pc: ScenarioPhaseContext, seq: number): Promise<void> => {
          const target = scenarioNamespace(ctx.runId, this.id, `tgt_${repeat}_${seq}`);
          await withNamespace(pc, this.id, this.name, target, async () => {
            const exported = await pc.op(this.id, this.name, "namespace.export_db", () => pc.client.namespaces.exportDb(source), { namespace: source });
            await pc.op(
              this.id,
              this.name,
              "namespace.import_db",
              () =>
                pc.client.namespaces.importDb(target, {
                  filename: `${target}.db`,
                  content: new Uint8Array(exported),
                  contentType: "application/octet-stream",
                }),
              { namespace: target },
            );

            const csvRows = Math.max(250, Math.floor(pc.profile.rows.basePerNamespace * 0.05));
            const csvRowsData = makeContacts(pc.random, csvRows, `${target}-csv-${Date.now()}-${seq}`);
            const csv = buildContactsCSV(csvRowsData);
            await pc.op(
              this.id,
              this.name,
              "namespace.import_csv",
              () =>
                pc.client.namespaces.importCsv(source, "contacts", {
                  filename: `contacts-${seq}.csv`,
                  content: csv,
                  contentType: "text/csv",
                }),
              { namespace: source, table: "contacts" },
            );
          });
        };

        await runPhaseLoop(warm, this.id, this.name, Math.min(ctx.profile.timing.warmupMs, 10_000), runTransfer);
        await runPhaseLoop(measure, this.id, this.name, Math.min(ctx.profile.timing.measureMs, 20_000), runTransfer);
      });
    }
  },
};

const scenarioS10: ScenarioDefinition = {
  id: "S10",
  name: "Stats Changelog",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      const setup = ctx.withPhase("setup", repeat);
      const ns = scenarioNamespace(ctx.runId, this.id, `repeat_${repeat}`);
      await withNamespace(setup, this.id, this.name, ns, async () => {
        await setup.op(
          this.id,
          this.name,
          "tables.create.audit",
          () =>
            setup.client.ns(ns).tables.create({
              type: "table",
              name: "audit",
              columns: [
                { name: "actor", type: "text", index: true },
                { name: "event", type: "text", not_null: true },
                { name: "severity", type: "select", options: ["low", "medium", "high"], index: true },
              ],
            }),
          { namespace: ns, table: "audit" },
        );

        const warm = ctx.withPhase("warmup", repeat);
        const measure = ctx.withPhase("measure", repeat);

        const runOps = async (pc: ScenarioPhaseContext, seq: number): Promise<void> => {
          await pc.op(this.id, this.name, "rows.insert.audit", () => pc.client.ns(ns).table("audit").rows.insert({ actor: `user-${seq % 20}`, event: `event-${seq}`, severity: "medium" }), {
            namespace: ns,
            table: "audit",
          });

          await Promise.all([
            pc.op(this.id, this.name, "stats.get", () => pc.client.ns(ns).stats.get(), { namespace: ns }),
            pc.op(this.id, this.name, "changelog.list", () => pc.client.ns(ns).changelog.list({ limit: 100, offset: seq % 100 }), { namespace: ns }),
          ]);
        };

        await runPhaseLoop(warm, this.id, this.name, ctx.profile.timing.warmupMs, runOps);
        await runPhaseLoop(measure, this.id, this.name, ctx.profile.timing.measureMs, runOps);
      });
    }
  },
};

const scenarioS11: ScenarioDefinition = {
  id: "S11",
  name: "Multi Tenant Isolation",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      const setup = ctx.withPhase("setup", repeat);
      const noisy = scenarioNamespace(ctx.runId, this.id, `noisy_${repeat}`);
      const neighbors = Array.from({ length: ctx.profile.noisyNeighbor.neighbors }, (_, i) =>
        scenarioNamespace(ctx.runId, this.id, `neighbor_${repeat}_${i}`),
      );

      await setup.op(this.id, this.name, "namespace.create.noisy", () => setup.client.namespaces.create({ name: noisy }), { namespace: noisy });
      for (const ns of neighbors) {
        await setup.op(this.id, this.name, "namespace.create.neighbor", () => setup.client.namespaces.create({ name: ns }), { namespace: ns });
      }

      try {
        const allNamespaces = [noisy, ...neighbors];
        for (const ns of allNamespaces) {
          await setup.op(
            this.id,
            this.name,
            "tables.create.items",
            () =>
              setup.client.ns(ns).tables.create({
                type: "table",
                name: "items",
                columns: [
                  { name: "label", type: "text", not_null: true, index: true },
                  { name: "state", type: "select", options: ["hot", "cold"], index: true },
                ],
              }),
            { namespace: ns, table: "items" },
          );
        }

        const warm = ctx.withPhase("warmup", repeat);
        const measure = ctx.withPhase("measure", repeat);

        const runOps = async (pc: ScenarioPhaseContext, seq: number): Promise<void> => {
          await Promise.all([
            pc.op(
              this.id,
              this.name,
              "noisy.rows.insert",
              () =>
                pc.client
                  .ns(noisy)
                  .table("items")
                  .rows.insert(
                    Array.from({ length: 200 }, (_, i) => ({ label: `n-${seq}-${i}`, state: i % 2 === 0 ? "hot" : "cold" })),
                  ),
              { namespace: noisy, table: "items" },
            ),
            ...neighbors.map((ns, idx) =>
              pc.op(
                this.id,
                this.name,
                "neighbor.rows.list",
                () => pc.client.ns(ns).table("items").rows.list({ state: idx % 2 === 0 ? "eq.hot" : "eq.cold", limit: 50, offset: seq % 25 }),
                { namespace: ns, table: "items" },
              ),
            ),
          ]);
        };

        await runPhaseLoop(warm, this.id, this.name, ctx.profile.timing.warmupMs, runOps);
        await runPhaseLoop(measure, this.id, this.name, ctx.profile.timing.measureMs, runOps);
      } finally {
        await setup.op(this.id, this.name, "namespace.delete.noisy", () => setup.client.namespaces.delete(noisy), { namespace: noisy });
        for (const ns of neighbors) {
          await setup.op(this.id, this.name, "namespace.delete.neighbor", () => setup.client.namespaces.delete(ns), { namespace: ns });
        }
      }
    }
  },
};

const scenarioS12: ScenarioDefinition = {
  id: "S12",
  name: "Cold Warm Behavior",
  async run(ctx) {
    for (let repeat = 0; repeat < ctx.profile.timing.repeats; repeat++) {
      const setup = ctx.withPhase("setup", repeat);
      const ns = scenarioNamespace(ctx.runId, this.id, `repeat_${repeat}`);
      await withNamespace(setup, this.id, this.name, ns, async () => {
        await setup.op(
          this.id,
          this.name,
          "tables.create.cache_probe",
          () =>
            setup.client.ns(ns).tables.create({
              type: "table",
              name: "cache_probe",
              columns: [
                { name: "key", type: "text", unique: true },
                { name: "payload", type: "text" },
              ],
            }),
          { namespace: ns, table: "cache_probe" },
        );

        const rows = Array.from({ length: Math.max(2000, Math.floor(ctx.profile.rows.basePerNamespace * 0.2)) }, (_, i) => ({
          key: `k-${i}`,
          payload: `payload-${i}-${"x".repeat((i % 7) * 10)}`,
        }));

        for (let i = 0; i < rows.length; i += setup.profile.rows.batchSize) {
          await setup.op(this.id, this.name, "rows.insert.seed", () => setup.client.ns(ns).table("cache_probe").rows.insert(rows.slice(i, i + setup.profile.rows.batchSize)), {
            namespace: ns,
            table: "cache_probe",
          });
        }

        const warm = ctx.withPhase("warmup", repeat);
        const measure = ctx.withPhase("measure", repeat);

        await runPhaseLoop(warm, this.id, this.name, Math.min(ctx.profile.timing.warmupMs, 5000), async (pc) => {
          await pc.op(this.id, this.name, "rows.list.warmup", () => pc.client.ns(ns).table("cache_probe").rows.list({ limit: 100, offset: 0 }), {
            namespace: ns,
            table: "cache_probe",
          });
        });

        const coldStart = performance.now();
        await measure.op(this.id, this.name, "rows.list.cold", () => measure.client.ns(ns).table("cache_probe").rows.list({ limit: 100, offset: 0 }), {
          namespace: ns,
          table: "cache_probe",
        });
        const coldLatency = performance.now() - coldStart;

        const warmDurations: number[] = [];
        await runPhaseLoop(measure, this.id, this.name, Math.min(ctx.profile.timing.measureMs, 15_000), async (pc, seq) => {
          const start = performance.now();
          await pc.op(this.id, this.name, "rows.list.warm", () => pc.client.ns(ns).table("cache_probe").rows.list({ limit: 100, offset: seq % 500 }), {
            namespace: ns,
            table: "cache_probe",
          });
          warmDurations.push(performance.now() - start);
        });

        const avgWarm = warmDurations.length === 0 ? 0 : warmDurations.reduce((a, b) => a + b, 0) / warmDurations.length;
        measure.note(this.id, this.name, `cold_latency_ms=${coldLatency.toFixed(2)} warm_avg_latency_ms=${avgWarm.toFixed(2)}`);
      });
    }
  },
};

export const allScenarios = (): ScenarioDefinition[] => [
  scenarioS1,
  scenarioS2,
  scenarioS3,
  scenarioS4,
  scenarioS5,
  scenarioS6,
  scenarioS7,
  scenarioS8,
  scenarioS9,
  scenarioS10,
  scenarioS11,
  scenarioS12,
];

export const asResult = <T>(value: T): RsqlResult<T> => {
  return {
    ok: true,
    data: value,
    status: 200,
    headers: new Headers(),
  };
};
