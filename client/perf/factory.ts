import type { SeededRandom } from "./types";

export interface ContactRow {
  id?: number;
  external_id: string;
  name: string;
  email: string;
  status: "active" | "inactive";
  city: string;
  segment: "enterprise" | "smb" | "startup";
  score: number;
  notes: string;
}

export interface OrderRow {
  id?: number;
  contact_external_id: string;
  total: number;
  currency: "EUR" | "USD";
  state: "draft" | "paid" | "cancelled";
  created_at: string;
}

export interface LineItemRow {
  id?: number;
  order_id_ref: string;
  sku: string;
  quantity: number;
  price: number;
}

export interface TicketRow {
  id?: number;
  contact_external_id: string;
  title: string;
  priority: "low" | "medium" | "high";
  state: "open" | "in_progress" | "closed";
  created_at: string;
}

export interface MetricRow {
  id?: number;
  ts_bucket: string;
  kind: "latency" | "errors" | "throughput";
  value: number;
  source: string;
}

const firstNames = ["Mia", "Noah", "Lena", "Emil", "Nora", "Paul", "Ella", "Finn", "Luca", "Mila"];
const lastNames = ["Meyer", "Schmidt", "Fischer", "Weber", "Becker", "Hoffmann", "Klein", "Wagner"];
const cities = ["Berlin", "Hamburg", "Munich", "Cologne", "Leipzig", "Stuttgart", "Frankfurt"];
const noteFragments = [
  "renewal expected next quarter",
  "high support volume in onboarding",
  "interested in enterprise SLA",
  "prefers async communication",
  "migrating from spreadsheets",
  "requires monthly export",
  "active champion in team",
];

const iso = (baseMs: number, offsetMinutes: number): string => new Date(baseMs + offsetMinutes * 60_000).toISOString();

export const makeContacts = (rng: SeededRandom, count: number, prefix: string): ContactRow[] => {
  const now = Date.now();
  const rows: ContactRow[] = [];

  for (let i = 0; i < count; i++) {
    const first = firstNames[rng.int(0, firstNames.length - 1)] ?? "Alex";
    const last = lastNames[rng.int(0, lastNames.length - 1)] ?? "Doe";
    const city = cities[rng.int(0, cities.length - 1)] ?? "Berlin";
    const segment = rng.pick(["enterprise", "smb", "startup"] as const);

    rows.push({
      external_id: `${prefix}-c-${i}`,
      name: `${first} ${last}`,
      email: `${prefix}.${i}.${first.toLowerCase()}@example.com`,
      status: rng.bool(0.8) ? "active" : "inactive",
      city,
      segment,
      score: rng.int(1, 100),
      notes: noteFragments[rng.int(0, noteFragments.length - 1)] ?? "customer profile",
    });

    if (rng.bool(0.02)) {
      rows[i]!.notes += ` | ts=${iso(now, i)}`;
    }
  }

  return rows;
};

export const makeOrders = (rng: SeededRandom, contactIDs: string[], count: number, prefix: string): OrderRow[] => {
  const base = Date.now() - 1000 * 60 * 60 * 24 * 30;
  const rows: OrderRow[] = [];

  for (let i = 0; i < count; i++) {
    rows.push({
      contact_external_id: contactIDs[rng.int(0, Math.max(0, contactIDs.length - 1))] ?? `${prefix}-c-0`,
      total: Number((rng.int(1000, 200000) / 100).toFixed(2)),
      currency: rng.bool(0.7) ? "EUR" : "USD",
      state: rng.pick(["draft", "paid", "cancelled"] as const),
      created_at: iso(base, i * 13),
    });
  }

  return rows;
};

export const makeLineItems = (rng: SeededRandom, orderRefs: string[], count: number): LineItemRow[] => {
  const rows: LineItemRow[] = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      order_id_ref: orderRefs[rng.int(0, Math.max(0, orderRefs.length - 1))] ?? "ord-0",
      sku: `SKU-${rng.int(100, 999)}-${rng.int(1000, 9999)}`,
      quantity: rng.int(1, 20),
      price: Number((rng.int(200, 40000) / 100).toFixed(2)),
    });
  }
  return rows;
};

export const makeTickets = (rng: SeededRandom, contactIDs: string[], count: number): TicketRow[] => {
  const base = Date.now() - 1000 * 60 * 60 * 24 * 7;
  const topics = [
    "sync issue",
    "dashboard mismatch",
    "query timeout",
    "csv import failed",
    "permission check",
    "notification delay",
  ];

  const rows: TicketRow[] = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      contact_external_id: contactIDs[rng.int(0, Math.max(0, contactIDs.length - 1))] ?? "contact-0",
      title: topics[rng.int(0, topics.length - 1)] ?? "support case",
      priority: rng.pick(["low", "medium", "high"] as const),
      state: rng.pick(["open", "in_progress", "closed"] as const),
      created_at: iso(base, i * 9),
    });
  }

  return rows;
};

export const makeMetrics = (rng: SeededRandom, count: number, source: string): MetricRow[] => {
  const base = Date.now() - 1000 * 60 * 60 * 4;
  const rows: MetricRow[] = [];

  for (let i = 0; i < count; i++) {
    const kind = rng.pick(["latency", "errors", "throughput"] as const);
    rows.push({
      ts_bucket: iso(base, i),
      kind,
      value: kind === "latency" ? rng.int(5, 1200) : kind === "errors" ? rng.int(0, 25) : rng.int(100, 10000),
      source,
    });
  }

  return rows;
};

export const buildContactsCSV = (rows: ContactRow[]): string => {
  const header = "external_id,name,email,status,city,segment,score,notes";
  const body = rows
    .map((row) => [row.external_id, row.name, row.email, row.status, row.city, row.segment, String(row.score), csvEscape(row.notes)].join(","))
    .join("\n");
  return `${header}\n${body}\n`;
};

const csvEscape = (value: string): string => {
  if (!value.includes(",") && !value.includes("\"") && !value.includes("\n")) {
    return value;
  }
  return `"${value.replaceAll("\"", "\"\"")}"`;
};
