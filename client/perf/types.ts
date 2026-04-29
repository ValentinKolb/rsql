import type { RsqlClient } from "../src";

export type PerfProfileName = "fast" | "deep";
export type PerfRunMode = "full" | "quick";

export interface ProfileConfig {
  name: PerfProfileName;
  seed: number;
  namespaces: {
    controlPlane: number;
    workload: number;
  };
  rows: {
    basePerNamespace: number;
    hotNamespaceRows: number;
    batchSize: number;
  };
  timing: {
    repeats: number;
    warmupMs: number;
    measureMs: number;
  };
  sse: {
    subscribers: number[];
  };
  noisyNeighbor: {
    neighbors: number;
    writerConcurrency: number;
  };
  pprof: {
    enabled: boolean;
    captureThresholdP95Ms: number;
    captureThresholdErrorRate: number;
    profileSeconds: number;
  };
  serverFlags: {
    queryTimeoutMs: number;
    namespaceIdleTimeoutMs: number;
    maxOpenNamespaces: number;
  };
}

export interface RunManifest {
  run_id: string;
  profile: PerfProfileName;
  mode: PerfRunMode;
  selected_scenarios: string[];
  scenario_set_key: string;
  started_at: string;
  ended_at?: string;
  duration_ms?: number;
  seed: number;
  repo: {
    git_commit: string;
    dirty_state: boolean;
  };
  versions: {
    go: string;
    bun: string;
  };
  host: {
    platform: string;
    arch: string;
    cpu_count: number;
    total_memory_bytes: number;
    hostname: string;
  };
  server: {
    url: string;
    data_dir: string;
    listen: string;
    pprof_enabled: boolean;
    pprof_listen?: string;
    flags: Record<string, string | number | boolean>;
  };
}

export interface PerfRunPaths {
  root: string;
  rawMetrics: string;
  scenarioSummary: string;
  runManifest: string;
  bottlenecks: string;
  telemetry: string;
  profilesDir: string;
}

export interface RawMetricEvent {
  timestamp: string;
  scenario_id: string;
  scenario_name: string;
  repeat: number;
  phase: "setup" | "warmup" | "measure" | "teardown";
  operation: string;
  namespace?: string;
  table?: string;
  status: number;
  ok: boolean;
  duration_ms: number;
  error_code?: string;
  error_message?: string;
}

export interface ScenarioSummary {
  id: string;
  name: string;
  repeats: number;
  warmup_ms: number;
  measure_ms: number;
  total_ops: number;
  error_count: number;
  error_rate: number;
  duration_ms: number;
  throughput_ops_per_sec: number;
  latency_ms: {
    min: number;
    p50: number;
    p95: number;
    p99: number;
    max: number;
    avg: number;
  };
  telemetry: {
    db_growth_bytes: number;
    wal_growth_bytes: number;
    avg_cpu_pct: number;
    peak_cpu_pct: number;
    avg_rss_mb: number;
    peak_rss_mb: number;
  };
  checks: {
    request_counter_delta: number;
    raw_request_count: number;
    request_counter_match_ratio: number;
  };
  notes: string[];
  pprof_profiles?: string[];
}

export interface PerfReport {
  run_id: string;
  profile: PerfProfileName;
  mode?: PerfRunMode;
  generated_at: string;
  totals: {
    scenarios: number;
    total_ops: number;
    total_errors: number;
    weighted_error_rate: number;
  };
  regressions: Array<{
    scenario_id: string;
    p95_delta_pct: number;
    throughput_delta_pct: number;
    flagged: boolean;
  }>;
  top_bottlenecks: Array<{
    scenario_id: string;
    scenario_name: string;
    score: number;
    why: string;
    evidence: string[];
    suggestions: string[];
  }>;
}

export interface RunningServer {
  url: string;
  token: string;
  dataDir: string;
  listen: string;
  pprofURL?: string;
  pid: number;
  stop: () => Promise<void>;
}

export interface TelemetrySample {
  timestamp: string;
  process: {
    cpu_pct: number;
    rss_mb: number;
  };
  metrics: {
    http_requests_total: number;
    go_goroutines: number;
    process_resident_memory_bytes: number;
    process_cpu_seconds_total: number;
  };
}

export interface TelemetryCollector {
  start: () => void;
  stop: () => Promise<void>;
  getSamples: () => TelemetrySample[];
}

export interface ScenarioContext {
  client: RsqlClient;
  profile: ProfileConfig;
  runId: string;
  random: SeededRandom;
  withPhase: (phase: RawMetricEvent["phase"], repeat: number) => ScenarioPhaseContext;
}

export interface ScenarioPhaseContext {
  client: RsqlClient;
  profile: ProfileConfig;
  runId: string;
  random: SeededRandom;
  phase: RawMetricEvent["phase"];
  repeat: number;
  op: <T>(
    scenarioId: string,
    scenarioName: string,
    operation: string,
    run: () => Promise<{ ok: true; data: T; status: number; headers: Headers } | { ok: false; error: { error: string; message: string }; status: number; headers: Headers }>,
    options?: { namespace?: string; table?: string; expectError?: boolean },
  ) => Promise<T>;
  note: (scenarioId: string, scenarioName: string, note: string) => void;
}

export interface ScenarioDefinition {
  id: string;
  name: string;
  run: (ctx: ScenarioContext) => Promise<void>;
}

export class SeededRandom {
  private state: number;

  constructor(seed: number) {
    this.state = seed >>> 0;
  }

  next(): number {
    this.state = (1664525 * this.state + 1013904223) >>> 0;
    return this.state / 4294967296;
  }

  int(minInclusive: number, maxInclusive: number): number {
    if (maxInclusive <= minInclusive) {
      return minInclusive;
    }
    const span = maxInclusive - minInclusive + 1;
    return minInclusive + Math.floor(this.next() * span);
  }

  pick<T>(values: T[]): T {
    if (values.length === 0) {
      throw new Error("cannot pick from empty list");
    }
    return values[Math.floor(this.next() * values.length)] as T;
  }

  bool(probability: number): boolean {
    return this.next() < probability;
  }

  zipfIndex(size: number, skew = 1.1): number {
    if (size <= 1) {
      return 0;
    }
    const r = this.next();
    const a = Math.pow(size, 1 - skew) - 1;
    const v = Math.pow(r * a + 1, 1 / (1 - skew));
    return Math.min(size - 1, Math.max(0, Math.floor(v) - 1));
  }
}
