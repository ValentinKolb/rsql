import { cpus, hostname, platform, totalmem, arch } from "node:os";
import { basename, dirname, join } from "node:path";
import { appendFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { createRsqlClient } from "../src";
import { allScenarios } from "./scenarios";
import { getProfile } from "./profiles";
import { createTelemetryCollector } from "./telemetry";
import { buildReport, writeBottleneckMarkdown } from "./reporter";
import { latencySummary, round, average } from "./stats";
import { startServer } from "./server";
import {
  appendJSONLine,
  buildDirSizes,
  ensureDir,
  gitInfo,
  makeRunId,
  nowISO,
  versions,
  writeJSON,
} from "./utils";
import type {
  PerfProfileName,
  PerfRunMode,
  PerfRunPaths,
  RawMetricEvent,
  RunManifest,
  ScenarioContext,
  ScenarioDefinition,
  ScenarioSummary,
  SeededRandom,
  TelemetrySample,
} from "./types";
import { SeededRandom as SeededRandomImpl } from "./types";

export interface RunnerOptions {
  profileName: PerfProfileName;
  scenarioFilter?: string[];
  quick?: boolean;
}

interface ScenarioRuntime {
  id: string;
  name: string;
  startedAt: number;
  endedAt: number;
  notes: string[];
  dbBefore: number;
  walBefore: number;
  dbAfter: number;
  walAfter: number;
  sampleStartIndex: number;
  sampleEndIndex: number;
}

interface ScenarioNoteState {
  items: string[];
  dropped: number;
}

const maxNotesPerScenario = 200;

export const runBenchmark = async (options: RunnerOptions): Promise<{
  runDir: string;
  summaries: ScenarioSummary[];
  reportPath: string;
}> => {
  const profile = getProfile(options.profileName);
  const mode: PerfRunMode = options.quick ? "quick" : "full";
  if (options.quick) {
    profile.timing.warmupMs = 2000;
    profile.timing.measureMs = 3000;
    profile.timing.repeats = 1;
    profile.rows.basePerNamespace = 2000;
    profile.rows.hotNamespaceRows = 8000;
  }

  const selectedScenarios = filterScenarios(allScenarios(), options.scenarioFilter);
  const selectedScenarioIDs = selectedScenarios.map((scenario) => scenario.id).sort((a, b) => a.localeCompare(b));
  const scenarioSetKey = selectedScenarioIDs.join(",");

  const perfRoot = fileURLToPath(new URL(".", import.meta.url));
  const runID = makeRunId(profile.name, mode);
  const runDir = join(perfRoot, "runs", runID);

  const paths: PerfRunPaths = {
    root: runDir,
    rawMetrics: join(runDir, "raw-metrics.jsonl"),
    scenarioSummary: join(runDir, "scenario-summary.json"),
    runManifest: join(runDir, "run-manifest.json"),
    bottlenecks: join(runDir, "bottlenecks.md"),
    telemetry: join(runDir, "telemetry.json"),
    profilesDir: join(runDir, "profiles"),
  };

  await ensureDir(paths.root);
  await ensureDir(paths.profilesDir);

  const repoRoot = fileURLToPath(new URL("../../", import.meta.url));
  const [git, versionInfo] = await Promise.all([gitInfo(repoRoot), versions(repoRoot)]);
  const server = await startServer({ profile, enablePprof: profile.pprof.enabled });
  const client = createRsqlClient({ url: server.url, token: server.token });

  const manifest: RunManifest = {
    run_id: runID,
    profile: profile.name,
    mode,
    selected_scenarios: selectedScenarioIDs,
    scenario_set_key: scenarioSetKey,
    started_at: nowISO(),
    seed: profile.seed,
    repo: {
      git_commit: git.commit,
      dirty_state: git.dirty,
    },
    versions: {
      go: versionInfo.go,
      bun: versionInfo.bun,
    },
    host: {
      platform: platform(),
      arch: arch(),
      cpu_count: cpus().length,
      total_memory_bytes: totalmem(),
      hostname: hostname(),
    },
    server: {
      url: server.url,
      data_dir: server.dataDir,
      listen: server.listen,
      pprof_enabled: Boolean(server.pprofURL),
      pprof_listen: server.pprofURL ? server.pprofURL.replace(/^https?:\/\//, "") : undefined,
      flags: {
        query_timeout_ms: profile.serverFlags.queryTimeoutMs,
        namespace_idle_timeout_ms: profile.serverFlags.namespaceIdleTimeoutMs,
        max_open_namespaces: profile.serverFlags.maxOpenNamespaces,
        pprof_enabled: profile.pprof.enabled,
      },
    },
  };

  await writeJSON(paths.runManifest, manifest);

  const telemetry = createTelemetryCollector({
    serverURL: server.url,
    token: server.token,
    pid: server.pid,
  });
  telemetry.start();

  const rawEvents: RawMetricEvent[] = [];
  const scenarioRuntimes: ScenarioRuntime[] = [];

  const rng: SeededRandom = new SeededRandomImpl(profile.seed);

  const phaseOp = async <T>(
    scenarioId: string,
    scenarioName: string,
    phase: RawMetricEvent["phase"],
    repeat: number,
    operation: string,
    run: () => Promise<{ ok: true; data: T; status: number; headers: Headers } | { ok: false; error: { error: string; message: string }; status: number; headers: Headers }>,
    options?: { namespace?: string; table?: string; expectError?: boolean },
  ): Promise<T> => {
    const started = performance.now();
    const timestamp = nowISO();

    try {
      const result = await run();
      const durationMs = performance.now() - started;

      if (!result.ok) {
        rawEvents.push({
          timestamp,
          scenario_id: scenarioId,
          scenario_name: scenarioName,
          repeat,
          phase,
          operation,
          namespace: options?.namespace,
          table: options?.table,
          status: result.status,
          ok: false,
          duration_ms: round(durationMs),
          error_code: result.error.error,
          error_message: result.error.message,
        });

        if (!options?.expectError) {
          throw new Error(`operation failed: ${operation} ${result.status} ${result.error.error} ${result.error.message}`);
        }

        return undefined as T;
      }

      rawEvents.push({
        timestamp,
        scenario_id: scenarioId,
        scenario_name: scenarioName,
        repeat,
        phase,
        operation,
        namespace: options?.namespace,
        table: options?.table,
        status: result.status,
        ok: true,
        duration_ms: round(durationMs),
      });

      return result.data;
    } catch (err) {
      const durationMs = performance.now() - started;
      rawEvents.push({
        timestamp,
        scenario_id: scenarioId,
        scenario_name: scenarioName,
        repeat,
        phase,
        operation,
        namespace: options?.namespace,
        table: options?.table,
        status: 0,
        ok: false,
        duration_ms: round(durationMs),
        error_code: "runtime_error",
        error_message: err instanceof Error ? err.message : String(err),
      });
      throw err;
    }
  };

  const scenarioNotes = new Map<string, ScenarioNoteState>();

  const makeContext = (): ScenarioContext => {
    return {
      client,
      profile,
      runId: runID,
      random: rng,
      withPhase(phase, repeat) {
        return {
          client,
          profile,
          runId: runID,
          random: rng,
          phase,
          repeat,
          op: (scenarioId, scenarioName, operation, run, opts) => phaseOp(scenarioId, scenarioName, phase, repeat, operation, run, opts),
          note: (scenarioId, _scenarioName, note) => {
            const state = scenarioNotes.get(scenarioId) ?? { items: [], dropped: 0 };
            if (state.items.length < maxNotesPerScenario) {
              state.items.push(note);
            } else {
              state.dropped += 1;
            }
            scenarioNotes.set(scenarioId, state);
          },
        };
      },
    };
  };

  try {
    for (const scenario of selectedScenarios) {
      const startedAt = Date.now();
      const sizesBefore = await buildDirSizes(server.dataDir);
      const sampleStartIndex = telemetry.getSamples().length;

      await scenario.run(makeContext());

      const sampleEndIndex = telemetry.getSamples().length;
      const sizesAfter = await buildDirSizes(server.dataDir);
      const endedAt = Date.now();

      scenarioRuntimes.push({
        id: scenario.id,
        name: scenario.name,
        startedAt,
        endedAt,
        notes: withDroppedNotes(scenarioNotes.get(scenario.id)),
        dbBefore: sizesBefore.dbBytes,
        walBefore: sizesBefore.walBytes,
        dbAfter: sizesAfter.dbBytes,
        walAfter: sizesAfter.walBytes,
        sampleStartIndex,
        sampleEndIndex,
      });
    }
  } finally {
    await telemetry.stop();
    await server.stop();
  }

  for (const event of rawEvents) {
    await appendJSONLine(paths.rawMetrics, event);
  }

  const telemetrySamples = telemetry.getSamples();
  await writeJSON(paths.telemetry, telemetrySamples);

  const summaries: ScenarioSummary[] = [];
  for (const runtime of scenarioRuntimes) {
    const summary = summarizeScenario(runtime, rawEvents, telemetrySamples, profile.timing.repeats, profile.timing.warmupMs, profile.timing.measureMs);

    if (profile.pprof.enabled && server.pprofURL && shouldCaptureProfile(summary, profile)) {
      const profiles = await capturePprofProfiles(server.pprofURL, paths.profilesDir, runtime.id, profile.pprof.profileSeconds);
      summary.pprof_profiles = profiles;
      summary.notes.push(
        `flamegraph command: go tool pprof -http=:0 ${profiles
          .map((path) => join(paths.profilesDir, path))
          .find((path) => path.endsWith("-cpu.pprof")) ?? "<cpu profile>"}`,
      );
    }

    summaries.push(summary);
  }

  await writeJSON(paths.scenarioSummary, summaries);

  const report = await buildReport(perfRoot, runID, profile.name, mode, scenarioSetKey, summaries);
  await writeBottleneckMarkdown(paths.bottlenecks, report);

  manifest.ended_at = nowISO();
  manifest.duration_ms = round(Date.parse(manifest.ended_at) - Date.parse(manifest.started_at));
  await writeJSON(paths.runManifest, manifest);

  return {
    runDir,
    summaries,
    reportPath: paths.bottlenecks,
  };
};

const filterScenarios = (scenarios: ScenarioDefinition[], filter?: string[]): ScenarioDefinition[] => {
  if (!filter || filter.length === 0) {
    return scenarios;
  }
  const wanted = new Set(filter.map((entry) => entry.toLowerCase()));
  return scenarios.filter((scenario) => wanted.has(scenario.id.toLowerCase()) || wanted.has(scenario.name.toLowerCase()));
};

const summarizeScenario = (
  runtime: ScenarioRuntime,
  events: RawMetricEvent[],
  samples: TelemetrySample[],
  repeats: number,
  warmupMs: number,
  measureMs: number,
): ScenarioSummary => {
  const measureEvents = events.filter((event) => event.scenario_id === runtime.id && event.phase === "measure");
  const durationMs = runtime.endedAt - runtime.startedAt;

  const latencies = measureEvents.map((event) => event.duration_ms);
  const errors = measureEvents.filter((event) => !event.ok);
  const totalOps = measureEvents.length;
  const errorCount = errors.length;
  const errorRate = totalOps === 0 ? 0 : errorCount / totalOps;
  const throughput = durationMs > 0 ? (totalOps / durationMs) * 1000 : 0;

  const sampleSlice = samples.slice(runtime.sampleStartIndex, runtime.sampleEndIndex);
  const measureStartTs = measureEvents.length > 0 ? Date.parse(measureEvents[0]?.timestamp ?? "") : Number.NaN;
  const measureEndTs = measureEvents.length > 0 ? Date.parse(measureEvents[measureEvents.length - 1]?.timestamp ?? "") : Number.NaN;
  const measureSampleSlice =
    Number.isNaN(measureStartTs) || Number.isNaN(measureEndTs)
      ? []
      : sampleSlice.filter((sample) => {
          const ts = Date.parse(sample.timestamp);
          return !Number.isNaN(ts) && ts >= measureStartTs && ts <= measureEndTs;
        });
  const counterSlice = measureSampleSlice.length >= 2 ? measureSampleSlice : sampleSlice;

  const cpuValues = sampleSlice.map((sample) => sample.process.cpu_pct);
  const rssValues = sampleSlice.map((sample) => sample.process.rss_mb);

  const requestCounterDeltaRaw =
    counterSlice.length >= 2
      ? Math.max(0, (counterSlice[counterSlice.length - 1]?.metrics.http_requests_total ?? 0) - (counterSlice[0]?.metrics.http_requests_total ?? 0))
      : 0;

  // Collector scrapes /metrics once per sample. Those requests inflate the server-side request counter.
  const estimatedMetricsScrapes = Math.max(0, counterSlice.length - 1);
  const requestCounterDelta = Math.max(0, requestCounterDeltaRaw - estimatedMetricsScrapes);

  const rawRequestCount = measureEvents.length;
  const requestCounterMatchRatio = rawRequestCount === 0 ? 1 : requestCounterDelta / rawRequestCount;

  const notes = [...runtime.notes];
  if (requestCounterMatchRatio < 0.8 || requestCounterMatchRatio > 1.25) {
    notes.push(
      `request_counter_ratio_outlier ratio=${round(requestCounterMatchRatio, 4)} adjusted_delta=${round(requestCounterDelta)} raw_measure_ops=${rawRequestCount}`,
    );
  }

  return {
    id: runtime.id,
    name: runtime.name,
    repeats,
    warmup_ms: warmupMs,
    measure_ms: measureMs,
    total_ops: totalOps,
    error_count: errorCount,
    error_rate: round(errorRate, 6),
    duration_ms: round(durationMs),
    throughput_ops_per_sec: round(throughput),
    latency_ms: latencySummary(latencies),
    telemetry: {
      db_growth_bytes: Math.max(0, runtime.dbAfter - runtime.dbBefore),
      wal_growth_bytes: Math.max(0, runtime.walAfter - runtime.walBefore),
      avg_cpu_pct: round(average(cpuValues)),
      peak_cpu_pct: round(Math.max(0, ...cpuValues)),
      avg_rss_mb: round(average(rssValues)),
      peak_rss_mb: round(Math.max(0, ...rssValues)),
    },
    checks: {
      request_counter_delta: round(requestCounterDelta),
      raw_request_count: rawRequestCount,
      request_counter_match_ratio: round(requestCounterMatchRatio, 4),
    },
    notes,
  };
};

const shouldCaptureProfile = (summary: ScenarioSummary, profile: ReturnType<typeof getProfile>): boolean => {
  return summary.latency_ms.p95 >= profile.pprof.captureThresholdP95Ms || summary.error_rate >= profile.pprof.captureThresholdErrorRate;
};

const capturePprofProfiles = async (
  pprofURL: string,
  profilesDir: string,
  scenarioID: string,
  profileSeconds: number,
): Promise<string[]> => {
  const files: string[] = [];

  const endpoints: Array<{ suffix: string; url: string }> = [
    { suffix: "cpu", url: `${pprofURL}/debug/pprof/profile?seconds=${profileSeconds}` },
    { suffix: "heap", url: `${pprofURL}/debug/pprof/heap` },
    { suffix: "mutex", url: `${pprofURL}/debug/pprof/mutex` },
    { suffix: "block", url: `${pprofURL}/debug/pprof/block` },
  ];

  for (const endpoint of endpoints) {
    try {
      const res = await fetch(endpoint.url);
      if (!res.ok) {
        continue;
      }
      const data = await res.arrayBuffer();
      const fileName = `${scenarioID.toLowerCase()}-${endpoint.suffix}.pprof`;
      const outPath = join(profilesDir, fileName);
      await Bun.write(outPath, new Uint8Array(data));
      files.push(fileName);
    } catch {
      // Best effort profiling only.
    }
  }

  if (files.length > 0) {
    await appendFile(join(profilesDir, "README.txt"), `Generated pprof files for ${scenarioID}: ${files.join(", ")}\n`, "utf8");
  }

  return files;
};

export const resolveRunDir = (runArg: string): string => {
  if (runArg.startsWith("/")) {
    return runArg;
  }
  const perfRoot = fileURLToPath(new URL(".", import.meta.url));
  if (runArg.startsWith("runs/")) {
    return join(perfRoot, runArg);
  }
  return join(perfRoot, "runs", basename(dirname("/" + runArg)) === "runs" ? basename(runArg) : runArg);
};

const withDroppedNotes = (state: ScenarioNoteState | undefined): string[] => {
  if (!state) {
    return [];
  }
  if (state.dropped <= 0) {
    return state.items;
  }
  return [...state.items, `... ${state.dropped} additional notes omitted`];
};
