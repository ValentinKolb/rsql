import { readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { PerfProfileName, PerfReport, PerfRunMode, ScenarioSummary } from "./types";
import { round } from "./stats";

export const buildReport = async (
  runRoot: string,
  runID: string,
  profile: PerfProfileName,
  mode: PerfRunMode,
  scenarioSetKey: string,
  summaries: ScenarioSummary[],
): Promise<PerfReport> => {
  const previous = await loadPreviousSummary(runRoot, profile, mode, scenarioSetKey, runID);

  const regressions = summaries.map((summary) => {
    const prev = previous?.find((entry) => entry.id === summary.id);
    const p95DeltaPct = prev && prev.latency_ms.p95 > 0 ? (summary.latency_ms.p95 - prev.latency_ms.p95) / prev.latency_ms.p95 : 0;
    const throughputDeltaPct =
      prev && prev.throughput_ops_per_sec > 0
        ? (summary.throughput_ops_per_sec - prev.throughput_ops_per_sec) / prev.throughput_ops_per_sec
        : 0;

    return {
      scenario_id: summary.id,
      p95_delta_pct: round(p95DeltaPct * 100),
      throughput_delta_pct: round(throughputDeltaPct * 100),
      flagged: p95DeltaPct > 0.15 || throughputDeltaPct < -0.15,
    };
  });

  const weightedErrorRate =
    summaries.reduce((acc, summary) => acc + summary.error_rate * summary.total_ops, 0) /
    Math.max(1, summaries.reduce((acc, summary) => acc + summary.total_ops, 0));

  const top_bottlenecks = summaries
    .map((summary) => {
      const regression = regressions.find((entry) => entry.scenario_id === summary.id);
      const regressionPenalty = regression?.flagged ? 80 : 0;
      const errorPenalty = summary.error_rate * 1000;
      const p95Penalty = summary.latency_ms.p95;
      const score = round(regressionPenalty + errorPenalty + p95Penalty);

      const evidence = [
        `p95 latency: ${summary.latency_ms.p95} ms`,
        `error rate: ${(summary.error_rate * 100).toFixed(2)}%`,
        `throughput: ${summary.throughput_ops_per_sec} ops/s`,
        `avg CPU: ${summary.telemetry.avg_cpu_pct.toFixed(2)}%`,
        `peak RSS: ${summary.telemetry.peak_rss_mb.toFixed(2)} MB`,
        `DB growth: ${summary.telemetry.db_growth_bytes} bytes`,
        `WAL growth: ${summary.telemetry.wal_growth_bytes} bytes`,
      ];

      const suggestions = inferSuggestions(summary, regression?.flagged ?? false);

      return {
        scenario_id: summary.id,
        scenario_name: summary.name,
        score,
        why: inferWhy(summary, regression?.flagged ?? false),
        evidence,
        suggestions,
      };
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);

  return {
    run_id: runID,
    profile,
    mode,
    generated_at: new Date().toISOString(),
    totals: {
      scenarios: summaries.length,
      total_ops: summaries.reduce((acc, summary) => acc + summary.total_ops, 0),
      total_errors: summaries.reduce((acc, summary) => acc + summary.error_count, 0),
      weighted_error_rate: round(weightedErrorRate, 6),
    },
    regressions,
    top_bottlenecks,
  };
};

const inferWhy = (summary: ScenarioSummary, flaggedRegression: boolean): string => {
  if (summary.error_rate > 0.01) {
    return "Elevated error rate under load indicates saturation or contention side effects.";
  }
  if (summary.telemetry.wal_growth_bytes > summary.telemetry.db_growth_bytes * 0.8) {
    return "WAL growth is high relative to DB growth, indicating write-heavy checkpoint pressure.";
  }
  if (summary.telemetry.avg_cpu_pct > 80) {
    return "High sustained CPU during scenario suggests compute-bound execution paths.";
  }
  if (flaggedRegression) {
    return "Scenario regressed versus baseline by configured threshold.";
  }
  return "Scenario shows the highest combined latency and resource footprint in this run.";
};

const inferSuggestions = (summary: ScenarioSummary, flaggedRegression: boolean): string[] => {
  const suggestions: string[] = [];

  if (summary.telemetry.wal_growth_bytes > summary.telemetry.db_growth_bytes * 0.8) {
    suggestions.push("Evaluate WAL checkpoint strategy and batch sizing for write-heavy paths.");
  }
  if (summary.telemetry.avg_cpu_pct > 80) {
    suggestions.push("Profile CPU hotspots with pprof and optimize row/query transformation hotspots.");
  }
  if (summary.error_rate > 0.005) {
    suggestions.push("Inspect timeout/busy-lock failures and tune busy_timeout / query_timeout.");
  }
  if (summary.name.includes("SSE")) {
    suggestions.push("Inspect subscriber fanout behavior and consider bounded buffering strategies.");
  }
  if (flaggedRegression) {
    suggestions.push("Run focused repeat for this scenario and compare pprof traces versus previous baseline.");
  }
  if (suggestions.length === 0) {
    suggestions.push("Increase scenario-specific instrumentation and isolate sub-phases to pinpoint dominant cost.");
  }

  return suggestions;
};

const loadPreviousSummary = async (
  runRoot: string,
  profile: PerfProfileName,
  mode: PerfRunMode,
  scenarioSetKey: string,
  currentRunID: string,
): Promise<ScenarioSummary[] | null> => {
  const runsDir = join(runRoot, "runs");
  let entries: string[] = [];
  try {
    entries = await readdir(runsDir);
  } catch {
    return null;
  }

  const candidates = entries.filter((entry) => entry !== currentRunID).sort((a, b) => a.localeCompare(b));

  if (candidates.length === 0) {
    return null;
  }

  for (let i = candidates.length - 1; i >= 0; i--) {
    const candidate = candidates[i];
    if (!candidate) {
      continue;
    }

    try {
      const manifestRaw = await readFile(join(runsDir, candidate, "run-manifest.json"), "utf8");
      const manifest = JSON.parse(manifestRaw) as {
        profile?: PerfProfileName;
        mode?: PerfRunMode;
        scenario_set_key?: string;
      };

      if (manifest.profile !== profile || manifest.mode !== mode || manifest.scenario_set_key !== scenarioSetKey) {
        continue;
      }

      const summaryRaw = await readFile(join(runsDir, candidate, "scenario-summary.json"), "utf8");
      return JSON.parse(summaryRaw) as ScenarioSummary[];
    } catch {
      // Continue scanning older candidates.
    }
  }

  return null;
};

export const writeBottleneckMarkdown = async (outPath: string, report: PerfReport): Promise<void> => {
  const lines: string[] = [];

  lines.push(`# Bottleneck Analysis (${report.profile})`);
  lines.push("");
  lines.push(`Run: \`${report.run_id}\``);
  lines.push(`Generated: ${report.generated_at}`);
  lines.push("");
  lines.push("## Top 3 Bottlenecks");
  lines.push("");

  report.top_bottlenecks.forEach((entry, index) => {
    lines.push(`### ${index + 1}. ${entry.scenario_id} — ${entry.scenario_name}`);
    lines.push(`- Score: ${entry.score}`);
    lines.push(`- Why: ${entry.why}`);
    lines.push(`- Evidence:`);
    for (const item of entry.evidence) {
      lines.push(`  - ${item}`);
    }
    lines.push(`- Suggestions:`);
    for (const suggestion of entry.suggestions) {
      lines.push(`  - ${suggestion}`);
    }
    lines.push("");
  });

  lines.push("## Regression Flags");
  lines.push("");

  for (const regression of report.regressions) {
    lines.push(
      `- ${regression.scenario_id}: p95 delta ${regression.p95_delta_pct}%, throughput delta ${regression.throughput_delta_pct}%, flagged=${regression.flagged}`,
    );
  }

  lines.push("");
  lines.push("## Totals");
  lines.push("");
  lines.push(`- Scenarios: ${report.totals.scenarios}`);
  lines.push(`- Total operations: ${report.totals.total_ops}`);
  lines.push(`- Total errors: ${report.totals.total_errors}`);
  lines.push(`- Weighted error rate: ${(report.totals.weighted_error_rate * 100).toFixed(4)}%`);

  await writeFile(outPath, `${lines.join("\n")}\n`, "utf8");
};
