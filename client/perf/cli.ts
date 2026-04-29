#!/usr/bin/env bun
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { buildReport, writeBottleneckMarkdown } from "./reporter";
import { runBenchmark } from "./runner";
import type { PerfProfileName, PerfRunMode, ScenarioSummary } from "./types";

const usage = `
Usage:
  bun run perf/cli.ts fast [--quick] [--scenarios=S1,S2,...]
  bun run perf/cli.ts deep [--scenarios=S1,S2,...]
  bun run perf/cli.ts report --run=<path-to-run-dir>
`;

const parseArgs = (args: string[]) => {
  const flags = new Map<string, string>();
  const positionals: string[] = [];

  for (const arg of args) {
    if (arg.startsWith("--")) {
      const [key, raw = "true"] = arg.slice(2).split("=", 2);
      flags.set(key, raw);
      continue;
    }
    positionals.push(arg);
  }

  return {
    flags,
    positionals,
  };
};

const main = async (): Promise<void> => {
  const { flags, positionals } = parseArgs(Bun.argv.slice(2));
  const command = positionals[0];

  if (!command || !(command === "fast" || command === "deep" || command === "report")) {
    console.error(usage.trim());
    process.exit(1);
  }

  if (command === "report") {
    const runArg = flags.get("run");
    if (!runArg) {
      console.error("missing --run=<path>\n");
      console.error(usage.trim());
      process.exit(1);
    }

    const runDir = runArg.startsWith("/")
      ? runArg
      : join(fileURLToPath(new URL(".", import.meta.url)), "runs", runArg);

    const summaryRaw = await readFile(join(runDir, "scenario-summary.json"), "utf8");
    const manifestRaw = await readFile(join(runDir, "run-manifest.json"), "utf8");

    const summaries = JSON.parse(summaryRaw) as ScenarioSummary[];
    const manifest = JSON.parse(manifestRaw) as {
      run_id: string;
      profile: PerfProfileName;
      mode?: PerfRunMode;
      scenario_set_key?: string;
      selected_scenarios?: string[];
    };

    const perfRoot = fileURLToPath(new URL(".", import.meta.url));
    const scenarioSetKey =
      manifest.scenario_set_key ??
      (manifest.selected_scenarios ?? summaries.map((summary) => summary.id).sort((a, b) => a.localeCompare(b))).join(",");

    const report = await buildReport(perfRoot, manifest.run_id, manifest.profile, manifest.mode ?? "full", scenarioSetKey, summaries);

    await writeFile(join(runDir, "report.json"), `${JSON.stringify(report, null, 2)}\n`, "utf8");
    await writeBottleneckMarkdown(join(runDir, "bottlenecks.md"), report);

    console.log(`Report refreshed: ${join(runDir, "bottlenecks.md")}`);
    return;
  }

  const scenarioFilter = flags.get("scenarios")?.split(",").map((entry) => entry.trim()).filter(Boolean);
  const quick = flags.get("quick") === "true";

  const result = await runBenchmark({
    profileName: command,
    scenarioFilter,
    quick,
  });

  console.log(`Run directory: ${result.runDir}`);
  console.log(`Scenarios: ${result.summaries.length}`);
  console.log(`Report: ${result.reportPath}`);
};

void main();
