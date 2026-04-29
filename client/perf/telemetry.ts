import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { TelemetryCollector, TelemetrySample } from "./types";
import { nowISO, parseCpuPercent, toMB } from "./utils";

const execFileAsync = promisify(execFile);

interface TelemetryOptions {
  serverURL: string;
  token: string;
  pid: number;
  intervalMs?: number;
}

export const createTelemetryCollector = ({
  serverURL,
  token,
  pid,
  intervalMs = 1000,
}: TelemetryOptions): TelemetryCollector => {
  const samples: TelemetrySample[] = [];
  let timer: ReturnType<typeof setInterval> | null = null;
  let active = false;

  const collect = async (): Promise<void> => {
    if (!active) {
      return;
    }

    const timestamp = nowISO();
    const [metrics, process] = await Promise.all([
      scrapePrometheus(serverURL, token),
      sampleProcess(pid),
    ]);

    samples.push({
      timestamp,
      metrics,
      process,
    });
  };

  return {
    start() {
      if (active) {
        return;
      }
      active = true;
      void collect();
      timer = setInterval(() => {
        void collect();
      }, intervalMs);
    },

    async stop() {
      active = false;
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
      await collect();
    },

    getSamples() {
      return samples;
    },
  };
};

const scrapePrometheus = async (
  serverURL: string,
  token: string,
): Promise<TelemetrySample["metrics"]> => {
  try {
    const res = await fetch(`${serverURL}/metrics`, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });
    if (!res.ok) {
      return emptyMetrics();
    }
    const text = await res.text();
    return parseMetrics(text);
  } catch {
    return emptyMetrics();
  }
};

const emptyMetrics = (): TelemetrySample["metrics"] => ({
  http_requests_total: 0,
  go_goroutines: 0,
  process_resident_memory_bytes: 0,
  process_cpu_seconds_total: 0,
});

const parseMetrics = (text: string): TelemetrySample["metrics"] => {
  const out = emptyMetrics();

  for (const line of text.split("\n")) {
    if (line === "" || line.startsWith("#")) {
      continue;
    }

    const parts = line.trim().split(/\s+/);
    if (parts.length < 2) {
      continue;
    }

    const key = parts[0] ?? "";
    const raw = parts[1] ?? "0";
    const value = Number(raw);
    if (Number.isNaN(value)) {
      continue;
    }

    if (key.startsWith("rsql_http_requests_total")) {
      out.http_requests_total += value;
      continue;
    }
    if (key === "go_goroutines") {
      out.go_goroutines = value;
      continue;
    }
    if (key === "process_resident_memory_bytes") {
      out.process_resident_memory_bytes = value;
      continue;
    }
    if (key === "process_cpu_seconds_total") {
      out.process_cpu_seconds_total = value;
      continue;
    }
  }

  return out;
};

const sampleProcess = async (pid: number): Promise<TelemetrySample["process"]> => {
  try {
    const { stdout } = await execFileAsync("ps", ["-o", "%cpu=,rss=", "-p", String(pid)]);
    const line = stdout.trim().split("\n")[0] ?? "";
    const [cpuRaw, rssRaw] = line.trim().split(/\s+/);

    return {
      cpu_pct: parseCpuPercent(cpuRaw ?? "0"),
      rss_mb: toMB(Number(rssRaw ?? "0") * 1024),
    };
  } catch {
    return {
      cpu_pct: 0,
      rss_mb: 0,
    };
  }
};
