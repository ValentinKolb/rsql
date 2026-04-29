import { appendFile, mkdir, readdir, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export const nowISO = (): string => new Date().toISOString();

export const makeRunId = (profile: string, mode: string): string => {
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const rand = Math.random().toString(36).slice(2, 8);
  return `${ts}-${profile}-${mode}-${rand}`;
};

export const ensureDir = async (path: string): Promise<void> => {
  await mkdir(path, { recursive: true });
};

export const writeJSON = async (path: string, value: unknown): Promise<void> => {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, "utf8");
};

export const appendJSONLine = async (path: string, value: unknown): Promise<void> => {
  await appendFile(path, `${JSON.stringify(value)}\n`, "utf8");
};

export const safeExec = async (cmd: string, args: string[], cwd: string): Promise<string> => {
  try {
    const { stdout } = await execFileAsync(cmd, args, { cwd });
    return String(stdout).trim();
  } catch {
    return "unknown";
  }
};

export const gitInfo = async (repoRoot: string): Promise<{ commit: string; dirty: boolean }> => {
  const commit = await safeExec("git", ["rev-parse", "HEAD"], repoRoot);
  const dirtyOut = await safeExec("git", ["status", "--porcelain"], repoRoot);
  return {
    commit,
    dirty: dirtyOut !== "",
  };
};

export const versions = async (repoRoot: string): Promise<{ go: string; bun: string }> => {
  const go = await safeExec("go", ["version"], repoRoot);
  const bun = await safeExec("bun", ["--version"], repoRoot);
  return { go, bun };
};

export const buildDirSizes = async (baseDir: string): Promise<{ dbBytes: number; walBytes: number }> => {
  let dbBytes = 0;
  let walBytes = 0;

  const walk = async (dir: string): Promise<void> => {
    let entries: string[] = [];
    try {
      entries = await readdir(dir);
    } catch {
      return;
    }
    for (const entry of entries) {
      const abs = join(dir, entry);
      let st;
      try {
        st = await stat(abs);
      } catch {
        continue;
      }
      if (st.isDirectory()) {
        await walk(abs);
        continue;
      }
      if (entry.endsWith(".db")) {
        dbBytes += st.size;
      }
      if (entry.endsWith("-wal")) {
        walBytes += st.size;
      }
    }
  };

  await walk(baseDir);

  return { dbBytes, walBytes };
};

export const mkTempDir = async (prefix: string): Promise<string> => {
  const path = join(tmpdir(), `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
  await ensureDir(path);
  return path;
};

export const sleep = async (ms: number): Promise<void> => {
  await Bun.sleep(ms);
};

export const parseCpuPercent = (raw: string): number => {
  const n = Number(raw.trim());
  if (Number.isNaN(n)) {
    return 0;
  }
  return n;
};

export const toMB = (bytes: number): number => bytes / (1024 * 1024);
