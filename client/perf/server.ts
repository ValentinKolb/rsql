import { createServer } from "node:net";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type { ProfileConfig, RunningServer } from "./types";

const repoRoot = fileURLToPath(new URL("../../", import.meta.url));
let binaryPathPromise: Promise<string> | null = null;

interface StartServerOptions {
  profile: ProfileConfig;
  enablePprof: boolean;
}

export const startServer = async ({ profile, enablePprof }: StartServerOptions): Promise<RunningServer> => {
  const binaryPath = await buildBinary();
  const port = await getFreePort();
  const token = `perf_token_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const dataDir = await mkdtemp(join(tmpdir(), "rsql-perf-data-"));
  const listen = `127.0.0.1:${port}`;
  const url = `http://${listen}`;

  let pprofURL: string | undefined;
  const args = [
    binaryPath,
    "serve",
    "--listen",
    listen,
    "--data-dir",
    dataDir,
    "--api-token",
    token,
    "--log-level",
    "error",
    "--query-timeout-ms",
    String(profile.serverFlags.queryTimeoutMs),
    "--namespace-idle-timeout-ms",
    String(profile.serverFlags.namespaceIdleTimeoutMs),
    "--max-open-namespaces",
    String(profile.serverFlags.maxOpenNamespaces),
  ];

  if (enablePprof) {
    const pprofPort = await getFreePort();
    const pprofListen = `127.0.0.1:${pprofPort}`;
    pprofURL = `http://${pprofListen}`;
    args.push("--pprof-enabled", "--pprof-listen", pprofListen);
  }

  const proc = Bun.spawn(args, {
    cwd: repoRoot,
    stdout: "pipe",
    stderr: "pipe",
  });

  await waitUntilHealthy(url, token, proc);

  if (enablePprof && pprofURL) {
    await waitUntilPprofReady(pprofURL, proc);
  }

  return {
    url,
    token,
    dataDir,
    listen,
    pprofURL,
    pid: proc.pid,
    stop: async () => {
      if (proc.exitCode !== null) {
        return;
      }
      proc.kill("SIGTERM");
      const exitedGracefully = await Promise.race([
        proc.exited.then(() => true),
        Bun.sleep(3_000).then(() => false),
      ]);
      if (exitedGracefully || proc.exitCode !== null) {
        return;
      }
      proc.kill("SIGKILL");
      await Promise.race([proc.exited, Bun.sleep(1_000)]);
    },
  };
};

const buildBinary = async (): Promise<string> => {
  if (binaryPathPromise) {
    return binaryPathPromise;
  }

  binaryPathPromise = (async () => {
    const buildDir = await mkdtemp(join(tmpdir(), "rsql-perf-bin-"));
    const binaryPath = join(buildDir, "rsql-perf");
    const build = Bun.spawn(["go", "build", "-o", binaryPath, "./cmd/rsql"], {
      cwd: repoRoot,
      stdout: "pipe",
      stderr: "pipe",
    });

    const exitCode = await build.exited;
    if (exitCode !== 0) {
      const stderr = await readText(build.stderr);
      throw new Error(`failed to build rsql binary:\n${stderr}`);
    }

    return binaryPath;
  })();

  return binaryPathPromise;
};

const waitUntilHealthy = async (url: string, token: string, proc: Bun.Subprocess): Promise<void> => {
  for (let i = 0; i < 300; i++) {
    if (proc.exitCode !== null) {
      const stderr = await readText(proc.stderr);
      throw new Error(`rsql exited early with code ${proc.exitCode}:\n${stderr}`);
    }

    try {
      const res = await fetch(`${url}/healthz`, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });
      if (res.status === 200) {
        return;
      }
    } catch {
      // retry
    }
    await Bun.sleep(50);
  }

  const stderr = await readText(proc.stderr);
  throw new Error(`rsql server did not become healthy in time.\n${stderr}`);
};

const waitUntilPprofReady = async (pprofURL: string, proc: Bun.Subprocess): Promise<void> => {
  for (let i = 0; i < 200; i++) {
    if (proc.exitCode !== null) {
      const stderr = await readText(proc.stderr);
      throw new Error(`rsql exited before pprof became ready with code ${proc.exitCode}:\n${stderr}`);
    }

    try {
      const res = await fetch(`${pprofURL}/debug/pprof/`);
      if (res.status === 200) {
        return;
      }
    } catch {
      // retry
    }

    await Bun.sleep(50);
  }

  throw new Error(`pprof did not become ready in time at ${pprofURL}`);
};

const getFreePort = async (): Promise<number> => {
  const server = createServer();

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("failed to determine free port");
  }

  const { port } = address;

  await new Promise<void>((resolve) => {
    server.close(() => resolve());
  });

  return port;
};

const readText = async (stream: ReadableStream<Uint8Array> | number | null | undefined): Promise<string> => {
  if (!stream || typeof stream === "number") {
    return "";
  }
  return new Response(stream).text();
};
