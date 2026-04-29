import { mkdtemp } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

export interface RunningServer {
  url: string;
  token: string;
  dataDir: string;
  stop: () => Promise<void>;
}

const repoRoot = fileURLToPath(new URL("../../..", import.meta.url));
let binaryPathPromise: Promise<string> | null = null;

export const startServer = async (): Promise<RunningServer> => {
  const binaryPath = await buildBinary();
  const port = await getFreePort();
  const token = `token_${Date.now()}`;
  const dataDir = await mkdtemp(join(tmpdir(), "rsql-client-e2e-"));
  const url = `http://127.0.0.1:${port}`;

  const proc = Bun.spawn([
    binaryPath,
    "serve",
    "--listen",
    `127.0.0.1:${port}`,
    "--data-dir",
    dataDir,
    "--api-token",
    token,
    "--log-level",
    "error",
  ], {
    cwd: repoRoot,
    stdout: "pipe",
    stderr: "pipe",
  });

  await waitUntilHealthy(url, token, proc);

  return {
    url,
    token,
    dataDir,
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
    const buildDir = await mkdtemp(join(tmpdir(), "rsql-client-bin-"));
    const binaryPath = join(buildDir, "rsql-test");

    const build = Bun.spawn(["go", "build", "-o", binaryPath, "./cmd/rsql"], {
      cwd: repoRoot,
      stdout: "pipe",
      stderr: "pipe",
    });

    const exitCode = await build.exited;
    if (exitCode !== 0) {
      const stderr = await readText(build.stderr);
      throw new Error(`failed to build rsql binary:\\n${stderr}`);
    }
    return binaryPath;
  })();

  return binaryPathPromise;
};

const waitUntilHealthy = async (url: string, token: string, proc: Bun.Subprocess): Promise<void> => {
  const maxAttempts = 200;

  for (let i = 0; i < maxAttempts; i++) {
    if (proc.exitCode !== null) {
      const stderr = await readText(proc.stderr);
      throw new Error(`rsql server exited early with code ${proc.exitCode}:\n${stderr}`);
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
      // Keep waiting.
    }

    await Bun.sleep(50);
  }

  const stderr = await readText(proc.stderr);
  throw new Error(`rsql server did not become healthy in time.\n${stderr}`);
};

const getFreePort = async (): Promise<number> => {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      resolve();
    });
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("failed to resolve free port");
  }

  const port = address.port;
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
