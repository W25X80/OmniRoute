import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const fixture = fileURLToPath(
  new URL("../fixtures/early-stream-finalizers.fixture.ts", import.meta.url)
);

test("stream finalizers are installed before awaited response hooks in the actual chat pipeline", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "omniroute-early-finalizers-"));
  const data = path.join(root, "data");
  const plugins = path.join(root, "plugins");
  mkdirSync(data);
  mkdirSync(plugins);
  try {
    const result = await new Promise<{
      code: number | null;
      signal: NodeJS.Signals | null;
      output: string;
    }>((resolve, reject) => {
      const child = spawn(
        process.execPath,
        ["--experimental-test-module-mocks", "--import", "tsx/esm", "--test", fixture],
        {
          cwd: process.cwd(),
          env: {
            PATH: process.env.PATH,
            LANG: "C.UTF-8",
            NODE_ENV: "test",
            APP_LOG_TO_FILE: "false",
            OMNIROUTE_DATABASE_BACKEND: "sqlite",
            DATA_DIR: data,
            OMNIROUTE_PLUGINS_DIR: plugins,
            API_KEY_SECRET: "early-finalizer-fixture-secret-00000000000000000000000000000000",
            STREAM_DISCONNECT_GRACE_PERIOD_MS: "0",
          },
          stdio: ["ignore", "pipe", "pipe"],
          timeout: 60_000,
        }
      );
      let output = "";
      child.stdout.setEncoding("utf8").on("data", (chunk: string) => {
        output += chunk;
      });
      child.stderr.setEncoding("utf8").on("data", (chunk: string) => {
        output += chunk;
      });
      child.once("error", reject);
      child.once("close", (code, signal) => resolve({ code, signal, output }));
    });
    assert.equal(result.code, 0, result.output);
    assert.equal(result.signal, null);
    assert.match(result.output, /ℹ tests 3/);
    assert.match(result.output, /ℹ pass 3/);
    assert.match(result.output, /ℹ fail 0/);
  } finally {
    rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  }
});
