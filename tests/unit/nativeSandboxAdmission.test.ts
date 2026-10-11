import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import type { ChildProcess } from "node:child_process";
import { mkdtempSync, writeFileSync, readFileSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ALL_PROVIDERS,
  resolveProvider,
  _resetProviderCacheForTests,
} from "../../src/lib/skills/containerProvider";
import { sandboxRunner } from "../../src/lib/skills/sandbox";
import { skillExecutor } from "../../src/lib/skills/executor";
import { skillRegistry } from "../../src/lib/skills/registry";
import type { Skill } from "../../src/lib/skills/types";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext";
import { stopNativeHostProbes } from "../../src/lib/nativeHostProbes";

const require = createRequire(import.meta.url);
const childProcess = require("node:child_process") as typeof import("node:child_process");
const directory = mkdtempSync(join(tmpdir(), "omniroute-native-sandbox-"));
const originalEnv = {
  PATH: process.env.PATH,
  backend: process.env.OMNIROUTE_DATABASE_BACKEND,
  runtime: process.env.SKILLS_SANDBOX_RUNTIME,
};
function executable(name: string, source: string): void {
  writeFileSync(join(directory, name), `#!${process.execPath}\n${source}\n`, { mode: 0o700 });
}
test.after(async () => {
  await stopNativeHostProbes();
  for (const [name, value] of [
    ["PATH", originalEnv.PATH],
    ["OMNIROUTE_DATABASE_BACKEND", originalEnv.backend],
    ["SKILLS_SANDBOX_RUNTIME", originalEnv.runtime],
  ] as const) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  rmSync(directory, { recursive: true, force: true });
});

test("native async detector advances a causal parent marker where the legacy sync probe cannot", async () => {
  process.env.PATH = directory;
  const marker = join(directory, "release");
  executable(
    "which",
    'process.exit(require("node:fs").existsSync(require("node:path").join(process.env.PATH, process.argv[2])) ? 0 : 1);'
  );
  executable(
    "docker",
    `const fs = require("node:fs"); const end = Date.now() + 100;
    const tick = () => { if (fs.existsSync(${JSON.stringify(marker)})) console.log("docker v1.2.3");
      else if (Date.now() > end) process.exit(1); else setTimeout(tick, 1); }; tick();`
  );
  const provider = ALL_PROVIDERS.find((item) => item.id === "docker")!;
  let markerAdvanced = false;
  setImmediate(() => {
    markerAdvanced = true;
    writeFileSync(marker, "ready");
  });
  assert.equal(
    provider.detect(),
    false,
    "source synchronous probe blocks the marker until child failure"
  );
  assert.equal(markerAdvanced, false);
  assert.equal(
    await provider.detectAsync!(),
    true,
    "async probe lets the marker release the child"
  );
  assert.equal(markerAdvanced, true);
});

test("native skill deadline is admitted before shared detection and prevents a late sandbox spawn", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.SKILLS_SANDBOX_RUNTIME = "docker";
  process.env.PATH = directory;
  const log = join(directory, "runs");
  for (const command of ["docker", "container", "wslc", "orbstack", "podman"]) {
    executable(
      command,
      `if (process.argv[2] === "--version") setTimeout(() => console.log("container v1.2.3"), 100);
      else { require("node:fs").appendFileSync(${JSON.stringify(log)}, JSON.stringify(process.argv.slice(2)) + "\\n"); console.log("ok"); }`
    );
  }
  _resetProviderCacheForTests();
  const originalSync = childProcess.spawnSync;
  const originalSpawn = childProcess.spawn;
  let runtimeSpawns = 0;
  const running: ChildProcess[] = [];
  childProcess.spawn = ((...args: Parameters<typeof childProcess.spawn>) => {
    const proc = originalSpawn(...args);
    if (args[1]?.[0] === "run") {
      runtimeSpawns += 1;
      running.push(proc);
    }
    return proc;
  }) as typeof childProcess.spawn;
  childProcess.spawnSync = (() => {
    throw new Error("sync native detection is forbidden");
  }) as typeof childProcess.spawnSync;
  const executor = skillExecutor as unknown as {
    runHandler(
      name: string,
      input: Record<string, unknown>,
      context: { apiKeyId: string; sessionId: string }
    ): Promise<{ status: string; errorMessage: string | null }>;
    setTimeout(value: number): void;
  };
  executor.setTimeout(10);
  try {
    // This unit owns only the already-loaded registry cache and predispatch boundary, not PG persistence.
    const binding = {
      assertReady() {},
      databaseIdentity: {},
    } as unknown as PostgresControlPlaneBinding;
    await withPostgresControlPlane(binding, async () => {
      const registry = skillRegistry as unknown as { registeredSkills: Map<string, Skill> };
      registry.registeredSkills.set("fixture:eval@1", {
        id: "fixture-skill",
        apiKeyId: "fixture",
        name: "eval",
        version: "1",
        description: "fixture",
        schema: { input: {}, output: {} },
        handler: "eval_code",
        enabled: true,
        createdAt: new Date(),
        updatedAt: new Date(),
      });
      const result = await executor.runHandler(
        "eval",
        { code: "1", language: "javascript" },
        { apiKeyId: "fixture", sessionId: "" }
      );
      assert.equal(result.status, "error");
      assert.match(result.errorMessage!, /Skill execution timed out/);
    });
    const shared = await resolveProvider();
    assert.equal(shared.id, "docker", "a caller timeout did not cancel shared runtime discovery");
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(existsSync(log), false, "expired handler cannot spawn after discovery settles");
    assert.equal(
      runtimeSpawns,
      0,
      "no actual runtime process was created after the native deadline"
    );
    assert.equal(sandboxRunner.getRunningCount(), 0);
    const result = await sandboxRunner.run("node:22-alpine", ["node", "--eval", "1"]);
    assert.equal(result.exitCode, 0);
    const argv = JSON.parse(readFileSync(log, "utf8").trim()) as string[];
    assert.ok(argv.includes("--cap-drop") && argv.includes("ALL") && argv.includes("--read-only"));
    assert.equal(argv[argv.indexOf("--network") + 1], "none");
  } finally {
    childProcess.spawnSync = originalSync;
    childProcess.spawn = originalSpawn;
    await Promise.all(
      running.map((proc) =>
        proc.exitCode !== null || proc.signalCode !== null
          ? Promise.resolve()
          : new Promise<void>((done) => proc.once("close", () => done()))
      )
    );
    executor.setTimeout(30000);
  }
});
