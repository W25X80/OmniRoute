import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { readNativeScratchDataDir } from "../../src/lib/db/backendSelection.ts";

const suites = [
  "postgresArtifactAuthority.test.ts",
  "postgresLiveWsScope.test.ts",
  "postgresRuntimeOwnedWork.test.ts",
];

for (const fixture of [
  { mode: "success", inherited: false, status: 0, children: 3 },
  { mode: "nonzero", inherited: true, status: 1, children: 3 },
  { mode: "spawn-error", inherited: true, status: 1, children: 1 },
  { mode: "cleanup-error", inherited: true, status: 1, children: 3 },
  { mode: "spawn-error-cleanup-error", inherited: true, status: 1, children: 1 },
  { mode: "nonzero-cleanup-error", inherited: true, status: 1, children: 3 },
  { mode: "readiness-spawn-error", inherited: true, status: 0, children: 3 },
  { mode: "command-output-overflow", inherited: true, status: 1, children: 0 },
]) {
  test(`PostgreSQL runner owns suite scratch on ${fixture.mode}`, { timeout: 20_000 }, () => {
    assert.throws(() => readNativeScratchDataDir({}), /explicit DATA_DIR/);
    const root = mkdtempSync(join(tmpdir(), "omniroute-runner-scratch-"));
    const inherited = join(root, "operator-data");
    const temporary = join(root, "temporary");
    const report = join(root, "report.jsonl");
    const events = join(root, "events.jsonl");
    mkdirSync(inherited);
    mkdirSync(temporary);
    writeFileSync(join(inherited, "sentinel"), "retain operator data");
    const env: NodeJS.ProcessEnv = {
      ...process.env,
      TMPDIR: temporary,
      TMP: temporary,
      TEMP: temporary,
      OMNIROUTE_RUNNER_FIXTURE_ROOT: temporary,
      OMNIROUTE_RUNNER_FIXTURE_REPORT: report,
      OMNIROUTE_RUNNER_FIXTURE_EVENTS: events,
      OMNIROUTE_RUNNER_FIXTURE_MODE: fixture.mode,
    };
    if (fixture.inherited) env.DATA_DIR = inherited;
    else delete env.DATA_DIR;
    try {
      const result = spawnSync(
        process.execPath,
        [
          "--import",
          "tsx/esm",
          "--import",
          new URL("../helpers/persistence/postgresRunnerScratchFixture.mjs", import.meta.url).href,
          fileURLToPath(new URL("../../scripts/test/postgres-runtime.mjs", import.meta.url)),
          ...suites,
        ],
        { cwd: process.cwd(), env, encoding: "utf8", timeout: 15_000 }
      );
      assert.ifError(result.error);
      assert.equal(result.status, fixture.status, result.stderr);
      assert.equal(
        existsSync(report),
        fixture.children > 0,
        "the suite process port must be reached only after preparation"
      );
      const children = existsSync(report)
        ? readFileSync(report, "utf8")
            .trim()
            .split("\n")
            .map((line) => JSON.parse(line) as { dataDir: string; test: string })
        : [];
      assert.equal(children.length, fixture.children);
      assert.equal(new Set(children.map((child) => child.dataDir)).size, fixture.children);
      for (const child of children) assert.equal(existsSync(child.dataDir), false);
      assert.deepEqual(
        readdirSync(temporary).filter((name) => name.startsWith("omniroute-pg-test-")),
        []
      );
      assert.deepEqual(readdirSync(inherited), ["sentinel"]);
      assert.equal(readFileSync(join(inherited, "sentinel"), "utf8"), "retain operator data");
      if (fixture.mode.startsWith("spawn-error"))
        assert.match(result.stderr, /fixture spawn failure/);
      if (fixture.mode.endsWith("cleanup-error"))
        assert.match(result.stderr, /fixture Docker removal failure/);
      if (fixture.mode === "command-output-overflow")
        assert.match(result.stderr, /command buffer limit/);
      if (fixture.mode === "readiness-spawn-error") {
        const calls = readFileSync(events, "utf8")
          .trim()
          .split("\n")
          .map((line) => JSON.parse(line) as { args?: string[] });
        assert.equal(calls.filter((entry) => entry.args?.includes("pg_isready")).length, 2);
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
}

type SignalEvent = {
  event: string;
  phase?: string;
  pid?: number;
  signal?: NodeJS.Signals;
  command?: string;
  args?: string[];
  dataDir?: string;
};
const alive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

for (const scenario of [
  ...["preparation", "create", "readiness", "suite"].flatMap((phase) =>
    (["SIGINT", "SIGTERM"] as const).map((signal) => ({ phase, signal }))
  ),
  { phase: "suite", signal: "SIGINT" as const, repeat: true },
  { phase: "suite", signal: "SIGTERM" as const, ignore: true },
  { phase: "create", signal: "SIGTERM" as const, ignore: true },
  { phase: "suite", signal: "SIGTERM" as const, cleanupError: true },
]) {
  const flags = scenario as typeof scenario & {
    repeat?: boolean;
    ignore?: boolean;
    cleanupError?: boolean;
  };
  test(
    `PostgreSQL runner joins and cleans ${scenario.phase} on ${scenario.signal}${flags.repeat ? " twice" : ""}${flags.ignore ? " with escalation" : ""}${flags.cleanupError ? " with cleanup failure" : ""}`,
    { timeout: 20_000 },
    async () => {
      const root = mkdtempSync(join(tmpdir(), "omniroute-runner-signal-"));
      const temporary = join(root, "temporary"),
        inherited = join(root, "operator");
      const report = join(root, "report.jsonl"),
        events = join(root, "events.jsonl");
      const container = join(root, "container");
      mkdirSync(temporary);
      mkdirSync(inherited);
      const otherRun = join(temporary, "omniroute-pg-test-other-run");
      mkdirSync(otherRun);
      writeFileSync(join(otherRun, "sentinel"), "retain another run");
      writeFileSync(join(inherited, "sentinel"), "retain operator data");
      const observations = (): SignalEvent[] =>
        existsSync(events)
          ? readFileSync(events, "utf8")
              .trim()
              .split("\n")
              .map((line) => JSON.parse(line))
          : [];
      const child = spawn(
        process.execPath,
        [
          "--import",
          "tsx/esm",
          "--import",
          new URL("../helpers/persistence/postgresRunnerScratchFixture.mjs", import.meta.url).href,
          fileURLToPath(new URL("../../scripts/test/postgres-runtime.mjs", import.meta.url)),
          ...suites,
        ],
        {
          cwd: process.cwd(),
          stdio: ["ignore", "ignore", "pipe"],
          env: {
            ...process.env,
            TMPDIR: temporary,
            TMP: temporary,
            TEMP: temporary,
            DATA_DIR: inherited,
            OMNIROUTE_RUNNER_FIXTURE_ROOT: temporary,
            OMNIROUTE_RUNNER_FIXTURE_REPORT: report,
            OMNIROUTE_RUNNER_FIXTURE_EVENTS: events,
            OMNIROUTE_RUNNER_FIXTURE_CONTAINER: container,
            OMNIROUTE_RUNNER_FIXTURE_PHASE: scenario.phase,
            OMNIROUTE_RUNNER_FIXTURE_MODE: flags.cleanupError ? "cleanup-error" : "success",
            OMNIROUTE_RUNNER_FIXTURE_IGNORE_SIGNAL: flags.ignore ? "1" : "0",
          },
        }
      );
      let stderr = "";
      child.stderr.on("data", (chunk) => {
        stderr += chunk;
      });
      let finished = false;
      const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
        (resolve, reject) => {
          child.once("error", reject);
          child.once("close", (code, signal) => {
            finished = true;
            resolve({ code, signal });
          });
        }
      );
      const waitFor = async (predicate: () => boolean) => {
        const deadline = Date.now() + 10_000;
        while (!predicate()) {
          assert.equal(finished, false, stderr);
          assert.ok(Date.now() < deadline, "fixture barrier not reached: " + stderr);
          await delay(10);
        }
      };
      try {
        await waitFor(() => observations().some((event) => event.event === "ready"));
        assert.equal(child.kill(scenario.signal), true);
        if (flags.repeat) {
          await waitFor(() => observations().some((event) => event.event === "signal"));
          assert.equal(child.kill("SIGTERM"), true);
        }
        await waitFor(() => finished);
        const result = await closed;
        assert.deepEqual(result, { code: null, signal: scenario.signal }, stderr);
        const seen = observations();
        const started = seen.filter((event) => event.event === "spawn");
        assert.equal(started.length, 1);
        const pid = started[0].pid!;
        assert.equal(alive(pid), false, "the real command/suite child must be joined");
        const closeIndex = seen.findIndex((event) => event.event === "close" && event.pid === pid);
        assert.ok(closeIndex >= 0, "actual close must precede resource cleanup");
        const removals = seen.filter(
          (event) => event.event === "command" && event.args?.[0] === "rm"
        );
        assert.equal(removals.length, scenario.phase === "preparation" ? 0 : 1);
        if (removals.length) {
          assert.ok(seen.indexOf(removals[0]) > closeIndex);
          const creation = seen.find(
            (event) => event.event === "command" && event.args?.[0] === "create"
          )!;
          assert.equal(removals[0].args!.at(-1), creation.args![2]);
        }
        assert.equal(existsSync(container), Boolean(flags.cleanupError));
        assert.deepEqual(
          readdirSync(temporary).filter((name) => name.startsWith("omniroute-pg-test-")),
          ["omniroute-pg-test-other-run"]
        );
        assert.equal(readFileSync(join(otherRun, "sentinel"), "utf8"), "retain another run");
        assert.deepEqual(readdirSync(inherited), ["sentinel"]);
        assert.equal(readFileSync(join(inherited, "sentinel"), "utf8"), "retain operator data");
        const suitesStarted = existsSync(report)
          ? readFileSync(report, "utf8").trim().split("\n")
          : [];
        assert.equal(suitesStarted.length, scenario.phase === "suite" ? 1 : 0);
        assert.equal(
          seen.filter((event) => event.event === "kill" && event.signal === scenario.signal).length,
          1
        );
        if (flags.ignore)
          assert.equal(
            seen.some((event) => event.event === "close" && event.signal === "SIGKILL"),
            true
          );
        if (flags.cleanupError) assert.match(stderr, /fixture Docker removal failure/);
      } finally {
        if (!finished) {
          child.kill("SIGKILL");
          await closed;
        }
        for (const event of observations().filter((event) => event.event === "spawn")) {
          if (event.pid && alive(event.pid)) {
            process.kill(event.pid, "SIGKILL");
            const deadline = Date.now() + 3000;
            while (alive(event.pid) && Date.now() < deadline) await delay(10);
            assert.equal(alive(event.pid), false, "fixture child cleanup failed");
          }
        }
        rmSync(root, { recursive: true, force: true });
      }
    }
  );
}
