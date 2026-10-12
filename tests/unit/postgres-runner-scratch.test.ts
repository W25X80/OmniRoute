import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
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
]) {
  test(`PostgreSQL runner owns suite scratch on ${fixture.mode}`, { timeout: 20_000 }, () => {
    assert.throws(() => readNativeScratchDataDir({}), /explicit DATA_DIR/);
    const root = mkdtempSync(join(tmpdir(), "omniroute-runner-scratch-"));
    const inherited = join(root, "operator-data");
    const temporary = join(root, "temporary");
    const report = join(root, "report.jsonl");
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
      assert.ok(existsSync(report), "the suite process port must be reached");
      const children = readFileSync(report, "utf8")
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as { dataDir: string; test: string });
      assert.equal(children.length, fixture.children);
      assert.equal(new Set(children.map((child) => child.dataDir)).size, fixture.children);
      for (const child of children) assert.equal(existsSync(child.dataDir), false);
      assert.deepEqual(
        readdirSync(temporary).filter((name) => name.startsWith("omniroute-pg-test-")),
        []
      );
      assert.deepEqual(readdirSync(inherited), ["sentinel"]);
      assert.equal(readFileSync(join(inherited, "sentinel"), "utf8"), "retain operator data");
      if (fixture.mode === "spawn-error") assert.match(result.stderr, /fixture spawn failure/);
      if (fixture.mode === "cleanup-error")
        assert.match(result.stderr, /fixture Docker removal failure/);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
}
