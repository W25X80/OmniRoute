import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, mkdir, writeFile, rm, realpath } from "node:fs/promises";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const loader = createRequire(import.meta.url).resolve("tsx/esm");
const fixture = fileURLToPath(
  new URL("../../helpers/persistence/nativeCoreColdImportFixture.ts", import.meta.url)
);
const tsconfig = fileURLToPath(new URL("../../../tsconfig.json", import.meta.url));

interface ColdImportResult {
  scenario: string;
  configuredPath: string | null;
  operations: Array<{ method: string; path: string | null }>;
}

async function run(
  scenario: string,
  configured = " scratch/../local scratch "
): Promise<ColdImportResult> {
  const directory = await realpath(
    await mkdtemp(path.join(os.tmpdir(), "omniroute-native-core-cold-"))
  );
  const firstCarrier = path.join(directory, "first-carrier");
  const secondCarrier = path.join(directory, "second-carrier");
  try {
    await mkdir(firstCarrier);
    await mkdir(secondCarrier);
    await writeFile(
      path.join(firstCarrier, "001_first_carrier.sql"),
      "CREATE TABLE first_fixture (id TEXT);\n"
    );
    await writeFile(
      path.join(secondCarrier, "002_second_carrier.sql"),
      "CREATE TABLE second_fixture (id TEXT);\n"
    );
    const output = await new Promise<string>((resolve, reject) => {
      execFile(
        process.execPath,
        ["--import", loader, fixture, scenario, firstCarrier, secondCarrier],
        {
          cwd: directory,
          env: {
            ...process.env,
            DATA_DIR: configured,
            OMNIROUTE_DATABASE_BACKEND: scenario.startsWith("native-") ? "postgres" : "sqlite",
            OMNIROUTE_EXTRA_MIGRATIONS_DIRS: "",
            TSX_TSCONFIG_PATH: tsconfig,
          },
          timeout: 30000,
          maxBuffer: 1024 * 1024,
        },
        (error, stdout, stderr) => {
          if (error) reject(new Error(`${scenario}: ${error.message}\n${stderr}\n${stdout}`));
          else resolve(stdout);
        }
      );
    });
    const marker = output.split("\n").find((line) => line.startsWith("CORE_COLD_RESULT="));
    assert.ok(marker, output);
    const result = JSON.parse(marker.slice("CORE_COLD_RESULT=".length)) as ColdImportResult;
    assert.equal(result.scenario, scenario);
    if (result.configuredPath)
      assert.equal(result.configuredPath, path.resolve(directory, configured.trim()));
    return result;
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

test("native ready scope can import two cold core copies without application synchronous filesystem work", async () => {
  assert.deepEqual((await run("native-configured")).operations, []);
});

test("native configured scratch preserves literal tilde and relative path normalization", async () => {
  assert.deepEqual((await run("native-configured", " ~/scratch ")).operations, []);
});

test("native missing or blank scratch fails startup before transports or filesystem fallback", async () => {
  assert.deepEqual((await run("native-missing")).operations, []);
  assert.deepEqual((await run("native-blank")).operations, []);
});

test("native SQLite migration carrier is resolved once only when its explicit owner is invoked", async () => {
  const result = await run("native-migration-lazy");
  assert.ok(result.operations.some((entry) => entry.method === "readdirSync"));
});

test("default SQLite keeps its eager directory creation policy", async () => {
  const result = await run("sqlite-configured");
  assert.ok(result.operations.some((entry) => entry.method === "mkdirSync"));
});

test("operation-scoped native factory under envsqlite retains default module initialization and SQLite rejection", async () => {
  const result = await run("context-only");
  assert.ok(result.operations.some((entry) => entry.method === "mkdirSync"));
});

test("default SQLite keeps the migration directory snapshot from module evaluation", async () => {
  await run("sqlite-migration-snapshot");
});
