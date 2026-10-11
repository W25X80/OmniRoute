import { NextRequest } from "next/server";
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { registerHooks, syncBuiltinESMExports } from "node:module";

// Only management-auth I/O is replaced. Production route/filter/file code remains real.
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (specifier === "@/lib/api/requireManagementAuth") {
      return {
        url: new URL("../../helpers/persistence/servingFileIoFixture.ts", import.meta.url).href,
        shortCircuit: true,
      };
    }
    return next(specifier, context);
  },
});
const { servingFileIoFixture } = await import("../../helpers/persistence/servingFileIoFixture.ts");
const directory = await fsp.mkdtemp(path.join(os.tmpdir(), "omniroute-native-serving-files-"));
const originalBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
const originalDataDir = process.env.DATA_DIR;
const originalLog = process.env.APP_LOG_FILE_PATH;
process.env.DATA_DIR = directory;
process.env.APP_LOG_FILE_PATH = path.join(directory, "app.log");
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
const consoleRoute = await import("../../../src/app/api/logs/console/route.ts");
const openApiRoute = await import("../../../src/app/api/openapi/spec/route.ts");
const raw = await import("../../../open-sse/services/compression/engines/rtk/rawOutput.ts");
const loader = await import("../../../open-sse/services/compression/engines/rtk/filterLoader.ts");
const toml =
  await import("../../../open-sse/services/compression/engines/rtk/tomlCompatibility.ts");
const rawRoute = await import("../../../src/app/api/context/rtk/raw-output/[id]/route.ts");
const filtersRoute = await import("../../../src/app/api/context/rtk/filters/route.ts");
const importRoute = await import("../../../src/app/api/context/rtk/import/route.ts");

function forbidSyncFiles() {
  const originals = new Map<string, unknown>();
  let calls = 0;
  for (const name of [
    "readFileSync",
    "writeFileSync",
    "appendFileSync",
    "mkdirSync",
    "readdirSync",
    "statSync",
    "existsSync",
    "renameSync",
    "chmodSync",
    "copyFileSync",
    "rmSync",
  ]) {
    originals.set(name, Reflect.get(fs, name));
    Reflect.set(fs, name, () => {
      calls++;
      throw new Error("Unexpected synchronous file operation");
    });
  }
  syncBuiltinESMExports();
  return {
    calls: () => calls,
    restore() {
      for (const [name, original] of originals) Reflect.set(fs, name, original);
      syncBuiltinESMExports();
    },
  };
}
const SAMPLE = `schema_version = 1
[filters.fixture]
match_command = "^fixture"
strip_lines_matching = ["^noise"]
`;

test.after(async () => {
  hooks.deregister();
  if (originalBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
  else process.env.OMNIROUTE_DATABASE_BACKEND = originalBackend;
  if (originalDataDir === undefined) delete process.env.DATA_DIR;
  else process.env.DATA_DIR = originalDataDir;
  if (originalLog === undefined) delete process.env.APP_LOG_FILE_PATH;
  else process.env.APP_LOG_FILE_PATH = originalLog;
  await fsp.rm(directory, { recursive: true, force: true });
});

test("native console reader preserves SQLite normalization/filter/order/lastN and yields", async () => {
  const now = new Date().toISOString();
  const records = Array.from({ length: 400 }, (_, i) =>
    JSON.stringify({
      time: now,
      level: i % 2 ? 40 : 30,
      component: "router",
      msg: { i },
      correlationId: i,
    })
  );
  records.splice(10, 0, "not-json", '{"msg":"bare CR"}\r{"msg":"second"}');
  records.push(
    JSON.stringify({
      timestamp: new Date(Date.now() - 7200000).toISOString(),
      level: "fatal",
      message: "old",
    })
  );
  await fsp.writeFile(process.env.APP_LOG_FILE_PATH!, records.join("\n") + "\n", "utf8");
  const request = () =>
    new NextRequest("http://localhost/api/logs/console?level=warn&limit=7&component=router");
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  const expected = await (await consoleRoute.GET(request())).json();
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const guard = forbidSyncFiles();
  let yielded = false;
  const tick = new Promise<void>((resolve) =>
    setImmediate(() => {
      yielded = true;
      resolve();
    })
  );
  try {
    const response = await consoleRoute.GET(request());
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), expected);
    assert.equal(yielded, true);
    assert.equal(guard.calls(), 0);
    assert.equal(response.headers.get("cache-control"), "no-store, no-cache, must-revalidate");
  } finally {
    guard.restore();
    await tick;
  }
});

test("native console missing file and auth denial preserve response behavior without file effects", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  await fsp.rm(process.env.APP_LOG_FILE_PATH!, { force: true });
  const guard = forbidSyncFiles();
  try {
    const missing = await consoleRoute.GET(new NextRequest("http://localhost/api/logs/console"));
    assert.equal(missing.status, 200);
    assert.deepEqual(await missing.json(), []);
    servingFileIoFixture.authError = new Response("denied", { status: 401 });
    const denied = await consoleRoute.GET(new NextRequest("http://localhost/api/logs/console"));
    assert.equal(denied, servingFileIoFixture.authError);
    assert.equal(guard.calls(), 0);
  } finally {
    servingFileIoFixture.authError = null;
    guard.restore();
  }
});

test("native OpenAPI reader returns the same catalog with no synchronous filesystem probe", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  const expected = await (await openApiRoute.GET()).json();
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const guard = forbidSyncFiles();
  try {
    const [first, second] = await Promise.all([openApiRoute.GET(), openApiRoute.GET()]);
    assert.equal(first.status, 200);
    assert.equal(second.status, 200);
    assert.deepEqual(await first.json(), expected);
    assert.deepEqual(await second.json(), expected);
    assert.equal(guard.calls(), 0);
  } finally {
    guard.restore();
  }
});

test("RTK async recovery and samples retain bucket/legacy/metadata/recent-order parity", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  const pointer = raw.maybePersistRtkRawOutput("secret token=private\nfixture output", {
    retention: "always",
    command: "fixture build",
  });
  assert.ok(pointer);
  const flat = path.join(
    directory,
    "rtk",
    "raw-output",
    "1-legacy_name-000000000000000000000001.log"
  );
  await fsp.writeFile(flat, "legacy", "utf8");
  const expected = raw.listRtkCommandSamples({ limit: 500 });
  const text = raw.readRtkRawOutput(pointer.id);
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const guard = forbidSyncFiles();
  try {
    assert.equal(await raw.readRtkRawOutputAsync(pointer.id), text);
    assert.equal(await raw.readRtkRawOutputAsync("000000000000000000000001"), "legacy");
    assert.deepEqual(await raw.listRtkCommandSamplesAsync({ limit: 500 }), expected);
    const response = await rawRoute.GET(
      new NextRequest("http://localhost/api/context/rtk/raw-output/id"),
      { params: Promise.resolve({ id: pointer.id }) }
    );
    assert.equal(response.status, 200);
    assert.equal(await response.text(), text);
    assert.equal(guard.calls(), 0);
  } finally {
    guard.restore();
  }
});

test("native RTK install serializes overwrite checks and preserves backup/content/cache semantics", async () => {
  const previous = process.env.DATA_DIR;
  const installDir = await fsp.mkdtemp(path.join(os.tmpdir(), "omniroute-native-rtk-install-"));
  process.env.DATA_DIR = installDir;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const guard = forbidSyncFiles();
  try {
    const outcomes = await Promise.allSettled([
      toml.installGlobalRtkTomlV1Async(SAMPLE),
      toml.installGlobalRtkTomlV1Async(SAMPLE),
    ]);
    assert.equal(outcomes.filter((outcome) => outcome.status === "fulfilled").length, 1);
    assert.equal(outcomes.filter((outcome) => outcome.status === "rejected").length, 1);
    const changed = SAMPLE.replace("^fixture", "^changed");
    const replacing = await toml.installGlobalRtkTomlV1Async(changed, {
      overwrite: true,
    });
    assert.equal(replacing.backupCreated, true);
    assert.equal(
      await fsp.readFile(path.join(installDir, "rtk", "filters.toml.bak"), "utf8"),
      SAMPLE
    );
    assert.equal(await fsp.readFile(path.join(installDir, "rtk", "filters.toml"), "utf8"), changed);
    await loader.loadRtkFiltersAsync({ refresh: true });
    const response = await filtersRoute.GET(
      new NextRequest("http://localhost/api/context/rtk/filters")
    );
    assert.equal(response.status, 200);
    const snapshot = (await response.json()) as {
      filters: Array<{ id: string; source: string }>;
    };
    assert.ok(
      snapshot.filters.some((filter) => filter.id === "fixture" && filter.source === "global")
    );
    const installed = await importRoute.POST(
      new NextRequest("http://localhost/api/context/rtk/import", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: "install",
          content: SAMPLE,
          overwrite: true,
        }),
      })
    );
    assert.equal(installed.status, 200);
    assert.equal(guard.calls(), 0);
  } finally {
    guard.restore();
    process.env.DATA_DIR = previous;
    await fsp.rm(installDir, { recursive: true, force: true });
  }
});

test("native project trust authorizes the exact loaded bytes and matches source filter precedence", async () => {
  const previous = process.cwd();
  const project = await fsp.mkdtemp(path.join(os.tmpdir(), "omniroute-native-rtk-project-"));
  await fsp.mkdir(path.join(project, ".rtk"));
  await fsp.writeFile(path.join(project, ".rtk", "filters.toml"), SAMPLE);
  await fsp.writeFile(
    path.join(project, ".rtk", "trust.json"),
    JSON.stringify({
      filtersTomlSha256: crypto.createHash("sha256").update(SAMPLE).digest("hex"),
    })
  );
  process.chdir(project);
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  const expected = loader.loadRtkFilters({ refresh: true });
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const guard = forbidSyncFiles();
  try {
    assert.deepEqual(await loader.loadRtkFiltersAsync({ refresh: true }), expected);
    await fsp.writeFile(
      path.join(project, ".rtk", "filters.toml"),
      SAMPLE.replace("^fixture", "^changed")
    );
    await loader.loadRtkFiltersAsync({ refresh: true });
    const snapshot = await loader.getRtkFilterCatalogSnapshotAsync();
    assert.equal(
      snapshot.filters.some((filter) => filter.source === "project"),
      false
    );
    assert.ok(snapshot.diagnostics.some((entry) => entry.message.includes("changed after trust")));
    assert.equal(guard.calls(), 0);
  } finally {
    guard.restore();
    process.chdir(previous);
    await fsp.rm(project, { recursive: true, force: true });
  }
});
