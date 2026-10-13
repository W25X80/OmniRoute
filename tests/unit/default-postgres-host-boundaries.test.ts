import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import path from "node:path";
import vm from "node:vm";
import test from "node:test";
import { parseDatabaseBackend } from "../../src/lib/db/backendSelection.ts";
import { getShutdownBudgetConfig } from "../../src/shared/utils/runtimeTimeouts.ts";
import { getShutdownBudgetConfig as packagedBudget } from "../../scripts/dev/main-server-timeouts.mjs";
import { isSqlitePersistenceRequested } from "../../scripts/dev/persistence-request-boundary.mjs";

const mitm = readFileSync(new URL("../../src/mitm/server.cjs", import.meta.url), "utf8");
const mitmSelection = mitm.slice(
  mitm.indexOf("const databaseBackend ="),
  mitm.indexOf("// Resolve data directory")
);
const cli = readFileSync(new URL("../../bin/omniroute.mjs", import.meta.url), "utf8");
const cliSelection = cli.match(/const databaseBackend = configuredBackend[\s\S]*?\n\}/)?.[0];
const electron = readFileSync(new URL("../../electron/main.js", import.meta.url), "utf8");
const electronStart = electron.indexOf(
  "const preferredEnvPath =",
  electron.indexOf("function startNextServer()")
);
const electronBootstrap = electron.slice(
  electronStart,
  electron.indexOf("const nodeExecutable =", electronStart)
);

test("TypeScript, packaged HTTP, MITM and shutdown selectors agree on the full backend domain", () => {
  for (const backend of [undefined, "postgres", "sqlite", "", "pg", "Postgres", "postgres "]) {
    const env = backend === undefined ? {} : { OMNIROUTE_DATABASE_BACKEND: backend };
    const nativeMitm = () =>
      vm.runInNewContext(`${mitmSelection}\nNATIVE_PERSISTENCE`, { process: { env } });
    const cliBackend = () =>
      vm.runInNewContext(`${cliSelection}\ndatabaseBackend`, { configuredBackend: backend });
    const selectors = [
      () => parseDatabaseBackend(env),
      () => isSqlitePersistenceRequested(env),
      () => getShutdownBudgetConfig(env),
      () => packagedBudget(env),
      nativeMitm,
      cliBackend,
    ];
    if (backend !== undefined && backend !== "sqlite" && backend !== "postgres") {
      for (const select of selectors) assert.throws(select, /must be sqlite or postgres/);
    } else {
      assert.equal(parseDatabaseBackend(env), backend === "sqlite" ? "sqlite" : "postgres");
      assert.equal(isSqlitePersistenceRequested(env), backend === "sqlite");
      assert.equal(nativeMitm(), backend !== "sqlite");
      assert.equal(cliBackend(), parseDatabaseBackend(env));
      assert.deepEqual(getShutdownBudgetConfig(env), packagedBudget(env));
    }
  }
});

function runCliBootstrap(backend: string | undefined, cwdEnv: string) {
  const files = new Map([
    ["/data/storage.sqlite", "existing-encrypted-database"],
    [
      "/data/server.env",
      "STORAGE_ENCRYPTION_KEY=existing-electron-key\nOMNIROUTE_DATABASE_BACKEND=postgres\n",
    ],
    ["/work/.env", cwdEnv],
  ]);
  const reads: string[] = [],
    writes: string[] = [];
  const env: NodeJS.ProcessEnv = {
    DATA_DIR: "/data",
    OMNIROUTE_CLI_SKIP_DEFAULT_DATA_ENV: "1",
    OMNIROUTE_CLI_SKIP_REPO_ENV: "1",
    ...(backend === undefined ? {} : { OMNIROUTE_DATABASE_BACKEND: backend }),
  };
  const bootstrap = cli.slice(
    cli.indexOf("function migrateElectronServerEnv"),
    cli.indexOf("// Next.js has no android branch")
  );
  const execute = () =>
    vm.runInNewContext(`${bootstrap}\ndatabaseBackend`, {
      process: { env, cwd: () => "/work" },
      ROOT: "/package",
      join: path.posix.join,
      getDefaultDataDir: () => "/home/default",
      existsSync: (file: string) => files.has(file),
      readFileSync: (file: string) => {
        reads.push(file);
        return files.get(file);
      },
      writePrivateFile: (file: string, content: string) => {
        writes.push(file);
        files.set(file, content);
      },
      parseEnvValue: (value: string) => value.trim(),
      describeVolatileEnvWarning: () => null,
      console: { log() {}, warn() {} },
    });
  return { env, files, reads, writes, execute };
}

test("CLI discovers explicit SQLite in cwd .env before restoring higher-priority Electron keys", () => {
  for (const conflictingKey of ["", "STORAGE_ENCRYPTION_KEY=lower-priority-cwd-key\n"]) {
    const state = runCliBootstrap(
      undefined,
      `OMNIROUTE_DATABASE_BACKEND=sqlite\n${conflictingKey}`
    );
    assert.equal(state.execute(), "sqlite");
    assert.equal(state.env.OMNIROUTE_DATABASE_BACKEND, "sqlite");
    assert.equal(state.env.STORAGE_ENCRYPTION_KEY, "existing-electron-key");
    assert.ok(state.reads.includes("/data/server.env"));
    assert.deepEqual(state.writes, ["/data/.env"]);
    assert.doesNotMatch(state.files.get("/data/.env")!, /OMNIROUTE_DATABASE_BACKEND/);
  }
});

test("CLI default native and invalid configured selectors never read legacy secret authority", () => {
  for (const backend of [undefined, "postgres", "", "invalid"]) {
    const state = runCliBootstrap(backend, "");
    if (backend === "" || backend === "invalid") {
      assert.throws(state.execute, /must be sqlite or postgres/);
    } else {
      assert.equal(state.execute(), "postgres");
      assert.equal(state.env.STORAGE_ENCRYPTION_KEY, undefined);
    }
    assert.equal(state.reads.includes("/data/server.env"), false);
    assert.deepEqual(state.writes, []);
  }
});

test("CLI SQLite opener rejects native/default profiles before local filesystem or driver work", async () => {
  const source = readFileSync(new URL("../../bin/cli/sqlite.mjs", import.meta.url), "utf8");
  const opener = source.match(/export async function openOmniRouteDb\(\)[\s\S]*?\n\}/)?.[0];
  assert.ok(opener);
  const guard = source.match(/export function assertSqliteCliBackend\(\)[\s\S]*?\n\}/)?.[0];
  assert.ok(guard);
  for (const backend of [undefined, "postgres", "sqlite", "", "invalid"]) {
    const effects: string[] = [];
    const env = backend === undefined ? {} : { OMNIROUTE_DATABASE_BACKEND: backend };
    const db = {
      pragma() {
        effects.push("pragma");
      },
    };
    const execute = vm.runInNewContext(
      `${guard.replace("export ", "")}; (${opener.replace("export ", "")})`,
      {
        process: { env },
        resolveDataDir: () => {
          effects.push("path");
          return "/data";
        },
        resolveStoragePath: () => "/data/storage.sqlite",
        fs: { mkdirSync: () => effects.push("mkdir") },
        openSqliteDatabase: async () => {
          effects.push("open");
          return db;
        },
        ensureSettingsSchema() {},
        ensureProviderSchema() {},
      }
    );
    if (backend === "sqlite") {
      assert.equal((await execute()).db, db);
      assert.deepEqual(effects, ["path", "mkdir", "open", "pragma"]);
    } else {
      await assert.rejects(execute(), /requires explicit OMNIROUTE_DATABASE_BACKEND=sqlite/);
      assert.deepEqual(effects, []);
    }
  }
});

test("CLI serve skips incompatible SQLite ABI checks for native profiles", () => {
  const source = readFileSync(new URL("../../bin/cli/commands/serve.mjs", import.meta.url), "utf8");
  const preflight = source.slice(
    source.indexOf("  const sqliteBinary ="),
    source.indexOf("  // Refuse to start a second instance")
  );
  for (const backend of [undefined, "postgres", "sqlite"]) {
    const probes: string[] = [],
      exits: number[] = [];
    vm.runInNewContext(preflight, {
      APP_DIR: "/app",
      join: path.posix.join,
      process: {
        env: backend === undefined ? {} : { OMNIROUTE_DATABASE_BACKEND: backend },
        versions: {},
        exit: (code: number) => exits.push(code),
      },
      existsSync: (file: string) => {
        probes.push(file);
        return true;
      },
      isNativeBinaryCompatible: () => false,
      platform: () => "linux",
      console: { error() {} },
    });
    assert.equal(probes.length, backend === "sqlite" ? 1 : 0);
    assert.deepEqual(exits, backend === "sqlite" ? [1] : []);
  }
});

function runElectron(env: NodeJS.ProcessEnv) {
  const filesystem: string[] = [],
    errors: string[] = [];
  const result = vm.runInNewContext(
    `(() => { ${electronBootstrap}; return { serverEnv, dataDir }; })()`,
    {
      process: { env },
      path,
      serverPort: 20128,
      getPreferredEnvFilePath: () => null,
      parseEnvFile: (name: string) => {
        filesystem.push(name);
        return {};
      },
      resolveDataDir: (_override: unknown, configured: Record<string, string>) =>
        configured.DATA_DIR ?? "/legacy/scratch",
      tightenServerEnv: () => filesystem.push("tighten"),
      chmodQuietly() {},
      hasEncryptedCredentials: () => {
        filesystem.push("sqlite-read");
        return false;
      },
      fs: {
        existsSync: () => false,
        mkdirSync: () => filesystem.push("mkdir"),
        writeFileSync: () => filesystem.push("write"),
      },
      crypto: { randomBytes: () => ({ toString: () => "fixture-generated-key" }) },
      console: { log() {}, warn() {}, error: (message: string) => errors.push(message) },
      sendToRenderer() {},
    }
  );
  return { result, filesystem, errors };
}

test("Electron's default native bootstrap neither reads nor provisions SQLite secrets", () => {
  for (const backend of [undefined, "postgres"]) {
    const env = {
      DATA_DIR: "/explicit/scratch",
      STORAGE_ENCRYPTION_KEY: "configured-key",
      ...(backend ? { OMNIROUTE_DATABASE_BACKEND: backend } : {}),
    };
    const state = runElectron(env);
    assert.equal(state.result.serverEnv.OMNIROUTE_DATABASE_BACKEND, "postgres");
    assert.equal(state.result.serverEnv.JWT_SECRET, undefined);
    assert.deepEqual(state.filesystem, []);
    assert.deepEqual(state.errors, []);
  }
  for (const env of [
    {},
    { DATA_DIR: "/explicit/scratch" },
    { OMNIROUTE_DATABASE_BACKEND: "" },
    { OMNIROUTE_DATABASE_BACKEND: "pg" },
  ]) {
    const state = runElectron(env);
    assert.equal(state.result, undefined);
    assert.equal(state.errors.length, 1);
    assert.deepEqual(state.filesystem, []);
  }
});

test("Electron's explicitly selected SQLite profile retains local secret provisioning", () => {
  const state = runElectron({ OMNIROUTE_DATABASE_BACKEND: "sqlite" });
  assert.equal(state.result.serverEnv.OMNIROUTE_DATABASE_BACKEND, "sqlite");
  assert.equal(state.result.serverEnv.STORAGE_ENCRYPTION_KEY, "fixture-generated-key");
  assert.ok(state.filesystem.includes("sqlite-read"));
  assert.ok(state.filesystem.includes("write"));
  assert.deepEqual(state.errors, []);
});

test(
  "default native CLI env inspection does not migrate or generate SQLite secrets",
  { timeout: 30000 },
  () => {
    const directory = mkdtempSync(path.join(tmpdir(), "omni-default-pg-cli-"));
    const serverEnvPath = path.join(directory, "server.env");
    writeFileSync(
      serverEnvPath,
      "JWT_SECRET=legacy-fixture-jwt\nSTORAGE_ENCRYPTION_KEY=legacy-fixture-key\n"
    );
    const env: NodeJS.ProcessEnv = {
      ...process.env,
      DATA_DIR: directory,
      HOME: directory,
      CI: "1",
      NO_UPDATE_NOTIFIER: "1",
      OMNIROUTE_CLI_SKIP_DEFAULT_DATA_ENV: "1",
      OMNIROUTE_CLI_SKIP_REPO_ENV: "1",
    };
    delete env.OMNIROUTE_DATABASE_BACKEND;
    delete env.STORAGE_ENCRYPTION_KEY;
    delete env.JWT_SECRET;
    delete env.API_KEY_SECRET;
    try {
      const child = spawnSync(
        process.execPath,
        [
          fileURLToPath(new URL("../../bin/omniroute.mjs", import.meta.url)),
          "env",
          "show",
          "--json",
        ],
        { env, cwd: directory, timeout: 20000, encoding: "utf8" }
      );
      assert.equal(
        child.status,
        0,
        "the read-only CLI command must complete without starting persistence"
      );
      assert.equal(existsSync(path.join(directory, ".env")), false);
      assert.equal(existsSync(path.join(directory, "storage.sqlite")), false);
      assert.equal(
        readFileSync(serverEnvPath, "utf8"),
        "JWT_SECRET=legacy-fixture-jwt\nSTORAGE_ENCRYPTION_KEY=legacy-fixture-key\n"
      );
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }
);

test("native CLI auth export cannot create a local database even when its storage key is set", () => {
  const directory = mkdtempSync(path.join(tmpdir(), "omni-native-cli-db-"));
  try {
    for (const backend of [undefined, "postgres"]) {
      const env: NodeJS.ProcessEnv = {
        ...process.env,
        DATA_DIR: directory,
        HOME: directory,
        CI: "1",
        NO_UPDATE_NOTIFIER: "1",
        STORAGE_ENCRYPTION_KEY: "configured-native-key",
        OMNIROUTE_CLI_SKIP_DEFAULT_DATA_ENV: "1",
        OMNIROUTE_CLI_SKIP_REPO_ENV: "1",
      };
      if (backend === undefined) delete env.OMNIROUTE_DATABASE_BACKEND;
      else env.OMNIROUTE_DATABASE_BACKEND = backend;
      const child = spawnSync(
        process.execPath,
        [
          fileURLToPath(new URL("../../bin/omniroute.mjs", import.meta.url)),
          "auth",
          "export",
          "--force",
        ],
        { env, cwd: directory, timeout: 20000, encoding: "utf8" }
      );
      assert.equal(child.status, 1);
      assert.match(child.stderr, /requires explicit OMNIROUTE_DATABASE_BACKEND=sqlite/);
      assert.equal(existsSync(path.join(directory, "storage.sqlite")), false);
      assert.equal(existsSync(path.join(directory, ".env")), false);
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("native CLI local backup and credential-reset commands refuse before filesystem effects", async () => {
  const backup = await import("../../bin/cli/commands/backup.mjs");
  const { runResetEncryptedColumns } =
    await import("../../bin/cli/commands/reset-encrypted-columns.mjs");
  const savedBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
  const savedDataDir = process.env.DATA_DIR;
  const directory = mkdtempSync(path.join(tmpdir(), "omni-native-cli-maintenance-"));
  const operations = [
    () => backup.runBackupCommand({}),
    () => backup.runRestoreCommand(undefined, { list: true }),
    () => backup.runRestoreCommand("fixture", { yes: true }),
    () => backup.runBackupAutoEnableCommand({}),
    () => backup.runBackupAutoDisableCommand(),
    () => backup.runBackupAutoStatusCommand(),
    () => runResetEncryptedColumns(["--force"]),
  ];
  try {
    process.env.DATA_DIR = directory;
    for (const backend of [undefined, "postgres", "", "invalid"]) {
      if (backend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
      else process.env.OMNIROUTE_DATABASE_BACKEND = backend;
      for (const execute of operations) {
        await assert.rejects(execute(), /requires explicit OMNIROUTE_DATABASE_BACKEND=sqlite/);
      }
      assert.deepEqual(readdirSync(directory), []);
    }
  } finally {
    if (savedBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = savedBackend;
    if (savedDataDir === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = savedDataDir;
    rmSync(directory, { recursive: true, force: true });
  }
});
