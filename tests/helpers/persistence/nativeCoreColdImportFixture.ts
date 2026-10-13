import assert from "node:assert/strict";
import fs from "node:fs";
import { registerHooks, syncBuiltinESMExports } from "node:module";
import path from "node:path";
import {
  getSelectedPostgresDatabase,
  UnsupportedPersistenceDomainError,
} from "../../../src/lib/db/controlPlaneContext.ts";
import { fixture, initializePostgresControlPlane } from "./runtimeOwnedWorkFixture.ts";

const scenario = process.argv[2];
const firstCarrier = process.argv[3];
const secondCarrier = process.argv[4];
const runtimeUrl = new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const coreUrl = new URL("../../../src/lib/db/core.ts", import.meta.url).href;
const migrationsUrl = new URL("../../../src/lib/db/migrationRunner.ts", import.meta.url).href;
const fixtureUrl = new URL("./runtimeOwnedWorkFixture.ts", import.meta.url).href;
const bootDependencies = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "../artifacts/s3ArtifactStore",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);

let startupImports = 0;

// Only startup transports are replaced. The runtime, context, core and migration owner are real.
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === runtimeUrl && bootDependencies.has(specifier)) {
      if (scenario === "native-default-no-config" && specifier === "./postgres/environment") {
        return nextResolve(specifier, context);
      }
      startupImports++;
      return { url: fixtureUrl, shortCircuit: true };
    }
    if (context.parentURL?.split("?")[0] === coreUrl && specifier === "./migrationRunner") {
      const bundle = new URL(context.parentURL).searchParams.get("core-cold") ?? "base";
      return { url: `${migrationsUrl}?core-cold=${bundle}`, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});

interface FileObservation {
  method: string;
  path: string | null;
}

function observeApplicationFileWork() {
  const operations: FileObservation[] = [];
  const originals = new Map<string, unknown>();
  const ownerFrame =
    /[\\/]src[\\/]lib[\\/](?:db[\\/](?:core|migrationRunner)|dataPaths)\.ts(?::|\?)/;
  for (const method of [
    "existsSync",
    "statSync",
    "mkdirSync",
    "mkdtempSync",
    "rmSync",
    "readdirSync",
    "openSync",
    "readFileSync",
    "writeFileSync",
  ]) {
    const original = Reflect.get(fs, method) as (...args: unknown[]) => unknown;
    originals.set(method, original);
    Reflect.set(fs, method, (...args: unknown[]) => {
      // Loader reads are distinct from source-owned path probes and directory creation.
      if (ownerFrame.test(new Error().stack ?? "")) {
        operations.push({ method, path: typeof args[0] === "string" ? args[0] : null });
      }
      return Reflect.apply(original, fs, args);
    });
  }
  syncBuiltinESMExports();
  return {
    operations,
    restore() {
      for (const [method, original] of originals) Reflect.set(fs, method, original);
      syncBuiltinESMExports();
    },
  };
}

async function migrationStatusAfterImport() {
  const { DatabaseSync } = await import("node:sqlite");
  const { createNodeSqliteAdapterFromDatabase } =
    await import("../../../src/lib/db/adapters/nodeSqliteShared.ts");
  const { getMigrationStatus } = await import(`${migrationsUrl}?core-cold=one`);
  const database = new DatabaseSync(":memory:");
  const adapter = createNodeSqliteAdapterFromDatabase(database, ":memory:");
  try {
    return getMigrationStatus(adapter);
  } finally {
    adapter.close();
  }
}

const native = scenario.startsWith("native-");
const defaultNative = scenario.startsWith("native-default");
const building = scenario === "native-default-build";
const missingPostgresConfig = scenario === "native-default-no-config";
const contextOnly = scenario === "context-only";
if (defaultNative) delete process.env.OMNIROUTE_DATABASE_BACKEND;
else process.env.OMNIROUTE_DATABASE_BACKEND = native ? "postgres" : "sqlite";
if (building) process.env.OMNIROUTE_BUILDING = "1";
if (missingPostgresConfig) {
  for (const key of Object.keys(process.env)) {
    if (key.startsWith("OMNIROUTE_POSTGRES_")) delete process.env[key];
  }
}
process.env.STORAGE_ENCRYPTION_KEY = "native-cold-core-source-fixture-key";
delete process.env.OMNIROUTE_MIGRATIONS_DIR;
if (scenario === "sqlite-migration-snapshot") {
  process.env.OMNIROUTE_MIGRATIONS_DIR = firstCarrier;
}
if (native && scenario.endsWith("-missing")) delete process.env.DATA_DIR;
if (native && scenario.endsWith("-blank")) process.env.DATA_DIR = " \t ";
fixture.reset();
let close: (() => Promise<void>) | undefined;
let selected: <T>(body: () => Promise<T>) => Promise<T> = (body) => body();
const missingScratch = native && (scenario.endsWith("-missing") || scenario.endsWith("-blank"));
const observation = observeApplicationFileWork();
let core: typeof import("../../../src/lib/db/core.ts") | undefined;
try {
  if (native && !building) {
    const runtime = await import(runtimeUrl);
    close = runtime.closePersistenceRuntime;
    if (missingScratch) {
      await assert.rejects(runtime.ensurePersistenceReady(), {
        message: "PostgreSQL requires an explicit DATA_DIR for local scratch",
      });
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "failed");
      assert.equal(startupImports, 0);
      assert.equal(fixture.environmentReads, 0);
      assert.equal(fixture.planeCreates, 0);
      assert.equal(fixture.artifactCreates, 0);
    } else if (missingPostgresConfig) {
      await assert.rejects(runtime.ensurePersistenceReady(), /Invalid PostgreSQL configuration/);
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "failed");
      assert.equal(fixture.planeCreates, 0);
      assert.equal(fixture.artifactCreates, 0);
      await assert.rejects(
        runtime.runPersistenceRequest(async () => assert.fail("failed native startup dispatched")),
        /unavailable/
      );
    } else {
      await runtime.ensurePersistenceReady();
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "ready");
      selected = runtime.runPersistenceRequest;
      assert.ok(getSelectedPostgresDatabase());
    }
  } else if (contextOnly) {
    const plane = await initializePostgresControlPlane();
    close = () => plane.close();
    selected = (body) => plane.run(body);
  }
  if (missingScratch || missingPostgresConfig) {
    assert.deepEqual(observation.operations, []);
  } else {
    core = await selected(() => import(`${coreUrl}?core-cold=one`));
    const configured = process.env.DATA_DIR!.trim();
    assert.equal(core.DATA_DIR, path.resolve(configured));
    assert.equal(core.SQLITE_FILE, path.join(path.resolve(configured), "storage.sqlite"));
    assert.equal(core.DB_BACKUPS_DIR, path.join(path.resolve(configured), "db_backups"));
    if (native) {
      assert.deepEqual(observation.operations, []);
      assert.equal(core.getDriverInfo(), null);
      if (!building) {
        await selected(async () => {
          assert.throws(() => core!.getDbInstance(), UnsupportedPersistenceDomainError);
        });
      } else {
        assert.equal(startupImports, 0);
        assert.equal(fixture.planeCreates, 0);
        assert.equal(fixture.environmentReads, 0);
        assert.equal(fixture.artifactCreates, 0);
        assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      }
      assert.deepEqual(observation.operations, []);
      const duplicate = await selected(() => import(`${coreUrl}?core-cold=two`));
      assert.notEqual(
        duplicate.getDbInstance,
        core.getDbInstance,
        "two cold core copies must evaluate"
      );
      assert.equal(duplicate.DATA_DIR, core.DATA_DIR);
      assert.deepEqual(observation.operations, []);
    } else {
      assert.ok(observation.operations.some((entry) => entry.method === "mkdirSync"));
      assert.ok(fs.existsSync(core.DATA_DIR));
      if (contextOnly) {
        await selected(async () => {
          assert.throws(() => core!.getDbInstance(), UnsupportedPersistenceDomainError);
        });
      }
    }
    if (scenario === "native-migration-lazy") {
      const beforeInvocation = observation.operations.length;
      process.env.OMNIROUTE_MIGRATIONS_DIR = firstCarrier;
      const first = await migrationStatusAfterImport();
      assert.deepEqual(first.pending, [
        {
          version: "001",
          name: "first_carrier",
          path: path.join(firstCarrier, "001_first_carrier.sql"),
        },
      ]);
      assert.ok(observation.operations.length > beforeInvocation);
      process.env.OMNIROUTE_MIGRATIONS_DIR = secondCarrier;
      assert.deepEqual((await migrationStatusAfterImport()).pending, first.pending);
    } else if (scenario === "sqlite-migration-snapshot") {
      process.env.OMNIROUTE_MIGRATIONS_DIR = secondCarrier;
      assert.deepEqual((await migrationStatusAfterImport()).pending, [
        {
          version: "001",
          name: "first_carrier",
          path: path.join(firstCarrier, "001_first_carrier.sql"),
        },
      ]);
    }
  }
  console.log(
    "CORE_COLD_RESULT=" +
      JSON.stringify({
        scenario,
        configuredPath: core?.DATA_DIR ?? null,
        operations: observation.operations,
      })
  );
} finally {
  observation.restore();
  await close?.();
  fixture.reset();
  hooks.deregister();
}
