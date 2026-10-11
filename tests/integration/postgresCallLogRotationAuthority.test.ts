import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import type { PostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";

/** Real SQL/runtime admission; only independent S3/bootstrap side effects use the existing fixture. */
test(
  "foreign call-log saves cannot schedule application rotation when admission capacity is free",
  {
    skip: !postgresTestsEnabled,
    timeout: 60000,
  },
  async (t) => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (
          context.parentURL === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        )
          return { url: fixtureUrl, shortCircuit: true };
        return nextResolve(specifier, context);
      },
    });
    const previous = new Map<string, string | undefined>(
      [
        "OMNIROUTE_DATABASE_BACKEND",
        "STORAGE_ENCRYPTION_KEY",
        "JWT_SECRET",
        "API_KEY_SECRET",
        "DATA_DIR",
        "NODE_ENV",
        "OMNIROUTE_DISABLE_BACKGROUND_SERVICES",
        "CALL_LOG_RETENTION_DAYS",
        "CALL_LOGS_TABLE_MAX_ROWS",
      ].map((name) => [name, process.env[name]])
    );
    const directory = mkdtempSync(join(tmpdir(), "omniroute-rotation-authority-"));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.DATA_DIR = directory;
    process.env.NODE_ENV = "test";
    process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES = "1";
    process.env.CALL_LOG_RETENTION_DAYS = "1";
    process.env.CALL_LOGS_TABLE_MAX_ROWS = "5000";
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    const prefix = `rotation-authority-${randomUUID()}`;
    const expiredId = `${prefix}-expired`,
      foreignLogId = `${prefix}-foreign`;
    const database = await createTestPostgresDatabase();
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let foreign: PostgresControlPlane | undefined;
    try {
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      const activeRuntime: typeof import("../../src/lib/db/persistenceRuntime.ts") = await import(
        runtimeUrl
      );
      runtime = activeRuntime;
      await activeRuntime.ensurePersistenceReady();
      const state = globalThis.__omniroutePersistenceRuntime!;
      const applicationIdentity = state.applicationIdentity;
      assert.ok(applicationIdentity);
      assert.equal(
        state.background.size,
        0,
        "no artificial admission saturation may mask rerouting"
      );
      const { initializePostgresControlPlane } =
        await import("../../src/lib/db/postgres/controlPlane.ts");
      const { withPostgresCatalogSnapshot } =
        await import("../../src/lib/db/postgres/catalogSnapshot.ts");
      const context = await import("../../src/lib/db/controlPlaneContext.ts");
      const { PostgresCallLogRepository } =
        await import("../../src/lib/db/postgres/callLogRepository.ts");
      const callLogs = await import("../../src/lib/usage/callLogs.ts");
      const rotation = await import("../../src/lib/usage/callLogRotation.ts");
      foreign = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
        logger: { warn() {} },
      });
      await database.transaction(async (tx) => {
        await tx.query("SELECT pg_advisory_xact_lock(18765,8)");
        await tx.query(
          "INSERT INTO omniroute.call_logs(id,timestamp,status) VALUES($1,'2000-01-01T00:00:00.000Z',200)",
          [expiredId]
        );
      });
      const effects: Array<{ kind: "delete" | "trim"; identity: object | undefined }> = [];
      const deleteBefore = PostgresCallLogRepository.prototype.deleteBefore;
      const trim = PostgresCallLogRepository.prototype.trim;
      t.mock.method(
        PostgresCallLogRepository.prototype,
        "deleteBefore",
        async function (
          this: InstanceType<typeof PostgresCallLogRepository>,
          cutoff: string,
          limit: number
        ) {
          effects.push({ kind: "delete", identity: context.getSelectedPersistenceIdentity() });
          return deleteBefore.call(this, cutoff, limit);
        }
      );
      t.mock.method(
        PostgresCallLogRepository.prototype,
        "trim",
        async function (
          this: InstanceType<typeof PostgresCallLogRepository>,
          maxRows: number,
          limit: number
        ) {
          effects.push({ kind: "trim", identity: context.getSelectedPersistenceIdentity() });
          return trim.call(this, maxRows, limit);
        }
      );
      const counts = async () =>
        (
          await database.transaction((tx) =>
            tx.query<{ id: string }>(
              "SELECT id FROM omniroute.call_logs WHERE id=ANY($1::text[]) ORDER BY id",
              [[expiredId, foreignLogId]]
            )
          )
        ).rows.map((row) => row.id);
      let foreignIdentity: object | undefined;
      await foreign.run((binding) =>
        withPostgresCatalogSnapshot(binding.database!, async () => {
          foreignIdentity = binding.databaseIdentity;
          assert.ok(foreignIdentity);
          assert.notEqual(foreignIdentity, applicationIdentity);
          assert.equal(context.getSelectedPersistenceIdentity(), foreignIdentity);
          assert.equal(runtime!.isApplicationPersistenceIdentity(foreignIdentity), false);
          // Free application slots are intentional: a wrong automatic schedule must be observable.
          assert.equal(state.background.size, 0);
          await callLogs.saveCallLog({ id: foreignLogId, noLog: true, status: 200 });
          assert.equal(state.background.size, 0);
          await rotation.closeCallLogRotationAsync();
        })
      );
      while (state.background.size) await Promise.allSettled([...state.background]);
      assert.equal(effects.length, 0, "a foreign save may not cause maintenance in either plane");
      assert.deepEqual(await counts(), [expiredId, foreignLogId].sort());

      // Standalone maintenance remains available through the explicit selected-plane contract.
      await foreign.run((binding) =>
        withPostgresCatalogSnapshot(binding.database!, async () => {
          await rotation.rotateCallLogsAsync();
          await rotation.closeCallLogRotationAsync();
        })
      );
      assert.ok(effects.length > 0);
      assert.ok(effects.every((effect) => effect.identity === foreignIdentity));
      assert.deepEqual(await counts(), [foreignLogId]);
      assert.equal(state.background.size, 0);
      effects.length = 0;

      // The foreign call did not consume the application plane's own rotation throttle.
      await activeRuntime.runPersistenceRequest(async () => {
        rotation.scheduleCallLogRotationAsync();
        assert.equal(state.background.size, 1);
        await rotation.closeCallLogRotationAsync();
      });
      assert.ok(effects.length > 0);
      assert.ok(effects.every((effect) => effect.identity === applicationIdentity));
      assert.equal(state.background.size, 0);
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
      await callLogs.closeCallLogSaves(10000);
    } finally {
      await foreign?.close();
      await runtime?.closePersistenceRuntime().catch(() => {});
      hooks.deregister();
      try {
        await database.transaction(async (tx) => {
          await tx.query("SELECT pg_advisory_xact_lock(18765,8)");
          await tx.query("DELETE FROM omniroute.call_logs WHERE id LIKE $1", [`${prefix}%`]);
        });
      } finally {
        await database.close();
        delete globalThis.__omniroutePersistenceRuntime;
        for (const [name, value] of previous) {
          if (value === undefined) delete process.env[name];
          else process.env[name] = value;
        }
        rmSync(directory, { recursive: true, force: true });
      }
    }
  }
);
