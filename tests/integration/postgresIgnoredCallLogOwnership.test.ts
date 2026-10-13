import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { mkdtempSync, realpathSync, rmSync, existsSync } from "node:fs";
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
import {
  barrier,
  callLogNativeFixture,
  nativeRuntimeFixture,
} from "../helpers/persistence/callLogNativeRuntimeFixture.ts";

test(
  "ignored application call logs own their native I/O until acknowledged or refused",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async (t) => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/callLogNativeRuntimeFixture.ts",
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
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "DATA_DIR",
      "NODE_ENV",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    const directory = mkdtempSync(join(tmpdir(), "omniroute-native-owned-call-log-"));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.DATA_DIR = directory;
    process.env.NODE_ENV = "test";
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    const prefix = `owned-call-log-${randomUUID()}`;
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    const database = await createTestPostgresDatabase();
    let foreign:
      | Awaited<
          ReturnType<
            typeof import("../../src/lib/db/postgres/controlPlane.ts").initializePostgresControlPlane
          >
        >
      | undefined;
    try {
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      const state = globalThis.__omniroutePersistenceRuntime!;
      assert.ok(state.applicationIdentity);
      const callLogs = await import("../../src/lib/usage/callLogs.ts");
      const { runWithCallLogApiKeyContext } =
        await import("../../src/lib/usage/callLogApiKeyContext.ts");
      const actions = await import("../../src/lib/usage/resilienceActionsContext.ts");
      const seedKey = async (id: string) =>
        database.transaction(async (tx) => {
          await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
          await tx.query(
            "INSERT INTO omniroute.api_keys(id,name,key,no_log,created_at) VALUES($1,'owned log key',$2,0,$3)",
            [id, randomUUID(), new Date().toISOString()]
          );
        });
      const rows = async (id: string) =>
        (
          await database.transaction((tx) =>
            tx.query<{
              id: string;
              api_key_id: string;
              api_key_name: string;
              resilience_actions: string;
              detail_state: string;
              artifact_relpath: string;
            }>(
              "SELECT id,api_key_id,api_key_name,resilience_actions,detail_state,artifact_relpath FROM omniroute.call_logs WHERE id=$1",
              [id]
            )
          )
        ).rows;

      await t.test(
        "an ignored save commits after its parent scope ends, with captured attribution/actions",
        async () => {
          const id = `${prefix}-ignored`,
            key = `${prefix}-key`;
          await seedKey(key);
          const gate = callLogNativeFixture.arm(),
            settled = barrier();
          let observer: AsyncResource | undefined;
          const entry = { id, status: 200, requestBody: { marker: "captured artifact" } };
          try {
            await runtime!.runPersistenceRequest(async () =>
              runWithCallLogApiKeyContext({ apiKeyId: key, apiKeyName: "captured name" }, () =>
                actions.runWithResilienceActionsContext(() => {
                  observer = new AsyncResource("call-log-resilience-observer");
                  actions.noteResilienceAction({ rotations: 1 });
                  void callLogs.saveCallLog(entry).then(settled.resolve);
                  assert.equal(
                    state.background.size,
                    1,
                    "admission is registered before save returns"
                  );
                  actions.noteResilienceAction({ continued: 1 });
                  entry.id = `${id}-mutated`;
                })
              )
            );
            await Promise.race([
              gate.entered,
              settled.promise.then(() =>
                assert.fail("ignored save ended before its artifact boundary")
              ),
            ]);
            assert.equal(state.requests.size, 0);
            assert.equal(state.background.size, 1);
            const idle = await database.transaction((tx) =>
              tx.query<{ count: string }>(
                "SELECT COUNT(*)::text AS count FROM pg_stat_activity WHERE datname=current_database() AND state='idle in transaction'"
              )
            );
            assert.equal(idle.rows[0].count, "0", "the S3 wait must not reserve a SQL transaction");
            const [pendingMetadata] = await rows(id);
            assert.ok(pendingMetadata, "metadata is durable before object dispatch");
            assert.equal(pendingMetadata.detail_state, "missing");
            assert.equal(pendingMetadata.artifact_relpath, null);
            gate.release();
            assert.equal(await callLogs.waitForCallLogSaves(10000), true);
            const [row] = await rows(id);
            assert.equal(row.api_key_id, key);
            assert.equal(row.api_key_name, "captured name");
            assert.deepEqual(JSON.parse(row.resilience_actions), { rotations: 1 });
            assert.equal(row.detail_state, "ready");
            assert.ok(callLogNativeFixture.objects.has(row.artifact_relpath));
            assert.deepEqual(observer!.runInAsyncScope(actions.readResilienceActions), {
              rotations: 1,
              continued: 1,
            });
          } finally {
            gate.release();
            observer?.emitDestroy();
          }
        }
      );

      await t.test(
        "a foreign standalone authority is awaited directly and never routed through application admission",
        async () => {
          const { initializePostgresControlPlane } =
            await import("../../src/lib/db/postgres/controlPlane.ts");
          const { withPostgresCatalogSnapshot } =
            await import("../../src/lib/db/postgres/catalogSnapshot.ts");
          foreign = await initializePostgresControlPlane({
            connection: await testPostgresOptions(),
            runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
            logger: { warn() {} },
          });
          const id = `${prefix}-foreign`,
            before = callLogNativeFixture.puts;
          while (state.background.size) await Promise.allSettled([...state.background]);
          const slots = Array.from({ length: 64 }, () => barrier());
          for (const slot of slots) state.background.add(slot.promise);
          try {
            await foreign.run((binding) =>
              withPostgresCatalogSnapshot(binding.database!, async () => {
                assert.equal(
                  runtime!.isApplicationPersistenceIdentity(binding.databaseIdentity),
                  false
                );
                // A rerouted save would be refused by the saturated application admission gate.
                await callLogs.saveCallLog({ id, noLog: true });
                assert.equal(state.background.size, 64);
              })
            );
            assert.equal((await rows(id)).length, 1);
            assert.equal(callLogNativeFixture.puts, before);
          } finally {
            for (const slot of slots) {
              slot.resolve();
              state.background.delete(slot.promise);
            }
          }
        }
      );

      await t.test(
        "cap refusal resolves best-effort, reports admission failure, and performs no artifact or insert",
        async () => {
          const id = `${prefix}-cap`,
            before = callLogNativeFixture.puts;
          while (state.background.size) await Promise.allSettled([...state.background]);
          const slots = Array.from({ length: 64 }, () => barrier());
          const messages: string[] = [],
            original = console.error;
          console.error = (...values: unknown[]) => messages.push(values.map(String).join(" "));
          for (const slot of slots) state.background.add(slot.promise);
          try {
            await runtime!.runPersistenceRequest(async () => {
              await assert.doesNotReject(
                callLogs.saveCallLog({ id, requestBody: { private: "not logged on refusal" } })
              );
            });
            assert.equal((await rows(id)).length, 0);
            assert.equal(callLogNativeFixture.puts, before);
            assert.ok(messages.some((message) => message.includes("background admission")));
            assert.ok(messages.every((message) => !message.includes("not logged on refusal")));
            assert.equal(await callLogs.waitForCallLogSaves(1000), true);
          } finally {
            console.error = original;
            for (const slot of slots) {
              slot.resolve();
              state.background.delete(slot.promise);
            }
          }
        }
      );

      await t.test(
        "changed held identity refuses before privacy/artifact/domain insert",
        async () => {
          const id = `${prefix}-identity`,
            before = callLogNativeFixture.puts,
            identity = state.applicationIdentity;
          const original = console.error;
          const messages: string[] = [];
          console.error = (...values: unknown[]) => messages.push(values.map(String).join(" "));
          try {
            await runtime!.runPersistenceRequest(async () => {
              const operation = callLogs.saveCallLog({
                id,
                requestBody: { secret: "never uploaded" },
              });
              state.applicationIdentity = {};
              await operation;
            });
            assert.equal((await rows(id)).length, 0);
            assert.equal(callLogNativeFixture.puts, before);
            assert.ok(messages.some((message) => message.includes("authority changed")));
          } finally {
            state.applicationIdentity = identity;
            console.error = original;
          }
        }
      );

      await t.test(
        "runtime close drains an accepted ignored save before closing the artifact store and pool",
        async () => {
          const id = `${prefix}-close`,
            gate = callLogNativeFixture.arm(),
            settled = barrier();
          let closing: Promise<void> | undefined;
          try {
            await runtime!.runPersistenceRequest(async () => {
              void callLogs.saveCallLog({ id, requestBody: { held: true } }).then(settled.resolve);
            });
            await Promise.race([
              gate.entered,
              settled.promise.then(() =>
                assert.fail("accepted save ended before its artifact boundary")
              ),
            ]);
            closing = runtime!.closePersistenceRuntime();
            void closing.catch(() => {});
            assert.equal(runtime!.getPersistenceRuntimeStatus().phase, "closing");
            assert.equal(nativeRuntimeFixture.plane?.state, "ready");
            assert.equal(callLogNativeFixture.closes, 0);
            gate.release();
            await closing;
            assert.equal((await rows(id)).length, 1);
            assert.equal(callLogNativeFixture.closes, 1);
            assert.equal(nativeRuntimeFixture.plane?.state, "closed");
            assert.equal(await callLogs.waitForCallLogSaves(1000), true);
            assert.equal(existsSync(join(directory, "storage.sqlite")), false);
          } finally {
            gate.release();
            await closing?.catch(() => {});
          }
        }
      );
      await callLogs.closeCallLogSaves(10000);
    } finally {
      callLogNativeFixture.gate?.resolve();
      await foreign?.close();
      await runtime?.closePersistenceRuntime().catch(() => {});
      hooks.deregister();
      try {
        await database.transaction(async (tx) => {
          await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
          await tx.query("SELECT pg_advisory_xact_lock(18765,8)");
          await tx.query("DELETE FROM omniroute.call_logs WHERE id LIKE $1", [`${prefix}%`]);
          await tx.query("DELETE FROM omniroute.api_keys WHERE id LIKE $1", [`${prefix}%`]);
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
