import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { setImmediate, setTimeout as delay } from "node:timers/promises";
import test, { mock } from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import type { PostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext.ts";

function deferred() {
  let done!: () => void;
  let resolved = false;
  const promise = new Promise<void>((finish) => {
    done = finish;
  });
  return {
    promise,
    get resolved() {
      return resolved;
    },
    resolve() {
      resolved = true;
      done();
    },
  };
}

async function waitUntil(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate() && Date.now() < deadline) await delay(10);
  assert.equal(predicate(), true, "bounded scheduler observation did not arrive");
}

// pg owns real timeout handles created before these cases. Mocking clearTimeout would
// leave their native timers armed; only scheduler polling intervals are virtualized.
function mockPollingIntervals(): void {
  const nativeSetTimeout = globalThis.setTimeout;
  const nativeClearTimeout = globalThis.clearTimeout;
  mock.timers.enable({ apis: ["setInterval"] });
  assert.equal(globalThis.setTimeout, nativeSetTimeout);
  assert.equal(globalThis.clearTimeout, nativeClearTimeout);
}

// Real PostgreSQL, runtime admission and startup timers; polling intervals and outbound sync
// are the only substituted owners.
test(
  "native health scheduler lifetimes retain authority through timers and drain accepted work",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const keys = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "FREE_PROXY_AUTO_SYNC_ENABLED",
      "FREE_PROXY_AUTO_SYNC_INTERVAL_MS",
      "FREE_PROXY_1PROXY_ENABLED",
      "NEXT_PHASE",
      "CREDENTIAL_HEALTH_CHECK_INTERVAL",
      "OMNIROUTE_DISABLE_BACKGROUND_SERVICES",
    ] as const;
    const originalEnv = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
    const originalRuntime = globalThis.__omniroutePersistenceRuntime;
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-health-scheduler-encryption-key";
    process.env.FREE_PROXY_AUTO_SYNC_ENABLED = "true";
    process.env.FREE_PROXY_AUTO_SYNC_INTERVAL_MS = "300000";
    process.env.FREE_PROXY_1PROXY_ENABLED = "true";
    delete process.env.NEXT_PHASE;
    process.env.CREDENTIAL_HEALTH_CHECK_INTERVAL = "10000";
    delete process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES;
    const { initializePostgresControlPlane } =
      await import("../../src/lib/db/postgres/controlPlane.ts");
    const { runPersistenceRequest } = await import("../../src/lib/db/persistenceRuntime.ts");
    const { getSelectedControlPlane } = await import("../../src/lib/db/controlPlaneContext.ts");
    const database = await createTestPostgresDatabase();
    let plane: PostgresControlPlane | undefined, second: PostgresControlPlane | undefined;
    let credential: typeof import("../../src/lib/credentialHealth/scheduler.ts") | undefined;
    let proxy: typeof import("../../src/lib/proxyHealth/scheduler.ts") | undefined;
    let free: typeof import("../../src/lib/freeProxyProviders/scheduler.ts") | undefined;
    try {
      // This is the disposable test database, never a configured application database.
      await database.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      const options = {
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      };
      plane = await initializePostgresControlPlane(options);
      const applicationIdentity = await plane.run((binding) => {
        assert.ok(binding.databaseIdentity);
        return binding.databaseIdentity;
      });
      const state: NonNullable<typeof globalThis.__omniroutePersistenceRuntime> = {
        backend: "postgres",
        phase: "ready",
        plane,
        applicationIdentity,
        requests: new Set(),
        attempts: new Set(),
        attemptAdmissions: new Set(),
        attemptCancellation: new AbortController(),
        background: new Set(),
        tails: new Set(),
        owners: new AsyncLocalStorage<{ active: boolean }>(),
      };
      globalThis.__omniroutePersistenceRuntime = state;
      await runPersistenceRequest(async () => {
        assert.equal(getSelectedControlPlane()!.databaseIdentity, applicationIdentity);
        credential = await import("../../src/lib/credentialHealth/scheduler.ts");
        proxy = await import("../../src/lib/proxyHealth/scheduler.ts");
        free = await import("../../src/lib/freeProxyProviders/scheduler.ts");
      });
      assert.equal(
        state.background.size,
        0,
        "native module imports must not start background work"
      );

      async function stopAll() {
        await plane!.run(async () => {
          await credential!.stopCredentialHealthCheckAsync();
          await proxy!.stopProxyHealthCheckAsync();
          await free!.stopFreeProxyAutoSyncAsync();
        });
      }
      async function settleBackground() {
        await waitUntil(() => state.background.size === 0);
        await setImmediate();
      }

      await t.test(
        "a delayed free-sync tick receives fresh authority and stop waits for callback effects",
        async () => {
          const entered = deferred(),
            release = deferred();
          let creator: PostgresControlPlaneBinding | undefined,
            effects = 0;
          free!._setSyncCycleRunnerForTests(async () => {
            const current = getSelectedControlPlane()!;
            current.assertReady();
            assert.notEqual(current, creator);
            await current.database!.transaction((tx) => tx.query("SELECT 1"));
            entered.resolve();
            await release.promise;
            current.assertReady();
            effects++;
            return { results: {}, lastSyncAt: "test-effect-completed" };
          });
          mockPollingIntervals();
          try {
            await runPersistenceRequest(() => {
              creator = getSelectedControlPlane();
              free!.initFreeProxyAutoSync();
            });
            assert.throws(() => creator!.assertReady(), /scope has ended/);
            await settleBackground();
            assert.equal(entered.resolved, false, "the real startup delay must still be pending");
            await waitUntil(() => entered.resolved, 10000);
            mock.timers.tick(300000); // Intervals must not overlap the held cycle.
            let stopped = false;
            const stopping = plane!
              .run(() => free!.stopFreeProxyAutoSyncAsync())
              .then(() => {
                stopped = true;
              });
            await setImmediate();
            assert.equal(stopped, false);
            assert.equal(effects, 0);
            release.resolve();
            await stopping;
            assert.equal(effects, 1);
            assert.equal(state.background.size, 0);
            mock.timers.tick(600000);
            await setImmediate();
            assert.equal(effects, 1);
          } finally {
            release.resolve();
            await stopAll();
            mock.timers.reset();
            free!._setSyncCycleRunnerForTests(null);
          }
        }
      );

      for (const family of ["credential", "proxy"] as const) {
        await t.test(
          `${family} stop waits for accepted catalog preparation and prevents late rearm`,
          async () => {
            const held = deferred(),
              release = deferred();
            let holder: Promise<unknown> | undefined, operation: Promise<void> | undefined;
            try {
              await runPersistenceRequest(async () => {
                holder = database.transaction(async (tx) => {
                  await tx.query(
                    "LOCK TABLE omniroute.provider_connections IN ACCESS EXCLUSIVE MODE"
                  );
                  held.resolve();
                  await release.promise;
                });
                await held.promise;
                operation =
                  family === "credential"
                    ? credential!.forceSweep()
                    : proxy!.forceProxyHealthSweep();
              });
              assert.equal(state.background.size, 1);
              let stopped = false;
              const stopping = plane!
                .run(() =>
                  family === "credential"
                    ? credential!.stopCredentialHealthCheckAsync()
                    : proxy!.stopProxyHealthCheckAsync()
                )
                .then(() => {
                  stopped = true;
                });
              await setImmediate();
              assert.equal(stopped, false);
              release.resolve();
              await holder;
              await operation;
              await stopping;
              assert.equal(state.background.size, 0);
            } finally {
              release.resolve();
              await holder?.catch(() => {});
              await operation?.catch(() => {});
              await stopAll();
            }
          }
        );
      }

      await t.test("stop during initial-delay preparation cannot arm a later timer", async () => {
        const held = deferred(),
          release = deferred();
        let holder: Promise<unknown> | undefined,
          calls = 0;
        free!._setSyncCycleRunnerForTests(async () => {
          calls++;
          return { results: {}, lastSyncAt: "no-call-expected" };
        });
        mockPollingIntervals();
        try {
          await runPersistenceRequest(async () => {
            holder = database.transaction(async (tx) => {
              await tx.query("LOCK TABLE omniroute.provider_connections IN ACCESS EXCLUSIVE MODE");
              held.resolve();
              await release.promise;
            });
            await held.promise;
            free!.initFreeProxyAutoSync();
          });
          let stopped = false;
          const stopping = plane!
            .run(() => free!.stopFreeProxyAutoSyncAsync())
            .then(() => {
              stopped = true;
            });
          await setImmediate();
          assert.equal(stopped, false);
          release.resolve();
          await holder;
          await stopping;
          mock.timers.tick(1000000);
          await delay(5100); // Observe the real startup horizon after accepted preparation drains.
          await setImmediate();
          assert.equal(calls, 0);
        } finally {
          release.resolve();
          await holder?.catch(() => {});
          await stopAll();
          mock.timers.reset();
          free!._setSyncCycleRunnerForTests(null);
        }
      });

      await t.test("an old delayed tick cannot run against a different native plane", async () => {
        second = await initializePostgresControlPlane(options);
        let calls = 0;
        free!._setSyncCycleRunnerForTests(async () => {
          calls++;
          return { results: {}, lastSyncAt: "wrong-authority" };
        });
        mockPollingIntervals();
        try {
          await runPersistenceRequest(() => free!.initFreeProxyAutoSync());
          await settleBackground();
          state.plane = second;
          await delay(5100); // Run the actual delayed tick while PostgreSQL timers remain real.
          await setImmediate();
          await settleBackground();
          assert.equal(calls, 0);
        } finally {
          state.plane = plane;
          await stopAll();
          mock.timers.reset();
          free!._setSyncCycleRunnerForTests(null);
        }
      });
    } finally {
      mock.timers.reset();
      if (plane && credential && proxy && free) {
        await plane.run(async () => {
          await credential!.stopCredentialHealthCheckAsync();
          await proxy!.stopProxyHealthCheckAsync();
          await free!.stopFreeProxyAutoSyncAsync();
          await waitUntil(() => globalThis.__omniroutePersistenceRuntime!.background.size === 0);
          await (
            await import("../../src/lib/proxyHealth/blockedHistory.ts")
          ).closeBlockedHistoryAsync();
        });
      }
      await second?.close();
      await plane?.close();
      await database.close();
      globalThis.__omniroutePersistenceRuntime = originalRuntime;
      for (const key of keys) {
        const previous = originalEnv[key];
        if (previous === undefined) delete process.env[key];
        else process.env[key] = previous;
      }
    }
  }
);
