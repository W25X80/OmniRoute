import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { mkdtempSync, realpathSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";
import { getSelectedPostgresDatabase } from "../../src/lib/db/controlPlaneContext.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const turn = () => new Promise<void>((done) => setImmediate(done));

test(
  "native subscription ticks survive request revocation and stop drains the accepted refresh",
  {
    skip: !postgresTestsEnabled,
    timeout: 60_000,
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
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "DATA_DIR",
      "NODE_ENV",
      "JWT_SECRET",
      "API_KEY_SECRET",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    const directory = mkdtempSync(join(tmpdir(), "native-subscription-scheduler-"));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.DATA_DIR = directory;
    process.env.NODE_ENV = "test";
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    const database = await createTestPostgresDatabase();
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let subscriptions:
      typeof import("../../src/lib/proxySubscription/subscriptionService.ts") | undefined;
    const fetched = barrier(),
      release = barrier();
    const originalFetch = globalThis.fetch;
    const active = new Set<{ fire: () => void; unref: () => void }>();
    const callbacks: Array<() => void> = [];
    let fetches = 0;
    try {
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      subscriptions = await import("../../src/lib/proxySubscription/subscriptionService.ts");
      const sub = await runtime.runPersistenceRequest(() =>
        subscriptions!.createSubscription({
          name: "scheduled fixture",
          url: "https://203.0.113.1/subscription",
          enabled: false,
        })
      );
      await database.transaction((tx) =>
        tx.query(
          "UPDATE omniroute.proxy_subscriptions SET enabled=1, last_fetched_at=NULL WHERE id=$1",
          [sub.id]
        )
      );
      globalThis.fetch = async (input) => {
        assert.equal(String(input), "https://203.0.113.1/subscription");
        fetches++;
        fetched.resolve();
        await release.promise;
        return new Response("http://198.51.100.1:8080\n");
      };
      const timeout = globalThis.setTimeout,
        interval = globalThis.setInterval;
      const clearTimeout = globalThis.clearTimeout,
        clearInterval = globalThis.clearInterval;
      const schedule = (callback: () => void, repeat: boolean) => {
        // Replaying a timer must retain its creation ALS context, just like a real Node timer.
        const bound = AsyncResource.bind(callback);
        const handle = {
          fire() {
            if (!repeat) active.delete(handle);
            bound();
          },
          unref() {},
        };
        active.add(handle);
        callbacks.push(handle.fire);
        return handle as unknown as NodeJS.Timeout;
      };
      t.mock.method(globalThis, "setTimeout", ((callback, delay, ...args) =>
        delay === 60_000
          ? schedule(() => callback(...args), false)
          : timeout(callback, delay, ...args)) as typeof setTimeout);
      t.mock.method(globalThis, "setInterval", ((callback, delay, ...args) =>
        delay === 60_000
          ? schedule(() => callback(...args), true)
          : interval(callback, delay, ...args)) as typeof setInterval);
      t.mock.method(globalThis, "clearTimeout", ((handle) => {
        if (!active.delete(handle as unknown as { fire: () => void; unref: () => void }))
          clearTimeout(handle);
      }) as typeof clearTimeout);
      t.mock.method(globalThis, "clearInterval", ((handle) => {
        if (!active.delete(handle as unknown as { fire: () => void; unref: () => void }))
          clearInterval(handle);
      }) as typeof clearInterval);
      let expired: ReturnType<typeof getSelectedPostgresDatabase>;
      process.env.NODE_ENV = "production";
      await runtime.runPersistenceRequest(async () => {
        expired = getSelectedPostgresDatabase();
        subscriptions!.startSubscriptionScheduler();
        await turn();
      });
      assert.throws(() => expired!.transaction(async () => {}), /scope has ended/);
      assert.equal(active.size, 1);
      callbacks[0]();
      await turn();
      assert.equal(globalThis.__omniroutePersistenceRuntime!.background.size, 1);
      await fetched.promise;
      callbacks.at(-1)!();
      await turn();
      assert.equal(fetches, 1, "overlapping polls must not admit another refresh");
      let stopped = false;
      const stop = runtime
        .runPersistenceShutdownWork(() => subscriptions!.stopSubscriptionSchedulerAsync())
        .then(() => {
          stopped = true;
        });
      await turn();
      assert.equal(stopped, false, "stop must join the accepted HTTP refresh and SQL effects");
      release.resolve();
      await stop;
      const refreshed = await runtime.runPersistenceRequest(() =>
        subscriptions!.getSubscriptionById(sub.id)
      );
      assert.ok(refreshed?.lastFetchedAt);
      assert.equal(refreshed?.status, "ok");
      assert.equal(active.size, 0);
      const timerCount = callbacks.length;
      await runtime.runPersistenceRequest(async () => subscriptions!.startSubscriptionScheduler());
      for (const fire of callbacks) fire();
      await turn();
      assert.equal(
        callbacks.length,
        timerCount,
        "late accepted callers cannot re-arm a stopped native producer"
      );
      assert.equal(fetches, 1);
    } finally {
      release.resolve();
      if (runtime && subscriptions)
        await runtime
          .runPersistenceShutdownWork(() => subscriptions!.stopSubscriptionSchedulerAsync())
          .catch(() => {});
      await runtime?.closePersistenceRuntime().catch(() => {});
      await database.close();
      globalThis.fetch = originalFetch;
      t.mock.restoreAll();
      hooks.deregister();
      delete globalThis.__omniroutePersistenceRuntime;
      Reflect.deleteProperty(
        globalThis,
        Symbol.for("omniroute.proxySubscriptions.nativeScheduler")
      );
      for (const [name, value] of previous) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
