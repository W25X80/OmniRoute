import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { registerHooks } from "node:module";
import test, { after } from "node:test";
import { startupFixture } from "../helpers/persistence/startupLifetimeFixture.ts";

const fixtureUrl = new URL("../helpers/persistence/startupLifetimeFixture.ts", import.meta.url)
  .href;
const instrumentationUrl = new URL("../../src/instrumentation-node.ts", import.meta.url).href;
const shutdownUrl = new URL("../../src/lib/startup/postgresShutdown.ts", import.meta.url).href;
const bootstrapUrl = new URL("../../src/lib/startup/postgresBootstrap.ts", import.meta.url).href;
const codexUrl = new URL("../../src/shared/services/codexCatalogRevalidation.ts", import.meta.url)
  .href;
const modelUrl = new URL("../../src/shared/services/modelSyncScheduler.ts", import.meta.url).href;
const modelDependencies = new Set([
  "@/lib/db/persistenceRuntime",
  "@/lib/db/controlPlaneContext",
  "@/lib/db/settings",
  "@/lib/db/providers",
  "@/lib/exclusiveLeaseIsolation",
  "@/lib/runtime/ports",
  "@/shared/utils/timingSafeCompare",
  "./codexCatalogRevalidation",
  "@omniroute/open-sse/utils/error.ts",
]);
const codexDependencies = new Set([
  "@/lib/db/persistenceRuntime",
  "@/lib/db/controlPlaneContext",
  "@/lib/db/settings",
  "@/lib/db/providers",
  "@/lib/db/models",
  "./modelSyncScheduler",
]);
const instrumentationDependencies = new Set([
  "@/lib/db/persistenceRuntime",
  "@/lib/serverLifecycle",
  "@/lib/startup/nonLoopbackApiKeyGuard",
  "@/lib/startup/nativeFileCaches",
  "@omniroute/open-sse/utils/error.ts",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === bootstrapUrl) {
      return { url: fixtureUrl, shortCircuit: true };
    }
    if (context.parentURL === shutdownUrl && specifier !== "./postgresBootstrap") {
      if (startupFixture.importFailure && specifier === "@/lib/proxyHealth/scheduler") {
        throw new Error("Optional producer module import failed");
      }
      return { url: fixtureUrl, shortCircuit: true };
    }
    if (
      (context.parentURL === modelUrl && modelDependencies.has(specifier)) ||
      (context.parentURL === codexUrl && codexDependencies.has(specifier)) ||
      (context.parentURL === instrumentationUrl && instrumentationDependencies.has(specifier))
    ) {
      return { url: fixtureUrl, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});
const shutdown = await import("../../src/lib/startup/postgresShutdown.ts");
const instrumentation = await import("../../src/instrumentation-node.ts");
const models = await import("../../src/shared/services/modelSyncScheduler.ts");
const codex = await import("../../src/shared/services/codexCatalogRevalidation.ts");
after(() => hooks.deregister());

function resetFixture() {
  // The production startup owner is process-wide and intentionally cannot be reset by callers.
  const symbol = Symbol.for("omniroute.postgresStartupOwner");
  delete (globalThis as typeof globalThis & { [symbol]?: unknown })[symbol];
  startupFixture.events.length = 0;
  startupFixture.modelGenerations.length = 0;
  startupFixture.stopBarrier = undefined;
  startupFixture.subscriptionBarrier = undefined;
  startupFixture.modelBarrier = undefined;
  startupFixture.codexBarrier = undefined;
  startupFixture.readyFailure = false;
  startupFixture.bootFailure = false;
  startupFixture.stopFailure = false;
  startupFixture.flushFailure = false;
  startupFixture.importFailure = false;
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((accept) => {
    resolve = accept;
  });
  return { promise, resolve };
}
const turn = () => new Promise<void>((resolve) => setImmediate(resolve));

// These are coordinator composition oracles, not PostgreSQL/database/transport tests.
test("native shutdown closes every producer before drains and closes the pool last", async () => {
  resetFixture();
  const barrier = deferred();
  startupFixture.stopBarrier = barrier.promise;
  const subscriptionBarrier = deferred();
  startupFixture.subscriptionBarrier = subscriptionBarrier.promise;
  let done = false;
  const closing = shutdown.shutdownPostgresApplication().then(() => {
    done = true;
  });
  await turn();
  assert.equal(done, false);
  assert.ok(startupFixture.events.includes("stop:cloud"));
  assert.ok(startupFixture.events.includes("stop:batch"));
  assert.equal(
    startupFixture.events.some((event) => event.startsWith("flush:")),
    false
  );
  barrier.resolve();
  await turn();
  assert.equal(done, false, "shutdown must also await the accepted subscription stop");
  assert.equal(
    startupFixture.events.some((event) => event.startsWith("flush:")),
    false
  );
  subscriptionBarrier.resolve();
  await closing;
  assert.equal(startupFixture.events.at(-1), "pool:close");
  const firstFlush = startupFixture.events.findIndex((event) => event.startsWith("flush:"));
  const lastStop = startupFixture.events.findLastIndex((event) => event.startsWith("stop:"));
  assert.ok(firstFlush > lastStop);
  for (const event of ["stop:proxyLogAdmission", "accepted:drain", "resource:proxyLogs"]) {
    assert.equal(startupFixture.events.filter((observed) => observed === event).length, 1);
  }
  assert.ok(
    startupFixture.events.indexOf("stop:proxyLogAdmission") <
      startupFixture.events.indexOf("accepted:drain")
  );
  assert.ok(
    startupFixture.events.indexOf("resource:proxyLogs") >
      startupFixture.events.indexOf("accepted:drain")
  );
  assert.ok(
    startupFixture.events.indexOf("resource:proxyLogs") <
      startupFixture.events.indexOf("pool:close")
  );
  assert.equal(startupFixture.events.filter((event) => event === "stop:subscription").length, 1);
  assert.ok(startupFixture.events.indexOf("stop:subscriptionSettled") < firstFlush);
  assert.ok(startupFixture.events.indexOf("shutdown:enter") < firstFlush);
  assert.deepEqual(
    startupFixture.events.filter((event) => event.startsWith("flush:")),
    ["flush:spend", "flush:callLogs", "flush:ccr", "flush:gemini", "flush:a2a", "flush:plugins"]
  );
});

test("one failed producer import/stop or flush does not bypass independent cleanup", async () => {
  resetFixture();
  startupFixture.importFailure = true;
  startupFixture.stopFailure = true;
  startupFixture.flushFailure = true;
  await assert.rejects(shutdown.shutdownPostgresApplication(), (error: unknown) => {
    assert.ok(error instanceof AggregateError);
    assert.equal(error.errors.length, 3);
    return true;
  });
  assert.ok(startupFixture.events.includes("stop:batch"));
  assert.ok(startupFixture.events.includes("flush:plugins"));
  assert.equal(startupFixture.events.at(-1), "pool:close");
});

test("PostgreSQL build/unsupported transport/factory failure never reaches ready", async () => {
  resetFixture();
  const saved = {
    backend: process.env.OMNIROUTE_DATABASE_BACKEND,
    phase: process.env.NEXT_PHASE,
    building: process.env.OMNIROUTE_BUILDING,
    marker: globalThis.__omniroutePersistenceTransportInstalled,
  };
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  delete process.env.OMNIROUTE_BUILDING;
  try {
    process.env.NEXT_PHASE = "phase-production-build";
    await instrumentation.registerNodejs();
    assert.deepEqual(startupFixture.events, []);
    delete process.env.NEXT_PHASE;
    globalThis.__omniroutePersistenceTransportInstalled = false;
    await assert.rejects(instrumentation.registerNodejs(), /supported OmniRoute HTTP transport/);
    assert.equal(startupFixture.events.includes("ready"), false);
    globalThis.__omniroutePersistenceTransportInstalled = true;
    startupFixture.readyFailure = true;
    await assert.rejects(instrumentation.registerNodejs(), /Required bootstrap failed/);
    assert.equal(startupFixture.events.includes("server:ready"), false);
    assert.equal(startupFixture.events.at(-1), "pool:close");
    assert.equal(globalThis.__omniroutePersistenceRequest, undefined);
    resetFixture();
    startupFixture.bootFailure = true;
    await assert.rejects(instrumentation.registerNodejs(), /Required registrar failed/);
    assert.ok(startupFixture.events.includes("stop:cloud"));
    assert.equal(startupFixture.events.at(-1), "pool:close");
    assert.equal(startupFixture.events.includes("server:ready"), false);
  } finally {
    for (const [name, value] of Object.entries({
      OMNIROUTE_DATABASE_BACKEND: saved.backend,
      NEXT_PHASE: saved.phase,
      OMNIROUTE_BUILDING: saved.building,
    })) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    globalThis.__omniroutePersistenceTransportInstalled = saved.marker;
  }
});

test("native model timers enter fresh scopes, skip overlap and drain before stop resolves", async () => {
  resetFixture();
  const previous = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const original = { setTimeout, setInterval, clearTimeout, clearInterval };
  type Handle = { unref(): void };
  const timers = new Map<Handle, { callback: () => void; delay: number }>();
  const arm = (callback: () => void, delay: number, once: boolean) => {
    const handle = { unref() {} };
    timers.set(handle, {
      callback: () => {
        if (once) timers.delete(handle);
        callback();
      },
      delay,
    });
    return handle;
  };
  globalThis.setTimeout = ((callback: () => void, delay: number) =>
    arm(callback, delay, true)) as unknown as typeof setTimeout;
  globalThis.setInterval = ((callback: () => void, delay: number) =>
    arm(callback, delay, false)) as unknown as typeof setInterval;
  globalThis.clearTimeout = ((handle: Handle) => {
    timers.delete(handle);
  }) as unknown as typeof clearTimeout;
  globalThis.clearInterval = ((handle: Handle) => {
    timers.delete(handle);
  }) as unknown as typeof clearInterval;
  const detached = new AsyncResource("model-sync-independent-timer");
  try {
    models.startModelSyncScheduler(undefined, 1000);
    await turn();
    const startup = [...timers.values()].find(
      (timer) => timer.delay === models.MODEL_SYNC_STARTUP_DELAY_MS
    )!;
    const phase = [...timers.values()].find(
      (timer) => timer.delay === models.MODEL_SYNC_STAGGER_OFFSET_MS
    )!;
    assert.ok(startup);
    assert.ok(phase);
    detached.runInAsyncScope(startup.callback);
    await turn();
    assert.equal(startupFixture.modelGenerations.length, 1);
    detached.runInAsyncScope(phase.callback);
    const recurring = [...timers.values()].find((timer) => timer.delay === 1000)!;
    const barrier = deferred();
    startupFixture.modelBarrier = barrier.promise;
    detached.runInAsyncScope(recurring.callback);
    await turn();
    detached.runInAsyncScope(recurring.callback);
    assert.equal(startupFixture.modelGenerations.length, 2);
    assert.notEqual(startupFixture.modelGenerations[0], startupFixture.modelGenerations[1]);
    let stopped = false;
    const stop = models.stopModelSyncSchedulerAsync().then(() => {
      stopped = true;
    });
    await turn();
    assert.equal(stopped, false);
    detached.runInAsyncScope(startup.callback);
    detached.runInAsyncScope(recurring.callback);
    assert.equal(startupFixture.modelGenerations.length, 2);
    barrier.resolve();
    await stop;
    assert.equal(timers.size, 0);
  } finally {
    models.stopModelSyncScheduler();
    Object.assign(globalThis, original);
    detached.emitDestroy();
    if (previous === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = previous;
  }
});

test("native Codex zero-delay revalidation owns its read lifetime and stop drains it", async () => {
  resetFixture();
  const previous = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const barrier = deferred();
  startupFixture.codexBarrier = barrier.promise;
  try {
    codex.scheduleCodexCatalogRevalidation();
    const deadline = Date.now() + 5000;
    while (!startupFixture.events.includes("codex:read") && Date.now() < deadline) {
      await turn();
    }
    assert.ok(startupFixture.events.includes("codex:read"));
    let stopped = false;
    const stopping = codex.stopCodexCatalogRevalidation().then(() => {
      stopped = true;
    });
    await turn();
    assert.equal(stopped, false);
    barrier.resolve();
    await stopping;
    assert.equal(stopped, true);
  } finally {
    barrier.resolve();
    await codex.stopCodexCatalogRevalidation();
    if (previous === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = previous;
  }
});

test("native Codex stop cancels accepted work before the zero-delay callback starts", async () => {
  resetFixture();
  const previous = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const original = { setTimeout, clearTimeout };
  type Handle = { unref(): void };
  const timers = new Map<Handle, () => void>();
  globalThis.setTimeout = ((callback: () => void) => {
    const handle = { unref() {} };
    timers.set(handle, callback);
    return handle;
  }) as unknown as typeof setTimeout;
  globalThis.clearTimeout = ((handle: Handle) => {
    timers.delete(handle);
  }) as unknown as typeof clearTimeout;
  try {
    codex.scheduleCodexCatalogRevalidation();
    await turn();
    assert.equal(timers.size, 1);
    await codex.stopCodexCatalogRevalidation();
    assert.equal(timers.size, 0);
    assert.equal(startupFixture.events.includes("codex:read"), false);
  } finally {
    await codex.stopCodexCatalogRevalidation();
    Object.assign(globalThis, original);
    if (previous === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = previous;
  }
});
