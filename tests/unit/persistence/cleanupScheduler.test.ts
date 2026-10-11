import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import type { TestContext } from "node:test";

type RuntimeModule = typeof import("../../../src/lib/db/persistenceRuntime.ts");
type SchedulerModule = typeof import("../../../src/lib/db/cleanupScheduler.ts");
const runtimeUrl = new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const contextUrl = new URL("../../../src/lib/db/controlPlaneContext.ts", import.meta.url).href;
const backendUrl = new URL("../../../src/lib/db/backendSelection.ts", import.meta.url).href;
const schedulerUrl = new URL("../../../src/lib/db/cleanupScheduler.ts", import.meta.url).href;
const pollerUrl = new URL("../../../src/lib/config/persistencePoller.ts", import.meta.url).href;
const fixtureUrl = new URL("../../helpers/persistence/runtimeOwnedWorkFixture.ts", import.meta.url)
  .href;
const dependencies = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "../artifacts/s3ArtifactStore",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    const parent = context.parentURL?.split("?")[0];
    if (parent === runtimeUrl && dependencies.has(specifier)) {
      return { url: fixtureUrl, shortCircuit: true };
    }
    if (
      (parent === schedulerUrl || parent === runtimeUrl) &&
      specifier === "./controlPlaneContext"
    ) {
      return { url: contextUrl, shortCircuit: true };
    }
    if (parent === schedulerUrl && specifier === "./persistenceRuntime") {
      return { url: runtimeUrl, shortCircuit: true };
    }
    if (parent === schedulerUrl && specifier === "../config/persistencePoller") {
      return { url: pollerUrl, shortCircuit: true };
    }
    if ((parent === runtimeUrl || parent === contextUrl) && specifier === "./backendSelection") {
      return { url: backendUrl, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});
const {
  getSelectedControlPlane,
  withPostgresControlPlane,
}: typeof import("../../../src/lib/db/controlPlaneContext.ts") = await import(contextUrl);
const { barrier, fixture }: typeof import("../../helpers/persistence/runtimeOwnedWorkFixture.ts") =
  await import(fixtureUrl);
const previous = {
  backend: process.env.OMNIROUTE_DATABASE_BACKEND,
  jwt: process.env.JWT_SECRET,
  key: process.env.API_KEY_SECRET,
};
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
const runtime: RuntimeModule = await import(runtimeUrl);
const scheduler: SchedulerModule = await import(schedulerUrl);
const duplicate: SchedulerModule = await import(`${schedulerUrl}?second-server-bundle`);
const interval = 6 * 60 * 60 * 1000;

async function settlePromises() {
  for (let i = 0; i < 24; i++) await Promise.resolve();
}

async function scenario(t: TestContext, body: () => Promise<void>): Promise<void> {
  assert.equal(globalThis.__omniroutePostgresCleanupScheduler, undefined);
  assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  delete globalThis.__omniroutePersistenceRuntime;
  fixture.reset();
  t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
  try {
    await runtime.ensurePersistenceReady();
    await body();
  } finally {
    fixture.releaseBarriers();
    await scheduler.stopPostgresCleanupSchedulerAsync();
    await runtime.closePersistenceRuntime();
    assert.equal(globalThis.__omniroutePostgresCleanupScheduler, undefined);
    assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  }
}

test.after(() => {
  hooks.deregister();
  fixture.reset();
  delete globalThis.__omniroutePersistenceRuntime;
  for (const [key, value] of [
    ["OMNIROUTE_DATABASE_BACKEND", previous.backend],
    ["JWT_SECRET", previous.jwt],
    ["API_KEY_SECRET", previous.key],
  ] as const) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

test("stop cancels preparation and startup before native work is admitted", (t) =>
  scenario(t, async () => {
    let passes = 0;
    scheduler.startPostgresCleanupScheduler(async () => {
      passes++;
    }, interval);
    scheduler.stopPostgresCleanupScheduler();
    t.mock.timers.tick(interval * 3);
    await scheduler.stopPostgresCleanupSchedulerAsync();
    assert.equal(passes, 0);
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    scheduler.startPostgresCleanupScheduler(async () => {
      passes++;
    }, interval);
    await settlePromises();
    scheduler.stopPostgresCleanupScheduler();
    t.mock.timers.tick(30_000 + interval);
    await scheduler.stopPostgresCleanupSchedulerAsync();
    assert.equal(passes, 0);
  }));

test("a request-inherited timer uses a fresh catalog for startup and every periodic pass", (t) =>
  scenario(t, async () => {
    const generations: Array<{ phase: string; generation: number | undefined }> = [];
    let requestGeneration: number | undefined;
    const first = barrier(),
      second = barrier();
    await runtime.runPersistenceRequest(async () => {
      requestGeneration = fixture.selectedGeneration();
      scheduler.startPostgresCleanupScheduler(async (phase) => {
        generations.push({ phase, generation: fixture.selectedGeneration() });
        if (generations.length === 1) first.resolve();
        else second.resolve();
      }, interval);
    });
    await settlePromises();
    t.mock.timers.tick(30_000);
    await first.promise;
    await settlePromises();
    t.mock.timers.tick(interval);
    await second.promise;
    await settlePromises();
    assert.deepEqual(
      generations.map((pass) => pass.phase),
      ["startup", "periodic"]
    );
    assert.ok(typeof requestGeneration === "number");
    const previousGeneration = requestGeneration;
    assert.ok(
      generations.every(
        (pass) => typeof pass.generation === "number" && pass.generation > previousGeneration
      )
    );
    assert.notEqual(generations[0].generation, generations[1].generation);
  }));

test("duplicate bundles share one owner and either bundle can stop it", (t) =>
  scenario(t, async () => {
    const entered = barrier();
    let originalPasses = 0,
      duplicatePasses = 0;
    scheduler.startPostgresCleanupScheduler(async () => {
      originalPasses++;
      entered.resolve();
    }, interval);
    duplicate.startPostgresCleanupScheduler(async () => {
      duplicatePasses++;
    }, interval);
    await settlePromises();
    t.mock.timers.tick(30_000);
    await entered.promise;
    await settlePromises();
    await duplicate.stopPostgresCleanupSchedulerAsync();
    t.mock.timers.tick(interval * 3);
    await settlePromises();
    assert.equal(originalPasses, 1);
    assert.equal(duplicatePasses, 0);
    assert.equal(globalThis.__omniroutePostgresCleanupScheduler, undefined);
  }));

test("single flight and async stop include artifact deletion/acknowledgement completion", (t) =>
  scenario(t, async () => {
    const entered = barrier(),
      databaseDone = barrier(),
      finishDatabase = barrier(),
      finishArtifact = barrier();
    let passes = 0,
      stopped = false;
    scheduler.startPostgresCleanupScheduler(async () => {
      passes++;
      entered.resolve();
      await finishDatabase.promise;
      databaseDone.resolve();
      await finishArtifact.promise;
      assert.ok(fixture.selectedGeneration());
    }, interval);
    await settlePromises();
    t.mock.timers.tick(30_000);
    await entered.promise;
    t.mock.timers.tick(interval * 4);
    await settlePromises();
    assert.equal(passes, 1);
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
    scheduler.stopPostgresCleanupScheduler();
    const draining = scheduler.stopPostgresCleanupSchedulerAsync().then(() => {
      stopped = true;
    });
    assert.throws(
      () => scheduler.startPostgresCleanupScheduler(async () => {}, interval),
      /draining/
    );
    finishDatabase.resolve();
    await databaseDone.promise;
    await settlePromises();
    assert.equal(stopped, false, "SQL completion must not release pending artifact I/O");
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
    t.mock.timers.tick(interval * 3);
    assert.equal(passes, 1);
    finishArtifact.resolve();
    await draining;
    assert.equal(stopped, true);
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    assert.equal(globalThis.__omniroutePostgresCleanupScheduler, undefined);
  }));

test("failed snapshot admission is sanitized and a later pass obtains fresh authority", (t) =>
  scenario(t, async () => {
    const failed = barrier(),
      entered = barrier();
    const messages: string[] = [];
    t.mock.method(console, "error", (message: string) => {
      messages.push(message);
      failed.resolve();
    });
    fixture.catalogFailure = new Error(
      "postgres://private-user:private-secret@private-host/database"
    );
    let passes = 0;
    scheduler.startPostgresCleanupScheduler(async () => {
      passes++;
      entered.resolve();
    }, interval);
    await settlePromises();
    t.mock.timers.tick(30_000);
    await failed.promise;
    await settlePromises();
    assert.equal(passes, 0);
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    assert.deepEqual(messages, ["[Cleanup] PostgreSQL scheduled cleanup failed"]);
    fixture.catalogFailure = undefined;
    t.mock.timers.tick(interval);
    await entered.promise;
    await settlePromises();
    assert.equal(passes, 1);
  }));

test("a selected identity mismatch cannot replace the live cleanup owner", (t) =>
  scenario(t, async () => {
    scheduler.startPostgresCleanupScheduler(async () => {}, interval);
    const originalOwner = globalThis.__omniroutePostgresCleanupScheduler;
    const binding = getSelectedControlPlane();
    assert.ok(binding);
    withPostgresControlPlane({ ...binding, databaseIdentity: Object.freeze({}) }, () => {
      assert.throws(
        () => duplicate.startPostgresCleanupScheduler(async () => {}, interval),
        /identity changed/
      );
    });
    assert.equal(globalThis.__omniroutePostgresCleanupScheduler, originalOwner);
  }));

test("draining runtime denies a timer inherited from an ended request", (t) =>
  scenario(t, async () => {
    const failed = barrier();
    let passes = 0;
    t.mock.method(console, "error", () => {
      failed.resolve();
    });
    await runtime.runPersistenceRequest(async () => {
      scheduler.startPostgresCleanupScheduler(async () => {
        passes++;
      }, interval);
    });
    await settlePromises();
    const permit = await runtime.acquirePersistenceTail();
    const closing = runtime.closePersistenceRuntime();
    try {
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
      t.mock.timers.tick(30_000);
      await failed.promise;
      await settlePromises();
      assert.equal(passes, 0);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    } finally {
      await scheduler.stopPostgresCleanupSchedulerAsync();
      await permit.release();
      await closing;
    }
  }));
