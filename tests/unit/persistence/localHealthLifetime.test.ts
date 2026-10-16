import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { mkdtempSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setImmediate } from "node:timers/promises";
import test, { mock } from "node:test";
import { getSelectedControlPlane } from "../../../src/lib/db/controlPlaneContext.ts";
import { barrier, fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { localHealthFixture } from "../../helpers/persistence/localHealthCheckFixture.ts";

const url = (path: string) => new URL(`../../../${path}`, import.meta.url).href;
const runtimeUrl = url("src/lib/db/persistenceRuntime.ts");
const healthUrl = url("src/lib/localHealthCheck.ts");
const runtimeFixtureUrl = url("tests/helpers/persistence/runtimeOwnedWorkFixture.ts");
const localFixtureUrl = url("tests/helpers/persistence/localHealthCheckFixture.ts");
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
  resolve(specifier, context, next) {
    if (context.parentURL === runtimeUrl && dependencies.has(specifier))
      return { url: runtimeFixtureUrl, shortCircuit: true };
    if (
      context.parentURL === healthUrl &&
      ["@/lib/db/readCache", "@/shared/utils/testProcess"].includes(specifier)
    )
      return { url: localFixtureUrl, shortCircuit: true };
    return next(specifier, context);
  },
});
const previous = new Map(
  [
    "OMNIROUTE_DATABASE_BACKEND",
    "OMNIROUTE_DISABLE_LOCAL_HEALTHCHECK",
    "JWT_SECRET",
    "API_KEY_SECRET",
    "NEXT_PHASE",
    "DATA_DIR",
  ].map((key) => [key, process.env[key]])
);
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
const scratch = mkdtempSync(join(tmpdir(), "omniroute-local-health-"));
process.env.DATA_DIR = scratch;
process.env.OMNIROUTE_DISABLE_LOCAL_HEALTHCHECK = "1";
const runtime = (await import(
  runtimeUrl
)) as typeof import("../../../src/lib/db/persistenceRuntime.ts");
const health = (await import(healthUrl)) as typeof import("../../../src/lib/localHealthCheck.ts");

// Preserve the actual AsyncLocalStorage context captured by each scheduler timer.
function timers() {
  const pending = new Map<object, { delay: number; run(): void }>();
  const resources: AsyncResource[] = [];
  mock.method(globalThis, "setTimeout", (callback: () => void, delay: number) => {
    const resource = new AsyncResource("local-health-test-timer");
    resources.push(resource);
    const token = {};
    pending.set(token, { delay, run: () => resource.runInAsyncScope(callback) });
    return token;
  });
  mock.method(globalThis, "clearTimeout", (token: object) => pending.delete(token));
  return {
    pending,
    fire() {
      const [token, timer] = pending.entries().next().value!;
      assert.ok(timer, "a scheduler timer must be armed");
      pending.delete(token);
      timer.run();
    },
    close() {
      for (const resource of resources) resource.emitDestroy();
    },
  };
}

async function scenario(body: () => Promise<void>) {
  fixture.reset();
  localHealthFixture.reads = 0;
  localHealthFixture.nodes = [];
  localHealthFixture.readBarrier = undefined;
  localHealthFixture.readFailure = undefined;
  delete globalThis.__omniroutePersistenceRuntime;
  health.getAllHealthStatuses();
  globalThis.__omnirouteLocalHC!.healthCache.clear();
  delete process.env.OMNIROUTE_DISABLE_LOCAL_HEALTHCHECK;
  delete process.env.NEXT_PHASE;
  try {
    await runtime.ensurePersistenceReady();
    await body();
  } finally {
    health.stopLocalHealthCheck();
    fixture.releaseBarriers();
    await runtime.closePersistenceRuntime();
    mock.restoreAll();
  }
}

test.after(() => {
  health.stopLocalHealthCheck();
  hooks.deregister();
  fixture.reset();
  delete globalThis.__omniroutePersistenceRuntime;
  rmSync(scratch, { recursive: true, force: true });
  for (const [key, value] of previous) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

test("initial and recurring timers replace ended request scopes with fresh admitted work", () =>
  scenario(async () => {
    const clock = timers();
    const errors = mock.method(console, "error", () => {});
    let creator: ReturnType<typeof getSelectedControlPlane>;
    try {
      await runtime.runPersistenceRequest(() => {
        creator = getSelectedControlPlane();
        health.initLocalHealthCheck();
      });
      assert.throws(() => creator!.assertReady(), /scope ended/);
      assert.equal([...clock.pending.values()][0].delay, 15000);
      clock.fire();
      await setImmediate();
      assert.equal(localHealthFixture.reads, 1);
      assert.equal([...clock.pending.values()][0].delay, 30000);
      clock.fire();
      await setImmediate();
      assert.equal(localHealthFixture.reads, 2);
      assert.equal(errors.mock.callCount(), 0);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
      health.stopLocalHealthCheck();
      assert.equal(clock.pending.size, 0);
    } finally {
      clock.close();
    }
  }));

test("overlapping sweeps are skipped; stop prevents an in-flight sweep from rearming", () =>
  scenario(async () => {
    const clock = timers();
    const release = barrier();
    try {
      localHealthFixture.readBarrier = release.promise;
      health.initLocalHealthCheck();
      clock.fire();
      await setImmediate();
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      await health.sweep();
      assert.equal(localHealthFixture.reads, 1);
      health.stopLocalHealthCheck();
      release.resolve();
      await setImmediate();
      assert.equal(clock.pending.size, 0);
      assert.equal(globalThis.__omnirouteLocalHC!.sweepInProgress, false);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    } finally {
      clock.close();
    }
  }));

test("admission refusal is observed without reading repositories or leaking a rejection", () =>
  scenario(async () => {
    const errors = mock.method(console, "error", () => {});
    runtime.beginPersistenceAdmissionClose();
    await health.sweep();
    assert.equal(localHealthFixture.reads, 0);
    assert.equal(errors.mock.callCount(), 1);
    assert.equal(globalThis.__omnirouteLocalHC!.sweepInProgress, false);
  }));

test("local node health, pruning, backoff and disabled/build guards retain their behavior", () =>
  scenario(async () => {
    const clock = timers();
    const fetch = mock.method(globalThis, "fetch", async () => new Response(null, { status: 503 }));
    try {
      process.env.OMNIROUTE_DISABLE_LOCAL_HEALTHCHECK = "1";
      health.initLocalHealthCheck();
      assert.equal(clock.pending.size, 0);
      delete process.env.OMNIROUTE_DISABLE_LOCAL_HEALTHCHECK;
      process.env.NEXT_PHASE = "phase-production-build";
      health.initLocalHealthCheck();
      assert.equal(clock.pending.size, 0);
      delete process.env.NEXT_PHASE;
      localHealthFixture.nodes = [
        { id: "local", prefix: "local", baseUrl: "http://127.0.0.1:20128/v1" },
        { id: "remote", prefix: "remote", baseUrl: "https://api.anthropic.com/v1" },
      ];
      health.initLocalHealthCheck();
      clock.fire();
      await setImmediate();
      assert.equal(fetch.mock.callCount(), 1);
      assert.equal(health.isNodeHealthy("local"), false);
      assert.equal([...clock.pending.values()][0].delay, 60000);
      localHealthFixture.nodes = [];
      clock.fire();
      await setImmediate();
      assert.equal(health.getHealthStatus("local"), undefined);
      assert.equal([...clock.pending.values()][0].delay, 30000);
    } finally {
      clock.close();
    }
  }));
