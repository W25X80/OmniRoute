import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { mkdtempSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setImmediate as tick } from "node:timers/promises";
import test, { mock } from "node:test";
import { getSelectedControlPlane } from "../../../src/lib/db/controlPlaneContext.ts";
import { barrier, fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { monitoringFixture as state } from "../../helpers/persistence/monitoringHealthFixture.ts";

const url = (path: string) => new URL(`../../../${path}`, import.meta.url).href;
const runtimeUrl = url("src/lib/db/persistenceRuntime.ts");
const routeUrl = url("src/app/api/monitoring/health/route.ts");
const runtimeFixtureUrl = url("tests/helpers/persistence/runtimeOwnedWorkFixture.ts");
const healthFixtureUrl = url("tests/helpers/persistence/monitoringHealthFixture.ts");
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
      context.parentURL === routeUrl &&
      specifier !== "next/server" &&
      specifier !== "@/lib/db/persistenceRuntime"
    )
      return { url: healthFixtureUrl, shortCircuit: true };
    return next(specifier, context);
  },
});
const previous = new Map(
  [
    "OMNIROUTE_DATABASE_BACKEND",
    "DATA_DIR",
    "DEEP_HEALTH_CHECK_ENABLED",
    "JWT_SECRET",
    "API_KEY_SECRET",
  ].map((key) => [key, process.env[key]])
);
const scratch = mkdtempSync(join(tmpdir(), "omniroute-monitoring-health-"));
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
process.env.DATA_DIR = scratch;
const runtime = (await import(
  runtimeUrl
)) as typeof import("../../../src/lib/db/persistenceRuntime.ts");
const route = (await import(
  routeUrl
)) as typeof import("../../../src/app/api/monitoring/health/route.ts");

function scheduler() {
  const pending: Array<{ resource: AsyncResource; run(): void }> = [];
  const resources: AsyncResource[] = [];
  mock.method(globalThis, "setImmediate", (callback: () => void) => {
    const resource = new AsyncResource("monitoring-health-test-immediate");
    resources.push(resource);
    pending.push({ resource, run: () => resource.runInAsyncScope(callback) });
    return {};
  });
  async function flush() {
    for (let count = 0; count < 20 && pending.length; count++) {
      pending.shift()!.run();
      await tick();
    }
  }
  async function flushUntil(predicate: () => boolean, message: string) {
    for (let count = 0; count < 50 && !predicate(); count++) {
      await tick();
      if (pending.length) await flush();
    }
    assert.ok(predicate(), message);
  }
  return {
    pending,
    flush,
    flushUntil,
    close() {
      for (const resource of resources) resource.emitDestroy();
    },
  };
}

function request(method = "GET", deep = false) {
  return new Request(`http://localhost/api/monitoring/health${deep ? "?deep=1" : ""}`, {
    method,
    headers: { "x-test-management": "1" },
  });
}

type Payload = {
  timestamp: string;
  providerBreakers: Array<{ version: number }>;
  deepHealth?: { status: number };
};

async function scenario(
  body: (clock: ReturnType<typeof scheduler>, advance: () => void) => Promise<void>
) {
  fixture.reset();
  Object.assign(state, {
    builds: 0,
    reads: 0,
    probes: 0,
    breakerVersion: 1,
    probeVersion: 200,
    readBarrier: undefined,
    buildBarrier: undefined,
    probeBarrier: undefined,
    buildEntered: undefined,
    probeEntered: undefined,
  });
  delete globalThis.__omniroutePersistenceRuntime;
  delete process.env.DEEP_HEALTH_CHECK_ENABLED;
  route.__test_resetMonitoringHealthPayloadCache();
  await runtime.ensurePersistenceReady();
  const clock = scheduler();
  let now = 1800000000000;
  mock.method(Date, "now", () => now);
  try {
    await body(clock, () => {
      now += 2000;
    });
  } finally {
    fixture.releaseBarriers();
    await clock.flush();
    await runtime.closePersistenceRuntime();
    clock.close();
    mock.restoreAll();
  }
}

async function get(clock: ReturnType<typeof scheduler>, deep = false): Promise<Payload> {
  const work = runtime.runPersistenceRequest(() => route.GET(request("GET", deep)));
  await tick();
  await clock.flush();
  return (await (await work).json()) as Payload;
}

test.after(() => {
  hooks.deregister();
  fixture.reset();
  delete globalThis.__omniroutePersistenceRuntime;
  rmSync(scratch, { recursive: true, force: true });
  for (const [key, value] of previous) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

test("concurrent cold health requests share one rebuild", { timeout: 5000 }, () =>
  scenario(async (clock) => {
    const requests = Array.from({ length: 4 }, () =>
      runtime.runPersistenceRequest(() => route.GET(request()))
    );
    await tick();
    await clock.flush();
    await Promise.all(requests);
    assert.equal(state.builds, 1);
  })
);

test("stale refresh survives the creator scope and stays coalesced", { timeout: 5000 }, () =>
  scenario(async (clock, advance) => {
    const first = await get(clock);
    const warnings = mock.method(console, "warn", () => {});
    advance();
    let creator: ReturnType<typeof getSelectedControlPlane>;
    const stale = await runtime.runPersistenceRequest(async () => {
      creator = getSelectedControlPlane();
      return (await (await route.GET(request())).json()) as Payload;
    });
    assert.throws(() => creator!.assertReady(), /scope ended/);
    assert.equal(stale.timestamp, first.timestamp);
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
    await runtime.runPersistenceRequest(() => route.GET(request()));
    assert.equal(clock.pending.length, 1);
    await clock.flush();
    const fresh = await get(clock);
    assert.notEqual(fresh.timestamp, first.timestamp);
    assert.equal(fresh.providerBreakers[0].version, 1);
    assert.equal(warnings.mock.callCount(), 0);
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
  })
);

test("unowned refresh refusal does not read the DB after admission closes", { timeout: 5000 }, () =>
  scenario(async (clock, advance) => {
    await get(clock);
    advance();
    runtime.beginPersistenceAdmissionClose();
    const before = state.reads;
    mock.method(console, "warn", () => {});
    await route.GET(request());
    await clock.flush();
    assert.equal(state.reads, before);
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
  })
);

test(
  "cold refresh refusal preserves the public view and permits a later retry",
  { timeout: 5000 },
  () =>
    scenario(async (clock) => {
      const capacity = barrier();
      const owners = Array.from({ length: 64 }, () =>
        runtime.runPersistenceBackgroundWork(() => capacity.promise)
      );
      mock.method(console, "error", () => {});
      try {
        assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 64);
        const response = await runtime.runPersistenceRequest(() =>
          route.GET(new Request("http://localhost/api/monitoring/health"))
        );
        assert.deepEqual(await response.json(), { status: "degraded" });
        assert.equal(state.reads, 0);
      } finally {
        capacity.resolve();
        await Promise.all(owners);
      }
      const recovered = await get(clock);
      assert.equal(recovered.providerBreakers[0].version, 1);
      assert.equal(state.builds, 1);
    })
);

test(
  "shutdown drains an already admitted refresh before closing persistence",
  { timeout: 5000 },
  () =>
    scenario(async (clock, advance) => {
      await get(clock);
      advance();
      const gate = barrier();
      state.readBarrier = gate.promise;
      const readsBeforeRefresh = state.reads;
      await runtime.runPersistenceRequest(() => route.GET(request()));
      await clock.flushUntil(
        () => state.reads > readsBeforeRefresh,
        "the admitted refresh must enter its settings read before shutdown starts"
      );
      const closing = runtime.closePersistenceRuntime();
      await tick();
      assert.equal(fixture.planeCloses, 0);
      gate.resolve();
      await closing;
      assert.equal(fixture.planeCloses, 1);
    })
);

test(
  "DELETE prevents an older builder from republishing pre-reset breakers",
  { timeout: 5000 },
  () =>
    scenario(async (clock, advance) => {
      await get(clock);
      advance();
      const entered = barrier(),
        release = barrier();
      state.buildBarrier = release.promise;
      state.buildEntered = entered.resolve;
      await runtime.runPersistenceRequest(() => route.GET(request()));
      await clock.flush();
      await entered.promise;
      await runtime.runPersistenceRequest(() => route.DELETE(request("DELETE")));
      const currentEntered = barrier(),
        currentRelease = barrier();
      state.buildBarrier = currentRelease.promise;
      state.buildEntered = currentEntered.resolve;
      const currentWork = get(clock);
      await tick();
      await clock.flush();
      await Promise.race([
        currentEntered.promise,
        tick().then(() => assert.fail("a reset must not join the held pre-reset refresh")),
      ]);
      release.resolve();
      await tick();
      const joinedWork = get(clock);
      await tick();
      await clock.flush();
      assert.equal(clock.pending.length, 0);
      assert.equal(state.builds, 2, "the old finalizer must preserve the newer in-flight build");
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      currentRelease.resolve();
      const [current, joined] = await Promise.all([currentWork, joinedWork]);
      assert.equal(current.providerBreakers[0].version, 2);
      assert.deepEqual(joined, current);
      const after = await get(clock);
      assert.equal(after.providerBreakers[0].version, 2);
    })
);

test("deep refresh cannot republish its verdict after reset", { timeout: 5000 }, () =>
  scenario(async (clock) => {
    process.env.DEEP_HEALTH_CHECK_ENABLED = "1";
    const entered = barrier(),
      release = barrier();
    state.probeBarrier = release.promise;
    state.probeEntered = entered.resolve;
    state.probeVersion = 503;
    await get(clock, true);
    assert.equal(state.probes, 1, "the admitted deep probe must reach its transport");
    await entered.promise;
    route.__test_resetMonitoringHealthPayloadCache();
    const currentEntered = barrier(),
      currentRelease = barrier();
    state.probeBarrier = currentRelease.promise;
    state.probeEntered = currentEntered.resolve;
    state.probeVersion = 200;
    await get(clock, true);
    await currentEntered.promise;
    release.resolve();
    await tick();
    await get(clock, true);
    assert.equal(state.probes, 2, "the old finalizer must preserve the newer in-flight probe");
    currentRelease.resolve();
    await tick();
    const after = await get(clock, true);
    assert.equal(after.deepHealth?.status, 200);
  })
);
