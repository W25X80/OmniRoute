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

type ScheduledImmediate = { resource: AsyncResource; run(): void };

type DeferredImmediate = {
  readonly registered: boolean;
  release(): void;
  capture(entry: ScheduledImmediate): void;
};

function scheduler() {
  const pending: Array<{ resource: AsyncResource; run(): void }> = [];
  const resources: AsyncResource[] = [];
  const armedRegistrations: DeferredImmediate[] = [];
  const deferredRegistrations = new Set<DeferredImmediate>();
  mock.method(globalThis, "setImmediate", (callback: () => void) => {
    const resource = new AsyncResource("monitoring-health-test-immediate");
    resources.push(resource);
    const entry = { resource, run: () => resource.runInAsyncScope(callback) };
    const deferred = armedRegistrations.shift();
    if (deferred) deferred.capture(entry);
    else pending.push(entry);
    return {};
  });
  async function flush() {
    for (let count = 0; count < 20 && pending.length; count++) {
      pending.shift()!.run();
      await tick();
    }
  }
  async function pumpUntil(predicate: () => boolean, message: string) {
    for (let count = 0; count < 50 && !predicate(); count++) {
      await tick();
      if (pending.length) await flush();
    }
    assert.ok(predicate(), message);
  }
  async function pumpUntilSettled<T>(work: Promise<T>, message: string): Promise<T> {
    let settled = false;
    void work.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      }
    );
    await pumpUntil(() => settled, message);
    return await work;
  }
  function deferNextRegistration(): DeferredImmediate {
    let registered = false;
    let released = false;
    let queued = false;
    let entry: ScheduledImmediate | undefined;
    const deferred: DeferredImmediate = {
      get registered() {
        return registered;
      },
      release() {
        released = true;
        if (entry && !queued) {
          pending.push(entry);
          queued = true;
          deferredRegistrations.delete(deferred);
        }
      },
      capture(value) {
        entry = value;
        registered = true;
        deferredRegistrations.add(deferred);
        if (released) deferred.release();
      },
    };
    armedRegistrations.push(deferred);
    deferredRegistrations.add(deferred);
    return deferred;
  }
  function releaseDeferredRegistrations() {
    for (const deferred of [...deferredRegistrations]) deferred.release();
  }
  return {
    pending,
    flush,
    pumpUntil,
    pumpUntilSettled,
    deferNextRegistration,
    releaseDeferredRegistrations,
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
    try {
      fixture.releaseBarriers();
    } finally {
      try {
        clock.releaseDeferredRegistrations();
      } finally {
        try {
          await clock.pumpUntilSettled(
            runtime.closePersistenceRuntime(),
            "persistence shutdown must settle after held fixture work is released"
          );
        } finally {
          try {
            clock.releaseDeferredRegistrations();
          } finally {
            try {
              clock.close();
            } finally {
              mock.restoreAll();
            }
          }
        }
      }
    }
  }
}

function startGet(deep = false) {
  let settled = false;
  let routeInvoked = false;
  const work = runtime
    .runPersistenceRequest(() => {
      const response = route.GET(request("GET", deep));
      routeInvoked = true;
      return response;
    })
    .then(
      (response) => {
        settled = true;
        return response;
      },
      (error: unknown) => {
        settled = true;
        throw error;
      }
    );
  return { work, isRouteInvoked: () => routeInvoked, isSettled: () => settled };
}

async function get(clock: ReturnType<typeof scheduler>, deep = false): Promise<Payload> {
  const started = startGet(deep);
  await clock.pumpUntil(
    () => started.isSettled(),
    "health GET must settle after its scheduled work"
  );
  return (await (await started.work).json()) as Payload;
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
    const requests = Array.from({ length: 4 }, () => startGet());
    await clock.pumpUntil(
      () => requests.every((work) => work.isSettled()),
      "all cold health requests must settle after the shared rebuild"
    );
    await Promise.all(requests.map((work) => work.work));
    assert.equal(state.builds, 1);
  })
);

test("stale refresh survives the creator scope and stays coalesced", { timeout: 5000 }, () =>
  scenario(async (clock, advance) => {
    const first = await get(clock);
    const warnings = mock.method(console, "warn", () => {});
    advance();
    const readGate = barrier();
    state.readBarrier = readGate.promise;
    const readsBeforeRefresh = state.reads;
    let creator: ReturnType<typeof getSelectedControlPlane>;
    const stale = await runtime.runPersistenceRequest(async () => {
      creator = getSelectedControlPlane();
      return (await (await route.GET(request())).json()) as Payload;
    });
    assert.throws(() => creator!.assertReady(), /scope ended/);
    assert.equal(stale.timestamp, first.timestamp);
    await clock.pumpUntil(
      () => state.reads > readsBeforeRefresh,
      "the coalesced refresh must enter its settings read"
    );
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
    const joined = startGet();
    await clock.pumpUntil(
      () => joined.isSettled(),
      "a stale coalesced GET must return its cached view"
    );
    await joined.work;
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
    readGate.resolve();
    await clock.pumpUntil(
      () => runtime.getPersistenceRuntimeStatus().backgroundOperations === 0,
      "the admitted refresh must finish after its settings read is released"
    );
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
    const warnings = mock.method(console, "warn", () => {});
    await route.GET(request());
    await clock.pumpUntil(
      () => warnings.mock.callCount() > 0,
      "the refused refresh must settle through its background-admission warning"
    );
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
        const response = runtime.runPersistenceRequest(() =>
          route.GET(new Request("http://localhost/api/monitoring/health"))
        );
        const settledResponse = await clock.pumpUntilSettled(
          response,
          "a refused cold refresh must return its degraded response"
        );
        assert.deepEqual(await settledResponse.json(), { status: "degraded" });
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
      const stale = runtime.runPersistenceRequest(() => route.GET(request()));
      await clock.pumpUntilSettled(stale, "stale health response must settle before shutdown");
      await clock.pumpUntil(
        () => state.reads > readsBeforeRefresh,
        "the admitted refresh must enter its settings read before shutdown starts"
      );
      const closing = runtime.closePersistenceRuntime();
      await tick();
      assert.equal(fixture.planeCloses, 0);
      gate.resolve();
      await clock.pumpUntilSettled(closing, "shutdown must drain the admitted refresh");
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
      const release = barrier();
      let oldBuildEntered = false;
      state.buildBarrier = release.promise;
      state.buildEntered = () => {
        oldBuildEntered = true;
      };
      const deferred = clock.deferNextRegistration();
      const stale = runtime.runPersistenceRequest(() => route.GET(request()));
      await clock.pumpUntilSettled(stale, "expired GET must return its cached payload");
      await clock.pumpUntil(
        () => deferred.registered,
        "the real background refresh must register its immediate callback"
      );
      await clock.flush();
      assert.equal(
        clock.pending.length,
        0,
        "the controlled callback remains outside the pump queue"
      );
      assert.equal(oldBuildEntered, false, "one-shot drain cannot run a callback behind its gate");
      deferred.release();
      await clock.pumpUntil(
        () => oldBuildEntered,
        "the old refresh must enter its held build after delayed registration is released"
      );
      await runtime.runPersistenceRequest(() => route.DELETE(request("DELETE")));
      const currentRelease = barrier();
      let currentBuildEntered = false;
      state.buildBarrier = currentRelease.promise;
      state.buildEntered = () => {
        currentBuildEntered = true;
      };
      const currentRegistration = clock.deferNextRegistration();
      const currentWork = startGet();
      await clock.pumpUntil(
        () => currentRegistration.registered,
        "the post-reset builder must register its own immediate callback"
      );
      currentRegistration.release();
      await clock.pumpUntil(
        () => currentBuildEntered,
        "the post-reset builder must enter its independent held build"
      );
      release.resolve();
      await clock.pumpUntil(
        () => runtime.getPersistenceRuntimeStatus().backgroundOperations === 1,
        "the pre-reset builder must settle while the post-reset builder remains held"
      );
      const joinedWork = startGet();
      await clock.pumpUntil(
        () => joinedWork.isRouteInvoked(),
        "the request during a held post-reset build must enter the route"
      );
      await tick();
      await clock.flush();
      assert.equal(state.builds, 2, "the old finalizer must preserve the newer in-flight build");
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      assert.equal(
        joinedWork.isSettled(),
        false,
        "the joining request must wait for the current build"
      );
      currentRelease.resolve();
      await clock.pumpUntilSettled(currentWork.work, "the post-reset build request must settle");
      await clock.pumpUntilSettled(
        joinedWork.work,
        "the joined request must share the post-reset build"
      );
      const [current, joined] = await Promise.all([
        currentWork.work.then(async (response) => (await response.json()) as Payload),
        joinedWork.work.then(async (response) => (await response.json()) as Payload),
      ]);
      assert.equal(current.providerBreakers[0].version, 2);
      assert.deepEqual(joined, current);
      const after = await get(clock);
      assert.equal(after.providerBreakers[0].version, 2);
    })
);

test("deep refresh cannot republish its verdict after reset", { timeout: 5000 }, () =>
  scenario(async (clock) => {
    await get(clock);
    process.env.DEEP_HEALTH_CHECK_ENABLED = "1";
    const release = barrier();
    let oldProbeEntered = false;
    state.probeBarrier = release.promise;
    state.probeEntered = () => {
      oldProbeEntered = true;
    };
    state.probeVersion = 503;
    const oldProbeRegistration = clock.deferNextRegistration();
    const firstDeep = startGet(true);
    await clock.pumpUntil(
      () => oldProbeRegistration.registered,
      "the old deep refresh must register after the warm payload request"
    );
    await clock.pumpUntil(
      () => firstDeep.isSettled(),
      "the initial deep GET must serve its cached payload"
    );
    await firstDeep.work;
    await clock.flush();
    assert.equal(
      oldProbeEntered,
      false,
      "the delayed deep probe is not part of the one-shot drain"
    );
    oldProbeRegistration.release();
    await clock.pumpUntil(
      () => oldProbeEntered,
      "the old deep probe must enter its held transport after registration is released"
    );
    assert.equal(state.probes, 1, "the admitted deep probe must reach its transport");
    route.__test_resetMonitoringHealthPayloadCache();
    await get(clock);
    const currentRelease = barrier();
    let currentProbeEntered = false;
    state.probeBarrier = currentRelease.promise;
    state.probeEntered = () => {
      currentProbeEntered = true;
    };
    state.probeVersion = 200;
    const currentProbeRegistration = clock.deferNextRegistration();
    const currentDeep = startGet(true);
    await clock.pumpUntil(
      () => currentProbeRegistration.registered,
      "the new deep refresh must register its own immediate callback"
    );
    await clock.pumpUntil(
      () => currentDeep.isSettled(),
      "the current deep GET must serve its cached payload"
    );
    await currentDeep.work;
    currentProbeRegistration.release();
    await clock.pumpUntil(
      () => currentProbeEntered,
      "the post-reset deep probe must enter its independent held transport"
    );
    release.resolve();
    await clock.pumpUntil(
      () => runtime.getPersistenceRuntimeStatus().backgroundOperations === 1,
      "the pre-reset deep probe must settle while the new probe remains held"
    );
    const joinedDeep = startGet(true);
    await clock.pumpUntil(
      () => joinedDeep.isRouteInvoked(),
      "a deep GET during the held post-reset probe must enter the route"
    );
    await tick();
    await clock.flush();
    assert.equal(
      joinedDeep.isSettled(),
      true,
      "a cached deep GET should serve while its probe is detached"
    );
    await joinedDeep.work;
    assert.equal(state.probes, 2, "the old finalizer must preserve the newer in-flight probe");
    currentRelease.resolve();
    await clock.pumpUntil(
      () => runtime.getPersistenceRuntimeStatus().backgroundOperations === 0,
      "the current deep probe must settle after its transport is released"
    );
    const after = await get(clock, true);
    assert.equal(after.deepHealth?.status, 200);
  })
);
