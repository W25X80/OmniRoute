import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import type { PostgresControlPlaneBinding } from "../../../src/lib/db/controlPlaneContext.ts";

const runtimeUrl = new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const contextUrl = new URL("../../../src/lib/db/controlPlaneContext.ts", import.meta.url).href;
const backendUrl = new URL("../../../src/lib/db/backendSelection.ts", import.meta.url).href;
const rotationUrl = new URL("../../../src/lib/usage/callLogRotation.ts", import.meta.url).href;
const ownerFixtureUrl = new URL(
  "../../helpers/persistence/runtimeOwnedWorkFixture.ts",
  import.meta.url
).href;
const rotationFixtureUrl = new URL(
  "../../helpers/persistence/callLogRotationAuthorityFixture.ts",
  import.meta.url
).href;
const runtimeDependencies = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "../artifacts/s3ArtifactStore",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);
const rotationDependencies = new Set([
  "../db/postgres/callLogRepository",
  "../db/core",
  "./callLogsBoundedQueries",
  "./callLogArtifacts",
  "../logEnv",
  "../db/healthCheck",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    const parent = context.parentURL?.split("?")[0];
    if (parent === rotationUrl) {
      if (rotationDependencies.has(specifier))
        return { url: rotationFixtureUrl, shortCircuit: true };
      if (specifier === "../db/persistenceRuntime") return { url: runtimeUrl, shortCircuit: true };
      if (specifier === "../db/controlPlaneContext") return { url: contextUrl, shortCircuit: true };
    }
    if (parent === runtimeUrl) {
      if (runtimeDependencies.has(specifier)) return { url: ownerFixtureUrl, shortCircuit: true };
      if (specifier === "./controlPlaneContext") return { url: contextUrl, shortCircuit: true };
    }
    if ((parent === runtimeUrl || parent === contextUrl) && specifier === "./backendSelection") {
      return { url: backendUrl, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});
const context: typeof import("../../../src/lib/db/controlPlaneContext.ts") = await import(
  contextUrl
);
const runtime: typeof import("../../../src/lib/db/persistenceRuntime.ts") = await import(
  runtimeUrl
);
const rotation: typeof import("../../../src/lib/usage/callLogRotation.ts") = await import(
  rotationUrl
);
const duplicate: typeof rotation = await import(`${rotationUrl}?second-next-bundle`);
const {
  fixture: ownerFixture,
}: typeof import("../../helpers/persistence/runtimeOwnedWorkFixture.ts") = await import(
  ownerFixtureUrl
);
const {
  rotationFixture,
}: typeof import("../../helpers/persistence/callLogRotationAuthorityFixture.ts") = await import(
  rotationFixtureUrl
);
const symbol = Symbol.for("omniroute.callLogRotation.postgres");
const symbols = globalThis as typeof globalThis & { [key: symbol]: unknown };
const previous = new Map<string, string | undefined>(
  ["OMNIROUTE_DATABASE_BACKEND", "JWT_SECRET", "API_KEY_SECRET"].map((name) => [
    name,
    process.env[name],
  ])
);

async function nativeScope<T>(identity: object, body: () => Promise<T>): Promise<T> {
  let live = true;
  const binding = {
    databaseIdentity: identity,
    assertReady() {
      if (!live) throw new Error("Fixture standalone scope ended");
    },
  } as unknown as PostgresControlPlaneBinding;
  try {
    return await context.withPostgresControlPlane(binding, body);
  } finally {
    live = false;
  }
}

async function scenario(body: () => Promise<void>) {
  assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  delete globalThis.__omniroutePersistenceRuntime;
  delete symbols[symbol];
  ownerFixture.reset();
  rotationFixture.reset();
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  try {
    await body();
  } finally {
    rotationFixture.releaseAll();
    ownerFixture.releaseBarriers();
    await runtime.closePersistenceRuntime();
    delete globalThis.__omniroutePersistenceRuntime;
    delete symbols[symbol];
    assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  }
}

test.after(() => {
  hooks.deregister();
  ownerFixture.reset();
  rotationFixture.reset();
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test("foreign automatic scheduling allocates neither application runtime nor rotation state", () =>
  scenario(async () => {
    const foreign = Object.freeze({});
    await nativeScope(foreign, async () => {
      assert.equal(rotation.scheduleCallLogRotationAsync(), undefined);
      await rotation.closeCallLogRotationAsync();
    });
    assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
    assert.equal(symbols[symbol], undefined);
    assert.deepEqual(rotationFixture.events, []);
  }));

test("manual rotations and close remain independent for two native planes", () =>
  scenario(async () => {
    const a = Object.freeze({}),
      b = Object.freeze({});
    const gateA = rotationFixture.arm(a),
      gateB = rotationFixture.arm(b);
    let doneA = false,
      doneB = false;
    const workA = nativeScope(a, async () => {
      await rotation.rotateCallLogsAsync();
      await rotation.closeCallLogRotationAsync();
    }).then(() => {
      doneA = true;
    });
    const workB = nativeScope(b, async () => {
      await rotation.rotateCallLogsAsync();
      await rotation.closeCallLogRotationAsync();
    }).then(() => {
      doneB = true;
    });
    try {
      await Promise.all([gateA.entered.promise, gateB.entered.promise]);
      let closedB = false;
      const closeB = nativeScope(b, async () => {
        await rotation.closeCallLogRotationAsync();
        closedB = true;
      });
      await Promise.resolve();
      assert.equal(closedB, false);
      gateB.release.resolve();
      await Promise.all([workB, closeB]);
      assert.equal(doneB, true);
      assert.equal(doneA, false);
      assert.equal(closedB, true);
      gateA.release.resolve();
      await workA;
      assert.deepEqual(
        rotationFixture.events
          .filter((event) => event.kind === "trim")
          .map((event) => event.identity),
        [b, a]
      );
    } finally {
      rotationFixture.releaseAll();
      await Promise.allSettled([workA, workB]);
    }
  }));

test("same-identity manual rotation coalesces across emitted Next bundles", () =>
  scenario(async () => {
    const identity = Object.freeze({});
    const gate = rotationFixture.arm(identity);
    const first = nativeScope(identity, () => rotation.rotateCallLogsAsync());
    await gate.entered.promise;
    const second = nativeScope(identity, () => duplicate.rotateCallLogsAsync());
    try {
      await Promise.resolve();
      assert.equal(rotationFixture.events.filter((event) => event.kind === "delete").length, 1);
      gate.release.resolve();
      await Promise.all([first, second]);
      assert.equal(rotationFixture.events.filter((event) => event.kind === "trim").length, 1);
    } finally {
      gate.release.resolve();
      await Promise.allSettled([first, second]);
    }
  }));

test("foreign scheduling cannot consume free application admission or its throttle", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const foreign = Object.freeze({});
    const state = globalThis.__omniroutePersistenceRuntime!;
    assert.equal(state.background.size, 0);
    await nativeScope(foreign, async () => {
      rotation.scheduleCallLogRotationAsync();
    });
    assert.equal(state.background.size, 0);
    assert.equal(
      (symbols[symbol] as WeakMap<object, unknown> | undefined)?.has(foreign),
      undefined
    );
    await runtime.runPersistenceRequest(async () => {
      rotation.scheduleCallLogRotationAsync();
      assert.equal(state.background.size, 1);
      await rotation.closeCallLogRotationAsync();
      duplicate.scheduleCallLogRotationAsync();
      assert.equal(state.background.size, 0, "the same identity is throttled after completion");
    });
    assert.deepEqual(
      rotationFixture.events.map((event) => event.identity),
      [state.applicationIdentity, state.applicationIdentity]
    );
  }));

test("explicit close waits application scheduling before rotation enters its first I/O", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const identity = globalThis.__omniroutePersistenceRuntime!.applicationIdentity!;
    const gate = rotationFixture.arm(identity);
    let closed = false;
    const operation = runtime.runPersistenceRequest(async () => {
      rotation.scheduleCallLogRotationAsync();
      await duplicate.closeCallLogRotationAsync();
      closed = true;
    });
    try {
      await gate.entered.promise;
      assert.equal(closed, false);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      gate.release.resolve();
      await operation;
      assert.equal(closed, true);
    } finally {
      gate.release.resolve();
      await Promise.allSettled([operation]);
    }
  }));

test("changed captured application identity refuses before repository effects", (t) =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const state = globalThis.__omniroutePersistenceRuntime!;
    const identity = state.applicationIdentity;
    const warnings: string[] = [];
    t.mock.method(console, "warn", (...values: unknown[]) => {
      warnings.push(values.map(String).join(" "));
    });
    try {
      await runtime.runPersistenceRequest(async () => {
        rotation.scheduleCallLogRotationAsync();
        state.applicationIdentity = {};
        await assert.rejects(rotation.closeCallLogRotationAsync(), /authority changed/);
      });
      assert.deepEqual(rotationFixture.events, []);
      assert.equal(state.background.size, 0);
      assert.deepEqual(warnings, ["[callLogs] Rotation failed: Error"]);
    } finally {
      state.applicationIdentity = identity;
    }
  }));

test("a native identity change between delete and trim cannot redirect the next effect", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const state = globalThis.__omniroutePersistenceRuntime!;
    const original = state.applicationIdentity!;
    const binding = context.getSelectedControlPlane()!;
    const gate = rotationFixture.arm(original);
    const operation = rotation.rotateCallLogsAsync();
    void operation.catch(() => {});
    try {
      await gate.entered.promise;
      const replacement = { ...binding, databaseIdentity: Object.freeze({}) };
      globalThis.__omniroutePersistenceContext!.application = replacement;
      gate.release.resolve();
      await assert.rejects(operation, /authority changed/);
      assert.deepEqual(
        rotationFixture.events.map((event) => event.kind),
        ["delete"]
      );
    } finally {
      globalThis.__omniroutePersistenceContext!.application = binding;
      gate.release.resolve();
      await Promise.allSettled([operation]);
    }
  }));

test("SQLite async facade keeps its synchronous scheduler path and no native state", () =>
  scenario(async () => {
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    assert.equal(rotation.scheduleCallLogRotationAsync(), undefined);
    await rotation.rotateCallLogsAsync();
    await rotation.closeCallLogRotationAsync();
    assert.equal(rotationFixture.sqliteCalls, 0);
    assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
    assert.equal(symbols[symbol], undefined);
  }));
