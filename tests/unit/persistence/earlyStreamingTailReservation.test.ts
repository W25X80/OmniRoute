import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { registerHooks } from "node:module";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const environmentNames = [
  "OMNIROUTE_DATABASE_BACKEND",
  "JWT_SECRET",
  "API_KEY_SECRET",
  "DATA_DIR",
  "APP_LOG_TO_FILE",
];
const previous = new Map(environmentNames.map((name) => [name, process.env[name]]));
const directory = mkdtempSync(join(tmpdir(), "omni-native-stream-authority-unit-"));
process.env.DATA_DIR = directory;
process.env.APP_LOG_TO_FILE = "false";
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";

const runtimeUrl = new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const contextUrl = new URL("../../../src/lib/db/controlPlaneContext.ts", import.meta.url).href;
const factoryUrl = new URL(
  "../../../open-sse/handlers/chatCore/executeProviderRequest.ts",
  import.meta.url
).href;
const recoveryUrl = new URL("../../../open-sse/services/streamRecovery.ts", import.meta.url).href;
const ownerFixtureUrl = new URL(
  "../../helpers/persistence/runtimeOwnedWorkFixture.ts",
  import.meta.url
).href;
const featureFixtureUrl = new URL(
  "../../helpers/persistence/nativeRecoveryAuthorityFixture.ts",
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
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === runtimeUrl && runtimeDependencies.has(specifier))
      return { url: featureFixtureUrl, shortCircuit: true };
    if (context.parentURL === recoveryUrl && specifier === "@/shared/utils/featureFlags")
      return { url: featureFixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const context: typeof import("../../../src/lib/db/controlPlaneContext.ts") = await import(
  contextUrl
);
const runtime: typeof import("../../../src/lib/db/persistenceRuntime.ts") = await import(
  runtimeUrl
);
const {
  createNativeStreamingPersistence,
}: typeof import("../../../open-sse/handlers/chatCore/executeProviderRequest.ts") = await import(
  factoryUrl
);
const {
  fixture: owner,
  barrier,
}: typeof import("../../helpers/persistence/runtimeOwnedWorkFixture.ts") = await import(
  ownerFixtureUrl
);
const {
  recoveryAuthorityFixture: flags,
}: typeof import("../../helpers/persistence/nativeRecoveryAuthorityFixture.ts") = await import(
  featureFixtureUrl
);

async function scenario(body: () => Promise<void>) {
  delete globalThis.__omniroutePersistenceRuntime;
  owner.reset();
  flags.flag = true;
  flags.reads = 0;
  flags.nativeReads = 0;
  flags.catalogWaits = [];
  flags.catalogEntered = undefined;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  try {
    await body();
  } finally {
    owner.releaseBarriers();
    await runtime.closePersistenceRuntime();
    delete globalThis.__omniroutePersistenceRuntime;
  }
}
test.after(() => {
  hooks.deregister();
  owner.reset();
  rmSync(directory, { recursive: true, force: true });
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test("native reservations are atomic at the existing 64-tail fence", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const capabilities: NonNullable<ReturnType<typeof createNativeStreamingPersistence>>[] = [];
    try {
      await runtime.runPersistenceRequest(async () => {
        for (let index = 0; index < 64; index++) {
          const capability = createNativeStreamingPersistence()!;
          capabilities.push(capability);
          await capability.reserve();
        }
        assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 64);
        assert.equal(owner.queries, 0, "a held permit does not own a SQL connection");
        const overflow = createNativeStreamingPersistence()!;
        capabilities.push(overflow);
        await assert.rejects(overflow.reserve(), runtime.isPersistenceStreamCapacityError);
        assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 64);
        await overflow.release();
        await assert.rejects(overflow.reserve(), /authority has ended/);
      });
    } finally {
      await Promise.all(capabilities.map((capability) => capability.release()));
    }
    assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
  }));

test("a concurrent reserve transfers exactly one permit and retires exactly once", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    let capability: NonNullable<ReturnType<typeof createNativeStreamingPersistence>>;
    let permit: Awaited<ReturnType<typeof runtime.acquirePersistenceTail>>;
    await runtime.runPersistenceRequest(async () => {
      capability = createNativeStreamingPersistence()!;
      const first = capability.reserve();
      const second = capability.reserve();
      assert.equal(first, second, "reservation is single-flight, not a count preflight");
      const raw = await first;
      assert.equal(await second, raw);
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 1);
      assert.throws(
        () => capability.adopt({ run: (body) => body(), release: async () => {} }),
        /does not match/
      );
      permit = capability.adopt(raw);
      assert.equal(await capability.reserve(), raw, "adoption cannot reserve a second permit");
      assert.throws(() => capability.adopt(raw), /already adopted/);
    });
    assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 1);
    assert.equal(
      await capability!.run(async () => context.getSelectedPersistenceIdentity()),
      owner.authority
    );
    const firstRelease = capability!.release();
    assert.equal(capability!.release(), firstRelease);
    await firstRelease;
    await permit!.release();
    assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
    await assert.rejects(
      capability!.run(async () => 1),
      /authority has ended/
    );
  }));

test("abort before reservation binding enters releases its late-acquired permit", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    await runtime.runPersistenceRequest(async () => {
      const capability = createNativeStreamingPersistence()!;
      const reserve = capability.reserve();
      const release = capability.release();
      assert.equal(capability.release(), release);
      await assert.rejects(reserve, /authority has ended/);
      await release;
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
      await assert.rejects(
        capability.run(async () => 1),
        /authority has ended/
      );
    });
  }));

test("a reserved callback keeps its authority when cancellation ends the request before handoff", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const entered = barrier(),
      finish = barrier();
    let capability: NonNullable<ReturnType<typeof createNativeStreamingPersistence>>;
    let work: Promise<object | undefined> | undefined;
    try {
      await runtime.runPersistenceRequest(async () => {
        capability = createNativeStreamingPersistence()!;
        await capability.reserve();
        work = capability.run(async () => {
          entered.resolve();
          await finish.promise;
          return context.getSelectedPersistenceIdentity();
        });
        // Observe immediately even when an assertion fails; never leave a test rejection detached.
        void work.catch(() => {});
        await entered.promise;
      });
      let released = false;
      const release = capability!.release().then(() => {
        released = true;
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(released, false, "release must join accepted pre-handoff callbacks");
      finish.resolve();
      assert.equal(await work, owner.authority);
      await release;
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
      await assert.rejects(
        capability!.run(async () => 1),
        /authority has ended/
      );
    } finally {
      finish.resolve();
      await work?.catch(() => {});
      await capability!?.release();
    }
  }));

test("a reserved callback re-enters its held authority from an ended request context", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    let capability: NonNullable<ReturnType<typeof createNativeStreamingPersistence>>;
    let stale: AsyncResource;
    try {
      await runtime.runPersistenceRequest(async () => {
        capability = createNativeStreamingPersistence()!;
        await capability.reserve();
        stale = new AsyncResource("cancelled-request-callback");
      });
      assert.throws(() => stale!.runInAsyncScope(context.getSelectedPersistenceIdentity), /ended/);
      assert.equal(
        await stale!.runInAsyncScope(() =>
          capability!.run(async () => context.getSelectedPersistenceIdentity())
        ),
        owner.authority
      );
    } finally {
      stale!?.emitDestroy();
      await capability!?.release();
    }
  }));

test("pre-handoff observation remains synchronous and its reserved tail joins accepted work", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const entered = barrier(),
      finish = barrier();
    let capability: NonNullable<ReturnType<typeof createNativeStreamingPersistence>>;
    let work: Promise<void>;
    let observed = 0;
    try {
      await runtime.runPersistenceRequest(async () => {
        capability = createNativeStreamingPersistence()!;
        await capability.reserve();
        work = capability.run(
          async () => {
            entered.resolve();
            await finish.promise;
          },
          {
            owner: owner.authority,
            observe: () => {
              observed++;
            },
          }
        );
        assert.equal(observed, 1, "the accepted scalar hint does not await catalog/SQL");
        await entered.promise;
        assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
        assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 1);
      });
      const release = capability!.release();
      const closing = runtime.closePersistenceRuntime();
      let closed = false;
      void closing.then(() => {
        closed = true;
      });
      await Promise.resolve();
      assert.equal(closed, false);
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 1);
      finish.resolve();
      await work!;
      await release;
      await closing;
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closed");
    } finally {
      finish.resolve();
      await work!?.catch(() => {});
      await capability!?.release();
    }
  }));

test("shutdown waits for the same reserved then adopted completion authority", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const entered = barrier(),
      finish = barrier();
    let capability: NonNullable<ReturnType<typeof createNativeStreamingPersistence>>;
    let permit: Awaited<ReturnType<typeof runtime.acquirePersistenceTail>>;
    await runtime.runPersistenceRequest(async () => {
      capability = createNativeStreamingPersistence()!;
      permit = capability.adopt(await capability.reserve());
    });
    const work = capability!.run(async () => {
      entered.resolve();
      await finish.promise;
    });
    await entered.promise;
    const closing = runtime.closePersistenceRuntime();
    const release = permit!.release();
    let released = false;
    void release.then(() => {
      released = true;
    });
    await Promise.resolve();
    assert.equal(released, false);
    assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 1);
    assert.equal(owner.poolCloses, 0);
    finish.resolve();
    await work;
    await release;
    await closing;
    assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
    assert.equal(owner.poolCloses, 1);
  }));

test("capacity classification preserves the canonical local public error and ignores provider health", () =>
  scenario(async () => {
    const { createErrorResult } = await import("../../../open-sse/utils/error.ts");
    const { isLocalQueueCapacityErrorBody, isRequestScopedUpstreamFailure } =
      await import("../../../open-sse/services/combo/comboPredicates.ts");
    const { classifyProviderBreakerResult } =
      await import("../../../src/sse/handlers/chatPredicates.ts");
    const result = createErrorResult(
      503,
      "Gateway streaming capacity is unavailable. Please retry later.",
      null,
      runtime.PERSISTENCE_STREAM_CAPACITY_CODE,
      "local_queue_capacity"
    );
    const body = await result.response.json();
    assert.equal(result.response.status, 503);
    assert.equal(body.error.code, runtime.PERSISTENCE_STREAM_CAPACITY_CODE);
    assert.equal(body.error.type, "local_queue_capacity");
    assert.equal(body.error.message.includes("at /"), false);
    assert.equal(
      isLocalQueueCapacityErrorBody(body),
      true,
      "priority and RR use this exact stop predicate"
    );
    assert.equal(
      isRequestScopedUpstreamFailure({
        code: result.errorCode,
        type: result.errorType,
      }),
      true
    );
    assert.equal(classifyProviderBreakerResult(result, false, false), "ignore");
    assert.equal(
      runtime.isPersistenceStreamCapacityError({
        name: "PersistenceStreamCapacityError",
      }),
      false,
      "public text cannot brand an internal refusal"
    );
  }));

test("bundled runtime owners recognize only their locally branded admission refusal", () =>
  scenario(async () => {
    const duplicate: typeof runtime = await import(`${runtimeUrl}?capacity-owner-copy`);
    assert.equal(
      duplicate.isPersistenceStreamCapacityError(new runtime.PersistenceStreamCapacityError()),
      true
    );
    assert.equal(
      runtime.isPersistenceStreamCapacityError(new duplicate.PersistenceStreamCapacityError()),
      true
    );
    assert.equal(
      runtime.isPersistenceStreamCapacityError(
        new Error("Persistence stream admission is unavailable")
      ),
      false
    );
  }));

test("fatal reservation failure retires without turning it into a local capacity response", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    await runtime.runPersistenceRequest(async () => {
      const capability = createNativeStreamingPersistence()!;
      const failure = new Error("Fixture plane failure");
      owner.runFailure = failure;
      await assert.rejects(capability.reserve(), (error) => error === failure);
      assert.equal(runtime.isPersistenceStreamCapacityError(failure), false);
      await capability.release();
      await assert.rejects(capability.reserve(), /authority has ended/);
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
    });
  }));
