import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { realpathSync } from "node:fs";
import { registerHooks } from "node:module";
import { setImmediate as settle } from "node:timers/promises";
import { pathToFileURL } from "node:url";
import test from "node:test";
import {
  getSelectedControlPlane,
  withPostgresControlPlane,
} from "../../../src/lib/db/controlPlaneContext.ts";
import { barrier, fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { batchFixture } from "../../helpers/persistence/batchProcessorLifetimeFixture.ts";

type BatchModule = typeof import("../../../open-sse/services/batchProcessor.ts");
const runtimeUrl = pathToFileURL(
  realpathSync(new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url))
).href;
const batchUrl = pathToFileURL(
  realpathSync(new URL("../../../open-sse/services/batchProcessor.ts", import.meta.url))
).href;
const runtimeFixtureUrl = new URL(
  "../../helpers/persistence/runtimeOwnedWorkFixture.ts",
  import.meta.url
).href;
const batchFixtureUrl = new URL(
  "../../helpers/persistence/batchProcessorLifetimeFixture.ts",
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
const batchDependencies = new Set([
  "@/lib/db/batches",
  "@/lib/db/files",
  "@/lib/db/apiKeys",
  "@/lib/batches/dispatch",
]);
const hooks = registerHooks({
  resolve(specifier, context, next) {
    const parent = context.parentURL?.split("?")[0];
    if (parent === runtimeUrl && runtimeDependencies.has(specifier))
      return { url: runtimeFixtureUrl, shortCircuit: true };
    if (parent === batchUrl && batchDependencies.has(specifier))
      return { url: batchFixtureUrl, shortCircuit: true };
    return next(specifier, context);
  },
});
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
const runtime = await import(runtimeUrl);
let sequence = 0;
async function scenario(body: (processor: BatchModule) => Promise<void>) {
  delete globalThis.__omniroutePersistenceRuntime;
  delete globalThis.__omnirouteNativeBatchProcessor;
  fixture.reset();
  batchFixture.reset();
  const processor: BatchModule = await import(`${batchUrl}?lifetime-case=${++sequence}`);
  try {
    await runtime.ensurePersistenceReady();
    await body(processor);
  } finally {
    fixture.releaseBarriers();
    await processor.stopBatchProcessorAsync();
    await runtime.closePersistenceRuntime();
    assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  }
}
test.after(() => {
  hooks.deregister();
  fixture.reset();
});

test(
  "native shutdown drains an accepted poll, its fresh worker and underlying dispatch lifetime",
  { timeout: 10000 },
  async (t) => {
    await scenario(async (processor) => {
      const poll = barrier();
      const pollEntered = barrier();
      const dispatchEntered = barrier();
      const response = barrier();
      batchFixture.pollGate = poll.promise;
      batchFixture.pollEntered = pollEntered.resolve;
      batchFixture.dispatchEntered = dispatchEntered.resolve;
      batchFixture.dispatchGate = response.promise.then(() => new Response("{}", { status: 200 }));
      t.after(() => response.resolve());
      let callback!: () => void;
      let timerResource!: AsyncResource;
      let deadline!: () => void;
      const handle = {
        unref() {
          return this;
        },
      };
      const originalTimeout = globalThis.setTimeout;
      const originalClearTimeout = globalThis.clearTimeout;
      let intervals = 0;
      t.mock.method(globalThis, "setInterval", (body: () => void) => {
        intervals++;
        callback = body;
        timerResource = new AsyncResource("captured-batch-timer");
        return handle as unknown as ReturnType<typeof setInterval>;
      });
      t.mock.method(globalThis, "clearInterval", () => {});
      t.mock.method(
        globalThis,
        "setTimeout",
        (body: (...args: unknown[]) => void, milliseconds: number, ...args: unknown[]) => {
          if (milliseconds === processor.BATCH_ITEM_DISPATCH_TIMEOUT_MS) {
            deadline = () => body(...args);
            return handle as unknown as ReturnType<typeof setTimeout>;
          }
          return originalTimeout(body, milliseconds, ...args);
        }
      );
      t.mock.method(globalThis, "clearTimeout", (timer: ReturnType<typeof setTimeout>) => {
        if (timer !== handle) originalClearTimeout(timer);
      });
      await runtime.runPersistenceRequest(async () => {
        processor.initBatchProcessor();
      });
      const duplicate: BatchModule = await import(`${batchUrl}?lifetime-duplicate=${sequence}`);
      await runtime.runPersistenceRequest(async () => {
        duplicate.initBatchProcessor();
      });
      assert.equal(intervals, 1, "native bundles share one timer owner");
      assert.throws(
        () => timerResource.runInAsyncScope(() => getSelectedControlPlane()!.assertReady()),
        /scope ended/,
        "the captured startup scope is already invalid before the timer fires"
      );
      timerResource.runInAsyncScope(callback);
      await pollEntered.promise;
      let processorClosed = false;
      let runtimeClosed = false;
      const closing = duplicate.stopBatchProcessorAsync().then(() => {
        processorClosed = true;
      });
      const runtimeClosing = runtime.closePersistenceRuntime().then(() => {
        runtimeClosed = true;
      });
      timerResource.runInAsyncScope(callback);
      await assert.rejects(processor.processPendingBatches(), /closing/);
      poll.resolve();
      await dispatchEntered.promise;
      deadline();
      for (let i = 0; i < 10 && batchFixture.batch?.status !== "completed"; i++) await settle();
      assert.equal(batchFixture.batch?.status, "completed");
      assert.equal(batchFixture.checkpoints.get(1)?.status, "errored");
      assert.equal(processorClosed, false, "a timeout does not settle the raw dispatch");
      assert.equal(runtimeClosed, false, "the accepted native worker remains owned");
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      const pollGeneration = batchFixture.events.find(
        ({ operation }) => operation === "pending"
      )!.generation;
      const workerGeneration = batchFixture.events.find(
        ({ operation }) => operation === "checkpoint-list"
      )!.generation;
      assert.notEqual(
        pollGeneration,
        workerGeneration,
        "the detached worker gets its own fresh scope"
      );
      assert.equal(
        batchFixture.events.filter(({ operation }) => operation === "dispatch").length,
        1
      );
      response.resolve();
      await Promise.all([closing, runtimeClosing]);
      assert.equal(fixture.poolCloses, 1);
      timerResource.emitDestroy();
    });
  }
);

test(
  "a changed opaque native authority fails before any batch repository effect",
  { timeout: 10000 },
  async (t) => {
    await scenario(async (processor) => {
      const handle = {
        unref() {
          return this;
        },
      };
      t.mock.method(
        globalThis,
        "setInterval",
        () => handle as unknown as ReturnType<typeof setInterval>
      );
      t.mock.method(globalThis, "clearInterval", () => {});
      await runtime.runPersistenceRequest(async () => {
        processor.initBatchProcessor();
      });
      const plane = globalThis.__omniroutePersistenceRuntime!.plane!;
      const originalRun = plane.run.bind(plane);
      const changed = Object.freeze({});
      const override = t.mock.method(plane, "run", (body) =>
        originalRun((binding) => {
          const replaced = { ...binding, databaseIdentity: changed };
          return withPostgresControlPlane(replaced, () => body(replaced));
        })
      );
      await assert.rejects(processor.processPendingBatches(), /authority changed/);
      assert.equal(batchFixture.events.length, 0);
      override.mock.restore();
    });
  }
);

test(
  "detached worker failure persistence remains owned after its poll ends",
  { timeout: 10000 },
  async () => {
    await scenario(async (processor) => {
      const entered = barrier();
      const worker = barrier();
      batchFixture.workerGate = worker.promise;
      batchFixture.workerEntered = entered.resolve;
      batchFixture.workerFailure = new Error("fixture worker failure");
      await processor.processPendingBatches();
      await entered.promise;
      assert.equal(batchFixture.batch?.status, "in_progress");
      const closing = processor.stopBatchProcessorAsync();
      const runtimeClosing = runtime.closePersistenceRuntime();
      worker.resolve();
      await Promise.all([closing, runtimeClosing]);
      assert.equal(batchFixture.batch?.status, "failed");
      const generation = batchFixture.events.find(
        ({ operation }) => operation === "checkpoint-list"
      )!.generation;
      const failure = batchFixture.events.find(
        ({ operation }) => operation === "batch-write:failed"
      );
      assert.equal(failure?.generation, generation);
      assert.equal(
        batchFixture.events.filter(({ operation }) => operation === "dispatch").length,
        0
      );
    });
  }
);
