import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { realpathSync } from "node:fs";
import { registerHooks } from "node:module";
import { pathToFileURL } from "node:url";
import test from "node:test";
import {
  getSelectedControlPlane,
  getSelectedPersistenceIdentity,
} from "../../../src/lib/db/controlPlaneContext.ts";
import { barrier, fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import type { PersistenceTailPermit } from "../../../src/lib/db/persistenceRuntime.ts";

type RuntimeModule = typeof import("../../../src/lib/db/persistenceRuntime.ts");
const runtimeUrl = pathToFileURL(
  realpathSync(new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url))
).href;
const contextUrl = pathToFileURL(
  realpathSync(new URL("../../../src/lib/db/controlPlaneContext.ts", import.meta.url))
).href;
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
    if (context.parentURL?.split("?")[0] === runtimeUrl) {
      if (dependencies.has(specifier)) return { url: fixtureUrl, shortCircuit: true };
      if (specifier === "./controlPlaneContext" && context.parentURL.includes("?")) {
        return { url: `${contextUrl}?owned-work-second-bundle`, shortCircuit: true };
      }
    }
    return nextResolve(specifier, context);
  },
});
const previous = {
  backend: process.env.OMNIROUTE_DATABASE_BACKEND,
  jwt: process.env.JWT_SECRET,
  key: process.env.API_KEY_SECRET,
};
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
const runtime: RuntimeModule = await import(runtimeUrl);
const duplicate: RuntimeModule = await import(`${runtimeUrl}?owned-work-second-bundle`);
const duplicateContext: typeof import("../../../src/lib/db/controlPlaneContext.ts") = await import(
  `${contextUrl}?owned-work-second-bundle`
);
const permits: PersistenceTailPermit[] = [];
const observe = <T>(work: Promise<T>): Promise<T> => {
  void work.catch(() => {});
  return work;
};

async function acquire(): Promise<PersistenceTailPermit> {
  const permit = await runtime.acquirePersistenceTail();
  permits.push(permit);
  return permit;
}
async function scenario(body: () => Promise<void>): Promise<void> {
  assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  delete globalThis.__omniroutePersistenceRuntime;
  fixture.reset();
  permits.length = 0;
  try {
    await body();
  } finally {
    fixture.releaseBarriers();
    await Promise.allSettled(permits.map((permit) => permit.release()));
    await runtime.closePersistenceRuntime().catch(() => {});
    assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  }
}
function assertCleanup() {
  assert.equal(fixture.artifactCloses, 1);
  assert.equal(fixture.applicationReleases, 1);
  assert.equal(fixture.planeCloses, 1);
  assert.equal(fixture.poolCloses, 1);
  assert.ok(
    fixture.events.indexOf("artifact-close") < fixture.events.indexOf("application-release")
  );
  assert.ok(fixture.events.indexOf("application-release") < fixture.events.indexOf("plane-close"));
  assert.ok(fixture.events.indexOf("plane-close") < fixture.events.indexOf("pool-close"));
}

test.after(() => {
  hooks.deregister();
  fixture.reset();
  delete globalThis.__omniroutePersistenceRuntime;
  for (const [key, value] of [
    ["OMNIROUTE_DATABASE_BACKEND", previous.backend],
    ["JWT_SECRET", previous.jwt],
    ["API_KEY_SECRET", previous.key],
  ]) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

test("root admission refuses capacity before preparing another catalog", { timeout: 10000 }, () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const release = barrier();
    const jobs = Array.from({ length: 64 }, () =>
      runtime.runPersistenceRequest(() => release.promise)
    );
    while (runtime.getPersistenceRuntimeStatus().activeRequests !== 64)
      await new Promise<void>((resolve) => setImmediate(resolve));
    const before = fixture.generation;
    let entered = false;
    try {
      await assert.rejects(
        runtime.runPersistenceRequest(() => {
          entered = true;
        }),
        /request admission/
      );
      assert.equal(entered, false);
      assert.equal(fixture.generation, before, "refusal must precede catalog allocation");
    } finally {
      release.resolve();
      await Promise.allSettled(jobs);
    }
    await runtime.runPersistenceRequest(() => {});
    assert.equal(fixture.generation, before + 1, "completed work returns its admission slot");
  })
);

test("root pressure refusal does not allocate a catalog or dispatch a handler", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const pressure = await import("../../../open-sse/utils/resourcePressure.ts");
    pressure.reloadResourcePressureRuntime({ heapThresholdMb: 1, immediateHeapUsedMb: () => 2 });
    const before = fixture.generation;
    try {
      await assert.rejects(
        runtime.runPersistenceRequest(() => assert.fail("handler dispatched")),
        /request admission/
      );
      assert.equal(fixture.generation, before);
      assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
    } finally {
      pressure.reloadResourcePressureRuntime();
    }
  }));

test("cold close latches admission before a same-turn first startup", { timeout: 10000 }, () =>
  scenario(async () => {
    assert.equal(runtime.getPersistenceRuntimeStatus().phase, "starting");
    const closing = runtime.closePersistenceRuntime();
    assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
    await assert.rejects(runtime.ensurePersistenceReady(), /unavailable/);
    let entered = false;
    await assert.rejects(
      runtime.runPersistenceRequest(() => {
        entered = true;
      }),
      /unavailable/
    );
    await closing;
    assert.equal(entered, false);
    assert.equal(fixture.environmentReads, 0);
    assert.equal(fixture.planeCreates, 0);
    assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closed");
  })
);

test(
  "close during boot hydration never republishes ready and cleans allocated owners",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      const entered = barrier();
      const boot = barrier();
      fixture.startupEntered = entered.resolve;
      fixture.startupBarrier = boot.promise;
      const startup = observe(runtime.ensurePersistenceReady());
      await entered.promise;
      assert.equal(fixture.artifactCreates, 1);
      assert.ok(globalThis.__omniroutePersistenceContext?.application);
      const closing = observe(runtime.closePersistenceRuntime());
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
      await assert.rejects(runtime.ensurePersistenceReady(), /unavailable/);
      let requestEntered = false;
      await assert.rejects(
        runtime.runPersistenceRequest(() => {
          requestEntered = true;
        }),
        /unavailable/
      );
      boot.resolve();
      await assert.rejects(startup, /draining/);
      await closing;
      assert.equal(requestEntered, false);
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closed");
      assertCleanup();
    })
);

test(
  "drain tracks child and grandchild accepted by live owners after root admission closes",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const parentEntered = barrier(),
        enqueueChild = barrier(),
        childEntered = barrier();
      const enqueueGrandchild = barrier(),
        grandchildEntered = barrier(),
        finishGrandchild = barrier();
      let child!: Promise<void>, grandchild!: Promise<void>;
      const parent = observe(
        runtime.runPersistenceRequest(async () => {
          parentEntered.resolve();
          await enqueueChild.promise;
          child = observe(
            runtime.runPersistenceBackgroundWork(async () => {
              childEntered.resolve();
              await enqueueGrandchild.promise;
              grandchild = observe(
                runtime.runPersistenceBackgroundWork(async () => {
                  grandchildEntered.resolve();
                  await finishGrandchild.promise;
                })
              );
            })
          );
        })
      );
      await parentEntered.promise;
      let closed = false;
      const closing = observe(runtime.closePersistenceRuntime());
      void closing.then(
        () => {
          closed = true;
        },
        () => {}
      );
      let rootEntered = false;
      await assert.rejects(
        runtime.runPersistenceBackgroundWork(async () => {
          rootEntered = true;
        }),
        /admission/
      );
      enqueueChild.resolve();
      await parent;
      await childEntered.promise;
      assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      assert.equal(closed, false);
      enqueueGrandchild.resolve();
      await child;
      await grandchildEntered.promise;
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      assert.equal(closed, false);
      finishGrandchild.resolve();
      await grandchild;
      await closing;
      assert.equal(rootEntered, false);
      assertCleanup();
    })
);

test(
  "an inherited resource from an ended request cannot admit a descendant during close",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const permit = await acquire();
      let stale!: AsyncResource;
      await runtime.runPersistenceRequest(() => {
        stale = new AsyncResource("ended-request-callback");
      });
      const closing = observe(runtime.closePersistenceRuntime());
      let entered = false;
      try {
        await assert.rejects(
          stale.runInAsyncScope(() =>
            runtime.runPersistenceBackgroundWork(async () => {
              entered = true;
            })
          ),
          /admission/
        );
        assert.equal(entered, false);
        assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
      } finally {
        stale.emitDestroy();
      }
      await permit.release();
      await closing;
    })
);

test(
  "background cap rejects the 65th body and releases capacity only after tracked completion",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const gates = Array.from({ length: 64 }, () => barrier());
      const entered = gates.map(() => barrier());
      const jobs = gates.map((gate, index) =>
        observe(
          runtime.runPersistenceBackgroundWork(async () => {
            entered[index].resolve();
            await gate.promise;
          })
        )
      );
      await Promise.all(entered.map((gate) => gate.promise));
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 64);
      let overflowEntered = false;
      await assert.rejects(
        runtime.runPersistenceBackgroundWork(async () => {
          overflowEntered = true;
        }),
        /admission/
      );
      assert.equal(overflowEntered, false);
      gates[0].resolve();
      await jobs[0];
      const replacementEntered = barrier(),
        replacementDone = barrier();
      const replacement = observe(
        runtime.runPersistenceBackgroundWork(async () => {
          replacementEntered.resolve();
          await replacementDone.promise;
        })
      );
      await replacementEntered.promise;
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 64);
      replacementDone.resolve();
      for (const gate of gates) gate.resolve();
      await Promise.all([...jobs, replacement]);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    })
);

test(
  "64 idle tail permits hold no SQL operation and re-enter fresh authority outside request ALS",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const external = new AsyncResource("external-transport-callback");
      let permit!: PersistenceTailPermit;
      await runtime.runPersistenceRequest(async () => {
        permit = await acquire();
      });
      const originalGeneration = fixture.generation;
      for (let i = 1; i < 64; i++) await acquire();
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 64);
      assert.equal(fixture.queries, 0);
      await assert.rejects(runtime.acquirePersistenceTail(), /stream admission/);
      const catalogBefore = fixture.generation;
      try {
        const observed = await external.runInAsyncScope(() =>
          permit.run(async () => ({
            identity: getSelectedPersistenceIdentity(),
            generation: fixture.selectedGeneration(),
            ready: Boolean(getSelectedControlPlane()),
          }))
        );
        assert.equal(observed.identity, fixture.authority);
        assert.equal(observed.generation, catalogBefore + 1);
        assert.ok(observed.generation! > originalGeneration);
        assert.equal(observed.ready, true);
      } finally {
        external.emitDestroy();
      }
      await Promise.all(permits.map((held) => held.release()));
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
    })
);

test(
  "a pre-admitted tail and its child finish during shutdown after HTTP request settles",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const external = new AsyncResource("transport-after-http");
      let permit!: PersistenceTailPermit;
      await runtime.runPersistenceRequest(async () => {
        permit = await acquire();
      });
      const childEntered = barrier(),
        childDone = barrier();
      let child!: Promise<void>;
      const closing = observe(runtime.closePersistenceRuntime());
      try {
        await external.runInAsyncScope(() =>
          permit.run(async () => {
            child = observe(
              runtime.runPersistenceBackgroundWork(async () => {
                childEntered.resolve();
                await childDone.promise;
              })
            );
          })
        );
        await childEntered.promise;
        assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
        assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
        await assert.rejects(
          runtime.runPersistenceRequest(() => {}),
          /unavailable/
        );
        await permit.release();
        assert.equal(fixture.poolCloses, 0);
        childDone.resolve();
        await child;
        await closing;
        assertCleanup();
      } finally {
        external.emitDestroy();
      }
    })
);

test(
  "tail release shares its pending outcome, forbids new callbacks and waits for accepted work",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const permit = await acquire();
      const entered = barrier(),
        done = barrier();
      const callback = observe(
        permit.run(async () => {
          entered.resolve();
          await done.promise;
        })
      );
      await entered.promise;
      let released = false;
      const release = permit.release();
      assert.equal(permit.release(), release);
      void release.then(() => {
        released = true;
      });
      const generation = fixture.generation;
      let newBody = false;
      await assert.rejects(
        permit.run(async () => {
          newBody = true;
        }),
        /authority has ended/
      );
      assert.equal(fixture.generation, generation);
      assert.equal(newBody, false);
      assert.equal(released, false);
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 1);
      done.resolve();
      await callback;
      await release;
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
    })
);

for (const channel of ["request", "background", "tail"] as const) {
  for (const point of ["body", "catalog"] as const) {
    test(
      `${channel} ${point} rejection clears tracking and revokes inherited ownership`,
      { timeout: 10000 },
      () =>
        scenario(async () => {
          await runtime.ensurePersistenceReady();
          const hold = await acquire();
          const failure = new Error(`${channel}-${point}-failed`);
          let bodyEntered = false;
          if (point === "catalog") fixture.catalogFailure = failure;
          const body = async () => {
            bodyEntered = true;
            throw failure;
          };
          const rejected =
            channel === "request"
              ? runtime.runPersistenceRequest(body)
              : channel === "background"
                ? runtime.runPersistenceBackgroundWork(body)
                : hold.run(body);
          await assert.rejects(rejected, (error) => error === failure);
          assert.equal(bodyEntered, point === "body");
          assert.equal(runtime.getPersistenceRuntimeStatus().activeRequests, 0);
          assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
          const resource = fixture.resources.at(-1)!;
          const closing = observe(runtime.closePersistenceRuntime());
          let descendantEntered = false;
          await assert.rejects(
            resource.runInAsyncScope(() =>
              runtime.runPersistenceBackgroundWork(async () => {
                descendantEntered = true;
              })
            ),
            /admission/
          );
          assert.equal(descendantEntered, false);
          await hold.release();
          await closing;
          assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
        })
    );
  }
}

test(
  "a rejected plane entry rejects tail acquisition without retaining a waiter or tail",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const failure = new Error("native-entry-rejected");
      fixture.runFailure = failure;
      await assert.rejects(runtime.acquirePersistenceTail(), (error) => error === failure);
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
      const permit = await acquire();
      await permit.release();
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
    })
);

for (const point of ["catalog", "flush", "artifact", "plane", "all"] as const) {
  test(
    `shutdown ${point} failure still attempts independent teardown and rejects its receipt`,
    { timeout: 10000 },
    () =>
      scenario(async () => {
        await runtime.ensurePersistenceReady();
        const sql = new Error("sql-shutdown-failed"),
          object = new Error("artifact-close-failed"),
          pool = new Error("pool-close-failed");
        if (point === "catalog") fixture.catalogFailure = sql;
        if (point === "flush" || point === "all") fixture.flushFailure = sql;
        if (point === "artifact" || point === "all") fixture.artifactFailure = object;
        if (point === "plane" || point === "all") fixture.planeFailure = pool;
        const closing = runtime.closePersistenceRuntime();
        assert.equal(runtime.closePersistenceRuntime(), closing);
        await assert.rejects(closing, (error) => {
          assert.ok(error instanceof AggregateError);
          const expected =
            point === "all"
              ? [sql, object, pool]
              : [point === "artifact" ? object : point === "plane" ? pool : sql];
          assert.deepEqual(error.errors, expected);
          return true;
        });
        assertCleanup();
        assert.equal(runtime.getPersistenceRuntimeStatus().phase, "failed");
      })
  );
}

test(
  "settlement drain never turns a rejected persistence producer into a successful write",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const entered = barrier(),
        reject = barrier();
      const failure = new Error("write-not-acknowledged");
      let acknowledgedWrites = 0;
      const write = observe(
        runtime.runPersistenceBackgroundWork(async () => {
          entered.resolve();
          await reject.promise;
          throw failure;
          // No fake persistence receipt may be issued from drain's allSettled result.
        })
      );
      const receipt = observe(
        write.then(() => {
          acknowledgedWrites++;
        })
      );
      await entered.promise;
      const closing = observe(runtime.closePersistenceRuntime());
      reject.resolve();
      await assert.rejects(write, (error) => error === failure);
      await assert.rejects(receipt, (error) => error === failure);
      await closing;
      assert.equal(acknowledgedWrites, 0);
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closed");
    })
);

test(
  "distinct runtime and context module identities share admission, tracking and one close",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      assert.notEqual(runtime.runPersistenceBackgroundWork, duplicate.runPersistenceBackgroundWork);
      assert.notEqual(getSelectedControlPlane, duplicateContext.getSelectedControlPlane);
      await runtime.ensurePersistenceReady();
      const oneEntered = barrier(),
        twoEntered = barrier(),
        oneDone = barrier(),
        twoDone = barrier();
      const one = observe(
        runtime.runPersistenceBackgroundWork(async () => {
          oneEntered.resolve();
          await oneDone.promise;
        })
      );
      const two = observe(
        duplicate.runPersistenceBackgroundWork(async () => {
          assert.equal(duplicateContext.getSelectedPersistenceIdentity(), fixture.authority);
          assert.equal(duplicateContext.getSelectedControlPlane(), getSelectedControlPlane());
          twoEntered.resolve();
          await twoDone.promise;
        })
      );
      await Promise.all([oneEntered.promise, twoEntered.promise]);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 2);
      assert.equal(duplicate.getPersistenceRuntimeStatus().backgroundOperations, 2);
      const closing = observe(runtime.closePersistenceRuntime());
      assert.equal(duplicate.closePersistenceRuntime(), closing);
      await assert.rejects(
        duplicate.runPersistenceBackgroundWork(async () => {}),
        /admission/
      );
      oneDone.resolve();
      twoDone.resolve();
      await Promise.all([one, two]);
      await closing;
      assertCleanup();
    })
);
