import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { registerHooks } from "node:module";
import test from "node:test";
import { fixture, barrier } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { shutdownFixture } from "../../helpers/persistence/shutdownOrderingFixture.ts";
import type { PersistenceTailPermit } from "../../../src/lib/db/persistenceRuntime.ts";
import { PostgresError } from "../../../src/lib/db/postgres/database.ts";

const runtimeUrl = new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const shutdownUrl = new URL("../../../src/lib/startup/postgresShutdown.ts", import.meta.url).href;
const runtimeFixtureUrl = new URL(
  "../../helpers/persistence/runtimeOwnedWorkFixture.ts",
  import.meta.url
).href;
const shutdownFixtureUrl = new URL(
  "../../helpers/persistence/shutdownOrderingFixture.ts",
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
    if (context.parentURL?.split("?")[0] === runtimeUrl && runtimeDependencies.has(specifier))
      return { url: runtimeFixtureUrl, shortCircuit: true };
    if (
      context.parentURL?.split("?")[0] === shutdownUrl &&
      specifier !== "@/lib/db/persistenceRuntime" &&
      specifier !== "./postgresBootstrap"
    )
      return { url: shutdownFixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const runtime = await import("../../../src/lib/db/persistenceRuntime.ts");
const shutdown = await import("../../../src/lib/startup/postgresShutdown.ts");
const sinks = await import("../../helpers/persistence/shutdownOrderingFixture.ts");
const previous = Object.fromEntries(
  ["OMNIROUTE_DATABASE_BACKEND", "JWT_SECRET", "API_KEY_SECRET"].map((key) => [
    key,
    process.env[key],
  ])
);
const ownerSymbol = Symbol.for("omniroute.postgresStartupOwner");
const ownerGlobals = globalThis as typeof globalThis & { [ownerSymbol]?: unknown };
const permits: PersistenceTailPermit[] = [];
const work: Promise<unknown>[] = [];
const observe = <T>(operation: Promise<T>): Promise<T> => {
  void operation.catch(() => {});
  work.push(operation);
  return operation;
};
const turn = () => new Promise<void>((done) => setImmediate(done));
async function scenario(body: () => Promise<void>): Promise<void> {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  delete globalThis.__omniroutePersistenceRuntime;
  delete ownerGlobals[ownerSymbol];
  fixture.reset();
  shutdownFixture.reset();
  permits.length = 0;
  work.length = 0;
  try {
    await body();
  } finally {
    fixture.releaseBarriers();
    await Promise.allSettled(permits.map((permit) => permit.release()));
    await Promise.allSettled(work);
    await runtime.closePersistenceRuntime().catch(() => {});
    shutdownFixture.reset();
    assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  }
}
test.after(() => {
  hooks.deregister();
  delete globalThis.__omniroutePersistenceRuntime;
  delete ownerGlobals[ownerSymbol];
  for (const [name, value] of Object.entries(previous)) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

for (const kind of ["transaction_failed", "commit_unknown"] as const) {
  test(
    `native shutdown rejects ${kind} cost residue while closing independent resources`,
    { timeout: 10000 },
    () =>
      scenario(async () => {
        await runtime.ensurePersistenceReady();
        sinks.recordTailCost(4);
        shutdownFixture.costFailure = new PostgresError(kind);
        await assert.rejects(shutdown.shutdownPostgresApplication(), (error: unknown) => {
          assert.ok(error instanceof AggregateError);
          assert.ok(
            error.errors.some(
              (entry) => entry instanceof Error && entry.message.includes("unsettled cost")
            )
          );
          return true;
        });
        assert.equal(sinks.getPendingTailCost(), 4);
        assert.equal(shutdownFixture.persistedCost, 0);
        assert.equal(fixture.poolCloses, 1);
        assert.equal(shutdownFixture.logClosing, true);
      })
  );
}

// Runtime and coordinator are real; SQL/producer transports and log admission are controlled.
test(
  "accepted tail accounting finishes before final cost flush and log closing",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const permit = await runtime.acquirePersistenceTail();
      permits.push(permit);
      const external = new AsyncResource("shutdown-accounting-tail");
      const entered = barrier(),
        complete = barrier(),
        childEntered = barrier(),
        childComplete = barrier();
      const tail = observe(
        external.runInAsyncScope(() =>
          permit.run(async () => {
            entered.resolve();
            await complete.promise;
            sinks.recordTailCost(3);
            await sinks.saveTailLog("late-call");
            await runtime.runPersistenceBackgroundWork(async () => {
              childEntered.resolve();
              await childComplete.promise;
              shutdownFixture.events.push("tail:child-complete");
            });
          })
        )
      );
      try {
        await entered.promise;
        let closed = false;
        const closing = observe(
          shutdown.shutdownPostgresApplication().then(() => {
            closed = true;
          })
        );
        assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
        await assert.rejects(
          runtime.runPersistenceRequest(async () => {}),
          /unavailable|draining/
        );
        await assert.rejects(
          runtime.runPersistenceBackgroundWork(async () => {}),
          /admission/
        );
        await turn();
        assert.ok(shutdownFixture.events.includes("producer:stop"));
        assert.equal(shutdownFixture.events.includes("cost:flush"), false);
        assert.equal(shutdownFixture.logClosing, false);
        assert.equal(fixture.poolCloses, 0);
        complete.resolve();
        await childEntered.promise;
        const releasing = observe(permit.release());
        await turn();
        assert.equal(closed, false);
        assert.equal(shutdownFixture.logClosing, false);
        assert.equal(shutdownFixture.persistedCost, 0);
        childComplete.resolve();
        await Promise.all([tail, releasing, closing]);
        assert.equal(shutdownFixture.persistedCost, 3);
        assert.equal(
          shutdownFixture.events.filter((event) => event === "proxy:admission-close").length,
          1
        );
        assert.equal(shutdownFixture.events.filter((event) => event === "proxy:close").length, 1);
        assert.ok(
          shutdownFixture.events.indexOf("proxy:admission-close") <
            shutdownFixture.events.indexOf("tail:child-complete")
        );
        assert.ok(
          shutdownFixture.events.indexOf("proxy:close") >
            shutdownFixture.events.indexOf("tail:child-complete")
        );
        assert.equal(
          shutdownFixture.events.filter((event) => event === "subscription:stop").length,
          1
        );
        assert.ok(
          shutdownFixture.events.indexOf("subscription:stopped") <
            shutdownFixture.events.indexOf("cost:flush")
        );
        assert.deepEqual(shutdownFixture.logRows, ["late-call"]);
        assert.ok(
          shutdownFixture.events.indexOf("tail:child-complete") <
            shutdownFixture.events.indexOf("cost:flush")
        );
        assert.ok(
          shutdownFixture.events.indexOf("cost:persisted") <
            shutdownFixture.events.indexOf("logs:close")
        );
        assert.equal(fixture.poolCloses, 1);
        assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closed");
        assert.ok(
          shutdownFixture.events.indexOf("maintenance:continuation:stop") >
            shutdownFixture.events.indexOf("tail:child-complete")
        );
        assert.ok(
          shutdownFixture.events.indexOf("maintenance:resources:stop") >
            shutdownFixture.events.indexOf("logs:close")
        );
      } finally {
        external.emitDestroy();
      }
    })
);

test(
  "dynamic accepted-work drain follows descendants added during closing",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const permit = await runtime.acquirePersistenceTail();
      permits.push(permit);
      const start = barrier(),
        childStarted = barrier(),
        childDone = barrier();
      let descendant: Promise<void> | undefined;
      const tail = observe(
        permit.run(async () => {
          await start.promise;
          descendant = observe(
            runtime.runPersistenceBackgroundWork(async () => {
              childStarted.resolve();
              await childDone.promise;
            })
          );
        })
      );
      runtime.beginPersistenceAdmissionClose();
      let drained = false;
      const draining = observe(
        runtime.drainPersistenceAcceptedWork().then(() => {
          drained = true;
        })
      );
      const releasing = observe(permit.release());
      start.resolve();
      await childStarted.promise;
      await Promise.all([tail, releasing]);
      await turn();
      assert.equal(drained, false);
      assert.equal(fixture.poolCloses, 0);
      assert.ok(globalThis.__omniroutePersistenceContext?.application);
      childDone.resolve();
      await Promise.all([descendant, draining]);
      assert.equal(drained, true);
      assert.equal(fixture.poolCloses, 0);
    })
);

test(
  "owned callers reject before coordinator admission changes or self-wait",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      await runtime.ensurePersistenceReady();
      const graceful = await import("../../../src/lib/gracefulShutdown.ts");
      const shutdownState = globalThis.__omnirouteShutdown;
      const lifecyclePhase = globalThis.__omnirouteServerLifecycle;
      await runtime.runPersistenceRequest(async () => {
        assert.throws(() => graceful.requestGracefulShutdown("TEST"), /outside admitted work/);
        assert.equal(globalThis.__omnirouteShutdown, shutdownState);
        assert.equal(globalThis.__omnirouteServerLifecycle, lifecyclePhase);
        assert.equal(ownerGlobals[ownerSymbol], undefined);
        assert.throws(() => shutdown.shutdownPostgresApplication(), /outside admitted work/);
        assert.equal(ownerGlobals[ownerSymbol], undefined);
        assert.equal(runtime.getPersistenceRuntimeStatus().phase, "ready");
        await assert.rejects(runtime.drainPersistenceAcceptedWork(), /outside admitted work/);
        assert.equal(fixture.poolCloses, 0);
      });
      await shutdown.shutdownPostgresApplication();
      assert.equal(fixture.poolCloses, 1);
    })
);

test("cold admission closure does not allocate or initialize persistence", { timeout: 10000 }, () =>
  scenario(async () => {
    runtime.beginPersistenceAdmissionClose();
    assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
    await runtime.drainPersistenceAcceptedWork();
    assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
    await shutdown.shutdownPostgresApplication();
    assert.equal(fixture.environmentReads, 0);
    assert.equal(fixture.planeCreates, 0);
    assert.equal(fixture.artifactCreates, 0);
  })
);
