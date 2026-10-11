import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import type { PostgresControlPlaneBinding } from "../../../src/lib/db/controlPlaneContext.ts";
import type { PendingRequestDetail } from "../../../src/lib/usage/usageHistory.ts";
import { CALL_LOG_BODY_OMITTED_FOR_SIZE_LIMIT } from "../../../src/shared/constants/callLogSizeLimitMarkers.ts";

const runtimeUrl = new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const contextUrl = new URL("../../../src/lib/db/controlPlaneContext.ts", import.meta.url).href;
const detailUrl = new URL("../../../src/lib/usage/completedRequestDetails.ts", import.meta.url)
  .href;
const ownerFixtureUrl = new URL(
  "../../helpers/persistence/runtimeOwnedWorkFixture.ts",
  import.meta.url
).href;
const detailFixtureUrl = new URL(
  "../../helpers/persistence/completedDetailAuthorityFixture.ts",
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
const detailDependencies = new Set([
  "../db/postgres/callLogRepository",
  "../db/core",
  "./callLogArtifacts",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    const parent = context.parentURL?.split("?")[0];
    if (parent === detailUrl && detailDependencies.has(specifier))
      return { url: detailFixtureUrl, shortCircuit: true };
    if (parent === runtimeUrl && runtimeDependencies.has(specifier))
      return { url: ownerFixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const context: typeof import("../../../src/lib/db/controlPlaneContext.ts") = await import(
  contextUrl
);
const runtime: typeof import("../../../src/lib/db/persistenceRuntime.ts") = await import(
  runtimeUrl
);
const completed: typeof import("../../../src/lib/usage/completedRequestDetails.ts") = await import(
  detailUrl
);
const {
  fixture: owner,
  barrier,
}: typeof import("../../helpers/persistence/runtimeOwnedWorkFixture.ts") = await import(
  ownerFixtureUrl
);
const {
  detailFixture: detail,
}: typeof import("../../helpers/persistence/completedDetailAuthorityFixture.ts") = await import(
  detailFixtureUrl
);
const previous = new Map(
  ["OMNIROUTE_DATABASE_BACKEND", "JWT_SECRET", "API_KEY_SECRET"].map((name) => [
    name,
    process.env[name],
  ])
);

function preview(id: string): PendingRequestDetail {
  return {
    id,
    provider: "openai",
    model: "fixture-model",
    connectionId: "fixture-connection",
    startedAt: Date.now(),
    providerResponse: null,
    clientResponse: null,
    tokens: {
      in: 1,
      out: 1,
      cacheRead: null,
      cacheCreation: null,
      reasoning: null,
      compressed: null,
    },
  };
}
async function eventually(predicate: () => boolean) {
  const deadline = Date.now() + 3000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Completed-detail authority oracle timed out");
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
}
async function scenario(body: () => Promise<void>) {
  assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  delete globalThis.__omniroutePersistenceRuntime;
  owner.reset();
  detail.reset();
  completed.clearCompletedDetails();
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  try {
    await body();
  } finally {
    owner.releaseBarriers();
    await runtime.closePersistenceRuntime();
    completed.clearCompletedDetails();
    delete globalThis.__omniroutePersistenceRuntime;
  }
}
test.after(() => {
  hooks.deregister();
  owner.reset();
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test("enrichment is admitted before SQL, outlives its caller tail, and drains before artifact closure", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const updated = preview("delayed-detail");
    completed.storeCompletedDetail(updated);
    detail.paths = [{ artifact_relpath: "fixture/pipeline.json" }];
    detail.payloads.set("fixture/pipeline.json", {
      pipeline: {
        providerResponse: { side: "provider" },
        clientResponse: { side: "client" },
      },
      responseBody: { side: "fallback" },
    });
    detail.entered = barrier();
    detail.release = barrier();
    const permit = await runtime.runPersistenceRequest(() => runtime.acquirePersistenceTail());
    try {
      let original: PostgresControlPlaneBinding | undefined;
      await permit.run(async () => {
        original = context.getSelectedControlPlane();
        assert.equal(
          completed.maybeEnrichCompletedDetail(updated, "fixture-connection"),
          undefined
        );
        assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      });
      await detail.entered.promise;
      await permit.release();
      assert.throws(() => original!.assertReady(), /scope ended/);
      const newerTokens = { ...updated.tokens!, in: 123, out: 456 };
      completed.storeCompletedDetail({ ...updated, tokens: newerTokens });
      let closed = false;
      const closing = runtime.closePersistenceRuntime().then(() => {
        closed = true;
      });
      await Promise.resolve();
      assert.equal(closed, false);
      assert.equal(owner.artifactCloses, 0);
      detail.release.resolve();
      await closing;
      assert.equal(detail.reads, 1);
      assert.equal(detail.sqliteCalls, 0);
      assert.equal(owner.artifactCloses, 1);
      const stored = completed.getCompletedDetails().get(updated.id)!;
      assert.deepEqual(stored.providerResponse, { side: "provider" });
      assert.deepEqual(stored.clientResponse, { side: "client" });
      assert.deepEqual(stored.tokens, newerTokens);
      assert.ok(completed.getCompletedDetailsByteSize() <= completed.MAX_COMPLETED_DETAILS_BYTES);
    } finally {
      detail.release.resolve();
      await permit.release();
    }
  }));

test("delayed artifact enrichment cannot resurrect an evicted completed preview", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const updated = preview("evicted-detail");
    completed.storeCompletedDetail(updated);
    detail.paths = [{ artifact_relpath: "fixture/evicted.json" }];
    detail.payloads.set("fixture/evicted.json", {
      responseBody: { visible: true },
    });
    detail.entered = barrier();
    detail.release = barrier();
    await runtime.runPersistenceRequest(async () => {
      completed.maybeEnrichCompletedDetail(updated, "fixture-connection");
    });
    await detail.entered.promise;
    completed.clearCompletedDetails();
    detail.release.resolve();
    await runtime.drainPersistenceAcceptedWork();
    assert.equal(completed.getCompletedDetails().has(updated.id), false);
    assert.equal(completed.getCompletedDetailsByteSize(), 0);
  }));

test("omitted or absent artifacts do not become provider/client payloads", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const updated = preview("omitted-detail");
    completed.storeCompletedDetail(updated);
    detail.paths = [
      { artifact_relpath: null },
      { artifact_relpath: "fixture/missing.json" },
      { artifact_relpath: "fixture/omitted.json" },
    ];
    detail.payloads.set("fixture/omitted.json", {
      responseBody: CALL_LOG_BODY_OMITTED_FOR_SIZE_LIMIT,
    });
    await runtime.runPersistenceRequest(async () => {
      completed.maybeEnrichCompletedDetail(updated, "fixture-connection");
    });
    await runtime.drainPersistenceAcceptedWork();
    assert.equal(updated.providerResponse, null);
    assert.equal(updated.clientResponse, null);
    assert.equal(completed.getCompletedDetails().get(updated.id)!.clientResponse, null);
    assert.equal(detail.sqliteCalls, 0);
  }));

test("a foreign explicit native identity cannot allocate or borrow the application owner", (t) =>
  scenario(async () => {
    const warnings: unknown[][] = [];
    t.mock.method(console, "warn", (...args: unknown[]) => warnings.push(args));
    const foreign = {
      databaseIdentity: Object.freeze({}),
      assertReady() {},
    } as unknown as PostgresControlPlaneBinding;
    await context.withPostgresControlPlane(foreign, async () => {
      completed.maybeEnrichCompletedDetail(preview("foreign"), "fixture-connection");
    });
    assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
    assert.equal(owner.planeCreates, 0);
    assert.equal(owner.artifactCreates, 0);
    assert.equal(detail.queries, 0);
    assert.equal(detail.reads, 0);
    assert.equal(warnings.length, 1);
    assert.match(String(warnings[0][1]), /not been ported/);
  }));

test("refused admission is observed and does not enter a repository or artifact reader", (t) =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const warnings: unknown[][] = [];
    t.mock.method(console, "warn", (...args: unknown[]) => warnings.push(args));
    runtime.beginPersistenceAdmissionClose();
    completed.maybeEnrichCompletedDetail(preview("refused"), "fixture-connection");
    await eventually(() => warnings.length === 1);
    assert.equal(detail.queries, 0);
    assert.equal(detail.reads, 0);
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    assert.match(String(warnings[0][1]), /admission is unavailable/);
  }));

test("native SQL failure is sanitized, observed, and removed from accepted work", (t) =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const warnings: unknown[][] = [];
    t.mock.method(console, "warn", (...args: unknown[]) => warnings.push(args));
    const credential = "sk-testsecret0123456789";
    detail.queryFailure = new Error(`SQL failure password=${credential} at /private/customer/data`);
    await runtime.runPersistenceRequest(async () => {
      completed.maybeEnrichCompletedDetail(preview("sql-failure"), "fixture-connection");
    });
    await runtime.drainPersistenceAcceptedWork();
    await eventually(() => warnings.length === 1);
    assert.equal(detail.reads, 0);
    assert.equal(detail.sqliteCalls, 0);
    assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    assert.equal(warnings.map(String).join(" ").includes(credential), false);
    assert.equal(warnings.map(String).join(" ").includes("/private/customer/data"), false);
  }));

// Fresh catalog admission crosses an actual async import in the runtime. Advancing only the
// clock after enqueue makes an uncaptured cutoff observably different from the source window.
test("native admission delay does not move the original thirty-second artifact selection window", (t) =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    let now = Date.UTC(2026, 9, 11, 12, 0, 0);
    t.mock.method(Date, "now", () => now);
    const admittedAt = now;
    await runtime.runPersistenceRequest(async () => {
      completed.maybeEnrichCompletedDetail(preview("stable-cutoff"), "fixture-connection");
      now += 60_000;
    });
    await runtime.drainPersistenceAcceptedWork();
    assert.deepEqual(detail.cutoffs, [new Date(admittedAt - 30_000).toISOString()]);
    assert.deepEqual(detail.queriedAt, [admittedAt + 60_000]);
    assert.equal(detail.sqliteCalls, 0);
  }));
