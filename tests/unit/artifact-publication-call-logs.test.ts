import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import { fixture, barrier } from "../helpers/persistence/artifactPublicationFixture.ts";
const mock = new URL("../helpers/persistence/artifactPublicationFixture.ts", import.meta.url).href;
const url = (path: string) => new URL("../../src/" + path, import.meta.url).href;
const runtimeUrl = url("lib/db/persistenceRuntime.ts"),
  logsUrl = url("lib/usage/callLogs.ts"),
  storeUrl = url("lib/artifacts/s3ArtifactStore.ts");
const runtimeOperands = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "../artifacts/s3ArtifactStore",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);
const logOperands = new Set([
  "../db/core",
  "../db/detailedLogs",
  "../compliance/noLog",
  "./migrations",
  "./usageHistory",
  "../db/responsesContinuationStore",
  "./callLogArtifactWriter",
  "./callLogRotation",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    const parent = context.parentURL?.split("?")[0];
    if (
      (parent === runtimeUrl && runtimeOperands.has(specifier)) ||
      (parent === logsUrl && logOperands.has(specifier)) ||
      (parent === storeUrl && ["@aws-sdk/client-s3", "node:crypto"].includes(specifier)) ||
      (parent === url("lib/logPayloads.ts") && specifier === "./piiSanitizer")
    )
      return { url: mock, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const previous = process.env.OMNIROUTE_DATABASE_BACKEND,
  previousDataDir = process.env.DATA_DIR;
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
const previousEncryptionKey = process.env.STORAGE_ENCRYPTION_KEY;
process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
process.env.DATA_DIR = new URL("../../.scratch-data", import.meta.url).pathname;
const storeModule = await import(storeUrl);
fixture.factory = storeModule.createS3ArtifactStore;
const runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") = await import(runtimeUrl);
const logs: typeof import("../../src/lib/usage/callLogs.ts") = await import(logsUrl);
const publication = await import("../../src/lib/usage/nativeArtifactPublication.ts");
const recovery = await import("../../src/lib/usage/nativeArtifactRecovery.ts");
const { getPersistenceArtifactStore } = runtime;
const log = (id: string) => ({
  id,
  apiKeyId: "00000000-0000-4000-8000-000000000001",
  model: "test-model",
  provider: "test-provider",
  status: 200,
  requestBody: { messages: [{ role: "user", content: "journal payload" }] },
  responseBody: { choices: [{ message: { content: "answer" } }] },
});
async function scenario(body: () => Promise<void>) {
  fixture.reset();
  delete globalThis.__omniroutePersistenceRuntime;
  await runtime.ensurePersistenceReady();
  const error = console.error;
  console.error = (...values: unknown[]) => {
    fixture.errors.push(values.map(String).join(" "));
  };
  try {
    await runtime.runPersistenceRequest(body);
  } finally {
    fixture.writeGate?.resolve();
    fixture.prepareGate?.resolve();
    console.error = error;
    await runtime.closePersistenceRuntime();
  }
}
test.after(() => {
  if (previousEncryptionKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
  else process.env.STORAGE_ENCRYPTION_KEY = previousEncryptionKey;
  if (previousDataDir === undefined) delete process.env.DATA_DIR;
  else process.env.DATA_DIR = previousDataDir;
  hooks.deregister();
  if (previous === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
  else process.env.OMNIROUTE_DATABASE_BACKEND = previous;
});
test("empty native deletion is an effect-free no-op without borrowing application authority", async () => {
  assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
  assert.deepEqual(await publication.deleteNativeCallArtifacts([]), {
    deletedArtifacts: 0,
    pendingArtifactDeletions: 0,
    errors: 0,
  });
});
test("actual saveCallLog journals before PUT, retries only log collisions, and keeps attached scope outside deletion counters", () =>
  scenario(async () => {
    fixture.collisions = 1;
    fixture.beforePut = () => {
      assert.equal(fixture.intents.size, 1);
      assert.ok(fixture.events.includes("sql-ack"));
    };
    await logs.saveCallLog(log("collision"));
    assert.equal(fixture.errors.length, 0);
    assert.equal(fixture.logs.size, 1);
    assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
    const row = [...fixture.intents.values()][0];
    assert.equal(row.publication_state, "attached");
    assert.equal(row.upload_state, "acknowledged");
    assert.equal(row.store_scope, getPersistenceArtifactStore().storageScope);
    const { getSelectedPostgresDatabase } = await import("../../src/lib/db/controlPlaneContext.ts");
    const { createPostgresCleanupRepository } =
      await import("../../src/lib/db/postgres/cleanupRepository.ts");
    assert.deepEqual(
      await createPostgresCleanupRepository(
        getSelectedPostgresDatabase()!
      ).pendingArtifactDeletions(),
      []
    );
  }));
test("actual saveCallLog retains ACK-lost metadata and positive GET attaches sole attempt", () =>
  scenario(async () => {
    fixture.putMode = "lost";
    await logs.saveCallLog(log("ack-loss"));
    assert.equal(fixture.logs.size, 1);
    const row = [...fixture.intents.values()][0];
    assert.equal(row.publication_state, "publishing");
    assert.equal(row.upload_state, "uncertain");
    assert.equal(fixture.objects.size, 1);
    assert.equal(await recovery.recoverNativeArtifact(row.relpath), true);
    assert.equal([...fixture.logs.values()][0].artifact_relpath, row.relpath);
    assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
    assert.equal(fixture.intents.get(row.relpath)?.staged_payload, null);
  }));
test("metadata SQL rejection prevents PUT", () =>
  scenario(async () => {
    fixture.sqlFailure = "definite";
    await logs.saveCallLog(log("sql-definite"));
    assert.equal(fixture.logs.size, 0);
    assert.equal(fixture.objects.size, 0);
    assert.equal(fixture.intents.size, 0);
  }));
test("actual saveCallLog lost SQL COMMIT never deletes a payload owned by committed current or legacy rows", () =>
  scenario(async () => {
    fixture.sqlFailure = "commit_unknown";
    await logs.saveCallLog(log("sql-unknown"));
    assert.equal(fixture.logs.size, 1);
    const row = [...fixture.intents.values()][0];
    assert.equal(await recovery.recoverNativeArtifact(row.relpath), true);
    const result = await publication.deleteNativeCallArtifacts([row.relpath]);
    assert.equal(result.deletedArtifacts, 0);
    assert.equal(result.pendingArtifactDeletions, 0);
    assert.equal(fixture.objects.size, 1);
    assert.equal(
      fixture.requests.some((entry) => entry.method === "DELETE"),
      false
    );
    fixture.logs.clear();
    fixture.legacy.add(row.relpath);
    row.publication_state = "delete_pending";
    assert.equal((await publication.deleteNativeCallArtifacts([row.relpath])).deletedArtifacts, 0);
    assert.equal(fixture.objects.size, 1);
  }));
for (const revoke of [false, true])
  test(
    "actual saveCallLog privacy flip after journal preparation omits payload and retains deletion ownership " +
      revoke,
    () =>
      scenario(async () => {
        fixture.beforePut = () => {
          if (revoke) fixture.keyExists = false;
          else fixture.noLog = true;
          for (const row of fixture.logs.values())
            Object.assign(row, {
              request_summary: null,
              has_request_body: 0,
              detail_state: "none",
            });
        };
        await logs.saveCallLog(log("privacy"));
        assert.equal(fixture.errors.length, 0);
        const stored = [...fixture.logs.values()][0];
        assert.equal(stored.artifact_relpath, null);
        assert.equal(stored.request_summary, null);
        assert.equal(stored.has_request_body, 0);
        assert.equal(fixture.intents.size, 0);
        assert.equal(fixture.objects.size, 0);
        assert.equal(fixture.requests.filter((entry) => entry.method === "DELETE").length, 1);
        assert.ok(fixture.events.indexOf("log-insert") < fixture.events.indexOf("delete-effect"));
      })
  );
test("conflicting same-reference preparation cannot mutate or release the first full publication lease", () =>
  scenario(async () => {
    fixture.fixedReferenceUuid = "01234567-0123-4123-8123-012345678901";
    fixture.writeGate = barrier();
    fixture.putEntered = barrier();
    const store = getPersistenceArtifactStore(),
      first = publication.createNativeArtifactPublication(),
      second = publication.createNativeArtifactPublication();
    const put = store.put(Buffer.from("same identity"), first.options);
    await fixture.putEntered.promise;
    const before = { ...[...fixture.intents.values()][0] };
    let failure: unknown;
    try {
      await store.put(Buffer.from("same identity"), second.options);
    } catch (error) {
      failure = error;
    }
    assert.ok(failure);
    await second.fail(failure);
    second.release();
    assert.deepEqual([...fixture.intents.values()][0], before);
    const result = await publication.deleteNativeCallArtifacts([before.relpath]);
    assert.equal(result.deletedArtifacts, 0);
    assert.equal(store.isPutActive!(before.relpath), true);
    assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
    assert.equal(
      fixture.requests.some((entry) => entry.method === "DELETE"),
      false
    );
    fixture.writeGate.resolve();
    const ack = await put;
    first.acknowledge(ack);
    await first.fail(new Error("no SQL consumer"));
    first.release();
    assert.equal(
      (await publication.deleteNativeCallArtifacts([before.relpath])).deletedArtifacts,
      1
    );
  }));
test("missing journal path contributes no pending deletion or external effect", () =>
  scenario(async () => {
    assert.deepEqual(
      await publication.deleteNativeCallArtifacts([
        "s3/01234567-0123-4123-8123-012345678901_" + "a".repeat(64) + ".json",
      ]),
      { deletedArtifacts: 0, pendingArtifactDeletions: 0, errors: 0 }
    );
    assert.equal(fixture.requests.length, 0);
  }));
test("lost admission COMMIT retains metadata and not-sent intent without PUT replay", () =>
  scenario(async () => {
    fixture.prepareCommitLost = true;
    await logs.saveCallLog(log("prepare-unknown"));
    assert.equal(fixture.logs.size, 1);
    const row = [...fixture.intents.values()][0];
    assert.equal(row.upload_state, "not_sent");
    assert.equal(fixture.requests.length, 0);
    assert.equal(await recovery.recoverNativeArtifact(row.relpath), true);
    assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
  }));
test("actual saveCallLog unknown sole PUT may finish after GET404; identity survives until verified positive GET and attachment", () =>
  scenario(async () => {
    fixture.putMode = "lost_before_effect";
    fixture.writeGate = barrier();
    await logs.saveCallLog(log("remote-late"));
    const row = [...fixture.intents.values()][0];
    assert.equal(row.upload_state, "uncertain");
    assert.equal(getPersistenceArtifactStore().isPutActive!(row.relpath), false);
    assert.equal(fixture.objects.size, 0);
    assert.equal(await recovery.recoverNativeArtifact(row.relpath), false);
    assert.equal(fixture.logs.size, 1);
    assert.ok(fixture.intents.has(row.relpath));
    assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
    assert.equal(
      fixture.requests.some((entry) => entry.method === "DELETE"),
      false
    );
    fixture.writeGate.resolve();
    await fixture.remoteEffect;
    assert.equal(await recovery.recoverNativeArtifact(row.relpath), true);
    assert.equal(fixture.intents.get(row.relpath)?.publication_state, "attached");
    assert.equal(fixture.intents.get(row.relpath)?.staged_payload, null);
  }));
