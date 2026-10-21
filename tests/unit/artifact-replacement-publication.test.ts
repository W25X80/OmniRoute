import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import { fixture, barrier } from "../helpers/persistence/artifactPublicationFixture.ts";

const helper = new URL("../helpers/persistence/artifactPublicationFixture.ts", import.meta.url)
  .href;
const source = (path: string) => new URL("../../src/" + path, import.meta.url).href;
const runtimeUrl = source("lib/db/persistenceRuntime.ts");
const logsUrl = source("lib/usage/callLogs.ts");
const storeUrl = source("lib/artifacts/s3ArtifactStore.ts");
const runtimeSeams = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "../artifacts/s3ArtifactStore",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);
const logSeams = new Set([
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
  resolve(specifier, context, next) {
    if (
      (context.parentURL === runtimeUrl && runtimeSeams.has(specifier)) ||
      (context.parentURL === logsUrl && logSeams.has(specifier)) ||
      (context.parentURL === storeUrl && specifier === "@aws-sdk/client-s3") ||
      (context.parentURL === source("lib/logPayloads.ts") && specifier === "./piiSanitizer")
    )
      return { url: helper, shortCircuit: true };
    return next(specifier, context);
  },
});
const names = ["OMNIROUTE_DATABASE_BACKEND", "DATA_DIR", "STORAGE_ENCRYPTION_KEY"];
const previous = new Map(names.map((name) => [name, process.env[name]]));
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
process.env.DATA_DIR = new URL("../../.scratch-data", import.meta.url).pathname;
process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
const storeModule = await import(storeUrl);
fixture.factory = storeModule.createS3ArtifactStore;
const runtime = await import(runtimeUrl);
const logs = await import(logsUrl);
const { recoverNativeArtifact } = await import("../../src/lib/usage/nativeArtifactRecovery.ts");
const { deleteNativeCallArtifacts } =
  await import("../../src/lib/usage/nativeArtifactPublication.ts");
test.after(() => {
  hooks.deregister();
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

// The actual orchestrator/repository SQL is admitted by the existing fake owner;
// the accompanying TLS PostgreSQL suite is authoritative for SQL concurrency.
test("fresh-reference recovery preserves staging, stops at two liabilities and reuses only verified cleanup credit", async (t) => {
  fixture.reset();
  delete globalThis.__omniroutePersistenceRuntime;
  await runtime.ensurePersistenceReady();
  const oldGate = barrier();
  const nextGate = barrier();
  t.mock.method(console, "warn", () => {});
  t.mock.method(console, "error", () => {});
  try {
    await runtime.runPersistenceRequest(async () => {
      fixture.putMode = "lost_before_effect";
      fixture.writeGate = oldGate;
      await logs.saveCallLog({
        id: "fresh-publication",
        apiKeyId: "00000000-0000-4000-8000-000000000001",
        model: "test-model",
        provider: "test-provider",
        status: 200,
        requestBody: { messages: [{ role: "user", content: "private body" }] },
        responseBody: { choices: [{ message: { content: "answer" } }] },
      });
      const original = [...fixture.intents.values()][0];
      const originalEffect = fixture.remoteEffect!;
      const ciphertext = original.staged_payload;
      const bytes = original.staged_bytes;
      assert.equal(await recoverNativeArtifact(original.relpath), false);
      assert.equal(fixture.intents.size, 1, "not-yet-due owner cannot allocate a successor");
      original.nextDue = Date.now() - 1;
      assert.equal(await recoverNativeArtifact(original.relpath), false);
      assert.equal(fixture.intents.size, 2);
      const successor = [...fixture.intents.values()].find(
        (row) => row.relpath !== original.relpath
      )!;
      assert.equal(original.publication_state, "delete_pending");
      assert.equal(original.upload_state, "uncertain");
      assert.equal(original.staged_payload, null);
      assert.equal(successor.staged_payload, ciphertext);
      assert.equal(successor.staged_bytes, bytes);
      assert.equal(successor.upload_state, "not_sent");
      assert.equal(fixture.requests.filter((value) => value.method === "PUT").length, 1);
      fixture.writeGate = nextGate;
      successor.nextDue = Date.now() - 1;
      assert.equal(await recoverNativeArtifact(successor.relpath), false);
      assert.equal(successor.upload_state, "uncertain");
      successor.nextDue = Date.now() - 1;
      assert.equal(await recoverNativeArtifact(successor.relpath), false);
      assert.equal(fixture.intents.size, 2, "uncertain predecessor consumes the second credit");
      assert.ok(successor.nextDue >= Date.now() + 290000);
      oldGate.resolve();
      await originalEffect;
      fixture.deleteRetries = 3;
      const failedDelete = await deleteNativeCallArtifacts([original.relpath]);
      assert.equal(failedDelete.errors, 1);
      assert.equal(original.upload_state, "acknowledged");
      successor.nextDue = Date.now() - 1;
      assert.equal(await recoverNativeArtifact(successor.relpath), false);
      assert.equal(fixture.intents.size, 2, "GET ACK does not free unacknowledged DELETE credit");
      fixture.deleteRetries = 0;
      assert.equal(
        (await deleteNativeCallArtifacts([original.relpath])).pendingArtifactDeletions,
        0
      );
      assert.equal(fixture.intents.size, 1);
      successor.nextDue = Date.now() - 1;
      assert.equal(await recoverNativeArtifact(successor.relpath), false);
      assert.equal(fixture.intents.size, 2, "terminal old cleanup permits a future fresh identity");
      assert.equal([...fixture.intents.values()].filter((row) => row.staged_payload).length, 1);
      const puts = fixture.requests.filter((value) => value.method === "PUT");
      assert.equal(puts.length, 2, "allocating references never sends them recursively");
      assert.equal(new Set(puts.map((value) => value.path)).size, 2);
    });
  } finally {
    oldGate.resolve();
    nextGate.resolve();
    await fixture.remoteEffect?.catch(() => {});
    await runtime.closePersistenceRuntime();
  }
});
