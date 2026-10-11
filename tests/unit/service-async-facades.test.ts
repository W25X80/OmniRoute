import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const dataDirectory = mkdtempSync(join(tmpdir(), "omniroute-service-async-facades-"));
process.env.DATA_DIR = dataDirectory;
const core = await import("../../src/lib/db/core.ts");
const files = await import("../../src/lib/db/files.ts");
const batches = await import("../../src/lib/db/batches.ts");
const keys = await import("../../src/lib/db/registeredKeys.ts");
const { JobRegistry } = await import("../../src/lib/jobRegistry/registry.ts");

test.after(() => {
  core.resetDbInstance();
  rmSync(dataDirectory, { recursive: true, force: true });
  delete process.env.DATA_DIR;
});

test("async file and batch facades preserve the SQLite byte and checkpoint contract", async () => {
  const content = Buffer.from([0, 255, 7]);
  const file = await files.createFileAsync({
    bytes: 3,
    filename: "async.jsonl",
    purpose: "batch",
    content,
    apiKeyId: "owner",
  });
  assert.deepEqual(await files.getFileContentAsync(file.id), content);
  assert.equal(Object.hasOwn((await files.getFileAsync(file.id))!, "content"), false);
  const batch = await batches.createBatchAsync({
    endpoint: "/v1/chat/completions",
    completionWindow: "24h",
    inputFileId: file.id,
    apiKeyId: "owner",
  });
  await batches.ensureBatchItemCheckpointsAsync(batch.id, [{ lineNumber: 1, customId: "request" }]);
  await batches.markBatchItemProcessingAsync(batch.id, { lineNumber: 1, customId: "request" });
  await batches.markBatchItemResultAsync(
    batch.id,
    { lineNumber: 1, customId: "request" },
    { response: "complete" }
  );
  assert.equal((await batches.listBatchItemCheckpointsAsync(batch.id))[0].status, "completed");
});

test("async issuance closes the await gap between quota preflight and insertion in SQLite", async () => {
  keys.setProviderKeyLimit("async-quota", { maxActiveKeys: 1 });
  const requests = await Promise.allSettled(
    ["first", "second"].map((name) =>
      keys.issueRegisteredKeyAsync({ name, provider: "async-quota" })
    )
  );
  assert.equal(requests.filter((request) => request.status === "fulfilled").length, 1);
  const rejected = requests.find((request) => request.status === "rejected");
  assert.ok(
    rejected?.status === "rejected" && rejected.reason instanceof keys.RegisteredKeyQuotaError
  );
  assert.equal(keys.listRegisteredKeys({ provider: "async-quota" }).length, 1);
});

test("async job registration and toggle remain visible through the legacy SQLite views", async () => {
  const registry = new JobRegistry();
  const now = new Date().toISOString();
  await registry.registerAsync({
    id: "async-job",
    type: "interval",
    cron: null,
    intervalMs: 10000,
    enabled: false,
    envFlag: null,
    config: {},
    createdAt: now,
    updatedAt: now,
    handler: async () => ({ success: true }),
  });
  assert.equal(registry.listJobs().find((job) => job.id === "async-job")!.enabled, false);
  await registry.setEnabledAsync("async-job", false);
  assert.equal(
    (await registry.listJobsAsync()).find((job) => job.id === "async-job")!.enabled,
    false
  );
  registry.dispose();
});
