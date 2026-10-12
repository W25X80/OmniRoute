import "../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import test, { mock } from "node:test";
import { getDbInstance, resetDbInstance } from "../../src/lib/db/core.ts";
import * as batches from "../../src/lib/db/batches.ts";
import * as files from "../../src/lib/db/files.ts";
import {
  processPendingBatches,
  stopBatchProcessor,
  waitForAllBatches,
} from "../../open-sse/services/batchProcessor.ts";
import { DEFAULT_BATCH_EXPIRATION_SECONDS } from "../../src/shared/constants/batch.ts";

const owner = "batch-expiry-owner";
const payload = Buffer.from(
  JSON.stringify({
    custom_id: "request-one",
    method: "POST",
    url: "/v1/embeddings",
    body: { model: "openai/text-embedding-3-small", input: "hello" },
  }) + "\n"
);

function setup() {
  const input = files.createFile({
    bytes: payload.length,
    content: payload,
    filename: "reusable.jsonl",
    purpose: "fine-tune",
    apiKeyId: owner,
  });
  const terminal = batches.createBatch({
    endpoint: "/v1/embeddings",
    completionWindow: "24h",
    inputFileId: input.id,
    apiKeyId: owner,
    status: "cancelled",
    cancelledAt: Math.floor(Date.now() / 1000) - DEFAULT_BATCH_EXPIRATION_SECONDS - 1,
  });
  const sibling = () =>
    batches.createBatchAsync({
      endpoint: "/v1/embeddings",
      completionWindow: "24h",
      inputFileId: input.id,
      apiKeyId: owner,
    });
  assert.equal(input.expiresAt, null, "the file's own TTL must not cause this deletion");
  return { input, terminal, sibling };
}

test.beforeEach(() => {
  mock.restoreAll();
  const db = getDbInstance();
  db.prepare("DELETE FROM batch_item_checkpoints").run();
  db.prepare("DELETE FROM batches").run();
  db.prepare("DELETE FROM files").run();
});

test.after(async () => {
  mock.restoreAll();
  stopBatchProcessor();
  await waitForAllBatches();
  resetDbInstance();
});

test("expiry cannot erase the input of a sibling accepted after its reference observation", async () => {
  const f = setup();
  // The HTTP route's earlier metadata/ownership check can succeed before cleanup starts.
  assert.equal(files.getFile(f.input.id)?.apiKeyId, owner);
  const db = getDbInstance();
  const prepare = db.prepare.bind(db);
  let scheduled = false;
  let created: Promise<batches.BatchRecord | null> | undefined;
  mock.method(db, "prepare", (sql: string) => {
    const statement = prepare(sql);
    if (sql.startsWith("SELECT 1 FROM batches") && sql.includes("AND id NOT IN")) {
      const get = statement.get.bind(statement);
      mock.method(statement, "get", (...params: unknown[]) => {
        const result = get(...params);
        if (!scheduled && params[0] === f.input.id && !result) {
          scheduled = true;
          created = new Promise((resolve, reject) => {
            queueMicrotask(() => {
              void f.sibling().then(resolve, (error: unknown) => {
                if (error instanceof Error && error.message === "Input file not found")
                  resolve(null);
                else reject(error);
              });
            });
          });
        }
        return result;
      });
    }
    return statement;
  });

  await processPendingBatches();
  assert.equal(scheduled, true, "the actual scheduler must reach the reference query");
  const sibling = await created!;
  if (sibling) {
    assert.equal(batches.getBatch(sibling.id)?.status, "validating");
    assert.deepEqual(files.getFileContent(f.input.id), payload, "accepted input must survive");
  } else {
    assert.equal(files.getFile(f.input.id), null, "cleanup won before sibling admission");
    assert.equal(batches.countBatches(owner), 1, "rejected admission must not insert a batch");
  }
});

test("batch creation rejects a tombstone even after an earlier successful input read", async () => {
  const f = setup();
  assert.ok(await files.getFileAsync(f.input.id));
  assert.equal(await files.deleteFileAsync(f.input.id), true);
  await assert.rejects(f.sibling(), /Input file not found/);
  assert.equal(batches.countBatches(owner), 1);
});

test("an accepted sibling keeps every reference live during terminal cleanup", async () => {
  const f = setup();
  const sibling = await f.sibling();
  await batches.deleteExpiredBatchFilesAsync(f.terminal.id);
  assert.deepEqual(files.getFileContent(f.input.id), payload);
  assert.equal(batches.getBatch(sibling.id)?.status, "validating");
});

test("scheduler rechecks current expiry after its terminal-list snapshot", async () => {
  const f = setup();
  const db = getDbInstance();
  const prepare = db.prepare.bind(db);
  let updated = false;
  mock.method(db, "prepare", (sql: string) => {
    const statement = prepare(sql);
    if (sql.includes("status IN ('completed', 'failed', 'cancelled', 'expired')")) {
      const all = statement.all.bind(statement);
      mock.method(statement, "all", (...params: unknown[]) => {
        const result = all(...params);
        if (!updated) {
          updated = true;
          queueMicrotask(() => {
            batches.updateBatch(f.terminal.id, { cancelledAt: Math.floor(Date.now() / 1000) });
          });
        }
        return result;
      });
    }
    return statement;
  });
  await processPendingBatches();
  assert.equal(updated, true);
  assert.deepEqual(files.getFileContent(f.input.id), payload);
});

test("terminal cleanup retains strict expiry boundaries and created-at output policy", async () => {
  const f = setup();
  const output = files.createFile({
    bytes: 3,
    content: Buffer.from("out"),
    filename: "output",
    purpose: "batch_output",
    apiKeyId: owner,
    expiresAt: null,
  });
  const error = files.createFile({
    bytes: 3,
    content: Buffer.from("err"),
    filename: "error",
    purpose: "batch_output",
    apiKeyId: owner,
    expiresAt: null,
  });
  const now = Math.floor(Date.now() / 1000);
  batches.updateBatch(f.terminal.id, {
    createdAt: now - 3600,
    cancelledAt: now - DEFAULT_BATCH_EXPIRATION_SECONDS,
    outputFileId: output.id,
    errorFileId: error.id,
    outputExpiresAfterAnchor: "created_at",
    outputExpiresAfterSeconds: 3600,
  });
  await batches.deleteExpiredBatchFilesAsync(f.terminal.id, now);
  for (const id of [f.input.id, output.id, error.id]) assert.ok(files.getFile(id));
  await batches.deleteExpiredBatchFilesAsync(f.terminal.id, now + 1);
  for (const id of [f.input.id, output.id, error.id]) assert.equal(files.getFileContent(id), null);
  assert.equal(batches.getBatch(f.terminal.id)?.status, "cancelled", "metadata stays retained");
});

test("a stale terminal candidate cannot remove files from a now-active batch", async () => {
  const f = setup();
  batches.updateBatch(f.terminal.id, { status: "in_progress" });
  await batches.deleteExpiredBatchFilesAsync(f.terminal.id);
  assert.deepEqual(files.getFileContent(f.input.id), payload);
});

test("explicit deletion, ownership guard and independent file TTL keep their existing policy", async () => {
  const f = setup();
  await f.sibling();
  assert.equal(files.deleteFileOwnedBy(f.input.id, "other-owner"), false);
  assert.deepEqual(files.getFileContent(f.input.id), payload);
  assert.equal(files.deleteFileOwnedBy(f.input.id, owner), true);
  assert.equal(files.getFileContent(f.input.id), null, "explicit owned deletion still wins");

  const now = Math.floor(Date.now() / 1000);
  const expiring = files.createFile({
    bytes: payload.length,
    content: payload,
    filename: "own-ttl.jsonl",
    purpose: "fine-tune",
    apiKeyId: owner,
    expiresAt: now - 1,
  });
  const batch = await batches.createBatchAsync({
    inputFileId: expiring.id,
    endpoint: "/v1/embeddings",
    completionWindow: "24h",
    apiKeyId: owner,
  });
  assert.equal(files.pruneExpiredFiles(now), 1);
  assert.equal(files.getFileContent(expiring.id), null, "file TTL is independent of batch refs");
  assert.equal(batches.getBatch(batch.id)?.status, "validating");
});

test("input liveness check does not add a purpose or owner-equality restriction", async () => {
  const f = setup();
  // The HTTP owner guard separately permits a manage principal to use another owner's file.
  const batch = await batches.createBatchAsync({
    inputFileId: f.input.id,
    endpoint: "/v1/embeddings",
    completionWindow: "24h",
    apiKeyId: "manage-principal",
  });
  assert.equal(batch.apiKeyId, "manage-principal");
  assert.equal(files.getFile(f.input.id)?.purpose, "fine-tune");
});
