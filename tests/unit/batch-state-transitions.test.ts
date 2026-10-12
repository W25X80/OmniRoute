import "../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import test from "node:test";
import { getDbInstance, resetDbInstance } from "../../src/lib/db/core.ts";
import { createFile, getFileContent } from "../../src/lib/db/files.ts";
import {
  createBatch,
  getBatch,
  transitionBatchAsync,
  completeBatchWithFilesAsync,
} from "../../src/lib/db/batches.ts";

test.after(() => resetDbInstance());
const completion = {
  requestCountsTotal: 1,
  requestCountsCompleted: 1,
  requestCountsFailed: 0,
  completedAt: Math.floor(Date.now() / 1000),
};
function setup() {
  const input = createFile({
    bytes: 1,
    content: Buffer.from("x"),
    filename: "input",
    purpose: "batch",
  });
  return createBatch({
    inputFileId: input.id,
    endpoint: "/v1/embeddings",
    completionWindow: "24h",
    status: "finalizing",
  });
}
test("SQLite async boundary preserves cancellation instead of completing from an old snapshot", async () => {
  const batch = setup();
  const stale = getBatch(batch.id)!;
  const cancel = await transitionBatchAsync(batch.id, ["finalizing"], { status: "cancelling" });
  assert.equal(stale.status, "finalizing");
  assert.equal(cancel.batch?.status, "cancelling");
  const filename = `${batch.id}-discarded-output`;
  const result = await completeBatchWithFilesAsync(
    batch.id,
    completion,
    { filename, content: Buffer.from("result"), expiresAt: null },
    null
  );
  assert.equal(result.batch?.status, "cancelled");
  assert.equal(
    (
      getDbInstance().prepare("SELECT count(*) AS n FROM files WHERE filename=?").get(filename) as {
        n: number;
      }
    ).n,
    0
  );
});
test("SQLite publication winner keeps its files and rejects cancellation/failure/recovery", async () => {
  const batch = setup();
  const done = await completeBatchWithFilesAsync(
    batch.id,
    completion,
    { filename: `${batch.id}-output`, content: Buffer.from("result"), expiresAt: null },
    null
  );
  assert.equal(done.batch?.status, "completed");
  assert.deepEqual(getFileContent(done.batch!.outputFileId!), Buffer.from("result"));
  for (const status of ["cancelling", "failed", "validating"] as const) {
    const result = await transitionBatchAsync(batch.id, ["in_progress", "finalizing"], { status });
    assert.equal(result.changed, false);
    assert.equal(result.batch?.status, "completed");
  }
});
