import assert from "node:assert/strict";
import test from "node:test";
import { SpendBatchWriter } from "../../src/lib/spend/batchWriter.ts";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";

test("an unknown financial COMMIT is quarantined and never replayed", async () => {
  let attempts = 0;
  const diagnostics: string[] = [];
  const writer = new SpendBatchWriter({
    persistEntries: async () => {
      attempts++;
      throw new PostgresError("commit_unknown");
    },
    logger: {
      log() {},
      error(message) {
        diagnostics.push(String(message));
      },
    },
  });
  writer.increment("key-a", 1.5, 123);
  try {
    assert.deepEqual(await writer.flush(), { flushedEntries: 0, uniqueKeys: 1, requeued: false });
    assert.equal(attempts, 1);
    assert.equal(writer.getUncertainEntries().length, 1);
    assert.equal(writer.getPendingCostTotal("key-a"), 1.5);
    await writer.flush();
    assert.equal(attempts, 1);
    assert.ok(diagnostics.some((entry) => entry.includes("reconciliation required")));
  } finally {
    writer.resetForTests();
  }
});

test("a definite failed COMMIT remains retryable", async () => {
  let attempts = 0;
  const writer = new SpendBatchWriter({
    persistEntries: async () => {
      if (++attempts === 1) throw new PostgresError("transaction_failed");
    },
    logger: { log() {}, error() {} },
  });
  writer.increment("key-b", 2, 123);
  try {
    assert.equal((await writer.flush()).requeued, true);
    assert.equal(writer.getUncertainEntries().length, 0);
    assert.equal((await writer.flush()).flushedEntries, 1);
    assert.equal(attempts, 2);
  } finally {
    writer.resetForTests();
  }
});

test("an uncertain error from another loaded module is not mistaken for a retryable failure", async () => {
  const variant = (await import(
    new URL("../../src/lib/db/postgres/database.ts?separate-bundle", import.meta.url).href
  )) as typeof import("../../src/lib/db/postgres/database.ts");
  const error = new variant.PostgresError("commit_unknown");
  assert.notEqual(variant.PostgresError, PostgresError);
  assert.equal(error instanceof PostgresError, false);
  let attempts = 0;
  const writer = new SpendBatchWriter({
    persistEntries: async () => {
      attempts++;
      throw error;
    },
    logger: { log() {}, error() {} },
  });
  writer.increment("bundled-key", 3, 123);
  try {
    assert.equal((await writer.flush()).requeued, false);
    await writer.flush();
    assert.equal(attempts, 1);
    assert.equal(writer.getUncertainEntries().length, 1);
  } finally {
    writer.resetForTests();
  }
});
