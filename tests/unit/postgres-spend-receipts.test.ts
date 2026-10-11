import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { SpendBatchWriter, type BufferedCostEntry } from "../../src/lib/spend/batchWriter.ts";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";
import {
  captureSpendEntries,
  PostgresSpendReceiptError,
} from "../../src/lib/db/postgres/spendReceipts.ts";

const quiet = { log() {}, error() {} };

test("native pending spend has an immutable identity across definite-failure retry", async () => {
  const identity = randomUUID();
  let generated = 0;
  const submissions: BufferedCostEntry[][] = [];
  const writer = new SpendBatchWriter({
    createReceiptId: () => {
      generated++;
      return identity;
    },
    persistEntries: async (entries) => {
      submissions.push(entries);
      if (submissions.length === 1) throw new PostgresError("transaction_failed");
    },
    logger: quiet,
  });
  writer.increment("native", 1.25, 123);
  try {
    const pending = writer.getBufferedEntries("native")[0];
    assert.equal(pending.receiptId, identity);
    assert.equal(Object.isFrozen(pending), true);
    assert.throws(() => {
      pending.cost = 99;
    }, TypeError);
    assert.equal((await writer.flush()).requeued, true);
    assert.equal(writer.getPendingCostTotal("native"), 1.25);
    assert.equal((await writer.flush()).flushedEntries, 1);
    assert.equal(generated, 1);
    assert.equal(submissions[0][0], submissions[1][0]);
    assert.equal(submissions[1][0].receiptId, identity);
  } finally {
    writer.resetForTests();
  }
});

test("SQLite pending entries retain their existing public shape", async () => {
  const submissions: BufferedCostEntry[][] = [];
  const writer = new SpendBatchWriter({
    persistEntries: (entries) => {
      submissions.push(entries);
    },
    logger: quiet,
  });
  writer.increment("sqlite", 2, 123);
  try {
    assert.deepEqual(writer.getBufferedEntries("sqlite"), [
      { apiKeyId: "sqlite", cost: 2, timestamp: 123 },
    ]);
    await writer.flush();
    assert.deepEqual(submissions, [[{ apiKeyId: "sqlite", cost: 2, timestamp: 123 }]]);
  } finally {
    writer.resetForTests();
  }
});

test("receipt reconciliation removes only confirmed quarantine and never retries uncertainty", async () => {
  const ids = [randomUUID(), randomUUID(), randomUUID()];
  let generated = 0,
    attempts = 0;
  const writer = new SpendBatchWriter({
    createReceiptId: () => ids[generated++],
    persistEntries: async () => {
      attempts++;
      throw new PostgresError("commit_unknown");
    },
    logger: quiet,
  });
  writer.increment("unknown", 1, 123);
  writer.increment("unknown", 2, 124);
  try {
    await writer.flush();
    writer.increment("unknown", 4, 125);
    assert.equal(writer.resolveCommittedReceipts([ids[0], ids[2]]), 1);
    assert.deepEqual(
      writer.getUncertainEntries().map((entry) => entry.receiptId),
      [ids[1]]
    );
    assert.equal(writer.getPendingCostTotal("unknown"), 6);
    // The unflushed event was not removed by an unrelated reconciliation acknowledgement.
    assert.equal(
      writer.getBufferedEntries("unknown").some((entry) => entry.receiptId === ids[2]),
      true
    );
    assert.equal(attempts, 1);
  } finally {
    writer.resetForTests();
  }
});

test("receipt conflicts are quarantined instead of endlessly requeued", async () => {
  let attempts = 0;
  const writer = new SpendBatchWriter({
    createReceiptId: randomUUID,
    persistEntries: async () => {
      attempts++;
      throw new PostgresSpendReceiptError();
    },
    logger: quiet,
  });
  writer.increment("conflict", 1, 123);
  try {
    assert.equal((await writer.flush()).requeued, false);
    await writer.flush();
    assert.equal(attempts, 1);
    assert.equal(writer.getUncertainEntries().length, 1);
  } finally {
    writer.resetForTests();
  }
});

test("captured spend does not follow caller mutations after admission", () => {
  const original = { apiKeyId: "captured", cost: 1, timestamp: 123, receiptId: randomUUID() };
  const [accepted] = captureSpendEntries([original]);
  original.cost = 999;
  original.apiKeyId = "changed";
  original.timestamp = 456;
  assert.deepEqual(accepted, {
    apiKeyId: "captured",
    cost: 1,
    timestamp: 123,
    receiptId: original.receiptId,
  });
  assert.throws(
    () => captureSpendEntries([{ apiKeyId: "captured", cost: Infinity, timestamp: 123 }]),
    TypeError
  );
  assert.throws(
    () =>
      captureSpendEntries([
        { apiKeyId: "captured", cost: 1, timestamp: 123, receiptId: "not-a-uuid" },
      ]),
    TypeError
  );
});
