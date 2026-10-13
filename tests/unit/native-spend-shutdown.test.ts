import assert from "node:assert/strict";
import test from "node:test";
import { SpendBatchWriter } from "../../src/lib/spend/batchWriter.ts";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";

const logger = { log() {}, error() {} };
const turn = () => new Promise<void>((resolve) => setImmediate(resolve));

test("native producer close stops its timer while accepting late owned increments", async (t) => {
  t.mock.timers.enable({ apis: ["setInterval"] });
  const costs: number[] = [];
  const writer = new SpendBatchWriter({
    flushIntervalMs: 100,
    maxBufferSize: 2,
    logger,
    persistEntries: async (entries) => {
      costs.push(...entries.map((entry) => entry.cost));
    },
  });
  try {
    writer.increment("key", 1);
    writer.beginNativeClose();
    writer.increment("key", 2);
    writer.start();
    t.mock.timers.tick(1000);
    await turn();
    assert.deepEqual(costs, [], "closing must not schedule timer or threshold flushes");
    assert.equal(writer.getPendingCostTotal("key"), 3);
    await writer.closeNative();
    assert.deepEqual(costs, [1, 2]);
    assert.equal(writer.getPendingCostTotal("key"), 0);
    assert.throws(() => writer.increment("key", 3), /closed/);
    t.mock.timers.tick(1000);
    await turn();
    assert.deepEqual(costs, [1, 2]);
  } finally {
    writer.resetForTests();
  }
});

for (const kind of ["transaction_failed", "commit_unknown"] as const) {
  test(`native final ${kind} cannot report a clean drain or replay on repeated close`, async () => {
    let calls = 0;
    const writer = new SpendBatchWriter({
      logger,
      persistEntries: async () => {
        calls++;
        throw new PostgresError(kind);
      },
    });
    try {
      writer.increment("key", 4);
      await assert.rejects(writer.closeNative(), /unsettled cost/);
      assert.equal(writer.getPendingCostTotal("key"), 4);
      assert.equal(writer.getUncertainEntries().length, kind === "commit_unknown" ? 1 : 0);
      await assert.rejects(writer.closeNative(), /unsettled cost/);
      assert.equal(calls, 1);
    } finally {
      writer.resetForTests();
    }
  });
}

test("native final close joins an accepted flush and then persists only later entries", async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const batches: number[][] = [];
  const writer = new SpendBatchWriter({
    logger,
    persistEntries: async (entries) => {
      batches.push(entries.map((entry) => entry.cost));
      if (batches.length === 1) await gate;
    },
  });
  try {
    writer.increment("key", 1);
    const first = writer.flush();
    writer.beginNativeClose();
    writer.increment("key", 2);
    let closed = false;
    const closing = writer.closeNative().then(() => {
      closed = true;
    });
    await turn();
    assert.equal(closed, false);
    release();
    await Promise.all([first, closing]);
    assert.deepEqual(batches, [[1], [2]]);
  } finally {
    release();
    writer.resetForTests();
  }
});

test("native close refuses existing quarantine even when no buffered batch remains", async () => {
  let calls = 0;
  const writer = new SpendBatchWriter({
    logger,
    persistEntries: async () => {
      calls++;
      throw new PostgresError("commit_unknown");
    },
  });
  try {
    writer.increment("key", 1);
    await writer.flush();
    await assert.rejects(writer.closeNative(), /unsettled cost/);
    assert.equal(calls, 1);
  } finally {
    writer.resetForTests();
  }
});

test("ordinary stop retains the existing restart behavior", async (t) => {
  t.mock.timers.enable({ apis: ["setInterval"] });
  let calls = 0;
  const writer = new SpendBatchWriter({
    logger,
    flushIntervalMs: 100,
    persistEntries: async () => {
      calls++;
    },
  });
  try {
    writer.increment("key", 1);
    await writer.stop();
    writer.increment("key", 2);
    t.mock.timers.tick(100);
    await turn();
    assert.equal(calls, 2);
  } finally {
    writer.resetForTests();
  }
});
