import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs";
import asyncFs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { setImmediate as nextTurn } from "node:timers/promises";

const require = createRequire(import.meta.url);
const { createAsyncStatsWriter } = require("../../../src/mitm/_internal/asyncStatsWriter.cjs");

function barrier() {
  let resolve!: () => void;
  return {
    promise: new Promise<void>((done) => {
      resolve = done;
    }),
    resolve: () => resolve(),
  };
}

test("native stats coalesce a burst, serialize physical writes and drain its final snapshot", async () => {
  const entered = barrier();
  const release = barrier();
  const snapshots: number[] = [];
  let concurrent = 0;
  let peak = 0;
  const writer = createAsyncStatsWriter("unused", {
    async writeSnapshot(snapshot: string) {
      concurrent++;
      peak = Math.max(peak, concurrent);
      if (!snapshots.length) {
        entered.resolve();
        await release.promise;
      }
      snapshots.push(JSON.parse(snapshot).requests);
      concurrent--;
    },
  });
  const mutable = { requests: 1 };
  writer.publish(mutable);
  await entered.promise;
  mutable.requests = 999; // A published snapshot must not change underneath the writer.
  for (let requests = 2; requests <= 100; requests++) writer.publish({ requests });
  let closed = false;
  const closing = writer.close({ requests: 101 }).then(() => {
    closed = true;
  });
  await nextTurn();
  assert.equal(closed, false);
  writer.publish({ requests: 102 }); // No new publications enter after close.
  release.resolve();
  await closing;
  assert.deepEqual(snapshots, [1, 101]);
  assert.equal(peak, 1);
  await writer.close({ requests: 103 });
  assert.deepEqual(snapshots, [1, 101]);
});

test("native stats actual filesystem path never uses synchronous writes and publishes final JSON", async (context) => {
  const scratch = await asyncFs.mkdtemp(path.join(os.tmpdir(), "native-mitm-stats-"));
  const file = path.join(scratch, "stats.json");
  const blocked = barrier();
  const entered = barrier();
  const originalWrite = asyncFs.writeFile;
  let writes = 0;
  context.mock.method(asyncFs, "writeFile", async (...args: Parameters<typeof originalWrite>) => {
    writes++;
    if (writes === 1) {
      entered.resolve();
      await blocked.promise;
    }
    return originalWrite(...args);
  });
  const sync = context.mock.method(fs, "writeFileSync", () => {
    throw new Error("Synchronous stats write is forbidden");
  });
  const writer = createAsyncStatsWriter(file);
  try {
    writer.publish({ requests: 1 });
    await entered.promise;
    for (let requests = 2; requests <= 20; requests++) writer.publish({ requests });
    const close = writer.close({ requests: 21 });
    let advanced = false;
    await nextTurn().then(() => {
      advanced = true;
    });
    assert.equal(advanced, true);
    blocked.resolve();
    await close;
    assert.deepEqual(JSON.parse(await asyncFs.readFile(file, "utf8")), { requests: 21 });
    assert.equal(sync.mock.callCount(), 0);
    assert.equal(writes, 2);
    assert.deepEqual(await asyncFs.readdir(scratch), ["stats.json"]);
  } finally {
    blocked.resolve();
    await writer.close();
    context.mock.restoreAll();
    await asyncFs.rm(scratch, { recursive: true, force: true });
  }
});

test("a failed stats write does not poison the next publication", async () => {
  const entered = barrier();
  const release = barrier();
  const snapshots: number[] = [];
  let calls = 0;
  const writer = createAsyncStatsWriter("unused", {
    async writeSnapshot(snapshot: string) {
      if (++calls === 1) {
        entered.resolve();
        await release.promise;
        throw new Error("Filesystem unavailable");
      }
      snapshots.push(JSON.parse(snapshot).requests);
    },
  });
  writer.publish({ requests: 1 });
  await entered.promise;
  writer.publish({ requests: 2 });
  release.resolve();
  await writer.close();
  assert.deepEqual(snapshots, [2]);
  assert.equal(calls, 2);
});
