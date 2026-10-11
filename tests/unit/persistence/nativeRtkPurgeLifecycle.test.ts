import assert from "node:assert/strict";
import files from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { setImmediate as yieldToEventLoop } from "node:timers/promises";
import {
  scheduleRtkRawOutputPurge,
  stopRtkRawOutputPurgeScheduler,
  closeRtkRawOutputPurgeScheduler,
  resetRtkRawOutputPurgeThrottle,
  purgeRtkRawOutput,
} from "../../../open-sse/services/compression/engines/rtk/rawOutput";

const key = Symbol.for("omniroute.rtkRawOutputPurge");
const at = Symbol.for("omniroute.rtkRawOutputPurgeAt");
function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
async function isolated(t: test.TestContext, run: (directory: string) => Promise<void>) {
  const descriptors = [key, at].map((symbol) =>
    Object.getOwnPropertyDescriptor(globalThis, symbol)
  );
  for (const symbol of [key, at]) Reflect.deleteProperty(globalThis, symbol);
  const oldData = process.env.DATA_DIR;
  const oldBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
  const directory = await files.mkdtemp(path.join(os.tmpdir(), "native-rtk-purge-"));
  process.env.DATA_DIR = directory;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  try {
    await run(directory);
  } finally {
    await closeRtkRawOutputPurgeScheduler();
    t.mock.restoreAll();
    resetRtkRawOutputPurgeThrottle();
    if (oldData === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = oldData;
    if (oldBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = oldBackend;
    await files.rm(directory, { recursive: true, force: true });
    [key, at].forEach((symbol, index) => {
      if (descriptors[index]) Object.defineProperty(globalThis, symbol, descriptors[index]!);
      else Reflect.deleteProperty(globalThis, symbol);
    });
  }
}

test("cold close records closed admission without allocating jobs, probing files or forcing a purge", async (t) =>
  isolated(t, async () => {
    let calls = 0;
    const access = files.access;
    t.mock.method(files, "access", async (...args: Parameters<typeof files.access>) => {
      if (String(args[0]).includes(path.join("rtk", "raw-output"))) calls++;
      return Reflect.apply(access, files, args);
    });
    await closeRtkRawOutputPurgeScheduler();
    scheduleRtkRawOutputPurge();
    await yieldToEventLoop();
    assert.equal(Reflect.get(globalThis, key), false);
    assert.equal(calls, 0);
  }));

test("native optional purge is owned before dispatch and stop cancels not-started work", async (t) =>
  isolated(t, async () => {
    let calls = 0;
    const access = files.access;
    t.mock.method(files, "access", async (...args: Parameters<typeof files.access>) => {
      if (String(args[0]).includes(path.join("rtk", "raw-output"))) calls++;
      return Reflect.apply(access, files, args);
    });
    scheduleRtkRawOutputPurge();
    scheduleRtkRawOutputPurge();
    const owner = Reflect.get(globalThis, key) as { accepted: Set<Promise<void>> };
    assert.equal(owner.accepted.size, 1);
    stopRtkRawOutputPurgeScheduler();
    await closeRtkRawOutputPurgeScheduler();
    await yieldToEventLoop();
    assert.equal(owner.accepted.size, 0);
    assert.equal(calls, 0);
  }));

test("native final close joins the actual running pass with its captured path and options", async (t) =>
  isolated(t, async (directory) => {
    const root = path.join(directory, "rtk", "raw-output");
    await files.mkdir(root, { recursive: true });
    const filename = path.join(root, "1-tool-output-ab.log");
    await files.writeFile(filename, "old");
    const entered = barrier();
    const release = barrier();
    const read = files.readdir;
    let seenPath = "";
    t.mock.method(files, "readdir", async (...args: Parameters<typeof files.readdir>) => {
      seenPath = String(args[0]);
      entered.resolve();
      await release.promise;
      return Reflect.apply(read, files, args);
    });
    scheduleRtkRawOutputPurge({ maxAgeDays: 1 });
    process.env.DATA_DIR = path.join(directory, "changed");
    try {
      await entered.promise;
      assert.equal(seenPath, root);
      let closed = false;
      const closing = closeRtkRawOutputPurgeScheduler().then(() => {
        closed = true;
      });
      await yieldToEventLoop();
      assert.equal(closed, false);
      release.resolve();
      await closing;
      assert.equal(closed, true);
      await assert.rejects(files.access(filename));
      assert.equal(
        (Reflect.get(globalThis, key) as { accepted: Set<Promise<void>> }).accepted.size,
        0
      );
    } finally {
      release.resolve();
    }
  }));

for (const scenario of [
  { name: "all aged", offsets: [3, 4, 5], expected: [] },
  { name: "none aged", offsets: [0, 0, 0], expected: ["b", "c"] },
  { name: "mixed", offsets: [3, 0, 0], expected: ["b", "c"] },
]) {
  test(`native purge preserves age/count policy and sidecars: ${scenario.name}`, async (t) =>
    isolated(t, async (directory) => {
      const now = 1700000000000;
      t.mock.method(Date, "now", () => now);
      const root = path.join(directory, "rtk", "raw-output");
      await files.mkdir(root, { recursive: true });
      const names: string[] = [];
      for (const [index, offset] of scenario.offsets.entries()) {
        const name = `${now - offset * 86400000}-${String.fromCharCode(97 + index)}.log`;
        names.push(name);
        await files.writeFile(path.join(root, name), `payload-${index}`);
        await files.writeFile(path.join(root, name.replace(/\.log$/, ".meta.json")), "{}");
      }
      const result = await purgeRtkRawOutput({ maxAgeDays: 1, maxFiles: 2 });
      assert.deepEqual(result, {
        skipped: false,
        scanned: 3,
        deleted: 3 - scenario.expected.length,
        errors: 0,
      });
      const remaining = (await files.readdir(root)).sort();
      const expected = names
        .filter((name) => scenario.expected.some((slug) => name.endsWith(`-${slug}.log`)))
        .flatMap((name) => [name, name.replace(/\.log$/, ".meta.json")])
        .sort();
      assert.deepEqual(remaining, expected);
    }));
}
