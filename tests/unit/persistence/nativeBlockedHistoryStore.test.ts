import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setImmediate as yieldToEventLoop } from "node:timers/promises";
import { NativeBlockedHistoryStore } from "../../../src/lib/proxyHealth/nativeBlockedHistoryStore";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext";
import { UnsupportedPersistenceDomainError } from "../../../src/lib/db/controlPlaneContext";
import {
  recordBlockedObservationAsync,
  getBlockedHistoriesAsync,
  closeBlockedHistoryAsync,
} from "../../../src/lib/proxyHealth/blockedHistory";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function binding(identity: object): PostgresControlPlaneBinding {
  return {
    databaseIdentity: identity,
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
}

test("one async load retains source counts/order and does not block the event loop", async () => {
  const entered = barrier();
  const release = barrier();
  let reads = 0;
  const files = new Map<string, string>();
  const store = new NativeBlockedHistoryStore("/fixture/history.json", () => () => {}, {
    async read() {
      reads++;
      entered.resolve();
      await release.promise;
      return JSON.stringify({
        p: {
          count: 2,
          firstSeen: 10,
          lastSeen: 20,
          lastCause: "unproven",
          lastStatus: 403,
        },
      });
    },
    async mkdir() {},
    async write(filename, text) {
      files.set(filename, text);
    },
    async rename(from, to) {
      files.set(to, files.get(from)!);
      files.delete(from);
    },
    async remove(filename) {
      files.delete(filename);
    },
  });
  const first = store.record("p", "target_refused", 401, 30);
  const second = store.record("q", "unclassified", null, 31);
  await entered.promise;
  await yieldToEventLoop();
  assert.equal(reads, 1);
  release.resolve();
  await Promise.all([first, second]);
  const result = await store.read(["p", "q"]);
  assert.deepEqual(result.p, {
    count: 3,
    firstSeen: 10,
    lastSeen: 30,
    lastCause: "target_refused",
    lastStatus: 401,
  });
  await store.close();
  assert.equal(JSON.parse(files.get("/fixture/history.json")!).q.count, 1);
});

test("mutation during a write persists a second revision without overlapping writes", async () => {
  const entered = barrier();
  const release = barrier();
  let writes = 0;
  let active = 0;
  let maximum = 0;
  const files = new Map<string, string>();
  const store = new NativeBlockedHistoryStore("/fixture/history.json", () => () => {}, {
    async read() {
      throw new Error("missing");
    },
    async mkdir() {},
    async write(filename, content) {
      active++;
      maximum = Math.max(maximum, active);
      writes++;
      files.set(filename, content);
      if (writes === 1) {
        entered.resolve();
        await release.promise;
      }
      active--;
    },
    async rename(from, to) {
      files.set(to, files.get(from)!);
      files.delete(from);
    },
    async remove(filename) {
      files.delete(filename);
    },
  });
  await store.record("p", "unproven", 403, 1);
  const flushing = store.flush();
  await entered.promise;
  // A slow disk does not prevent a newly accepted observation from updating memory.
  await store.record("p", "unproven", 403, 2);
  assert.equal((await store.read(["p"])).p.count, 2);
  const joining = store.flush();
  release.resolve();
  await Promise.all([flushing, joining]);
  assert.equal(writes, 2);
  assert.equal(maximum, 1);
  assert.equal(JSON.parse(files.get("/fixture/history.json")!).p.count, 2);
  await store.close();
  await assert.rejects(store.record("late", "unproven", 403, 3), /closed/);
});

test("a failed rename does not acknowledge the revision and a flush retries it", async () => {
  const files = new Map<string, string>();
  let fail = true;
  const store = new NativeBlockedHistoryStore("/fixture/history.json", () => () => {}, {
    async read() {
      throw new Error("missing");
    },
    async mkdir() {},
    async write(filename, content) {
      files.set(filename, content);
    },
    async rename(from, to) {
      if (fail) {
        fail = false;
        throw new Error("unavailable");
      }
      files.set(to, files.get(from)!);
      files.delete(from);
    },
    async remove(filename) {
      files.delete(filename);
    },
  });
  await store.record("p", "unproven", 403, 1);
  await assert.rejects(store.flush(), /unavailable/);
  assert.equal(files.has("/fixture/history.json"), false);
  await store.flush();
  assert.equal(JSON.parse(files.get("/fixture/history.json")!).p.count, 1);
  await store.close();
});

test("native authority rejects a foreign plane without allocating the application runtime/store", async () => {
  const original = Object.getOwnPropertyDescriptor(globalThis, "__omniroutePersistenceRuntime");
  delete globalThis.__omniroutePersistenceRuntime;
  try {
    await withPostgresControlPlane(binding({}), async () => {
      await assert.rejects(
        recordBlockedObservationAsync("foreign", "unproven", 403, 1),
        UnsupportedPersistenceDomainError
      );
    });
    assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
    assert.equal(Reflect.get(globalThis, Symbol.for("omniroute.nativeBlockedHistory")), undefined);
  } finally {
    if (original) Object.defineProperty(globalThis, "__omniroutePersistenceRuntime", original);
  }
});

test("final close persists a late accepted observation and retains source recovery/prune semantics", async () => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "omniroute-native-history-"));
  const originalDataDir = process.env.DATA_DIR;
  const originalRuntime = Object.getOwnPropertyDescriptor(
    globalThis,
    "__omniroutePersistenceRuntime"
  );
  const identity = {};
  process.env.DATA_DIR = directory;
  Object.defineProperty(globalThis, "__omniroutePersistenceRuntime", {
    configurable: true,
    writable: true,
    value: { backend: "postgres", applicationIdentity: identity },
  });
  try {
    await withPostgresControlPlane(binding(identity), async () => {
      await recordBlockedObservationAsync("p", "unproven", 403, 1);
      // This represents a request admitted before producer shutdown; the sink remains open.
      await recordBlockedObservationAsync("p", "target_refused", 429, 2);
      assert.equal((await getBlockedHistoriesAsync(["p"])).p.count, 2);
      await closeBlockedHistoryAsync();
    });
    const stored = JSON.parse(
      await fs.readFile(path.join(directory, "blocked-history.json"), "utf8")
    );
    assert.equal(stored.p.firstSeen, 1);
    assert.equal(stored.p.lastSeen, 2);
    assert.equal(stored.p.lastStatus, 429);
    const restored = new NativeBlockedHistoryStore(
      path.join(directory, "blocked-history.json"),
      () => () => {}
    );
    await restored.remove("p");
    await restored.record("p", "unclassified", null, 3);
    assert.equal((await restored.read(["p"])).p.firstSeen, 3);
    await restored.record("stale", "unproven", 403, 4);
    assert.equal(await restored.prune(["p"]), 1);
    await restored.close();
  } finally {
    if (originalDataDir === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = originalDataDir;
    if (originalRuntime)
      Object.defineProperty(globalThis, "__omniroutePersistenceRuntime", originalRuntime);
    else delete globalThis.__omniroutePersistenceRuntime;
    await fs.rm(directory, { recursive: true, force: true });
  }
});

test("a rejected background dispatch releases the debounce slot for the next observation", async () => {
  let scheduled = 0;
  let dispatched!: () => void;
  const store = new NativeBlockedHistoryStore(
    "/fixture/history.json",
    (_flush, elapsed) => {
      scheduled++;
      dispatched = elapsed;
      return () => {};
    },
    {
      async read() {
        throw new Error("missing");
      },
      async mkdir() {},
      async write() {},
      async rename() {},
      async remove() {},
    }
  );
  await store.record("p", "unproven", 403, 1);
  assert.equal(scheduled, 1);
  // Admission can fail before flush begins; a later observation must still schedule persistence.
  dispatched();
  await store.record("p", "unproven", 403, 2);
  assert.equal(scheduled, 2);
  await store.close();
});

test("native history retains the source 5000-entry cap and touched-entry eviction order", async () => {
  const store = new NativeBlockedHistoryStore("/fixture/history.json", () => () => {}, {
    async read() {
      throw new Error("missing");
    },
    async mkdir() {},
    async write() {},
    async rename() {},
    async remove() {},
  });
  for (let i = 0; i <= 5000; i++) await store.record(`p${i}`, "unproven", 403, i);
  assert.equal((await store.read(["p0"])).p0, undefined);
  await store.record("p1", "unproven", 403, 6000);
  await store.record("new", "unproven", 403, 6001);
  const entries = await store.read(Array.from({ length: 5001 }, (_, i) => `p${i}`).concat("new"));
  assert.equal(Object.keys(entries).length, 5000);
  assert.equal(entries.p2, undefined);
  assert.equal(entries.p1.count, 2);
  assert.equal(entries.p1.firstSeen, 1);
  await store.close();
});
