import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import test from "node:test";
import workerThreads, { type Worker } from "node:worker_threads";

import {
  CompressionWorkerPool,
  CompressionWorkerError,
  resolveWorkerFileAsync,
} from "../../../open-sse/services/compression/compressionWorkerPool.ts";
import {
  __resetLlmlinguaWorkerForTests,
  __setLlmlinguaWorkerHarnessForTests,
  closeNativeLlmlinguaWorker,
  depsAvailableAsync,
  workerBackend,
} from "../../../open-sse/services/compression/engines/llmlingua/worker.ts";
import { solveDeepSeekPowAsync } from "../../../open-sse/lib/deepseek-pow.ts";
import {
  LLMLINGUA_WORKER_IDLE_MS,
  LLMLINGUA_WORKER_TIMEOUT_MS,
} from "../../../open-sse/services/compression/engines/llmlingua/constants.ts";
import { markServerReady, markServerStarting } from "../../../src/lib/serverLifecycle.ts";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function until(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (predicate()) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  assert.fail("controlled operation did not reach its expected state");
}

class TestWorker extends EventEmitter {
  messages: { id: number; text?: string; body?: Record<string, unknown> }[] = [];
  terminations = 0;
  postMessage(message: (typeof this.messages)[number]): void {
    this.messages.push(message);
  }
  terminate(): Promise<number> {
    this.terminations++;
    return Promise.resolve(0);
  }
  asWorker(): Worker {
    return this as unknown as Worker;
  }
}

function forbidSyncMetadata(): () => number {
  let count = 0;
  for (const name of ["existsSync", "statSync", "readFileSync"] as const) {
    const original = fs[name];
    test.mock.method(fs, name, (...args: unknown[]) => {
      count++;
      return Reflect.apply(original, fs, args);
    });
  }
  syncBuiltinESMExports();
  return () => count;
}

const previousBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
const pools: CompressionWorkerPool[] = [];
test.beforeEach(() => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  markServerReady();
  Reflect.deleteProperty(globalThis, Symbol.for("omniroute.nativeDeepSeekPowCapacity"));
  __resetLlmlinguaWorkerForTests();
});
test.afterEach(async () => {
  await closeNativeLlmlinguaWorker();
  await Promise.all(pools.splice(0).map((pool) => pool.close()));
  __resetLlmlinguaWorkerForTests();
  test.mock.restoreAll();
  syncBuiltinESMExports();
  markServerStarting();
  if (previousBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
  else process.env.OMNIROUTE_DATABASE_BACKEND = previousBackend;
});

test("native generic resolver awaits metadata and retains dev placeholder selection", async () => {
  const gate = deferred<void>();
  let probes = 0;
  test.mock.method(fsPromises, "access", async (candidate: unknown) => {
    probes++;
    await gate.promise;
    const file = String(candidate);
    if (file === path.join(process.cwd(), "open-sse/services/compression/package.json")) {
      throw Object.assign(new Error("missing"), { code: "ENOENT" });
    }
  });
  const syncCount = forbidSyncMetadata();
  let settled = false;
  const pending = resolveWorkerFileAsync().then((value) => {
    settled = true;
    return value;
  });
  await until(() => probes === 1);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(settled, false);
  gate.resolve();
  assert.equal(
    await pending,
    path.join(process.cwd(), "open-sse/services/compression/compressionWorker.ts")
  );
  assert.equal(syncCount(), 0);
});

test("native generic pool reserves pending creation and serves FIFO within capacity", async () => {
  const creations = [deferred<Worker>(), deferred<Worker>()];
  const workers = [new TestWorker(), new TestWorker()];
  let spawns = 0;
  const pool = new CompressionWorkerPool({
    size: 2,
    workerFactory: () => creations[spawns++].promise,
  });
  pools.push(pool);
  const jobs = Array.from({ length: 6 }, (_, sequence) => pool.run({ sequence }, "stacked"));
  assert.equal(spawns, 1, "one pending factory owns the vacant slot");
  creations[0].resolve(workers[0].asWorker());
  await until(() => spawns === 2);
  assert.equal(workers[0].messages[0].body?.sequence, 0);
  creations[1].resolve(workers[1].asWorker());
  await until(() => workers[1].messages.length === 1);
  assert.equal(workers[1].messages[0].body?.sequence, 1);
  for (let sequence = 0; sequence < 6; sequence++) {
    const worker = workers[sequence % 2];
    await until(() => worker.messages.some((message) => message.body?.sequence === sequence));
    const message = worker.messages.find((message) => message.body?.sequence === sequence)!;
    worker.emit("message", {
      type: "result",
      id: message.id,
      result: { body: { sequence }, compressed: false, stats: null },
    });
    assert.equal((await jobs[sequence]).body.sequence, sequence);
    assert.equal(spawns, 2);
  }
});

test("native generic close joins pending preparation and cannot publish a late worker", async () => {
  const gate = deferred<Worker>();
  const late = new TestWorker();
  const pool = new CompressionWorkerPool({ size: 1, workerFactory: () => gate.promise });
  pools.push(pool);
  const pending = pool.run({ sequence: 0 }, "stacked");
  let closed = false;
  const close = pool.close().then(() => {
    closed = true;
  });
  assert.equal((await pending).compressed, false);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(closed, false);
  gate.resolve(late.asWorker());
  await close;
  assert.equal(late.messages.length, 0);
  assert.equal(late.terminations, 1);
  assert.equal((await pool.run({ sequence: 1 }, "stacked")).compressed, false);
});

test("native generic default factory cannot construct after close during metadata", async () => {
  const gate = deferred<void>();
  let probes = 0;
  let constructors = 0;
  test.mock.method(fsPromises, "access", async () => {
    probes++;
    await gate.promise;
  });
  test.mock.method(workerThreads, "Worker", function () {
    constructors++;
    return new TestWorker();
  });
  const syncCount = forbidSyncMetadata();
  const pool = new CompressionWorkerPool({ size: 2 });
  pools.push(pool);
  const first = pool.run({ sequence: 0 }, "stacked");
  const second = pool.run({ sequence: 1 }, "stacked");
  await until(() => probes === 1);
  assert.equal(constructors, 0);
  const close = pool.close();
  assert.equal((await first).compressed, false);
  assert.equal((await second).compressed, false);
  gate.resolve();
  await close;
  assert.equal(constructors, 0);
  assert.equal(syncCount(), 0);
});

test("native generic idle eviction and timeout retain termination and CPU retry classification", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const workers = [new TestWorker(), new TestWorker()];
  let constructors = 0;
  const pool = new CompressionWorkerPool({
    size: 1,
    idleMs: 100,
    timeoutMs: 50,
    workerFactory: async () => workers[constructors++].asWorker(),
  });
  pools.push(pool);
  const first = pool.run({ sequence: 0 }, "stacked");
  await until(() => workers[0].messages.length === 1);
  workers[0].emit("message", {
    type: "result",
    id: workers[0].messages[0].id,
    result: { body: { sequence: 0 }, compressed: false, stats: null },
  });
  await first;
  t.mock.timers.tick(100);
  assert.equal(workers[0].terminations, 1);
  const second = pool.run({ sequence: 1 }, "stacked");
  const failure = second.catch((error: unknown) => error);
  await until(() => workers[1].messages.length === 1);
  t.mock.timers.tick(50);
  const error = await failure;
  assert.ok(error instanceof CompressionWorkerError);
  assert.equal(error.retryInProcess, false);
  assert.equal(workers[1].terminations, 1);
});

test("native generic asynchronous spawn failure is retryable on a later wave", async () => {
  const ready = new TestWorker();
  let spawns = 0;
  const pool = new CompressionWorkerPool({
    size: 1,
    workerFactory: async () => {
      if (++spawns === 1) throw Object.assign(new Error("resource exhaustion"), { code: "EMFILE" });
      return ready.asWorker();
    },
  });
  pools.push(pool);
  await assert.rejects(
    pool.run({ sequence: 0 }, "stacked"),
    (error: unknown) => error instanceof CompressionWorkerError && error.retryInProcess
  );
  const second = pool.run({ sequence: 1 }, "stacked");
  await until(() => ready.messages.length === 1);
  ready.emit("message", {
    type: "result",
    id: ready.messages[0].id,
    result: { body: { sequence: 1 }, compressed: false, stats: null },
  });
  await second;
  assert.equal(spawns, 2);
});

test("native LLMLingua dependency preparation is shared and asynchronous", async () => {
  const gate = deferred<void>();
  let probes = 0;
  test.mock.method(fsPromises, "access", async () => {
    probes++;
    await gate.promise;
  });
  __setLlmlinguaWorkerHarnessForTests({ factory: null, depsAvailable: null });
  const syncCount = forbidSyncMetadata();
  const first = depsAvailableAsync();
  const second = depsAvailableAsync();
  assert.equal(first, second);
  await until(() => probes === 1);
  let ticker = false;
  await new Promise<void>((resolve) =>
    setImmediate(() => {
      ticker = true;
      resolve();
    })
  );
  assert.equal(ticker, true);
  gate.resolve();
  assert.equal(await first, true);
  assert.equal(await depsAvailableAsync(), true);
  assert.equal(probes, 1);
  assert.equal(syncCount(), 0);
});

test("native LLMLingua pending metadata preserves FIFO and one constructor", async () => {
  const gate = deferred<void>();
  let probes = 0;
  const ready = new TestWorker();
  let spawns = 0;
  test.mock.method(fsPromises, "access", async () => {
    probes++;
    await gate.promise;
  });
  __setLlmlinguaWorkerHarnessForTests({
    depsAvailable: true,
    factory: () => {
      spawns++;
      return ready.asWorker();
    },
  });
  const syncCount = forbidSyncMetadata();
  const first = workerBackend("first");
  const second = workerBackend("second");
  await until(() => probes > 0);
  assert.equal(spawns, 0);
  gate.resolve();
  await until(() => ready.messages.length === 1);
  assert.equal(ready.messages[0].text, "first");
  ready.emit("message", { id: ready.messages[0].id, ok: true, text: "one" });
  assert.equal(await first, "one");
  await until(() => ready.messages.length === 2);
  assert.equal(ready.messages[1].text, "second");
  ready.emit("message", { id: ready.messages[1].id, ok: true, text: "two" });
  assert.equal(await second, "two");
  assert.equal(spawns, 1);
  assert.equal(syncCount(), 0);
});

test("native LLMLingua close drains accepted jobs and excludes late construction", async () => {
  const gate = deferred<void>();
  let probes = 0;
  let spawns = 0;
  test.mock.method(fsPromises, "access", async () => {
    probes++;
    await gate.promise;
  });
  __setLlmlinguaWorkerHarnessForTests({
    depsAvailable: true,
    factory: () => {
      spawns++;
      return new TestWorker().asWorker();
    },
  });
  const first = workerBackend("first");
  const second = workerBackend("second");
  await until(() => probes > 0);
  let closed = false;
  const close = closeNativeLlmlinguaWorker().then(() => {
    closed = true;
  });
  assert.deepEqual(await Promise.all([first, second]), ["first", "second"]);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(closed, false);
  gate.resolve();
  await close;
  assert.equal(spawns, 0);
  assert.equal(await workerBackend("after"), "after");
});

test("native LLMLingua missing dependency stays cached and never spawns", async () => {
  let probes = 0;
  let constructors = 0;
  test.mock.method(fsPromises, "access", async () => {
    probes++;
    throw Object.assign(new Error("absent"), { code: "ENOENT" });
  });
  test.mock.method(fsPromises, "stat", async () => {
    probes++;
    throw Object.assign(new Error("absent"), { code: "ENOENT" });
  });
  __setLlmlinguaWorkerHarnessForTests({
    depsAvailable: null,
    factory: () => {
      constructors++;
      return new TestWorker().asWorker();
    },
  });
  const syncCount = forbidSyncMetadata();
  assert.equal(await workerBackend("first"), "first");
  const before = probes;
  assert.ok(before > 0);
  assert.equal(await workerBackend("second"), "second");
  assert.equal(probes, before);
  assert.equal(constructors, 0);
  assert.equal(syncCount(), 0);
});

test("native LLMLingua warm timeout and idle eviction keep source budgets", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const workers = [new TestWorker(), new TestWorker()];
  let constructors = 0;
  test.mock.method(fsPromises, "access", async () => undefined);
  __setLlmlinguaWorkerHarnessForTests({
    depsAvailable: true,
    factory: () => workers[constructors++].asWorker(),
  });
  const first = workerBackend("first");
  await until(() => workers[0].messages.length === 1);
  workers[0].emit("message", { id: workers[0].messages[0].id, ok: true, text: "one" });
  assert.equal(await first, "one");
  const second = workerBackend("second");
  await until(() => workers[0].messages.length === 2);
  t.mock.timers.tick(LLMLINGUA_WORKER_TIMEOUT_MS);
  assert.equal(await second, "second");
  assert.equal(workers[0].terminations, 0, "timeout preserves the loading worker");
  t.mock.timers.tick(LLMLINGUA_WORKER_IDLE_MS);
  assert.equal(workers[0].terminations, 1);
  const third = workerBackend("third");
  await until(() => workers[1].messages.length === 1);
  let settled = false;
  void third.then(() => {
    settled = true;
  });
  t.mock.timers.tick(LLMLINGUA_WORKER_TIMEOUT_MS);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(settled, false, "idle eviction clears model warmth");
  t.mock.timers.tick(60_000 - LLMLINGUA_WORKER_TIMEOUT_MS);
  assert.equal(await third, "third");
  assert.equal(constructors, 2);
});

test("native LLMLingua error fails open then respawns without stale reply release", async () => {
  const workers = [new TestWorker(), new TestWorker()];
  let spawns = 0;
  test.mock.method(fsPromises, "access", async () => undefined);
  __setLlmlinguaWorkerHarnessForTests({
    depsAvailable: true,
    factory: () => workers[spawns++].asWorker(),
  });
  const first = workerBackend("first");
  const second = workerBackend("second");
  await until(() => workers[0].messages.length === 1);
  workers[0].emit("error", new Error("worker fixture fault"));
  assert.equal(await first, "first");
  await until(() => workers[1].messages.length === 1);
  let secondSettled = false;
  void second.then(() => {
    secondSettled = true;
  });
  workers[0].emit("message", { id: workers[0].messages[0].id, ok: true, text: "late" });
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(secondSettled, false);
  workers[1].emit("message", { id: workers[1].messages[0].id, ok: true, text: "two" });
  assert.equal(await second, "two");
  assert.equal(spawns, 2);
  assert.equal(workers[0].terminations, 1);
});

test("native DeepSeek reserves capacity before async metadata and abort releases it", async () => {
  const gate = deferred<void>();
  let probes = 0;
  const workers: TestWorker[] = [];
  test.mock.method(fsPromises, "access", async () => {
    probes++;
    await gate.promise;
  });
  test.mock.method(workerThreads, "Worker", function () {
    const worker = new TestWorker();
    workers.push(worker);
    return worker;
  });
  const syncCount = forbidSyncMetadata();
  const controller = new AbortController();
  const solve = (signal?: AbortSignal) =>
    solveDeepSeekPowAsync("DeepSeekHashV1", "a".repeat(64), "fixture", 1, Date.now() + 60_000, {
      signal,
    });
  const first = solve(controller.signal);
  const firstOutcome = first.catch((error: unknown) => error);
  const second = solve();
  await assert.rejects(solve(), /capacity reached/);
  assert.equal(probes, 2);
  controller.abort();
  assert.equal(((await firstOutcome) as Error).name, "AbortError");
  const third = solve();
  assert.equal(probes, 3);
  gate.resolve();
  await until(() => workers.length === 2);
  for (const worker of workers) worker.emit("message", -1);
  assert.deepEqual(await Promise.all([second, third]), [-1, -1]);
  assert.equal(syncCount(), 0);
});

test("native DeepSeek missing metadata does not leak a concurrency slot", async () => {
  test.mock.method(fsPromises, "access", async () => {
    throw Object.assign(new Error("missing"), { code: "ENOENT" });
  });
  const syncCount = forbidSyncMetadata();
  for (let attempt = 0; attempt < 3; attempt++) {
    await assert.rejects(
      solveDeepSeekPowAsync("DeepSeekHashV1", "a".repeat(64), "fixture", 1, Date.now() + 60_000),
      /worker script not found/
    );
  }
  assert.equal(syncCount(), 0);
});
