import assert from "node:assert/strict";
import { AsyncLocalStorage, AsyncResource } from "node:async_hooks";
import test from "node:test";
import {
  PendingContinuationBridge,
  type PendingContinuationBridgeOptions,
  type PendingContinuationState,
} from "../../../src/lib/db/pendingContinuationBridge.ts";

function payload(text = "original"): PendingContinuationState {
  return { input: [{ content: text }], output: [{ content: "reply" }] };
}

function clock(options: PendingContinuationBridgeOptions["limits"] = {}) {
  let now = 0;
  let callback: (() => void) | undefined;
  let resource: AsyncResource | undefined;
  let starts = 0;
  let stops = 0;
  let unrefs = 0;
  const bridge = new PendingContinuationBridge({
    limits: { ttlMs: 100, sweepIntervalMs: 10, ...options },
    now: () => now,
    startSweep(sweep) {
      starts++;
      callback = sweep;
      resource = new AsyncResource("pending-continuation-test-maintenance");
      return { unref: () => unrefs++ } as ReturnType<typeof setInterval>;
    },
    stopSweep() {
      stops++;
      resource?.emitDestroy();
    },
  });
  return {
    bridge,
    advance(value: number) {
      now = value;
    },
    tick() {
      resource?.runInAsyncScope(callback!);
    },
    get timers() {
      return { starts, stops, unrefs };
    },
  };
}

test("maintenance expires an unread state at its deadline and releases its reservation", () => {
  const time = clock();
  const { bridge, advance, tick } = time;
  bridge.initialize();
  bridge.initialize();
  assert.ok(bridge.admit("unread", "key", {}, payload()));
  assert.ok(bridge.stats().bytes > 0);
  advance(100);
  tick();
  assert.deepEqual(bridge.stats(), {
    entries: 0,
    bytes: 0,
    refused: 0,
    expired: 1,
    maintenanceRunning: true,
  });
  bridge.stop();
  assert.deepEqual(time.timers, { starts: 1, stops: 1, unrefs: 1 });
});

test("request admission never creates timers or transfers request ALS to startup maintenance", () => {
  const context = new AsyncLocalStorage<{ payload: string }>();
  let maintenanceContext: unknown = "not observed";
  let now = 0;
  let sweep!: () => void;
  let owner!: AsyncResource;
  let starts = 0;
  const bridge = new PendingContinuationBridge({
    limits: { ttlMs: 100 },
    now: () => {
      maintenanceContext = context.getStore();
      return now;
    },
    startSweep(callback) {
      starts++;
      sweep = callback;
      owner = new AsyncResource("neutral-continuation-maintenance");
      return { unref() {} } as ReturnType<typeof setInterval>;
    },
    stopSweep() {
      owner.emitDestroy();
    },
  });
  bridge.initialize();
  context.run({ payload: "request graph" }, () => {
    assert.ok(bridge.admit("request", "key", {}, payload()));
    assert.equal(starts, 1);
  });
  now = 100;
  owner.runInAsyncScope(sweep);
  assert.equal(maintenanceContext, undefined);
  assert.equal(bridge.stats().entries, 0);
  bridge.stop();
});

test("reads do not renew TTL and a delayed old cancellation cannot remove a replacement", () => {
  const { bridge, advance, tick } = clock();
  const authority = {};
  bridge.initialize();
  const old = bridge.admit("same", "key", authority, payload("old"))!;
  advance(50);
  const replacement = bridge.admit("same", "key", authority, payload("new"))!;
  bridge.clearOwned(old);
  advance(100);
  tick();
  assert.deepEqual(bridge.read("same", "key", authority), payload("new"));
  advance(149);
  assert.ok(bridge.read("same", "key", authority));
  advance(150);
  tick();
  assert.equal(bridge.read("same", "key", authority), null);
  bridge.clearOwned(replacement);
  assert.equal(bridge.stats().bytes, 0);
  bridge.stop();
});

test("expired standalone states are reclaimed on unrelated admission without starting a timer", () => {
  const { bridge, advance, timers } = clock({ maxEntries: 1 });
  assert.ok(bridge.admit("old", "key", undefined, payload()));
  advance(100);
  assert.ok(bridge.admit("new", "key", {}, payload()));
  assert.equal(bridge.stats().entries, 1);
  assert.equal(bridge.stats().expired, 1);
  assert.equal(timers.starts, 0);
  bridge.stop();
});

test("aggregate byte admission is independent of count and refunds exact replacement/removal bytes", () => {
  const sample = new PendingContinuationBridge();
  sample.admit("first", "key", undefined, payload());
  const oneEntry = sample.stats().bytes;
  sample.stop();
  const bridge = new PendingContinuationBridge({ limits: { maxBytes: oneEntry * 2 - 1 } });
  const first = bridge.admit("first", "key", undefined, payload())!;
  assert.ok(first);
  assert.equal(bridge.admit("other", "key", {}, payload()), undefined);
  assert.equal(bridge.stats().entries, 1);
  assert.equal(bridge.stats().refused, 1);
  const replacement = bridge.admit("first", "key", undefined, payload())!;
  assert.ok(replacement);
  assert.equal(bridge.stats().bytes, oneEntry);
  bridge.clearOwned(first);
  assert.equal(bridge.stats().bytes, oneEntry);
  bridge.clearOwned(replacement);
  assert.equal(bridge.stats().bytes, 0);
  bridge.stop();
});

test("oversize replacement fails closed before cloning rather than exposing an old transcript", (t) => {
  const bridge = new PendingContinuationBridge({ limits: { maxEntryBytes: 2048 } });
  const original = bridge.admit("same", "key", undefined, payload());
  assert.ok(original);
  let copies = 0;
  const copy = globalThis.structuredClone;
  t.mock.method(globalThis, "structuredClone", (...args: Parameters<typeof structuredClone>) => {
    copies++;
    return Reflect.apply(copy, globalThis, args);
  });
  assert.equal(bridge.admit("same", "key", undefined, payload("x".repeat(4096))), undefined);
  assert.equal(copies, 0);
  assert.equal(bridge.read("same", "key", undefined), null);
  assert.equal(bridge.stats().bytes, 0);
  bridge.stop();
});

test("admission rejects accessors, proxies, exotic graphs and sparse expansion before copying", (t) => {
  const bridge = new PendingContinuationBridge();
  let getterCalls = 0;
  let proxyCalls = 0;
  let copies = 0;
  t.mock.method(globalThis, "structuredClone", () => {
    copies++;
    throw new Error("Rejected input must not be cloned");
  });
  const accessor = {
    get content() {
      getterCalls++;
      return "secret";
    },
  };
  const proxy = new Proxy(
    {},
    {
      getPrototypeOf() {
        proxyCalls++;
        return Object.prototype;
      },
    }
  );
  const deep: Record<string, unknown> = {};
  let node = deep;
  for (let index = 0; index < 100; index++) {
    const child: Record<string, unknown> = {};
    node.next = child;
    node = child;
  }
  for (const value of [accessor, proxy, new Uint8Array(16), new Array(1_000_000_000), deep]) {
    assert.equal(
      bridge.admit("invalid", "key", undefined, { input: [value], output: ["ok"] }),
      undefined
    );
  }
  assert.equal(getterCalls, 0);
  assert.equal(proxyCalls, 0);
  assert.equal(copies, 0);
  assert.equal(bridge.stats().refused, 5);
  bridge.stop();
});

test("caller and reader mutation cannot change the admitted graph or its retained-byte accounting", () => {
  const bridge = new PendingContinuationBridge();
  const state = payload();
  bridge.admit("owned", "key", undefined, state);
  const reserved = bridge.stats().bytes;
  (state.input[0] as Record<string, unknown>).content = "x".repeat(4096);
  const read = bridge.read("owned", "key", undefined)!;
  (read.output[0] as Record<string, unknown>).content = "y".repeat(4096);
  assert.deepEqual(bridge.read("owned", "key", undefined), payload());
  assert.equal(bridge.stats().bytes, reserved);
  bridge.stop();
});

test("shutdown removes payloads and a stale sweep callback cannot expire a newer deadline", () => {
  const { bridge, advance, tick } = clock();
  const authority = {};
  bridge.initialize();
  bridge.admit("same", "key", authority, payload("old"));
  bridge.stop();
  assert.equal(bridge.stats().bytes, 0);
  advance(100);
  bridge.admit("same", "key", authority, payload("new"));
  tick();
  assert.deepEqual(bridge.read("same", "key", authority), payload("new"));
  bridge.stop();
});

test("the real maintenance timer is unreferenced and shutdown releases it", () => {
  let timer: ReturnType<typeof setInterval> | undefined;
  const bridge = new PendingContinuationBridge({
    startSweep(callback, intervalMs) {
      timer = setInterval(callback, intervalMs);
      return timer;
    },
  });
  bridge.initialize();
  assert.equal(timer!.hasRef(), false);
  bridge.stop();
  assert.equal(bridge.stats().maintenanceRunning, false);
});

test("the real timer releases expired graphs without another admission or lookup", async () => {
  const bridge = new PendingContinuationBridge({ limits: { ttlMs: 20, sweepIntervalMs: 5 } });
  bridge.initialize();
  try {
    assert.ok(bridge.admit("timer-owned", "key", {}, payload()));
    assert.equal(bridge.stats().entries, 1);
    const deadline = Date.now() + 2_000;
    while (bridge.stats().entries && Date.now() < deadline)
      await new Promise<void>((done) => setTimeout(done, 10));
    assert.equal(bridge.stats().entries, 0);
    assert.equal(bridge.stats().bytes, 0);
    assert.equal(bridge.stats().expired, 1);
  } finally {
    bridge.stop();
  }
});
