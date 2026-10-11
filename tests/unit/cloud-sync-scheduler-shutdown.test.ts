import test from "node:test";
import assert from "node:assert/strict";
import { setImmediate as settle } from "node:timers/promises";

process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
const { CloudSyncScheduler, closeCloudSyncScheduler } =
  await import("@/shared/services/cloudSyncScheduler");

function timers(t: import("node:test").TestContext) {
  const callbacks: Array<() => void> = [];
  const cleared: unknown[] = [];
  const create = (callback: () => void) => {
    callbacks.push(callback);
    return { unref() {} };
  };
  t.mock.method(globalThis, "setInterval", create);
  t.mock.method(globalThis, "setTimeout", create);
  t.mock.method(globalThis, "clearInterval", (timer: unknown) => {
    cleared.push(timer);
  });
  t.mock.method(globalThis, "clearTimeout", (timer: unknown) => {
    cleared.push(timer);
  });
  return { callbacks, cleared };
}

test("Cloud close clears both timers and waits the complete accepted sync", async (t) => {
  const clock = timers(t);
  const scheduler = new CloudSyncScheduler("machine-test");
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let calls = 0;
  t.mock.method(scheduler, "syncWithRetry", async () => {
    calls++;
    await gate;
    return null;
  });
  t.after(async () => {
    release();
    await scheduler.close();
  });
  await Promise.all([scheduler.start(), scheduler.start()]);
  assert.equal(clock.callbacks.length, 2, "concurrent start creates only one pair of timers");
  clock.callbacks[1]();
  clock.callbacks[0]();
  await settle();
  assert.equal(calls, 1);
  let closed = false;
  const close = scheduler.close().then(() => {
    closed = true;
  });
  await settle();
  assert.equal(closed, false);
  assert.equal(scheduler.isRunning(), false);
  clock.callbacks[0]();
  clock.callbacks[1]();
  release();
  await close;
  assert.equal(calls, 1);
  await assert.rejects(scheduler.start(), /closing/);
});

test("stop during initialization does not create later timers", async (t) => {
  const clock = timers(t);
  const scheduler = new CloudSyncScheduler("machine-test");
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  t.mock.method(scheduler, "initializeMachineId", () => gate);
  const start = scheduler.start();
  scheduler.stop();
  release();
  await start;
  assert.equal(clock.callbacks.length, 0);
  await scheduler.close();
});

test("old startup callback cannot lose a restarted startup timer", async (t) => {
  const clock = timers(t);
  const scheduler = new CloudSyncScheduler("machine-test");
  let calls = 0;
  t.mock.method(scheduler, "syncWithRetry", async () => {
    calls++;
    return null;
  });
  await scheduler.start();
  scheduler.stop();
  await scheduler.start();
  clock.callbacks[0]();
  await scheduler.close();
  assert.equal(calls, 0);
  assert.equal(
    clock.cleared.length,
    4,
    "both generations' startup and interval handles are cleared"
  );
});

test("global close does not instantiate an unused Cloud scheduler", async () => {
  assert.equal(globalThis.__omnirouteCloudSyncScheduler, undefined);
  await closeCloudSyncScheduler();
  assert.equal(globalThis.__omnirouteCloudSyncScheduler, undefined);
});
