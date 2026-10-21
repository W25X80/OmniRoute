import assert from "node:assert/strict";
import { AsyncLocalStorage, createHook } from "node:async_hooks";
import { afterEach, test } from "node:test";

import {
  acquire,
  getStats,
  markRateLimited,
  prepareRateLimitSemaphoreMaintenance,
  resetAll,
} from "../../open-sse/services/rateLimitSemaphore.ts";

afterEach(() => resetAll());
const options = { maxConcurrency: 1, timeoutMs: 120_000 };
const flush = async () => {
  await Promise.resolve();
  await Promise.resolve();
};

test("overlapping cooldowns keep the later deadline in both arrival orders", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 1_000_000 });
  for (const [first, next] of [
    [60_000, 5_000],
    [5_000, 60_000],
  ]) {
    const model = `cooldown-${first}`;
    const release = await acquire(model, options);
    markRateLimited(model, first);
    const start = Date.now();
    t.mock.timers.tick(1_000);
    markRateLimited(model, next);
    const until = Math.max(start + first, Date.now() + next);
    assert.equal(Date.parse(getStats()[model].rateLimitedUntil!), until);
    let acquired = false;
    const waiting = acquire(model, options).then((releaseNext) => {
      acquired = true;
      return releaseNext;
    });
    release();
    t.mock.timers.tick(5_100);
    await flush();
    assert.equal(acquired, false, "the earlier timer must not bypass the later window");
    t.mock.timers.tick(until - Date.now() + 50);
    await flush();
    assert.equal(acquired, true);
    (await waiting)();
    assert.equal(getStats()[model], undefined);
  }
});

test("releasing the last running request cannot discard an active idle cooldown", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 2_000_000 });
  const release = await acquire("idle-cooldown", options);
  markRateLimited("idle-cooldown", 60_000);
  release();
  assert.equal(Date.parse(getStats()["idle-cooldown"].rateLimitedUntil!), Date.now() + 60_000);
  await assert.rejects(acquire("idle-cooldown", { ...options, maxQueueSize: 0 }), {
    code: "SEMAPHORE_QUEUE_FULL",
  });
  assert.ok(getStats()["idle-cooldown"], "rejecting an unqueued waiter cannot remove cooldown");
  t.mock.timers.tick(60_050);
  assert.equal(getStats()["idle-cooldown"], undefined);
  const fresh = await acquire("idle-cooldown", options);
  fresh();
});

test("an expired window can start again and drains exactly the configured concurrency", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 3_000_000 });
  markRateLimited("renewed-cooldown", 10);
  t.mock.timers.tick(60);
  markRateLimited("renewed-cooldown", 20);
  let entered = 0;
  const queued = Array.from({ length: 3 }, () =>
    acquire("renewed-cooldown", options).then((r) => {
      entered += 1;
      return r;
    })
  );
  t.mock.timers.tick(70);
  await flush();
  assert.equal(entered, 1);
  assert.equal(getStats()["renewed-cooldown"].max, 1);
  for (let i = 0; i < queued.length; i += 1) {
    (await queued[i])();
    await flush();
    assert.equal(entered, Math.min(i + 2, queued.length));
  }
  assert.equal(getStats()["renewed-cooldown"], undefined);
});

test("reset isolates stale releases and cooldown timers from the new gate", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 4_000_000 });
  const staleRelease = await acquire("reset-cooldown", options);
  markRateLimited("reset-cooldown", 1_000);
  const staleWaiter = acquire("reset-cooldown", options).catch((e: Error) => e.message);
  resetAll();
  assert.equal(await staleWaiter, "Semaphore reset");
  const release = await acquire("reset-cooldown", options);
  markRateLimited("reset-cooldown", 60_000);
  staleRelease();
  assert.equal(getStats()["reset-cooldown"].running, 1);
  t.mock.timers.tick(1_100);
  assert.equal(getStats()["reset-cooldown"].running, 1);
  assert.equal(Date.parse(getStats()["reset-cooldown"].rateLimitedUntil!), 4_060_000);
  release();
});

test("cooldown maintenance timers do not inherit a caller request context", async () => {
  prepareRateLimitSemaphoreMaintenance();
  const context = new AsyncLocalStorage<object>();
  const inherited: unknown[] = [];
  const hook = createHook({
    init(_id, type) {
      if (type === "Timeout") inherited.push(context.getStore());
    },
  });
  hook.enable();
  try {
    context.run({ request: "private-body" }, () => markRateLimited("neutral-timer", 60_000));
  } finally {
    hook.disable();
  }
  assert.deepEqual(inherited, [undefined]);
});
