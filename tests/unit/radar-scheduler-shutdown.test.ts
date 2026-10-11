import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setImmediate as settle } from "node:timers/promises";

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-radar-shutdown-"));
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
const { ensureRadarSyncScheduler, stopRadarSyncScheduler, stopRadarSyncSchedulerAsync } =
  await import("@/lib/radar/scheduler");
const { resetDbInstance } = await import("@/lib/db/core");
const directory = process.env.DATA_DIR;

test.after(() => {
  resetDbInstance();
  fs.rmSync(directory, { recursive: true, force: true });
});

test("Radar shutdown waits an accepted tick and ignores stale timer callbacks", async (t) => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const callbacks: Array<() => void> = [];
  let calls = 0;
  let cleared = 0;
  const now = Date.parse("2026-10-11T10:00:00Z");
  const deps = {
    getFlag: () => true,
    getSettings: () => ({ optIn: true }),
    getCache: () => null,
    getReferralsCache: () => ({ fetchedAt: new Date(now).toISOString() }),
    getOffersCache: () => ({ fetchedAt: new Date(now).toISOString() }),
    getIntelCache: () => ({ fetchedAt: new Date(now).toISOString() }),
    now: () => now,
    sync: async () => {
      calls++;
      await gate;
      return { status: "updated", version: "2026.10.11.1", tier: "live" } as const;
    },
    setIntervalFn: ((callback: () => void) => {
      callbacks.push(callback);
      return { unref() {} } as ReturnType<typeof setInterval>;
    }) as typeof setInterval,
    clearIntervalFn: (() => {
      cleared++;
    }) as typeof clearInterval,
  };
  t.after(async () => {
    release();
    await stopRadarSyncSchedulerAsync(deps);
  });
  assert.equal(ensureRadarSyncScheduler(deps), true);
  await settle();
  assert.equal(calls, 1);
  callbacks[0]();
  await settle();
  assert.equal(calls, 1, "an overlapping tick must not be admitted");
  stopRadarSyncScheduler(deps);
  assert.equal(ensureRadarSyncScheduler(deps), true, "legacy stop allows restart");
  callbacks[0]();
  await settle();
  assert.equal(calls, 1, "a stale generation must not dispatch a tick");
  let closed = false;
  const close = stopRadarSyncSchedulerAsync(deps).then(() => {
    closed = true;
  });
  await settle();
  assert.equal(closed, false);
  assert.equal(ensureRadarSyncScheduler(deps), false);
  callbacks[1]();
  release();
  await close;
  assert.equal(calls, 1);
  assert.equal(cleared, 2);
});
