import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setImmediate as settle } from "node:timers/promises";
import type { JobDefinition } from "@/lib/jobRegistry/core";

const directory = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-job-shutdown-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";
const { JobRegistry } = await import("@/lib/jobRegistry/registry");
const core = await import("@/lib/db/core");
const { getRuns } = await import("@/lib/db/jobRegistryDb");

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function definition(id: string, handler: JobDefinition["handler"]): JobDefinition {
  const now = new Date().toISOString();
  return {
    id,
    handler,
    type: "interval",
    cron: null,
    intervalMs: 60_000,
    enabled: true,
    envFlag: null,
    config: {},
    createdAt: now,
    updatedAt: now,
  };
}

test.after(() => {
  core.resetDbInstance();
  fs.rmSync(directory, { recursive: true, force: true });
});

test("close drains an accepted handler and its terminal history and pruning", async (t) => {
  const registry = new JobRegistry();
  const gate = deferred();
  t.after(async () => {
    gate.resolve();
    await registry.close();
  });
  let calls = 0;
  let pruneAttempts = 0;
  const db = core.getDbInstance();
  const originalPrepare = db.prepare.bind(db);
  t.mock.method(db, "prepare", (sql: string) => {
    const statement = originalPrepare(sql);
    if (!/^\s*DELETE FROM job_runs\b/.test(sql)) return statement;
    return {
      ...statement,
      run(...parameters: unknown[]) {
        pruneAttempts++;
        return statement.run(...parameters);
      },
    };
  });
  registry.register(
    definition("drain-active", async () => {
      calls++;
      await gate.promise;
      return { success: true, recordsAffected: 7 };
    })
  );
  assert.deepEqual(await registry.runNow("drain-active"), { started: true });
  let closed = false;
  const close = registry.close().then(() => {
    closed = true;
  });
  await settle();
  assert.equal(closed, false);
  assert.deepEqual(await registry.runNow("drain-active"), { started: false, reason: "closed" });
  gate.resolve();
  await close;
  assert.equal(calls, 1);
  assert.equal(pruneAttempts, 1);
  assert.deepEqual(
    getRuns("drain-active").map(({ status, recordsAffected }) => ({ status, recordsAffected })),
    [{ status: "success", recordsAffected: 7 }]
  );
  assert.throws(() => registry.start("drain-active"), /closing/);
});

test("close preserves an accepted queued run but rejects another admission", async (t) => {
  const registry = new JobRegistry();
  const firstGate = deferred();
  const secondGate = deferred();
  t.after(async () => {
    firstGate.resolve();
    secondGate.resolve();
    await registry.close();
  });
  let calls = 0;
  registry.register(
    definition("drain-queue", async () => {
      calls++;
      await (calls === 1 ? firstGate.promise : secondGate.promise);
      return { success: true };
    })
  );
  assert.deepEqual(await registry.runNow("drain-queue"), { started: true });
  const queued = registry.runNow("drain-queue");
  await settle();
  assert.deepEqual(await registry.runNow("drain-queue"), {
    started: false,
    reason: "already_queued",
  });
  let closed = false;
  const close = registry.close().then(() => {
    closed = true;
  });
  firstGate.resolve();
  assert.deepEqual(await queued, { started: true });
  await settle();
  assert.equal(calls, 2);
  assert.equal(closed, false);
  secondGate.resolve();
  await close;
  assert.equal(getRuns("drain-queue").length, 2);
});

test("saved interval callback cannot admit work after close", async (t) => {
  const registry = new JobRegistry();
  let callback!: () => void;
  const timer = { unref() {} };
  t.mock.method(globalThis, "setInterval", (body: () => void) => {
    callback = body;
    return timer as unknown as ReturnType<typeof setInterval>;
  });
  t.mock.method(globalThis, "clearInterval", () => {});
  t.mock.method(globalThis, "clearTimeout", () => {});
  let calls = 0;
  registry.register(
    definition("saved-tick", async () => {
      calls++;
      return { success: true };
    })
  );
  registry.start("saved-tick");
  await registry.close();
  callback();
  await settle();
  assert.equal(calls, 1);
  assert.equal(getRuns("saved-tick").length, 1);
});

test("close prevents a pending async start from creating a timer", async () => {
  const registry = new JobRegistry();
  let calls = 0;
  registry.register(
    definition("start-race", async () => {
      calls++;
      return { success: true };
    })
  );
  const start = registry.startAsync("start-race");
  await registry.close();
  await assert.rejects(start, /closing/);
  assert.equal(calls, 0);
});

test("legacy stop remains restartable before close", async () => {
  const registry = new JobRegistry();
  let calls = 0;
  registry.register(
    definition("legacy-restart", async () => {
      calls++;
      return { success: true };
    })
  );
  registry.start("legacy-restart");
  await settle();
  registry.stopAll();
  registry.start("legacy-restart");
  await registry.close();
  assert.equal(calls, 2);
});

test("an environment-gated cron tick does not create skipped-run history", async (t) => {
  const registry = new JobRegistry();
  let callback!: () => void;
  const timer = { unref() {} };
  t.mock.method(globalThis, "setTimeout", (body: () => void) => {
    callback = body;
    return timer as unknown as ReturnType<typeof setTimeout>;
  });
  t.mock.method(globalThis, "clearTimeout", () => {});
  t.mock.method(globalThis, "clearInterval", () => {});
  const flag = "OMNIROUTE_TEST_CLOSED_CRON_GATE";
  process.env[flag] = "0";
  t.after(async () => {
    delete process.env[flag];
    await registry.close();
  });
  let calls = 0;
  registry.register({
    ...definition("owned-cron-gate", async () => {
      calls++;
      return { success: true };
    }),
    type: "cron",
    cron: "* * * * *",
    intervalMs: null,
    envFlag: flag,
  });
  registry.start("owned-cron-gate");
  callback();
  await settle();
  assert.equal(calls, 0);
  assert.equal(getRuns("owned-cron-gate").length, 0);
  process.env[flag] = "1";
  callback();
  await registry.close();
  assert.equal(calls, 1);
  assert.equal(getRuns("owned-cron-gate").length, 1);
});
