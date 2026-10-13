import assert from "node:assert/strict";
import test from "node:test";
import { getShutdownBudgetConfig } from "../../src/shared/utils/runtimeTimeouts.ts";
import { getShutdownBudgetConfig as packagedBudget } from "../../scripts/dev/main-server-timeouts.mjs";
import {
  controlledBarrier,
  controlledOwnerModule,
} from "../helpers/persistence/controlledOwnerModule.ts";

test("native and packaged budgets agree and remain below the existing task stop deadline", () => {
  for (const backend of [undefined, "sqlite", "postgres"]) {
    for (const value of [undefined, "", "NaN", "-1", "0", "100", "90000", "120000", "1000000000"]) {
      const env = { OMNIROUTE_DATABASE_BACKEND: backend, SHUTDOWN_TIMEOUT_MS: value };
      const budget = getShutdownBudgetConfig(env);
      assert.deepEqual(budget, packagedBudget(env));
      if (backend !== "sqlite") assert.ok(budget.forceExitMs <= 110_000);
    }
  }
  assert.deepEqual(getShutdownBudgetConfig({ OMNIROUTE_DATABASE_BACKEND: "postgres" }), {
    drainMs: 90_000,
    cleanupMs: 20_000,
    forceExitMs: 110_000,
  });
  assert.deepEqual(getShutdownBudgetConfig({}), {
    drainMs: 90_000,
    cleanupMs: 20_000,
    forceExitMs: 110_000,
  });
  assert.deepEqual(getShutdownBudgetConfig({ OMNIROUTE_DATABASE_BACKEND: "sqlite" }), {
    drainMs: 30_000,
    cleanupMs: 5_000,
    forceExitMs: 35_000,
  });
});

async function fixture() {
  const stopped = controlledBarrier();
  let started = false,
    cancelled = false;
  const graceful = await controlledOwnerModule<{
    requestGracefulShutdown(signal: string): Promise<void>;
    trackRequest(): () => void;
  }>(
    new URL("../../src/lib/gracefulShutdown.ts", import.meta.url),
    {
      "@/lib/db/backendSelection": { parseDatabaseBackend: () => "postgres" },
      "@/lib/db/persistenceRuntime": {
        beginPersistenceAdmissionClose() {},
        cancelPersistenceAttempts() {
          cancelled = true;
        },
        getPersistenceRuntimeStatus: () => ({ activeAttempts: 1 }),
      },
      "@/shared/utils/runtimeTimeouts": {
        getShutdownBudgetConfig: () => ({ drainMs: 1000, forceExitMs: 1200 }),
      },
      "@/lib/serverLifecycle": { markServerStopping() {} },
      "@/lib/startup/postgresShutdown": {
        async shutdownPostgresApplication() {
          started = true;
          await stopped.promise;
        },
      },
      "@/lib/vncSession/service": { listSessions: () => [] },
      "@omniroute/open-sse/executors/chatgpt-web-codex/runtime.ts": {
        async stopChatGptWebCodexRuntime() {},
      },
      "@/shared/utils/loggerResource": { async closeSharedLoggerResource() {} },
      "@/lib/logRotation": { closeLogRotation() {} },
    },
    { setTimeout, clearTimeout, Date }
  );
  return { graceful, stopped, started: () => started, cancelled: () => cancelled };
}

test("native shutdown stops producers before waiting for HTTP requests", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const f = await fixture();
  const done = f.graceful.trackRequest();
  const closing = f.graceful.requestGracefulShutdown("TEST");
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(f.started(), true);
  done();
  f.stopped.release();
  t.mock.timers.tick(250);
  await closing;
  assert.equal(f.cancelled(), false);
});

test("budget cancellation retains cleanup joining and reports an unsuccessful drain", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const f = await fixture();
  let settled = false;
  const closing = f.graceful.requestGracefulShutdown("TEST");
  void closing.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    }
  );
  await new Promise<void>((resolve) => setImmediate(resolve));
  t.mock.timers.tick(1000);
  assert.equal(f.cancelled(), true);
  assert.equal(settled, false, "cancellation is not settlement");
  f.stopped.release();
  await assert.rejects(closing, /drain exceeded/);
});
