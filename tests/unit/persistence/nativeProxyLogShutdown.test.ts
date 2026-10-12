import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { setImmediate as nextTurn } from "node:timers/promises";
import { NativeProxyLogBuffer } from "../../../src/lib/proxyLogBuffer.ts";
import {
  controlledBarrier,
  controlledOwnerModule,
} from "../../helpers/persistence/controlledOwnerModule.ts";

interface ProxyOwner {
  logProxyEvent(entry: { correlationId: string }): unknown;
  getProxyLogBufferStatus(): { bufferedRows: number };
  beginNativeProxyLogClose?(): void;
  closeProxyLogSavesAsync(): Promise<void>;
}

async function proxyFixture(appendError?: Error) {
  const sharedGlobal = {};
  const timers = new Set<object>();
  const committed: string[] = [];
  let attempts = 0;
  const entered = controlledBarrier();
  const release = controlledBarrier();
  const ports = {
    "./proxyLogBuffer": { NativeProxyLogBuffer },
    "./db/proxyLogs": { updateAttemptTimingAsync: async () => true },
    "./db/backendSelection": { parseDatabaseBackend: () => "postgres" },
    "./db/persistenceRuntime": {
      runPersistenceBackgroundWork: (body: () => Promise<unknown>) => body(),
    },
    "./db/postgres/proxyLogStore": {
      getSelectedProxyLogStore: () => ({
        append: async (rows: Array<{ correlationId: string }>) => {
          attempts++;
          entered.release();
          await release.promise;
          if (appendError) throw appendError;
          committed.push(...rows.map((row) => row.correlationId));
        },
      }),
    },
    uuid: { v4: randomUUID },
    "@omniroute/open-sse/utils/errorSanitization.ts": { sanitizeErrorMessage: String },
    "@omniroute/open-sse/utils/timingMs.ts": {
      sanitizeTimingMs: (value: unknown) => (typeof value === "number" ? value : null),
    },
    "./db/core": { isCloud: false, isBuildPhase: false },
    "./db/schemaColumns": {},
    "./proxyLogHost": { normalizeProxyHostForLog: (value: string) => value },
  };
  const globals = {
    globalThis: sharedGlobal,
    setInterval: () => {
      const timer = { unref() {} };
      timers.add(timer);
      return timer;
    },
    clearInterval: (timer: object) => timers.delete(timer),
  };
  const load = () =>
    controlledOwnerModule<ProxyOwner>(
      new URL("../../../src/lib/proxyLogger.ts", import.meta.url),
      ports,
      globals
    );
  return { load, timers, committed, entered, release, attempts: () => attempts };
}

test("actual native shutdown stops every logger copy and flushes buffered accepted tails before pool close", async () => {
  const f = await proxyFixture();
  const first = await f.load();
  const second = await f.load();
  first.logProxyEvent({ correlationId: "first-copy" });
  second.logProxyEvent({ correlationId: "second-copy" });
  let phase = "ready";
  let poolClosed = false;
  const emptyModule = new Proxy(
    {},
    {
      get: (_target, name) =>
        name === "then"
          ? undefined
          : name === "__esModule"
            ? true
            : name === "getJobRegistry"
              ? () => ({ close() {} })
              : name === "autoRefreshDaemon"
                ? { stop() {} }
                : () => {},
    }
  );
  const runtime = {
    beginPersistenceAdmissionClose: () => {
      phase = "closing";
    },
    getPersistenceRuntimeStatus: () => ({ phase }),
    runPersistenceShutdownWork: (body: () => Promise<void>) => body(),
    drainPersistenceAcceptedWork: async () => {
      first.logProxyEvent({ correlationId: "accepted-tail" });
      await nextTurn();
    },
    closePersistenceRuntime: async () => {
      assert.equal(first.getProxyLogBufferStatus().bufferedRows, 0);
      assert.equal(second.getProxyLogBufferStatus().bufferedRows, 0);
      poolClosed = true;
    },
  };
  const shutdown = await controlledOwnerModule<{ shutdownPostgresApplication(): Promise<void> }>(
    new URL("../../../src/lib/startup/postgresShutdown.ts", import.meta.url),
    {
      "@/lib/db/persistenceRuntime": runtime,
      "./postgresBootstrap": { runPostgresShutdown: (body: () => Promise<void>) => body() },
      "@/lib/proxyLogger": first,
    },
    {
      require: (name: string) =>
        name === "@/lib/proxyLogger"
          ? first
          : name === "./postgresBootstrap"
            ? { runPostgresShutdown: (body: () => Promise<void>) => body() }
            : name === "@/lib/db/persistenceRuntime"
              ? runtime
              : emptyModule,
    }
  ).catch((error) => {
    throw error;
  });
  // Runtime ports are supplied separately so unknown producer modules can be inert.
  const work = shutdown.shutdownPostgresApplication();
  const rejected = work.catch(() => {});
  await Promise.race([f.entered.promise, work]);
  assert.equal(poolClosed, false);
  assert.equal(f.timers.size, 0);
  f.release.release();
  await work;
  await rejected;
  assert.deepEqual([...f.committed].sort(), ["accepted-tail", "first-copy", "second-copy"]);
  first.logProxyEvent({ correlationId: "after-close" });
  assert.equal(f.timers.size, 0);
  assert.equal(first.getProxyLogBufferStatus().bufferedRows, 0);
});

test("logger shutdown retains and reports an unsuccessful batch without replaying close", async () => {
  const f = await proxyFixture(new Error("unknown COMMIT acknowledgement"));
  const owner = await f.load();
  owner.logProxyEvent({ correlationId: "accepted" });
  const closing = owner.closeProxyLogSavesAsync();
  const rejected = assert.rejects(closing, /Proxy telemetry shutdown failed/);
  await f.entered.promise;
  f.release.release();
  await rejected;
  assert.equal(owner.getProxyLogBufferStatus().bufferedRows, 1);
  assert.equal(f.timers.size, 0);
  await assert.rejects(owner.closeProxyLogSavesAsync(), /Proxy telemetry shutdown failed/);
  assert.equal(f.attempts(), 1);
});
