import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const directory = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-background-init-"));
process.env.DATA_DIR = directory;

process.env.OMNIROUTE_ENABLE_RUNTIME_BACKGROUND_TASKS = "1";
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
delete process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES;
const { ensureCloudSyncInitialized } = await import("@/lib/initCloudSync");
const { closeCloudSyncScheduler } = await import("@/shared/services/cloudSyncScheduler");

test("required native initialization failures reject and leave initialization retryable", async (t) => {
  const errors: unknown[][] = [];
  const timer = { unref() {} };
  t.mock.method(globalThis, "setTimeout", () => timer);
  t.mock.method(globalThis, "setInterval", () => timer);
  t.mock.method(globalThis, "clearTimeout", () => {});
  t.mock.method(globalThis, "clearInterval", () => {});
  t.after(async () => {
    await closeCloudSyncScheduler();
    await globalThis.__omnirouteJobRegistry?.close();
    fs.rmSync(directory, { recursive: true, force: true });
  });
  t.mock.method(console, "error", (...values: unknown[]) => {
    errors.push(values);
  });
  // No PostgreSQL authority has been installed. Falling back to SQLite or
  // acknowledging initialization here would conceal a failed native startup.
  const attempts = await Promise.allSettled([
    ensureCloudSyncInitialized(),
    ensureCloudSyncInitialized(),
  ]);
  assert.equal(attempts[0].status, "rejected");
  assert.equal(attempts[1].status, "rejected");
  if (attempts[0].status === "rejected" && attempts[1].status === "rejected") {
    assert.equal(
      attempts[0].reason,
      attempts[1].reason,
      "concurrent callers share one initialization"
    );
  }
  assert.equal(globalThis.__omnirouteBackgroundSyncInitialization?.initialized, false);
  assert.equal(globalThis.__omnirouteBackgroundSyncInitialization?.pending, undefined);
  // Initialization may have created producers before a required job read fails.
  // The coordinator closes those producers when it observes this rejection.
  assert.ok(globalThis.__omnirouteCloudSyncScheduler);
  assert.ok(globalThis.__omnirouteJobRegistry);
  await assert.rejects(ensureCloudSyncInitialized());
  assert.equal(
    errors.filter(
      ([message]) => message === "[ServerInit] Error initializing background sync services:"
    ).length,
    2
  );
});
