import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const TEST_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-provider-limits-sync-"));
process.env.DATA_DIR = TEST_DATA_DIR;
process.env.NODE_ENV = "test";
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";

const schedulerModule = await import("../../src/shared/services/providerLimitsSyncScheduler.ts");
const core = await import("../../src/lib/db/core.ts");

test.after(() => {
  core.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

test("provider limits sync scheduler exposes start and awaitable shutdown", async () => {
  assert.equal(typeof schedulerModule.startProviderLimitsSyncScheduler, "function");
  assert.equal(typeof schedulerModule.stopProviderLimitsSyncScheduler, "function");
  const stopping = schedulerModule.stopProviderLimitsSyncScheduler();
  assert.ok(stopping instanceof Promise);
  await stopping;
});
