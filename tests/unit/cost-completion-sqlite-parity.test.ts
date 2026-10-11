import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const directory = mkdtempSync(join(tmpdir(), "omniroute-cost-completion-sqlite-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";
process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES = "true";
const cost = await import("../../src/domain/costRules.ts");
const { spendBatchWriter } = await import("../../src/lib/spend/batchWriter.ts");
const { resetDbInstance } = await import("../../src/lib/db/core.ts");

test("SQLite async facade preserves the source processing clock and ignores native-only completion metadata", async () => {
  const before = Date.now();
  await cost.recordCostAsync("sqlite-clock", 1, { completedAt: -1 });
  const entry = spendBatchWriter.getBufferedEntries("sqlite-clock")[0];
  assert.ok(entry.timestamp >= before && entry.timestamp <= Date.now());
  assert.equal(entry.receiptId, undefined);
});

test("SQLite chat async facade keeps the source guard and one buffered admission", async () => {
  await cost.recordChatCallCostAsync(null, 1, {}, true);
  await cost.recordChatCallCostAsync({ id: "sqlite-chat" }, 0, {}, true);
  assert.equal(spendBatchWriter.getBufferedEntries("sqlite-chat").length, 0);
  await cost.recordChatCallCostAsync({ id: "sqlite-chat" }, 2, { completedAt: 0 }, true);
  assert.equal(spendBatchWriter.getPendingCostTotal("sqlite-chat"), 2);
  assert.ok(spendBatchWriter.getBufferedEntries("sqlite-chat")[0].timestamp > 0);
});

test("SQLite token accounting ignores the native-only clock and retains the source current window", async () => {
  const { upsertTokenLimit, getWindowUsage } = await import("../../src/lib/db/tokenLimits.ts");
  const { recordTokenUsageAsync } = await import("../../open-sse/services/tokenLimitCounter.ts");
  const limit = upsertTokenLimit({
    apiKeyId: "sqlite-token-clock",
    scopeType: "global",
    tokenLimit: 100,
    resetInterval: "monthly",
  });
  await assert.doesNotReject(
    recordTokenUsageAsync(limit.apiKeyId, "test-provider", "test-model", 5, undefined, -1)
  );
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(getWindowUsage(limit), 5);
  assert.equal(getWindowUsage(limit, 0), 0);
});

test.after(() => {
  spendBatchWriter.resetForTests();
  resetDbInstance();
  rmSync(directory, { recursive: true, force: true });
});
