import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const directory = mkdtempSync(join(tmpdir(), "omniroute-combo-failure-ack-"));
process.env.DATA_DIR = directory;
const { getDbInstance, resetDbInstance } = await import("../../../src/lib/db/core.ts");
const { recordSessionModelUsage, getLastSessionModel } =
  await import("../../../src/lib/db/contextHandoffs.ts");
const { recordComboFailure, __resetComboFailureTrackerForTests } =
  await import("../../../open-sse/services/combo/failureTracker.ts");

test.after(() => {
  resetDbInstance();
  rmSync(directory, { recursive: true, force: true });
});

test("a failed SQLite cleanup is not acknowledged and a later failure retries", () => {
  __resetComboFailureTrackerForTests();
  const database = getDbInstance();
  recordSessionModelUsage("failed-session", "failed-combo", "old-model", "openai");
  database.exec(`CREATE TRIGGER reject_session_pin_delete BEFORE DELETE ON session_model_history
    BEGIN SELECT RAISE(FAIL,'injected cleanup failure'); END`);
  try {
    recordComboFailure("failed-session", "failed-combo");
    recordComboFailure("failed-session", "failed-combo");
    assert.deepEqual(recordComboFailure("failed-session", "failed-combo"), {
      count: 3,
      pinClearedNow: false,
    });
    assert.equal(getLastSessionModel("failed-session", "failed-combo"), "old-model");
  } finally {
    database.exec("DROP TRIGGER reject_session_pin_delete");
  }
  assert.deepEqual(recordComboFailure("failed-session", "failed-combo"), {
    count: 4,
    pinClearedNow: true,
  });
  assert.equal(getLastSessionModel("failed-session", "failed-combo"), null);
  assert.deepEqual(recordComboFailure("failed-session", "failed-combo"), {
    count: 5,
    pinClearedNow: false,
  });
});

test("delimiter-bearing session/combo pairs have independent counters and pins", () => {
  __resetComboFailureTrackerForTests();
  recordSessionModelUsage("a::b", "c", "first-model", "openai");
  recordSessionModelUsage("a", "b::c", "second-model", "openai");
  recordComboFailure("a::b", "c");
  recordComboFailure("a::b", "c");
  assert.deepEqual(recordComboFailure("a", "b::c"), { count: 1, pinClearedNow: false });
  assert.equal(getLastSessionModel("a::b", "c"), "first-model");
  assert.equal(getLastSessionModel("a", "b::c"), "second-model");
  assert.deepEqual(recordComboFailure("a::b", "c"), { count: 3, pinClearedNow: true });
  assert.equal(getLastSessionModel("a::b", "c"), null);
  assert.equal(getLastSessionModel("a", "b::c"), "second-model");
});
