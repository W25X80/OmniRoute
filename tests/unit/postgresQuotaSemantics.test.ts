import assert from "node:assert/strict";
import { test } from "node:test";
import {
  parseDays,
  parseReserves,
  quotaAllocationFingerprint,
  rowToQuotaAllocation,
  rowToQuotaPlan,
  rowToQuotaSchedule,
  serializeQuotaDays,
} from "../../src/lib/db/quotaSemantics.ts";

test("quota schedule row interpretation preserves restrictive malformed-row defaults", () => {
  const schedule = rowToQuotaSchedule({
    id: "schedule",
    pool_id: "pool",
    label: null,
    days: "1,1,,7,-1,2.5, 6",
    start_minute: 30,
    end_minute: 60,
    timezone: "Europe/Berlin",
    mode: "unknown",
    reserves: '[{"window":"weekly","percent":30},{"window":"unknown","percent":10}]',
    budget_value: 25,
    budget_unit: "percent",
    budget_window: "unknown",
    priority: 2,
    enabled: 0,
  });
  assert.deepEqual(schedule.days, [1, 6]);
  assert.equal(schedule.mode, "block");
  assert.deepEqual(schedule.reserves, [{ window: "weekly", percent: 30 }]);
  assert.equal(schedule.budgetWindow, "daily");
  assert.equal(schedule.enabled, false);
  assert.equal(schedule.budgetValue, undefined);
  assert.equal(schedule.budgetUnit, undefined);
});

test("quota parser retains zero/100 reserve endpoints and filters malformed entries", () => {
  assert.deepEqual(parseDays(",, "), []);
  assert.equal(serializeQuotaDays([6, 0, 0, -1, 1.5, 7]), "0,6");
  assert.deepEqual(
    parseReserves(
      '[{"window":"any","percent":0},{"window":"5h","percent":100},{"window":"daily","percent":101},{"window":"weekly","percent":"10"}]'
    ),
    [
      { window: "any", percent: 0 },
      { window: "5h", percent: 100 },
    ]
  );
  assert.deepEqual(parseReserves("corrupt"), []);
});

test("unknown allocation policies remain hard and fingerprints ignore allocation order", () => {
  assert.equal(
    rowToQuotaAllocation({
      pool_id: "pool",
      api_key_id: "key",
      weight: 50,
      cap_value: null,
      cap_unit: null,
      policy: "corrupt",
    }).policy,
    "hard"
  );
  const a = { apiKeyId: "a", weight: 50, policy: "hard" as const };
  const b = {
    apiKeyId: "b",
    weight: 50,
    policy: "burst" as const,
    capValue: 10,
    capUnit: "tokens" as const,
  };
  assert.equal(quotaAllocationFingerprint([a, b]), quotaAllocationFingerprint([b, a]));
  assert.notEqual(
    quotaAllocationFingerprint([a]),
    quotaAllocationFingerprint([{ ...a, weight: 25 }])
  );
});

test("malformed persisted plan JSON keeps the existing empty-plan fallback", () => {
  assert.deepEqual(
    rowToQuotaPlan({
      connection_id: "connection",
      provider: "codex",
      source: "manual",
      dimensions_json: "bad-json",
      updated_at: "2026-10-11",
    }),
    {
      connectionId: "connection",
      provider: "codex",
      source: "manual",
      dimensions: [],
    }
  );
});
