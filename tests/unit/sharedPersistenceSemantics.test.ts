import "../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import test from "node:test";
import * as batches from "../../src/lib/db/batches.ts";
import * as batchSemantics from "../../src/lib/db/batches/persistenceSemantics.ts";
import * as evals from "../../src/lib/db/evals.ts";
import * as evalSemantics from "../../src/lib/db/evals/persistenceSemantics.ts";
import * as radar from "../../src/lib/db/radar.ts";
import * as radarSemantics from "../../src/lib/db/radar/persistenceSemantics.ts";
import * as version from "../../src/lib/db/versionManager.ts";
import * as versionSemantics from "../../src/lib/db/versionManager/persistenceSemantics.ts";
import * as keys from "../../src/lib/db/registeredKeys.ts";
import { RegisteredKeyQuotaError } from "../../src/lib/db/registeredKeys/errors.ts";
import * as tokens from "../../src/lib/db/tokenLimits.ts";
import * as tokenSemantics from "../../src/lib/db/tokenLimits/persistenceSemantics.ts";
import * as costs from "../../src/domain/costRules.ts";
import { getBudgetWindow } from "../../src/domain/budgetWindow.ts";
import { getBatch } from "../../src/lib/db/postgres/serviceBatches.ts";
import { issueRegisteredKey } from "../../src/lib/db/postgres/serviceRegisteredKeys.ts";
import { PostgresTokenLimitRepository } from "../../src/lib/db/postgres/tokenLimitRepository.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";

function transaction(read: (sql: string) => Record<string, unknown>[]): PostgresTransaction {
  return {
    async query<Row>(sql: string) {
      const rows = read(sql);
      return { rows: rows as Row[], rowCount: rows.length, command: "SELECT" };
    },
  };
}

test("facade exports retain exact shared function and error constructor identity", () => {
  assert.equal(batches.parseBatchRow, batchSemantics.parseBatchRow);
  assert.equal(batches.parseBatchItemCheckpoint, batchSemantics.parseBatchItemCheckpoint);
  assert.equal(batches.INSTANCE_SWEEP_CHUNK, batchSemantics.INSTANCE_SWEEP_CHUNK);
  assert.equal(batches.MAX_CHUNKS_PER_REQUEST, batchSemantics.MAX_CHUNKS_PER_REQUEST);
  for (const name of [
    "sanitizeEvalCaseInput",
    "sanitizeEvalExpected",
    "createScorecardFromRuns",
    "serializeEvalTargetKey",
    "toPersistedEvalRun",
    "toEvalCaseRecord",
    "toEvalSuiteRecord",
  ] as const)
    assert.equal(evals[name], evalSemantics[name]);
  assert.equal(radar.normalizeRadarIdentity, radarSemantics.normalizeRadarIdentity);
  assert.equal(radar.normalizeDisplayName, radarSemantics.normalizeDisplayName);
  assert.equal(version.rowToVersionManager, versionSemantics.rowToVersionManager);
  assert.equal(version.stringifyConfigOverrides, versionSemantics.stringifyConfigOverrides);
  assert.equal(keys.RegisteredKeyQuotaError, RegisteredKeyQuotaError);
  assert.equal(tokens.TokenLimitConflictError, tokenSemantics.TokenLimitConflictError);
  assert.equal(tokens.TokenLimitNotFoundError, tokenSemantics.TokenLimitNotFoundError);
  assert.equal(tokens.resetWindowIfElapsed, tokenSemantics.resetWindowIfElapsed);
  assert.equal(costs.getBudgetWindow, getBudgetWindow);
});

test("batch projection preserves malformed JSON/date behavior through native reads", async () => {
  const row = {
    id: "batch",
    status: "completed",
    metadata: '{"owner":"A"}',
    errors: "bad json",
    usage: '{"input_tokens":4}',
    created_at: "42",
    in_progress_at: "Infinity",
    completed_at: "7",
  };
  const expected = batchSemantics.parseBatchRow(row);
  assert.deepEqual(expected.metadata, { owner: "A" });
  assert.equal(expected.errors, null);
  assert.equal(expected.createdAt, 42);
  assert.equal(expected.inProgressAt, null);
  assert.equal(expected.completedAt, 7);
  const native = await getBatch(
    transaction(() => [{ ...row, insertion_order: 99 }]),
    "batch"
  );
  assert.deepEqual(native, expected);
  assert.deepEqual(
    batchSemantics.parseBatchItemCheckpoint({
      batch_id: "b",
      line_number: "2",
      custom_id: null,
      status: "completed",
      result_json: '{"ok":true}',
      error_json: "invalid",
      created_at: "4",
      updated_at: "6",
    }),
    {
      batchId: "b",
      lineNumber: 2,
      customId: null,
      status: "completed",
      result: { ok: true },
      error: null,
      createdAt: 4,
      updatedAt: 6,
    }
  );
});

test("eval validation and fallback projections retain source edge cases", () => {
  assert.deepEqual(
    evalSemantics.sanitizeEvalCaseInput({
      messages: [{ role: "user", content: "hello" }, { role: "", content: "drop" }, null],
      max_tokens: "5.9",
    }),
    { messages: [{ role: "user", content: "hello" }], max_tokens: 5 }
  );
  assert.deepEqual(evalSemantics.sanitizeEvalExpected({ strategy: "unknown", value: "value" }), {
    strategy: "contains",
    value: "value",
  });
  assert.equal(evalSemantics.toPersistedEvalRun(null), null);
  const run = evalSemantics.toPersistedEvalRun({
    id: "run",
    target_type: "invalid",
    summary_json: "bad json",
    total: "3",
    passed: "1",
    failed: "2",
    pass_rate: "33",
    results_json: '[{"case":"a"},4]',
    outputs_json: '{"one":1,"empty":null}',
  });
  assert.deepEqual(run?.summary, { total: 3, passed: 1, failed: 2, passRate: 33 });
  assert.deepEqual(run?.outputs, { one: "1", empty: "" });
  assert.deepEqual(run?.results, [{ case: "a" }]);
  assert.equal(run?.target.key, "suite-default:__default__");
});

test("Radar normalizers preserve absent/null values, limits and control-character rejection", () => {
  assert.deepEqual(radarSemantics.normalizeRadarIdentity(" OpenAI ", " model/v1 "), {
    provider: "OpenAI",
    modelId: "model/v1",
  });
  assert.equal(radarSemantics.normalizeRadarIdentity("bad provider", "model"), null);
  assert.equal(radarSemantics.normalizeRadarIdentity("provider", "x".repeat(201)), null);
  assert.equal(radarSemantics.normalizeDisplayName(undefined), undefined);
  assert.equal(radarSemantics.normalizeDisplayName(null), null);
  assert.equal(radarSemantics.normalizeDisplayName("hello\u0000world"), undefined);
  assert.equal(radarSemantics.normalizeDisplayName("  name  "), "name");
});

test("version projection preserves boolean/string/default/config encoding", () => {
  const row = versionSemantics.rowToVersionManager({
    id: 7,
    tool: "service",
    auto_start: "1",
    auto_update: true,
    auto_restart_adopted: 1,
    provider_expose: "1",
    config_overrides: '{"port":42}',
  });
  assert.equal(row.port, 8317);
  assert.equal(row.autoStart, true);
  assert.equal(row.autoUpdate, true);
  assert.equal(row.autoRestartAdopted, true);
  assert.equal(row.providerExpose, true);
  assert.deepEqual(row.configOverrides, { port: 42 });
  assert.equal(versionSemantics.stringifyConfigOverrides(null), null);
  const circular: Record<string, unknown> = {};
  circular.self = circular;
  assert.equal(versionSemantics.stringifyConfigOverrides(circular), null);
});

test("native registered-key quota errors retain public constructor/result identity", async () => {
  const tx = transaction((sql) =>
    sql.startsWith("SELECT * FROM omniroute.provider_key_limits")
      ? [
          {
            hourly_issue_limit: 1,
            hourly_issued: 1,
            daily_issue_limit: null,
            max_active_keys: null,
          },
        ]
      : []
  );
  await assert.rejects(issueRegisteredKey(tx, { name: "key", provider: "provider" }), (error) => {
    assert.ok(error instanceof keys.RegisteredKeyQuotaError);
    assert.equal(error.name, "RegisteredKeyQuotaError");
    assert.equal(error.result.errorCode, "PROVIDER_QUOTA_EXCEEDED");
    assert.match(error.message, /Hourly issue limit \(1\)/);
    return true;
  });
});

test("native token conflict/not-found errors retain public constructors", async () => {
  for (const conflict of [true, false]) {
    const tx = transaction((sql) =>
      conflict && sql.startsWith("SELECT id FROM omniroute.api_key_token_limits")
        ? [{ id: "other" }]
        : []
    );
    const database = {
      transaction: async <T>(body: (tx: PostgresTransaction) => Promise<T>) => body(tx),
    } as PostgresDatabase;
    const repository = new PostgresTokenLimitRepository(database);
    await assert.rejects(
      repository.upsert({
        id: "limit",
        apiKeyId: "key",
        scopeType: "global",
        resetInterval: "weekly",
        tokenLimit: 4,
      }),
      (error) => {
        assert.ok(
          error instanceof
            (conflict ? tokens.TokenLimitConflictError : tokens.TokenLimitNotFoundError)
        );
        assert.equal(
          (error as Error).name,
          conflict ? "TokenLimitConflictError" : "TokenLimitNotFoundError"
        );
        return true;
      }
    );
  }
});

test("UTC daily/weekly/monthly reset boundaries retain billing/token parity", () => {
  const fixtures = [
    {
      interval: "daily" as const,
      now: Date.UTC(2026, 0, 4, 1),
      time: "02:30",
      start: Date.UTC(2026, 0, 3, 2, 30),
      next: Date.UTC(2026, 0, 4, 2, 30),
    },
    {
      interval: "weekly" as const,
      now: Date.UTC(2026, 0, 4, 23),
      time: "00:00",
      start: Date.UTC(2025, 11, 29),
      next: Date.UTC(2026, 0, 5),
    },
    {
      interval: "monthly" as const,
      now: Date.UTC(2024, 1, 29, 23),
      time: "00:00",
      start: Date.UTC(2024, 1, 1),
      next: Date.UTC(2024, 2, 1),
    },
  ];
  for (const fixture of fixtures) {
    const window = getBudgetWindow(fixture.interval, fixture.time, fixture.now);
    assert.deepEqual(window, { periodStartAt: fixture.start, nextResetAt: fixture.next });
    const limit = { resetInterval: fixture.interval, resetTime: fixture.time } as tokens.TokenLimit;
    assert.deepEqual(tokenSemantics.resetWindowIfElapsed(limit, fixture.now), {
      windowStart: String(fixture.start),
      didReset: false,
      ...window,
    });
  }
});
