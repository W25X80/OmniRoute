import assert from "node:assert/strict";
import test from "node:test";
import { resolveUserDatabaseSettings } from "../../src/lib/db/databaseSettingsSemantics.ts";

test("database settings preserve legacy precedence without a database handle", () => {
  const settings = resolveUserDatabaseSettings(
    {
      databaseSettings: { retention: { callLogs: 11 }, logs: { callLogPipelineEnabled: false } },
      retention: { callLogs: 12 },
      call_log_pipeline_enabled: "true",
    },
    {
      callLogs: 13,
      "retention.callLogs": 14,
      "logs.callLogPipelineEnabled": false,
    }
  );
  assert.equal(settings.retention.callLogs, 14);
  assert.equal(settings.logs.callLogPipelineEnabled, true);
  assert.equal(settings.retention.quotaSnapshots, 7);
});

test("database settings do not share mutable defaults between resolutions", () => {
  const first = resolveUserDatabaseSettings({}, {});
  first.retention.callLogs = 90;
  assert.equal(resolveUserDatabaseSettings({}, {}).retention.callLogs, 30);
});

test("database settings normalize malformed cache-size values", () => {
  assert.equal(
    resolveUserDatabaseSettings({}, { "optimization.cacheSize": "bad" }).optimization.cacheSize,
    65536
  );
  assert.equal(
    resolveUserDatabaseSettings({}, { "optimization.cacheSize": 2_000_000 }).optimization.cacheSize,
    1_000_000
  );
  assert.equal(
    resolveUserDatabaseSettings({}, { "optimization.cacheSize": 4.9 }).optimization.cacheSize,
    4
  );
});
