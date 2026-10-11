import assert from "node:assert/strict";
import test from "node:test";
import {
  computeDiff,
  configAuditEntryParams,
  createConfigAuditEntry,
  rowToConfigAuditEntry,
  summarizeConfigAuditRows,
  type ConfigAuditRow,
} from "../../src/lib/db/configAuditModel.ts";

function row(patch: Partial<ConfigAuditRow> = {}): ConfigAuditRow {
  return {
    id: "audit-a",
    timestamp: "2026-10-11T08:00:00.000Z",
    action: "update",
    target: "settings",
    target_id: "runtime",
    target_name: "Runtime",
    before_json: null,
    after_json: null,
    diff_json: '{"added":[],"removed":[],"changed":[],"isEmpty":true}',
    source: "api",
    note: null,
    ...patch,
  };
}

test("config audit diff preserves source top-level keys and JSON value comparison", () => {
  assert.deepEqual(
    computeDiff(
      { kept: { a: 1 }, gone: 1, changed: [1] },
      { kept: { a: 1 }, added: 2, changed: [1, 2] }
    ),
    {
      added: ["added"],
      removed: ["gone"],
      changed: [{ key: "changed", from: [1], to: [1, 2] }],
      isEmpty: false,
    }
  );
  assert.deepEqual(computeDiff(null, null), { added: [], removed: [], changed: [], isEmpty: true });
  assert.equal(computeDiff({ nested: { a: 1, b: 2 } }, { nested: { b: 2, a: 1 } }).isEmpty, false);
});

test("config snapshots serialize eagerly and retain literal source fields without redaction", () => {
  const before = { apiKey: "literal-source-payload", nested: { n: 1 } };
  const entry = createConfigAuditEntry(
    "audit-test",
    "update",
    "settings",
    "runtime",
    "Runtime",
    before,
    { nested: { n: 2 } },
    "cli"
  );
  const params = configAuditEntryParams(entry);
  before.nested.n = 7;
  assert.deepEqual(JSON.parse(params.beforeJson!), {
    apiKey: "literal-source-payload",
    nested: { n: 1 },
  });
  assert.equal(entry.before, before);
  assert.equal(params.note, null);
  assert.equal(params.id, "audit-test");
});

test("config audit projection preserves SQL-null versus JSON-null and corruption errors", () => {
  assert.equal(rowToConfigAuditEntry(row()).before, null);
  assert.equal(rowToConfigAuditEntry(row({ before_json: "null" })).before, null);
  assert.deepEqual(rowToConfigAuditEntry(row({ before_json: '{"x":1}' })).before, { x: 1 });
  assert.throws(() => rowToConfigAuditEntry(row({ diff_json: "broken" })), SyntaxError);
});

test("summary uses source counts and caller-provided ordering without parsing snapshots", () => {
  const newest = row({ id: "new", target: "provider", source: "dashboard", diff_json: "broken" });
  const oldest = row({ id: "old", timestamp: "2026-10-10T08:00:00.000Z", action: "create" });
  assert.deepEqual(summarizeConfigAuditRows([newest, oldest]), {
    totalEntries: 2,
    byTarget: { provider: 1, settings: 1 },
    byAction: { update: 1, create: 1 },
    bySource: { dashboard: 1, api: 1 },
    oldestEntry: oldest.timestamp,
    newestEntry: newest.timestamp,
  });
  assert.equal(summarizeConfigAuditRows([]).oldestEntry, null);
});
