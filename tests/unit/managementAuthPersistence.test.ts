import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { after, beforeEach, test } from "node:test";

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "management-persistence-"));
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DATA_DIR = scratch;
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";
process.env.API_KEY_SECRET = "management-tests-api-key-secret";
const core = await import("../../src/lib/db/core.ts");
const settings = await import("../../src/lib/db/settings.ts");
const auth = await import("../../src/lib/db/authSettings.ts");
const tokens = await import("../../src/lib/sync/tokens.ts");
const records = await import("../../src/lib/db/syncTokens.ts");
const keys = await import("../../src/lib/db/apiKeys.ts");
const { ApiKeyScopeConflictError } =
  await import("../../src/domain/persistence/apiKeyRepository.ts");
const { validatePostgresSyncToken } =
  await import("../../src/lib/db/postgres/authSessionRepository.ts");
type PostgresDatabase = import("../../src/lib/db/postgres/database.ts").PostgresDatabase;

const migratedGeneration = (await auth.getAuthSettingsSnapshot()).generation;

beforeEach(async () => {
  core.getDbInstance().prepare("DELETE FROM sync_tokens").run();
  await settings.updateSettings({ managementAuthGeneration: randomUUID(), requireLogin: false });
});
after(() => {
  core.resetDbInstance();
  fs.rmSync(scratch, { recursive: true, force: true });
});

test("migration initializes one UUID and retains nullable legacy sync issuer fields", async () => {
  assert.match(migratedGeneration!, /^[a-f0-9-]{36}$/);
  const first = await auth.getAuthSettingsSnapshot();
  assert.match(first.generation!, /^[a-f0-9-]{36}$/);
  assert.equal((await auth.getAuthSettingsSnapshot()).generation, first.generation);
  const legacy = await records.createSyncTokenRecord({
    name: "legacy",
    tokenHash: tokens.hashSyncToken("legacy-secret"),
  });
  assert.equal(legacy.issuedBy, null);
  assert.equal(await tokens.validateSyncToken("legacy-secret"), null);
});

test("sync validity depends independently on issuer, expiry, generation and linked key lifecycle", async () => {
  const key = await keys.createApiKey("linked", "management-persistence-machine", ["manage"]);
  const generation = (await auth.getAuthSettingsSnapshot()).generation!;
  const issued = await tokens.issueSyncToken({
    name: "operator",
    issuedBy: "session",
    authGeneration: generation,
    syncApiKeyId: key.id,
  });
  assert.ok(await tokens.validateSyncToken(issued.token));
  const lifetime = Date.parse(issued.record.expiresAt!) - Date.now();
  assert.ok(lifetime > 29 * 86400000 && lifetime <= 30 * 86400000);
  const db = core.getDbInstance();
  for (const [column, invalid] of [
    ["issued_by", "open"],
    ["issued_by", null],
    ["issued_by", "unknown"],
    ["expires_at", "invalid"],
    ["expires_at", "2000-01-01 00:00:00"],
    ["auth_generation", randomUUID()],
  ] as const) {
    const previous = db
      .prepare(`SELECT ${column} AS value FROM sync_tokens WHERE id=?`)
      .get(issued.record.id) as { value: unknown };
    db.prepare(`UPDATE sync_tokens SET ${column}=? WHERE id=?`).run(invalid, issued.record.id);
    assert.equal(await tokens.validateSyncToken(issued.token), null, column + ":" + invalid);
    db.prepare(`UPDATE sync_tokens SET ${column}=? WHERE id=?`).run(
      previous.value,
      issued.record.id
    );
  }
  for (const [column, invalid, valid] of [
    ["is_active", 0, 1],
    ["is_banned", 1, 0],
    ["revoked_at", "2026-10-19", null],
    ["expires_at", "invalid", null],
    ["expires_at", "2000-01-01 00:00:00", null],
  ] as const) {
    db.prepare(`UPDATE api_keys SET ${column}=? WHERE id=?`).run(invalid, key.id);
    assert.equal(await tokens.markSyncTokenUsed(issued.record), false, column);
    db.prepare(`UPDATE api_keys SET ${column}=? WHERE id=?`).run(valid, key.id);
  }
  assert.equal(await tokens.markSyncTokenUsed(issued.record), true);
  await records.revokeSyncToken(issued.record.id);
  assert.equal(await tokens.markSyncTokenUsed(issued.record), false);
});

test("password and generation write is atomic under live revision CAS", async () => {
  const previous = await auth.getAuthSettingsSnapshot();
  await settings.updateSettings(
    { password: "test-fixture-hash", theme: "dark" },
    { expectedRevision: previous.revision }
  );
  const next = await auth.getAuthSettingsSnapshot();
  assert.notEqual(next.generation, previous.generation);
  assert.equal(next.revision, previous.revision + 1);
  await assert.rejects(
    settings.updateSettings(
      { password: "stale", theme: "light" },
      { expectedRevision: previous.revision }
    ),
    settings.SettingsRevisionConflictError
  );
  const after = await auth.getAuthSettingsSnapshot();
  assert.equal(after.generation, next.generation);
  assert.equal(after.settings.password, "test-fixture-hash");
  assert.equal(after.settings.theme, "dark");
});

test("scope grant between regenerate read and SQL mutation cannot disclose a management key", async () => {
  const key = await keys.createApiKey("inference", "management-persistence-machine", []);
  const observed = await keys.getApiKeyById(key.id);
  await keys.updateApiKeyPermissions(key.id, { scopes: ["manage"] });
  await assert.rejects(
    keys.regenerateApiKey(key.id, { expectedScopes: observed!.scopes }),
    ApiKeyScopeConflictError
  );
  assert.equal((await keys.getApiKeyById(key.id))!.key, key.key);
  const regenerated = await keys.regenerateApiKey(key.id, { expectedScopes: ["manage"] });
  assert.ok(regenerated?.key);
  assert.notEqual(regenerated.key, key.key);
});

test("native final authorization observes joined authority and fences before its use update", async () => {
  const generation = randomUUID();
  const row = {
    id: "native",
    name: "native",
    token_hash: "digest",
    sync_api_key_id: null,
    issued_by: "cli",
    expires_at: "2999-01-01T00:00:00Z",
    auth_generation: generation,
    revoked_at: null,
    last_used_at: null,
    created_at: "2026-10-19 00:00:00",
    updated_at: "2026-10-19",
    current_generation: JSON.stringify(generation),
    linked_id: null,
  };
  const statements: Array<{ sql: string; values?: unknown[] }> = [];
  const database = {
    transaction: async (
      body: (tx: {
        query: (sql: string, values?: unknown[]) => Promise<unknown>;
      }) => Promise<unknown>
    ) =>
      body({
        query: async (sql, values) => {
          statements.push({ sql, values });
          return { rows: sql.startsWith("SELECT t.*") ? [row] : [], rowCount: 1 };
        },
      }),
  } as PostgresDatabase;
  assert.ok(await validatePostgresSyncToken(database, "digest", "2026-10-19T01:00:00Z"));
  assert.deepEqual(
    statements.slice(0, 2).map((entry) => entry.values),
    [[3], [5]]
  );
  assert.match(statements[2].sql, /JOIN omniroute.key_value/);
  assert.match(statements[2].sql, /FOR UPDATE OF t$/);
  assert.match(statements[3].sql, /^UPDATE omniroute.sync_tokens/);
  statements.length = 0;
  row.auth_generation = randomUUID();
  assert.equal(await validatePostgresSyncToken(database, "digest", "2026-10-19T01:00:00Z"), null);
  assert.equal(
    statements.some((entry) => entry.sql.startsWith("UPDATE")),
    false
  );
});
