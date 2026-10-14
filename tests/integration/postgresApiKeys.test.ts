import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import type { ApiKeyRepository } from "../../src/domain/persistence/apiKeyRepository.ts";
import type { ApiKeyGroupsRepository } from "../../src/domain/persistence/apiKeyGroupsRepository.ts";
import {
  createPostgresApiKeyRepository,
  type PostgresApiKeyDependencies,
  type PostgresApiKeyEffects,
  PostgresApiKeyEffectsError,
} from "../../src/lib/db/postgres/apiKeyRepository.ts";
import { createPostgresApiKeyGroupsRepository } from "../../src/lib/db/postgres/apiKeyGroupsRepository.ts";
import {
  observeApiKeyPolicyVersion,
  publishApiKeyNoLog,
  readApiKeyPolicyVersion,
} from "../../src/lib/db/postgres/apiKeyPolicyState.ts";
import {
  PostgresError,
  type PostgresDatabase,
  type PostgresTransaction,
  type PostgresTransactionOptions,
} from "../../src/lib/db/postgres/database.ts";
import { deleteApiKeyDependents } from "../../src/lib/db/postgres/controlPlaneEffects.ts";
import { deferArtifactRecovery } from "../../src/lib/db/postgres/artifactRecoveryRepository.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { API_KEY_COLUMN_FALLBACKS } from "../../src/lib/db/apiKeyColumnFallbacks.ts";
import { resolveProviderId } from "../../src/shared/constants/providers.ts";
import { registerApiKeyRepositoryConformance } from "../helpers/persistence/apiKeyRepositoryConformance.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

const noEffects: PostgresApiKeyEffects = {
  setNoLog() {},
  invalidateAuthCaches() {},
  invalidateModelCatalogCache() {},
  invalidateReasoningRoutingRuleCache() {},
  deleteRedisAuthCacheEntries() {},
  logAuditEvent() {},
};
const dependencies: PostgresApiKeyDependencies = {
  effects: noEffects,
  modelPolicy: {
    async preferClaudeCodeForUnprefixedClaudeModels() {
      return false;
    },
    resolveModelAlias() {
      return null;
    },
    async findPublishedModel() {
      return null;
    },
    isPublishedModelHidden() {
      return false;
    },
    async canonicalProviderForAlias(prefix) {
      const canonical = resolveProviderId(prefix);
      return canonical !== prefix ? canonical : null;
    },
  },
  async deleteDependents() {},
};
process.env.API_KEY_SECRET ||= "isolated-api-key-conformance-test-secret";

test("privacy publication orders each durable authority independently", () => {
  const authorityA = randomUUID(),
    authorityB = randomUUID();
  const values = new Map<string, boolean>();
  const effects = {
    setNoLog(id: string, value: boolean) {
      values.set(id, value);
    },
  };
  assert.equal(
    observeApiKeyPolicyVersion(effects, { authorityId: authorityA, generation: "7" }),
    true
  );
  assert.equal(
    observeApiKeyPolicyVersion(effects, { authorityId: authorityB, generation: "7" }),
    true
  );
  assert.equal(
    observeApiKeyPolicyVersion(effects, { authorityId: authorityA, generation: "8" }),
    true
  );
  assert.equal(
    observeApiKeyPolicyVersion(effects, { authorityId: authorityB, generation: "7" }),
    true
  );
  assert.equal(
    observeApiKeyPolicyVersion(effects, { authorityId: authorityB, generation: "6" }),
    false
  );
  assert.equal(
    publishApiKeyNoLog(effects, { authorityId: authorityA, generation: "8" }, "key", true),
    true
  );
  assert.equal(
    publishApiKeyNoLog(effects, { authorityId: authorityB, generation: "7" }, "key", false),
    true
  );
  assert.equal(values.get("key"), false, "unrelated authorities have no shared epoch ordering");
});

test("unrelated policy observations do not suppress needed privacy publication", () => {
  const authorityId = randomUUID();
  const values = new Map<string, boolean>();
  const effects = {
    setNoLog(id: string, value: boolean) {
      values.set(id, value);
    },
  };
  observeApiKeyPolicyVersion(effects, { authorityId, generation: "2" });
  assert.equal(publishApiKeyNoLog(effects, { authorityId, generation: "1" }, "A", true), true);
  publishApiKeyNoLog(effects, { authorityId, generation: "2" }, "B", true);
  assert.equal(values.get("A"), true);
  assert.equal(publishApiKeyNoLog(effects, { authorityId, generation: "1" }, "B", false), false);
  assert.equal(values.get("B"), true);
});

test("explicit privacy authority joins wrappers and failed setters do not advance publication marks", () => {
  const privacyAuthority = {},
    authorityId = randomUUID();
  let value = false;
  const first = {
    privacyAuthority,
    setNoLog(_id: string, next: boolean) {
      value = next;
    },
  };
  const second = {
    privacyAuthority,
    setNoLog(_id: string, next: boolean) {
      value = next;
    },
  };
  assert.notEqual(first.setNoLog, second.setNoLog);
  publishApiKeyNoLog(second, { authorityId, generation: "2" }, "key", true);
  assert.equal(publishApiKeyNoLog(first, { authorityId, generation: "1" }, "key", false), false);
  assert.equal(value, true);
  const rejected = {
    privacyAuthority,
    setNoLog() {
      throw new Error("isolated setter rejection");
    },
  };
  assert.throws(() => publishApiKeyNoLog(rejected, { authorityId, generation: "4" }, "key", false));
  assert.equal(publishApiKeyNoLog(first, { authorityId, generation: "3" }, "key", true), true);
});

/** Delay a native privacy readback only after its real transaction acknowledged COMMIT. */
function delayPrivacyReadback(database: PostgresDatabase) {
  let release!: () => void, started!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const ready = new Promise<void>((resolve) => {
    started = resolve;
  });
  let pause = true;
  const intercepted = Object.create(database) as PostgresDatabase;
  intercepted.transaction = async (body) => {
    const statements: string[] = [];
    const result = await database.transaction((tx) =>
      body({
        async query(text, values) {
          statements.push(text.replace(/\s+/g, " ").trim());
          return tx.query(text, values);
        },
      })
    );
    const rowRead = statements.some(
      (sql) => sql.startsWith("SELECT ") && sql.includes("FROM omniroute.api_keys WHERE id=")
    );
    const versionRead = statements.some(
      (sql) => sql.startsWith("SELECT ") && sql.includes("FROM omniroute.api_key_policy_epoch")
    );
    const writes = statements.some((sql) =>
      /^(UPDATE|INSERT|DELETE|TRUNCATE|ALTER|CREATE|DROP)\b/.test(sql)
    );
    if (pause && rowRead && versionRead && !writes) {
      pause = false;
      started();
      await gate;
    }
    return result;
  };
  return { database: intercepted, ready, release };
}

function asynchronousAdapter<T>(module: Record<string, unknown>): T {
  return Object.fromEntries(
    Object.entries(module)
      .filter(([, value]) => typeof value === "function")
      .map(([name, value]) => [
        name,
        async (...args: unknown[]) =>
          Reflect.apply(value as (...args: unknown[]) => unknown, undefined, args),
      ])
  ) as T;
}

test.describe("SQLite API-key repository conformance", () => {
  const directory = mkdtempSync(join(tmpdir(), "omniroute-key-conformance-"));
  const original = process.env.DATA_DIR;
  const originalBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
  let core: typeof import("../../src/lib/db/core.ts");
  let apiKeys: typeof import("../../src/lib/db/apiKeys.ts");
  let groups: typeof import("../../src/lib/db/apiKeyGroups.ts");
  test.before(async () => {
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    process.env.DATA_DIR = directory;
    core = await import("../../src/lib/db/core.ts");
    apiKeys = await import("../../src/lib/db/apiKeys.ts");
    groups = await import("../../src/lib/db/apiKeyGroups.ts");
  });
  test.after(() => {
    core?.resetDbInstance();
    if (originalBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = originalBackend;
    if (original === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = original;
    rmSync(directory, { recursive: true, force: true });
  });
  registerApiKeyRepositoryConformance(async () => ({
    keys: asynchronousAdapter<ApiKeyRepository>(apiKeys),
    groups: asynchronousAdapter<ApiKeyGroupsRepository>(groups),
    async reset() {
      apiKeys.resetApiKeyState();
      core.resetDbInstance();
      rmSync(directory, { recursive: true, force: true });
      mkdirSync(directory, { recursive: true });
      core.getDbInstance();
    },
  }));
});

if (!postgresTestsEnabled) {
  test(
    "PostgreSQL API-key repositories require the isolated integration runner",
    { skip: true },
    () => {}
  );
} else {
  test.describe("PostgreSQL API-key repository conformance", () => {
    let database: PostgresDatabase, keys: ApiKeyRepository, groups: ApiKeyGroupsRepository;
    async function reset() {
      await database.transaction(async (tx) => {
        await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
        await tx.query(
          "TRUNCATE omniroute.api_keys,omniroute.key_groups,omniroute.group_model_permissions,omniroute.key_group_members RESTART IDENTITY CASCADE"
        );
        await tx.query(
          "UPDATE omniroute.api_key_policy_epoch SET generation=generation+1 WHERE singleton"
        );
      });
    }
    test.before(async () => {
      database = await createTestPostgresDatabase({ maxConnections: 8, lockTimeoutMs: 2_000 });
      await migratePostgresSchema(database);
      keys = createPostgresApiKeyRepository(database, dependencies);
      groups = createPostgresApiKeyGroupsRepository(database, noEffects);
    });
    test.after(async () => {
      await database?.close();
    });
    registerApiKeyRepositoryConformance(async () => ({ keys, groups, reset }));

    test("native API-key schema preserves every current SQLite key column", async () => {
      const { rows } = await database.transaction((tx) =>
        tx.query<{ column_name: string }>(
          "SELECT column_name FROM information_schema.columns WHERE table_schema='omniroute' AND table_name='api_keys'"
        )
      );
      const expected = new Set([
        "id",
        "name",
        "key",
        "machine_id",
        "created_at",
        ...API_KEY_COLUMN_FALLBACKS.map((column) => column.name),
      ]);
      assert.deepEqual(
        rows
          .map((row) => row.column_name)
          .filter((column) => column !== "insertion_order")
          .sort(),
        [...expected].sort()
      );
    });

    test("a late authentication snapshot cannot authorize a revoked credential", async () => {
      await reset();
      const created = await keys.createApiKey("late-auth", "machine");
      let release!: () => void, started!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const ready = new Promise<void>((resolve) => {
        started = resolve;
      });
      let pause = true;
      const intercepted = Object.create(database) as PostgresDatabase;
      intercepted.transaction = async (body) => {
        const statements: string[] = [];
        const result = await database.transaction((tx) =>
          body({
            async query(text, values) {
              statements.push(text);
              return tx.query(text, values);
            },
          })
        );
        if (pause && statements.some((sql) => sql.includes("WHERE key=$1 OR key_hash=$2"))) {
          pause = false;
          started();
          await gate;
        }
        return result;
      };
      const reader = createPostgresApiKeyRepository(intercepted, dependencies);
      const writer = createPostgresApiKeyRepository(intercepted, dependencies);
      const pending = reader.validateApiKey(created.key);
      try {
        await ready;
        await writer.revokeApiKey(created.id);
        release();
        assert.equal(await pending, false);
      } finally {
        release();
        await pending;
      }
    });

    test("published model resolution retains effort-base denials, hidden aliases and Claude wildcard exemption", async () => {
      await reset();
      const created = await keys.createApiKey("published", "machine");
      await keys.updateApiKeyPermissions(created.id, {
        disableNonPublicModels: true,
        blockedModels: ["codex/base"],
      });
      const hidden: string[][] = [];
      const specialized = createPostgresApiKeyRepository(database, {
        ...dependencies,
        modelPolicy: {
          ...dependencies.modelPolicy,
          async preferClaudeCodeForUnprefixedClaudeModels() {
            return true;
          },
          async findPublishedModel(provider, model) {
            return {
              providerId: resolveProviderId(provider),
              publishedModelId: model === "base-high" ? "base" : model,
            };
          },
          isPublishedModelHidden(provider, alias, model) {
            hidden.push([provider, alias, model]);
            return model === "hidden";
          },
        },
      });
      assert.equal(await specialized.isModelAllowedForKey(created.key, "cx/base-high"), false);
      assert.equal(await specialized.isModelAllowedForKey(created.key, "cx/hidden"), false);
      assert.deepEqual(hidden.at(-1), ["codex", "cx", "hidden"]);
      await keys.updateApiKeyPermissions(created.id, {
        allowedModels: ["cc/*"],
        blockedModels: [],
      });
      assert.equal(await specialized.isModelAllowedForKey(created.key, "sonnet"), true);
    });

    async function queued<T>(operations: () => Promise<T>[], expected: number): Promise<T[]> {
      let release!: () => void, ready!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const acquired = new Promise<void>((resolve) => {
        ready = resolve;
      });
      const owner = database.transaction(async (tx) => {
        await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
        ready();
        await gate;
      });
      let pending: Promise<T>[] = [];
      try {
        await acquired;
        pending = operations();
        let count = 0;
        for (let attempt = 0; attempt < 100 && count < expected; attempt++) {
          const { rows } = await database.transaction((tx) =>
            tx.query<{ count: string }>(
              `SELECT count(*) AS count FROM pg_locks WHERE locktype='advisory' AND classid=18765 AND objid=5 AND mode='ExclusiveLock' AND NOT granted`
            )
          );
          count = Number(rows[0].count);
          if (count < expected) await delay(10);
        }
        assert.ok(count >= expected, "contenders reach the PostgreSQL lock barrier");
        release();
        await owner;
        return await Promise.all(pending);
      } finally {
        release();
        await owner;
        await Promise.allSettled(pending);
      }
    }

    test("concurrent permission writes preserve independent columns and exact previous scopes", async () => {
      await reset();
      const created = await keys.createApiKey("concurrent", "machine");
      const audit: Array<{ scopes: unknown; previous: unknown }> = [];
      const active = createPostgresApiKeyRepository(database, {
        ...dependencies,
        effects: {
          ...noEffects,
          logAuditEvent(event) {
            if (event.details?.scopes)
              audit.push({ scopes: event.details.scopes, previous: event.details.previous });
          },
        },
      });
      await queued(
        () => [
          active.updateApiKeyPermissions(created.id, { scopes: ["manage"], noLog: true }),
          active.updateApiKeyPermissions(created.id, {
            scopes: ["self:usage"],
            compressionEnabled: false,
          }),
        ],
        2
      );
      const metadata = await keys.getApiKeyMetadata(created.key);
      assert.ok(metadata);
      assert.equal(metadata.noLog, true);
      assert.equal(metadata.compressionEnabled, false);
      assert.equal(audit.length, 2);
      const epochs = new Set(audit.map((event) => JSON.stringify(event)));
      assert.ok(
        epochs.has(JSON.stringify({ scopes: ["manage"], previous: [] })) ||
          epochs.has(JSON.stringify({ scopes: ["self:usage"], previous: [] }))
      );
      const final = audit.find(
        (event) => JSON.stringify(event.scopes) === JSON.stringify(metadata.scopes)
      );
      assert.ok(final);
      assert.notDeepEqual(final.previous, []);
    });

    test("concurrent rotations invalidate both retired credentials without replay", async () => {
      await reset();
      const created = await keys.createApiKey("rotate", "machine");
      const removed: string[] = [];
      const rotating = createPostgresApiKeyRepository(database, {
        ...dependencies,
        effects: {
          ...noEffects,
          deleteRedisAuthCacheEntries(...hashes) {
            removed.push(...hashes);
          },
        },
      });
      const results = await queued(
        () => [rotating.regenerateApiKey(created.id), rotating.regenerateApiKey(created.id)],
        2
      );
      assert.ok(results[0]);
      assert.ok(results[1]);
      assert.notEqual(results[0].key, results[1].key);
      const current = await keys.getApiKeyById(created.id);
      assert.ok(current);
      assert.equal(await keys.validateApiKey(created.key), false);
      for (const result of results)
        assert.equal(await keys.validateApiKey(result!.key), result!.key === current.key);
      assert.equal(
        new Set(removed).size,
        3,
        "both retired hashes plus final hash must be invalidated"
      );
    });

    test("revoke wins over a late policy/catalog read across independent repository instances", async () => {
      await reset();
      const created = await keys.createApiKey("late", "machine");
      await keys.updateApiKeyPermissions(created.id, { disableNonPublicModels: true });
      let release!: () => void, started!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const ready = new Promise<void>((resolve) => {
        started = resolve;
      });
      const reader = createPostgresApiKeyRepository(database, {
        ...dependencies,
        modelPolicy: {
          ...dependencies.modelPolicy,
          async findPublishedModel() {
            started();
            await gate;
            return { providerId: "openai", publishedModelId: "model" };
          },
        },
      });
      const pending = reader.isModelAllowedForKey(created.key, "openai/model");
      try {
        await ready;
        assert.equal(await keys.revokeApiKey(created.id), true);
        release();
        assert.equal(
          await pending,
          false,
          "a stale allow result must fail the durable epoch fence"
        );
        assert.equal(await reader.validateApiKey(created.key), false);
        assert.equal((await reader.getApiKeyMetadata(created.key))?.isActive, false);
      } finally {
        release();
        await pending;
      }
    });

    test("group policy changes invalidate a late model permission read", async () => {
      await reset();
      const created = await keys.createApiKey("late-group", "machine");
      const group = await groups.createKeyGroup("group");
      await groups.addKeyToGroup(created.id, group.id);
      await groups.addGroupPermission(group.id, "*", "allow");
      await keys.updateApiKeyPermissions(created.id, { disableNonPublicModels: true });
      let release!: () => void, started!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const ready = new Promise<void>((resolve) => {
        started = resolve;
      });
      const reader = createPostgresApiKeyRepository(database, {
        ...dependencies,
        modelPolicy: {
          ...dependencies.modelPolicy,
          async findPublishedModel() {
            started();
            await gate;
            return { providerId: "openai", publishedModelId: "model" };
          },
        },
      });
      const pending = reader.isModelAllowedForKey(created.key, "openai/model");
      try {
        await ready;
        await groups.addGroupPermission(group.id, "*", "deny");
        release();
        assert.equal(await pending, false);
      } finally {
        release();
        await pending;
      }
    });

    test("same-transaction cleanup failure rolls back key, groups and generation without effects", async () => {
      await reset();
      const created = await keys.createApiKey("atomic-delete", "machine");
      const group = await groups.createKeyGroup("group");
      await groups.addKeyToGroup(created.id, group.id);
      const epoch = await database.transaction((tx) =>
        tx.query("SELECT generation FROM omniroute.api_key_policy_epoch")
      );
      const effects: string[] = [];
      const broken = createPostgresApiKeyRepository(database, {
        ...dependencies,
        effects: {
          ...noEffects,
          invalidateAuthCaches() {
            effects.push("auth");
          },
          setNoLog() {
            effects.push("nolog");
          },
        },
        async deleteDependents(tx) {
          await tx.query("SELECT definitely_unsupported_sqlite_function()");
        },
      });
      await assert.rejects(
        broken.deleteApiKey(created.id),
        (error: PostgresError) => error.kind === "query_failed" && error.sqlState === "42883"
      );
      assert.ok(await keys.getApiKeyById(created.id));
      assert.equal((await groups.getGroupMembers(group.id)).length, 1);
      assert.deepEqual(effects, []);
      assert.deepEqual(
        (
          await database.transaction((tx) =>
            tx.query("SELECT generation FROM omniroute.api_key_policy_epoch")
          )
        ).rows,
        epoch.rows
      );
    });

    test("database rejection trigger rolls back permission writes and policy generation", async () => {
      await reset();
      const created = await keys.createApiKey("trigger", "machine");
      await database.transaction(async (tx) => {
        await tx.query(
          `CREATE OR REPLACE FUNCTION omniroute.reject_api_key_update() RETURNS trigger LANGUAGE plpgsql AS $body$ BEGIN RAISE EXCEPTION 'isolated rejection' USING ERRCODE='23514'; END $body$`
        );
        await tx.query(
          "CREATE TRIGGER reject_api_key_update BEFORE UPDATE ON omniroute.api_keys FOR EACH ROW EXECUTE FUNCTION omniroute.reject_api_key_update()"
        );
      });
      const epoch = (
        await database.transaction((tx) =>
          tx.query("SELECT generation FROM omniroute.api_key_policy_epoch")
        )
      ).rows;
      try {
        await assert.rejects(
          keys.updateApiKeyPermissions(created.id, { noLog: true, scopes: ["manage"] }),
          (error: PostgresError) => error.sqlState === "23514"
        );
        const metadata = await keys.getApiKeyMetadata(created.key);
        assert.equal(metadata?.noLog, false);
        assert.deepEqual(metadata?.scopes, []);
        assert.deepEqual(
          (
            await database.transaction((tx) =>
              tx.query("SELECT generation FROM omniroute.api_key_policy_epoch")
            )
          ).rows,
          epoch
        );
      } finally {
        await database.transaction((tx) =>
          tx.query("DROP TRIGGER reject_api_key_update ON omniroute.api_keys")
        );
        await database.transaction((tx) =>
          tx.query("DROP FUNCTION omniroute.reject_api_key_update()")
        );
      }
    });

    test("delayed committed effects refresh no-log from the newer policy authority", async () => {
      await reset();
      const created = await keys.createApiKey("ordered-privacy", "machine");
      let release!: () => void, started!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const ready = new Promise<void>((resolve) => {
        started = resolve;
      });
      const observed: boolean[] = [];
      const slow = createPostgresApiKeyRepository(database, {
        ...dependencies,
        effects: {
          ...noEffects,
          async invalidateAuthCaches() {
            started();
            await gate;
          },
          setNoLog(_id, value) {
            observed.push(value);
          },
        },
      });
      const fast = createPostgresApiKeyRepository(database, {
        ...dependencies,
        effects: {
          ...noEffects,
          setNoLog(_id, value) {
            observed.push(value);
          },
        },
      });
      const pending = slow.updateApiKeyPermissions(created.id, { noLog: true });
      try {
        await ready;
        await fast.updateApiKeyPermissions(created.id, { noLog: false });
        release();
        assert.equal(await pending, true);
        assert.deepEqual(
          observed,
          [false, false],
          "a delayed old receipt must publish current state"
        );
      } finally {
        release();
        await pending;
      }
    });

    test("independent pools reject a late false no-log readback after a newer true publication", async () => {
      await reset();
      const created = await keys.createApiKey("late-privacy", "machine");
      const second = await createTestPostgresDatabase();
      assert.notEqual(second, database);
      assert.deepEqual(
        await second.transaction(readApiKeyPolicyVersion),
        await database.transaction(readApiKeyPolicyVersion)
      );
      const delayed = delayPrivacyReadback(database);
      const observed: boolean[] = [];
      let noLog = false;
      const effects = {
        ...noEffects,
        setNoLog(_id: string, value: boolean) {
          noLog = value;
          observed.push(value);
        },
      };
      const slow = createPostgresApiKeyRepository(delayed.database, { ...dependencies, effects });
      const fast = createPostgresApiKeyRepository(second, { ...dependencies, effects });
      const pending = slow.updateApiKeyPermissions(created.id, { noLog: false });
      try {
        await delayed.ready;
        await fast.updateApiKeyPermissions(created.id, { noLog: true });
        assert.equal(noLog, true);
        delayed.release();
        assert.equal(await pending, true);
        assert.deepEqual(
          observed,
          [true],
          "the retired false snapshot cannot clear the shared privacy cache"
        );
        assert.equal(noLog, true);
      } finally {
        delayed.release();
        await Promise.allSettled([pending]);
        await second.close();
      }
    });

    test("independent pools preserve privacy publication for an unrelated key with an older global epoch", async () => {
      await reset();
      const keyA = await keys.createApiKey("privacy-A", "machine");
      const keyB = await keys.createApiKey("privacy-B", "machine");
      const second = await createTestPostgresDatabase();
      const delayed = delayPrivacyReadback(database);
      const values = new Map<string, boolean>();
      const effects = {
        ...noEffects,
        setNoLog(id: string, value: boolean) {
          values.set(id, value);
        },
      };
      const slow = createPostgresApiKeyRepository(delayed.database, { ...dependencies, effects });
      const fast = createPostgresApiKeyRepository(second, { ...dependencies, effects });
      const pending = slow.updateApiKeyPermissions(keyA.id, { noLog: true });
      try {
        await delayed.ready;
        await fast.updateApiKeyPermissions(keyB.id, { noLog: true });
        assert.equal(values.get(keyB.id), true);
        delayed.release();
        assert.equal(await pending, true);
        assert.equal(
          values.get(keyA.id),
          true,
          "B's publication never proves A's privacy state was published"
        );
        assert.equal(values.get(keyB.id), true);
      } finally {
        delayed.release();
        await Promise.allSettled([pending]);
        await second.close();
      }
    });

    test("all committed effects run despite failures and never claim rollback", async () => {
      await reset();
      const created = await keys.createApiKey("effects", "machine");
      const observed: string[] = [];
      const failed = createPostgresApiKeyRepository(database, {
        ...dependencies,
        effects: {
          setNoLog() {
            observed.push("no_log");
          },
          invalidateAuthCaches() {
            observed.push("auth");
            throw new Error("isolated cache failure");
          },
          invalidateModelCatalogCache() {
            observed.push("catalog");
          },
          invalidateReasoningRoutingRuleCache() {
            observed.push("reasoning");
          },
          deleteRedisAuthCacheEntries() {
            observed.push("redis");
            throw new Error("isolated redis failure");
          },
          logAuditEvent() {
            observed.push("audit");
          },
        },
      });
      await assert.rejects(
        failed.updateApiKeyPermissions(created.id, {
          noLog: true,
          modelAccessMode: "restricted",
          scopes: ["manage"],
        }),
        (error: PostgresApiKeyEffectsError) =>
          error.committed &&
          error.failedEffects.includes("auth_caches") &&
          error.failedEffects.includes("redis_auth")
      );
      assert.deepEqual(observed, ["auth", "no_log", "catalog", "redis", "audit"]);
      const metadata = await keys.getApiKeyMetadata(created.key);
      assert.equal(metadata?.noLog, true);
      assert.deepEqual(metadata?.scopes, ["manage"]);
    });

    test("key deletion acquires accounting before artifact revocation triggers", async (t) => {
      await reset();
      const created = await keys.createApiKey("delete-fence", "machine");
      const reference = "delete-fence-" + randomUUID();
      const id = "delete-fence-" + randomUUID();
      await database.transaction(async (tx) => {
        await tx.query(
          "INSERT INTO omniroute.call_logs(id,timestamp,api_key_id,detail_state,detail_reason) VALUES($1,$2,$3,'missing','publication_pending')",
          [id, new Date().toISOString(), created.id]
        );
        await tx.query(
          "INSERT INTO omniroute.artifact_deletion_queue(relpath,publication_state,call_log_id,call_log_order) SELECT $1,'publishing',id,insertion_order FROM omniroute.call_logs WHERE id=$2",
          [reference, id]
        );
        await tx.query(
          "INSERT INTO omniroute.domain_budgets(api_key_id,daily_limit_usd) VALUES($1,1)",
          [created.id]
        );
        await tx.query(
          "INSERT INTO omniroute.domain_cost_history(api_key_id,cost,timestamp) VALUES($1,0.125,$2)",
          [created.id, Date.now()]
        );
      });
      const other = await createTestPostgresDatabase();
      const deleting = createPostgresApiKeyRepository(other, {
        ...dependencies,
        deleteDependents: deleteApiKeyDependents,
      });
      let held!: () => void, release!: () => void;
      const entered = new Promise<void>((resolve) => {
        held = resolve;
      });
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      let holderPid = 0;
      const original = database.transaction;
      t.mock.method(database, "transaction", function <
        T,
      >(this: PostgresDatabase, body: (tx: PostgresTransaction) => Promise<T>, options?: PostgresTransactionOptions): Promise<T> {
        return original.call(
          this,
          async (tx: PostgresTransaction) =>
            body({
              noteAdvisoryLock: (owner, mode) => tx.noteAdvisoryLock?.(owner, mode),
              async query<Row>(text: string, values?: unknown[]) {
                if (
                  options?.operation === "artifact.defer" &&
                  text.startsWith("UPDATE omniroute.artifact_deletion_queue")
                ) {
                  holderPid = (await tx.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))
                    .rows[0].pid;
                  held();
                  await gate;
                }
                return tx.query<Row>(text, values);
              },
            }),
          options
        ) as Promise<T>;
      });
      const deferred = deferArtifactRecovery(database, reference);
      void deferred.catch(() => {});
      let deletion: Promise<boolean> | undefined;
      try {
        await Promise.race([entered, deferred]);
        deletion = deleting.deleteApiKey(created.id);
        void deletion.catch(() => {});
        let waiting = false;
        const deadline = performance.now() + 700;
        while (!waiting && performance.now() < deadline) {
          waiting = (
            await database.transaction((tx) =>
              tx.query<{ present: boolean }>(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND wait_event='advisory' AND $1=ANY(pg_blocking_pids(pid))) AS present",
                [holderPid]
              )
            )
          ).rows[0].present;
          if (!waiting) await delay(10);
        }
        assert.equal(waiting, true, "key deletion must actually wait on the accounting owner");
        release();
        await deferred;
        assert.equal(await deletion, true);
        const state = await database.transaction((tx) =>
          tx.query(
            `SELECT c.detail_state,q.publication_state,
            (SELECT count(*) FROM omniroute.api_keys WHERE id=$2)::text AS keys,
            (SELECT count(*) FROM omniroute.domain_budgets WHERE api_key_id=$2)::text AS budgets,
            (SELECT count(*) FROM omniroute.domain_cost_history WHERE api_key_id=$2)::text AS costs
           FROM omniroute.call_logs c JOIN omniroute.artifact_deletion_queue q ON q.call_log_id=c.id
           WHERE c.id=$1`,
            [id, created.id]
          )
        );
        assert.deepEqual(state.rows, [
          {
            detail_state: "none",
            publication_state: "delete_pending",
            keys: "0",
            budgets: "0",
            costs: "0",
          },
        ]);
      } finally {
        release();
        await Promise.allSettled([deferred, ...(deletion ? [deletion] : [])]);
        await other.close();
        await database.transaction(async (tx) => {
          await tx.query("DELETE FROM omniroute.call_logs WHERE id=$1", [id]);
          await tx.query("DELETE FROM omniroute.artifact_deletion_queue WHERE relpath=$1", [
            reference,
          ]);
        });
      }
    });

    test("native groups preserve malformed wildcard literals, cascading deletion and missing membership", async () => {
      await reset();
      const created = await keys.createApiKey("group-edge", "machine");
      const group = await groups.createKeyGroup("group");
      assert.equal(await groups.addKeyToGroup(created.id, "missing"), false);
      assert.equal(await groups.addKeyToGroup(created.id, group.id), true);
      await groups.addGroupPermission(group.id, "gpt-4(*", "allow");
      assert.equal((await groups.checkKeyModelAccess(created.id, "gpt-4(x")).allowed, true);
      assert.equal((await groups.checkKeyModelAccess(created.id, "gpt-4ox")).allowed, false);
      assert.equal(await keys.deleteApiKey(created.id), true);
      assert.deepEqual(await groups.getGroupMembers(group.id), []);
    });
  });
}
