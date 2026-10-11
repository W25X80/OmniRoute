import assert from "node:assert/strict";
import { setTimeout as delay, setImmediate as nextTurn } from "node:timers/promises";
import test from "node:test";

import type { SettingsRepository } from "../../src/domain/persistence/settingsRepository.ts";
import {
  advanceApiKeyPolicyEpoch,
  lockApiKeyPolicy,
  readApiKeyPolicyEpoch,
} from "../../src/lib/db/postgres/apiKeyPolicyState.ts";
import { PostgresError, type PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createPostgresSettingsRepository,
  PostgresSettingsEffectsError,
  type PostgresSettingsEffects,
} from "../../src/lib/db/postgres/settingsRepository.ts";
import {
  SETTINGS_REVISION_KEY,
  SettingsRevisionConflictError,
  type SettingsEnvironment,
} from "../../src/lib/db/settings/persistenceSemantics.ts";
import { registerSettingsRepositoryConformance } from "../helpers/persistence/settingsRepositoryConformance.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

const noEffects: PostgresSettingsEffects = {
  invalidateSettingsCache() {},
  bumpProxyConfigGeneration() {},
  applyRuntimeSettings() {},
  scheduleCatalogRevalidationAfterInit() {},
};
let database: PostgresDatabase;
let settings: SettingsRepository;
let environment: SettingsEnvironment = {};
const originalEncryptionKey = process.env.STORAGE_ENCRYPTION_KEY;

function repository(effects: PostgresSettingsEffects = noEffects): SettingsRepository {
  return createPostgresSettingsRepository(database, effects, { environment: () => environment });
}

async function resetStorage(): Promise<void> {
  environment = {};
  await database.transaction(async (transaction) => {
    await transaction.query("SELECT pg_advisory_xact_lock(18765, 3)");
    await transaction.query("DELETE FROM omniroute.key_value WHERE namespace = 'settings'");
  });
}

async function readRaw(key: string): Promise<string | null> {
  const { rows } = await database.transaction((transaction) =>
    transaction.query<{ value: string }>(
      "SELECT value FROM omniroute.key_value WHERE namespace = 'settings' AND key = $1",
      [key]
    )
  );
  return rows[0]?.value ?? null;
}

async function putRaw(key: string, value: string): Promise<void> {
  await database.transaction((transaction) =>
    transaction.query(
      `INSERT INTO omniroute.key_value(namespace, key, value) VALUES ('settings', $1, $2)
       ON CONFLICT (namespace, key) DO UPDATE SET value = EXCLUDED.value`,
      [key, value]
    )
  );
}

async function queuedUpdates<T>(
  operations: () => Promise<T>[]
): Promise<PromiseSettledResult<T>[]> {
  let release!: () => void;
  let acquired!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const ready = new Promise<void>((resolve) => {
    acquired = resolve;
  });
  const owner = database.transaction(async (transaction) => {
    await transaction.query("SELECT pg_advisory_xact_lock(18765, 3)");
    acquired();
    await gate;
  });
  let pending: Promise<T>[] = [];
  try {
    await ready;
    pending = operations();
    // Attach rejection handlers before releasing either contender.
    const settled = Promise.allSettled(pending);
    let blocked = 0;
    for (let attempt = 0; attempt < 50 && blocked < pending.length; attempt++) {
      const { rows } = await database.transaction((transaction) =>
        transaction.query<{ count: string }>(
          `SELECT count(*) AS count FROM pg_locks WHERE locktype = 'advisory'
           AND classid = 18765 AND objid = 3 AND mode = 'ExclusiveLock' AND NOT granted`
        )
      );
      blocked = Number(rows[0].count);
      if (blocked < pending.length) await delay(10);
    }
    assert.equal(blocked, pending.length, "every contender reached the PostgreSQL lock barrier");
    release();
    await owner;
    return await settled;
  } finally {
    release();
    await owner;
    await Promise.allSettled(pending);
  }
}

async function installWriteRejection(): Promise<void> {
  await database.transaction(async (transaction) => {
    await transaction.query(`CREATE FUNCTION omniroute.reject_settings_write_test() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test settings rejection'; END $$`);
    await transaction.query(
      `CREATE TRIGGER reject_settings_write_test BEFORE INSERT OR UPDATE
       ON omniroute.key_value FOR EACH ROW
       WHEN (NEW.namespace = 'settings' AND NEW.key IN ('blocked', 'requireLogin'))
       EXECUTE FUNCTION omniroute.reject_settings_write_test()`
    );
  });
}

async function removeWriteRejection(): Promise<void> {
  await database.transaction(async (transaction) => {
    await transaction.query("DROP TRIGGER reject_settings_write_test ON omniroute.key_value");
    await transaction.query("DROP FUNCTION omniroute.reject_settings_write_test()");
  });
}

if (!postgresTestsEnabled) {
  test("PostgreSQL settings require the isolated integration runner", { skip: true }, () => {});
} else {
  test.before(async () => {
    process.env.STORAGE_ENCRYPTION_KEY = "settings-conformance-test-only-encryption-key";
    database = await createTestPostgresDatabase({ maxConnections: 6, lockTimeoutMs: 2_000 });
    await migratePostgresSchema(database);
    settings = repository();
  });
  test.after(async () => {
    await database?.close();
    if (originalEncryptionKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
    else process.env.STORAGE_ENCRYPTION_KEY = originalEncryptionKey;
  });

  registerSettingsRepositoryConformance(async () => ({
    settings,
    reset: resetStorage,
    setEnvironment(next) {
      environment = next;
    },
    putRaw,
    readRaw,
  }));

  test("PostgreSQL settings CAS contenders conflict after reaching the same real lock", async () => {
    await resetStorage();
    const attempted: string[] = [];
    const contender = repository({
      ...noEffects,
      invalidateSettingsCache() {
        attempted.push("settings_cache");
      },
    });
    const results = await queuedUpdates(() => [
      contender.update({ marker: "first" }, { expectedRevision: 0 }),
      contender.update({ marker: "second" }, { expectedRevision: 0 }),
    ]);
    const accepted = results.filter((result) => result.status === "fulfilled");
    const rejected = results.filter((result) => result.status === "rejected");
    assert.equal(accepted.length, 1);
    assert.equal(rejected.length, 1);
    assert.ok(rejected[0].reason instanceof SettingsRevisionConflictError);
    assert.equal(rejected[0].reason.currentRevision, 1);
    assert.equal(await settings.getRevision(), 1);
    assert.equal((await settings.get()).marker, accepted[0].value.marker);
    assert.deepEqual(attempted, ["settings_cache"]);
  });

  test("PostgreSQL settings advance the composed policy epoch atomically and never on CAS conflict", async () => {
    await resetStorage();
    const readEpoch = () => database.transaction(readApiKeyPolicyEpoch);
    const initialEpoch = BigInt(await readEpoch());
    const subject = createPostgresSettingsRepository(database, noEffects, {
      environment: () => environment,
      async markPolicyChanged(transaction) {
        await lockApiKeyPolicy(transaction);
        await advanceApiKeyPolicyEpoch(transaction);
      },
    });
    const aliases = { "short-name": "openai/gpt-4o" };
    await subject.update({ modelAliases: aliases }, { expectedRevision: 0 });
    assert.equal(BigInt(await readEpoch()), initialEpoch + 1n);
    await assert.rejects(
      subject.update({ modelAliases: {} }, { expectedRevision: 0 }),
      SettingsRevisionConflictError
    );
    assert.equal(BigInt(await readEpoch()), initialEpoch + 1n);
    await installWriteRejection();
    try {
      await assert.rejects(
        subject.update({ modelAliases: {}, blocked: true }, { expectedRevision: 1 }),
        (error: PostgresError) => error.sqlState === "P0001"
      );
      assert.equal(BigInt(await readEpoch()), initialEpoch + 1n);
      assert.equal(await subject.getRevision(), 1);
      assert.deepEqual((await subject.get()).modelAliases, aliases);
    } finally {
      await removeWriteRejection();
    }
  });

  test("PostgreSQL settings committed cache failures attempt independent effects and cannot replay", async () => {
    await resetStorage();
    const attempted: string[] = [];
    const subject = repository({
      async invalidateSettingsCache() {
        assert.equal(await readRaw("proxyEnabled"), "false");
        assert.equal(await readRaw(SETTINGS_REVISION_KEY), "1");
        attempted.push("settings_cache");
        throw new Error("private callback details");
      },
      async bumpProxyConfigGeneration() {
        assert.equal(await readRaw("setupComplete"), "true");
        attempted.push("proxy_config");
        throw new Error("private callback details");
      },
      applyRuntimeSettings(next, options) {
        assert.equal(next.proxyEnabled, false);
        assert.equal(options.source, "settings:update");
        attempted.push("runtime");
        throw new Error("private callback details");
      },
      scheduleCatalogRevalidationAfterInit() {
        attempted.push("catalog");
        throw new Error("private callback details");
      },
    });
    await assert.rejects(
      subject.update({ setupComplete: true, proxyEnabled: false }, { expectedRevision: 0 }),
      (error: PostgresSettingsEffectsError) => {
        assert.equal(error.committed, true);
        assert.deepEqual(error.failedEffects, ["settings_cache", "proxy_config"]);
        assert.equal(error.cause, undefined);
        assert.doesNotMatch(error.message, /private callback/);
        return true;
      }
    );
    await nextTurn();
    assert.deepEqual(attempted, ["settings_cache", "proxy_config", "runtime", "catalog"]);
    assert.equal(await settings.getRevision(), 1);
    assert.equal((await settings.get()).setupComplete, true);
    await assert.rejects(
      subject.update({ proxyEnabled: true }, { expectedRevision: 0 }),
      SettingsRevisionConflictError
    );
    assert.equal(await readRaw("proxyEnabled"), "false");
    assert.equal(attempted.length, 4);
  });

  test("PostgreSQL settings hot reload and first-setup scheduling remain best effort", async () => {
    await resetStorage();
    const attempted: string[] = [];
    const subject = repository({
      ...noEffects,
      applyRuntimeSettings() {
        attempted.push("runtime");
        return Promise.reject(new Error("private hot-reload failure"));
      },
      scheduleCatalogRevalidationAfterInit() {
        attempted.push("catalog");
        return Promise.reject(new Error("private catalog failure"));
      },
    });
    assert.equal((await subject.update({ setupComplete: true })).setupComplete, true);
    await nextTurn();
    assert.deepEqual(attempted, ["runtime", "catalog"]);
    await subject.update({ setupComplete: true });
    await nextTurn();
    assert.deepEqual(attempted, ["runtime", "catalog", "runtime"]);
    assert.equal(await subject.getRevision(), 2);
  });

  test("PostgreSQL settings failed readback retains acknowledged COMMIT and cache-failure evidence", async () => {
    await resetStorage();
    let release!: () => void;
    let acquired!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const ready = new Promise<void>((resolve) => {
      acquired = resolve;
    });
    let owner: Promise<void> | undefined;
    const attempted: string[] = [];
    const subject = repository({
      ...noEffects,
      async invalidateSettingsCache() {
        // Hold the real settings lock only AFTER update's COMMIT acknowledgement.
        assert.equal(await readRaw(SETTINGS_REVISION_KEY), "1");
        owner = database.transaction(async (transaction) => {
          await transaction.query("SELECT pg_advisory_xact_lock(18765, 3)");
          acquired();
          await gate;
        });
        void owner.catch(() => {});
        await Promise.race([ready, owner]);
        attempted.push("settings_cache");
        throw new Error("private cache callback details");
      },
      applyRuntimeSettings() {
        attempted.push("runtime");
      },
    });
    try {
      await assert.rejects(
        subject.update({ marker: "committed" }),
        (error: PostgresSettingsEffectsError) => {
          assert.equal(error.committed, true);
          assert.deepEqual(error.failedEffects, ["settings_cache", "settings_readback"]);
          assert.equal(error.cause, undefined);
          assert.doesNotMatch(error.message, /private/);
          return true;
        }
      );
      assert.equal(await readRaw("marker"), '"committed"');
      assert.equal(await readRaw(SETTINGS_REVISION_KEY), "1");
      assert.deepEqual(attempted, ["settings_cache"]);
    } finally {
      release();
      await owner;
    }
    assert.equal((await settings.get()).marker, "committed");
    assert.equal(await settings.getRevision(), 1);
  });

  test("PostgreSQL settings rejected write rolls back every field and emits no effects", async () => {
    await resetStorage();
    const attempted: string[] = [];
    const subject = repository({
      invalidateSettingsCache() {
        attempted.push("settings_cache");
      },
      bumpProxyConfigGeneration() {
        attempted.push("proxy_config");
      },
      applyRuntimeSettings() {
        attempted.push("runtime");
      },
      scheduleCatalogRevalidationAfterInit() {
        attempted.push("catalog");
      },
    });
    await installWriteRejection();
    try {
      await assert.rejects(
        subject.update({ proxyEnabled: false, blocked: true }),
        (error: PostgresError) => error.sqlState === "P0001"
      );
      assert.equal(await readRaw("proxyEnabled"), null);
      assert.equal(await readRaw("blocked"), null);
      assert.equal(await readRaw(SETTINGS_REVISION_KEY), null);
      assert.deepEqual(attempted, []);
    } finally {
      await removeWriteRejection();
    }
  });

  test("PostgreSQL headless onboarding flags commit atomically and leave revision unchanged", async () => {
    await resetStorage();
    environment = { INITIAL_PASSWORD: "test-only-headless-password" };
    await installWriteRejection();
    try {
      await assert.rejects(settings.get(), (error: PostgresError) => error.sqlState === "P0001");
      assert.equal(await readRaw("setupComplete"), null);
      assert.equal(await readRaw("requireLogin"), null);
      assert.equal(await settings.getRevision(), 0);
    } finally {
      await removeWriteRejection();
    }
    const readResults = await queuedUpdates(() => [settings.get(), settings.get()]);
    assert.equal(readResults.filter((result) => result.status === "fulfilled").length, 2);
    assert.equal(await readRaw("setupComplete"), "true");
    assert.equal(await readRaw("requireLogin"), "true");
    assert.equal(await settings.getRevision(), 0);
    assert.equal(await readRaw(SETTINGS_REVISION_KEY), null);
  });
}
