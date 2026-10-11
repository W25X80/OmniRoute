import assert from "node:assert/strict";
import test from "node:test";
import type { HookConfig } from "../../src/lib/middleware/types.ts";
import { encrypt } from "../../src/lib/db/encryption.ts";
import { PersistenceCapabilityError } from "../../src/lib/db/persistenceCapabilities.ts";
import { PostgresError, type PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { createPostgresOperationsRepository } from "../../src/lib/db/postgres/operationsRepository.ts";
import { createPostgresMiddlewareRepository } from "../../src/lib/db/postgres/middlewareRepository.ts";
import {
  createPostgresCleanupRepository,
  queuePostgresArtifactDeletions,
} from "../../src/lib/db/postgres/cleanupRepository.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

let database: PostgresDatabase;
const originalEncryptionKey = process.env.STORAGE_ENCRYPTION_KEY;

if (!postgresTestsEnabled) {
  test("PostgreSQL operations require the isolated integration runner", { skip: true }, () => {});
} else {
  test.before(async () => {
    process.env.STORAGE_ENCRYPTION_KEY = "operations-test-encryption-key";
    database = await createTestPostgresDatabase();
    await migratePostgresSchema(database);
  });
  test.beforeEach(async () => {
    await database.transaction(async (transaction) => {
      await transaction.query(
        "DELETE FROM omniroute.key_value WHERE namespace IN ('settings', 'databaseSettings')"
      );
      await transaction.query("DELETE FROM omniroute.middleware_hooks");
    });
  });
  test.after(async () => {
    await database?.close();
    if (originalEncryptionKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
    else process.env.STORAGE_ENCRYPTION_KEY = originalEncryptionKey;
  });

  test("portable settings preserve precedence, masks and encrypted legacy copies", async () => {
    const repository = createPostgresOperationsRepository(database);
    await database.transaction(async (transaction) => {
      await transaction.query(
        "INSERT INTO omniroute.key_value(namespace,key,value) VALUES ('settings','databaseSettings',$1), ('databaseSettings','retention.callLogs','17')",
        [
          JSON.stringify({
            retention: { callLogs: 11 },
            cache: { semanticCacheEmbeddingApiKey: "legacy-secret" },
          }),
        ]
      );
    });
    const current = await repository.getUserDatabaseSettings();
    assert.equal(current.retention.callLogs, 17);
    assert.equal(current.cache.semanticCacheEmbeddingApiKey, "legacy-secret");
    const updated = await repository.updateDatabaseSettings({
      cache: {
        ...current.cache,
        semanticCacheEmbeddingApiKey: "********",
        semanticCacheMaxSize: 42,
      },
    });
    assert.equal(updated.cache.semanticCacheEmbeddingApiKey, "legacy-secret");
    assert.equal(updated.cache.semanticCacheMaxSize, 42);
    const raw = await database.transaction((transaction) =>
      transaction.query<{ value: string }>(
        "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key='databaseSettings'"
      )
    );
    assert.equal(raw.rows[0].value.includes("legacy-secret"), false);
    assert.equal(raw.rows[0].value.includes("enc:v1:"), true);
    assert.equal(
      (await repository.getUserDatabaseSettings()).cache.semanticCacheEmbeddingApiKey,
      "legacy-secret"
    );
  });

  test("concurrent portable settings patches preserve independent sections", async () => {
    const repository = createPostgresOperationsRepository(database);
    const current = await repository.getUserDatabaseSettings();
    await Promise.all([
      repository.updateDatabaseSettings({ logs: { ...current.logs, ringBufferSize: 321 } }),
      repository.updateDatabaseSettings({ retention: { ...current.retention, callLogs: 19 } }),
    ]);
    const settings = await repository.getUserDatabaseSettings();
    assert.equal(settings.logs.ringBufferSize, 321);
    assert.equal(settings.retention.callLogs, 19);
  });

  test("a rejected setting write rolls the whole patch back", async () => {
    const repository = createPostgresOperationsRepository(database);
    const current = await repository.getUserDatabaseSettings();
    await database.transaction((transaction) =>
      transaction.query(
        `ALTER TABLE omniroute.key_value ADD CONSTRAINT operations_test_reject
       CHECK (NOT(namespace='databaseSettings' AND key='logs.ringBufferSize' AND value='9999'))`
      )
    );
    try {
      await assert.rejects(
        repository.updateDatabaseSettings({
          logs: { ...current.logs, detailedLogsEnabled: true, ringBufferSize: 9999 },
        }),
        (error) => error instanceof PostgresError && error.sqlState === "23514"
      );
      const after = await repository.getUserDatabaseSettings();
      assert.equal(after.logs.detailedLogsEnabled, false);
      assert.equal(after.logs.ringBufferSize, current.logs.ringBufferSize);
    } finally {
      await database.transaction((transaction) =>
        transaction.query("ALTER TABLE omniroute.key_value DROP CONSTRAINT operations_test_reject")
      );
    }
  });

  test("PostgreSQL statistics and settings do not fabricate SQLite page metrics", async () => {
    const repository = createPostgresOperationsRepository(database);
    const stats = await repository.getDatabaseStats();
    assert.equal(stats.backend, "postgresql");
    assert.equal(stats.pageCount, null);
    assert.equal(stats.pageSize, null);
    assert.equal(stats.cacheSize, null);
    assert.equal(stats.walSize, null);
    assert.equal(
      stats.tables.every((table) => table.rowCountApproximate === true),
      true
    );
    assert.equal(
      stats.tables.some((table) => table.name === "middleware_hooks"),
      true
    );
    const settings = await repository.getDatabaseSettings();
    assert.equal(settings.backend, "postgresql");
    assert.equal(settings.stats.freelistCount, null);
    assert.equal(settings.location.walSizeBytes, null);
    assert.equal(settings.capabilities?.databaseFileBackup, false);
  });

  test("SQLite-only settings and repair are rejected before mutation", async () => {
    const repository = createPostgresOperationsRepository(database);
    const current = await repository.getUserDatabaseSettings();
    await assert.rejects(
      repository.updateDatabaseSettings({ backup: current.backup }),
      (error) =>
        error instanceof PersistenceCapabilityError && error.capability === "sqlite-file-backup"
    );
    await assert.rejects(
      repository.updateDatabaseSettings({ optimization: current.optimization }),
      (error) =>
        error instanceof PersistenceCapabilityError && error.capability === "sqlite-optimization"
    );
    await assert.rejects(
      repository.healthCheck({ autoRepair: true }),
      (error) =>
        error instanceof PersistenceCapabilityError &&
        error.capability === "sqlite-integrity-repair"
    );
    assert.deepEqual(await repository.getUserDatabaseSettings(), current);
  });

  test("middleware partial updates and concurrent execution counters retain state", async () => {
    const repository = createPostgresMiddlewareRepository(database);
    const hook: HookConfig = {
      name: "operations-hook",
      description: "test",
      priority: 200,
      scope: { type: "global" },
      enabled: true,
      code: "return {}",
      createdAt: "",
      updatedAt: "",
      runCount: 0,
    };
    await repository.create(hook);
    await Promise.all([
      repository.recordExecution(hook.name),
      repository.recordExecution(hook.name, "failure"),
    ]);
    await repository.update(hook.name, { description: "changed" });
    const stored = await repository.get(hook.name);
    assert.equal(stored?.runCount, 2);
    assert.equal(stored?.description, "changed");
    assert.equal(stored?.code, "return {}");
    assert.equal((await repository.list(true)).length, 1);
    assert.equal(await repository.delete(hook.name), true);
    assert.equal(await repository.get(hook.name), undefined);
  });

  test("retention distinguishes ISO timestamps and epoch milliseconds", async () => {
    const repository = createPostgresCleanupRepository(database);
    const cutoff = "2026-10-11T12:00:00.000Z";
    await database.transaction(async (transaction) => {
      await transaction.query(
        "DELETE FROM omniroute.domain_cost_history WHERE api_key_id='operations-retention'"
      );
      await transaction.query(
        "INSERT INTO omniroute.domain_cost_history(api_key_id,cost,timestamp) VALUES ('operations-retention',1,$1), ('operations-retention',2,$2)",
        [Date.parse(cutoff) - 1, Date.parse(cutoff)]
      );
    });
    await repository.deleteBefore(
      { table: "domain_cost_history", column: "timestamp", cutoff: "epochMs" },
      cutoff
    );
    const rows = await database.transaction((transaction) =>
      transaction.query<{ cost: number }>(
        "SELECT cost FROM omniroute.domain_cost_history WHERE api_key_id='operations-retention'"
      )
    );
    assert.deepEqual(rows.rows, [{ cost: 2 }]);
    await assert.rejects(
      repository.deleteBefore({ table: "api_keys", column: "created_at", cutoff: "iso" }, cutoff),
      /Unknown PostgreSQL retention target/
    );
  });

  test("credential bytes are never rewritten by portable settings read", async () => {
    const ciphertext = encrypt("stored-cache-secret");
    await database.transaction((transaction) =>
      transaction.query(
        "INSERT INTO omniroute.key_value(namespace,key,value) VALUES ('databaseSettings','cache.semanticCacheEmbeddingApiKey',$1)",
        [JSON.stringify(ciphertext)]
      )
    );
    assert.equal(
      (await createPostgresOperationsRepository(database).getUserDatabaseSettings()).cache
        .semanticCacheEmbeddingApiKey,
      "stored-cache-secret"
    );
    const raw = await database.transaction((transaction) =>
      transaction.query<{ value: string }>(
        "SELECT value FROM omniroute.key_value WHERE namespace='databaseSettings' AND key='cache.semanticCacheEmbeddingApiKey'"
      )
    );
    assert.equal(JSON.parse(raw.rows[0].value), ciphertext);
  });
  test("artifact deletion references commit atomically and survive until acknowledgment", async () => {
    const repository = createPostgresCleanupRepository(database);
    const committedPath = "2026-10-11/operations-committed.json";
    const rolledBackPath = "2026-10-11/operations-rollback.json";
    await database.transaction((transaction) =>
      transaction.query(
        "DELETE FROM omniroute.artifact_deletion_queue WHERE relpath = ANY($1::text[])",
        [[committedPath, rolledBackPath]]
      )
    );
    await assert.rejects(
      database.transaction(async (transaction) => {
        await queuePostgresArtifactDeletions(transaction, [rolledBackPath]);
        throw new Error("rollback requested");
      })
    );
    assert.equal((await repository.pendingArtifactDeletions()).includes(rolledBackPath), false);
    await database.transaction((transaction) =>
      queuePostgresArtifactDeletions(transaction, [committedPath, committedPath])
    );
    assert.equal(
      (await repository.pendingArtifactDeletions()).filter((value) => value === committedPath)
        .length,
      1
    );
    await repository.acknowledgeArtifactDeletion(committedPath);
    assert.equal((await repository.pendingArtifactDeletions()).includes(committedPath), false);
  });

  test("PostgreSQL diagnostics are read-only and declare their actual integrity scope", async () => {
    const before = await database.transaction((transaction) =>
      transaction.query<{ count: string }>(
        "SELECT COUNT(*)::text AS count FROM omniroute.domain_budgets"
      )
    );
    const result = await createPostgresOperationsRepository(database).healthCheck();
    assert.equal(result.driver.name, "postgresql");
    assert.equal(result.integrityScope, "connectivity-and-application-references");
    assert.equal(result.autoRepair, false);
    assert.equal(result.repairedCount, 0);
    const after = await database.transaction((transaction) =>
      transaction.query<{ count: string }>(
        "SELECT COUNT(*)::text AS count FROM omniroute.domain_budgets"
      )
    );
    assert.deepEqual(after.rows, before.rows);
  });
}
