import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext.ts";
import { withPostgresControlPlane } from "../../src/lib/db/controlPlaneContext.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { createPostgresOperationsRepository } from "../../src/lib/db/postgres/operationsRepository.ts";
import { PostgresError, type PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import {
  assertPostgresCatalogSnapshotCurrent,
  catalogGeneration,
  withPostgresCatalogSnapshot,
} from "../../src/lib/db/postgres/catalogSnapshot.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

function selectedBinding(database: PostgresDatabase): PostgresControlPlaneBinding {
  // Native settings/catalog owners only; the unrelated repositories are not used.
  return {
    database,
    databaseIdentity: Object.freeze({}),
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
}

test(
  "prepared native database settings retain merge/decryption and publish acknowledged changes",
  {
    skip: !postgresTestsEnabled,
    timeout: 30_000,
  },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "pg-prepared-settings-"));
    const saved = {
      DATA_DIR: process.env.DATA_DIR,
      STORAGE_ENCRYPTION_KEY: process.env.STORAGE_ENCRYPTION_KEY,
    };
    process.env.DATA_DIR = directory;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-prepared-settings-encryption-key";
    const database = await createTestPostgresDatabase({ maxConnections: 4 });
    const contender = await createTestPostgresDatabase({ maxConnections: 2 });
    try {
      await migratePostgresSchema(database);
      const { getUserDatabaseSettings, getUserDatabaseSettingsAsync, updateDatabaseSettingsAsync } =
        await import("../../src/lib/db/databaseSettings.ts");
      const binding = selectedBinding(database);
      const selected = <T>(body: () => T | Promise<T>): Promise<T> =>
        withPostgresControlPlane(binding, () => withPostgresCatalogSnapshot(database, body));
      const clear = () =>
        database.transaction((tx) =>
          tx.query(
            "DELETE FROM omniroute.key_value WHERE namespace IN ('settings', 'databaseSettings')"
          )
        );
      await t.test(
        "empty prepared namespace is a valid default, unprepared reads never fall back",
        async () => {
          await clear();
          await selected(async () => {
            assert.deepEqual(getUserDatabaseSettings(), await getUserDatabaseSettingsAsync());
          });
          await withPostgresControlPlane(binding, async () => {
            assert.throws(() => getUserDatabaseSettings(), /snapshot was not prepared/);
            assert.ok((await getUserDatabaseSettingsAsync()).cache);
          });
          assert.equal(existsSync(join(directory, "storage.sqlite")), false);
        }
      );
      await t.test(
        "pure reads do no database work and preserve exact precedence/decryption",
        async () => {
          await clear();
          const { encryptCacheValue } = await import("../../src/lib/db/cacheSecrets.ts");
          const secret = "prepared-cache-credential";
          await database.transaction(async (tx) => {
            await tx.query(
              `INSERT INTO omniroute.key_value(namespace,key,value) VALUES
          ('settings','databaseSettings',$1), ('settings','cache',$2),
          ('databaseSettings','semanticCacheMaxSize','31'),
          ('databaseSettings','cache.semanticCacheMaxSize','37'),
          ('databaseSettings','cache.semanticCacheEmbeddingApiKey',$3),
          ('settings','call_log_pipeline_enabled','true')`,
              [
                JSON.stringify({ cache: { semanticCacheMaxSize: 11 } }),
                JSON.stringify({ semanticCacheMaxSize: 17 }),
                JSON.stringify(encryptCacheValue("semanticCacheEmbeddingApiKey", secret)),
              ]
            );
          });
          await selected(async () => {
            const expected = await getUserDatabaseSettingsAsync();
            const noQueries = new Proxy(database, {
              get(target, property, receiver) {
                if (property === "transaction")
                  return () => {
                    assert.fail("pure prepared getter queried the database");
                  };
                return Reflect.get(target, property, receiver);
              },
            });
            withPostgresControlPlane({ ...binding, database: noQueries }, () => {
              const actual = getUserDatabaseSettings();
              assert.deepEqual(actual, expected);
              assert.equal(actual.cache.semanticCacheMaxSize, 37);
              assert.equal(actual.cache.semanticCacheEmbeddingApiKey, secret);
              assert.equal(actual.logs.callLogPipelineEnabled, true);
              actual.cache.semanticCacheMaxSize = 999;
              assert.equal(getUserDatabaseSettings().cache.semanticCacheMaxSize, 37);
            });
          });
        }
      );
      await t.test(
        "facade refreshes the same scope only after commit acknowledgement",
        async () => {
          await selected(async () => {
            const before = catalogGeneration();
            await updateDatabaseSettingsAsync({ cache: { semanticCacheMaxSize: 47 } });
            assert.notEqual(catalogGeneration(), before);
            assert.equal(getUserDatabaseSettings().cache.semanticCacheMaxSize, 47);
            await assertPostgresCatalogSnapshotCurrent();
          });
        }
      );
      await t.test(
        "an independent default repository mutation invalidates stale prepared authority",
        async () => {
          const otherWriter = createPostgresOperationsRepository(contender);
          await selected(async () => {
            const before = catalogGeneration();
            assert.equal(getUserDatabaseSettings().cache.semanticCacheMaxSize, 47);
            await otherWriter.updateDatabaseSettings({ cache: { semanticCacheMaxSize: 53 } });
            assert.equal(catalogGeneration(), before);
            assert.equal(getUserDatabaseSettings().cache.semanticCacheMaxSize, 47);
            await assert.rejects(assertPostgresCatalogSnapshotCurrent(), /Catalog policy changed/);
          });
          await selected(() => {
            assert.equal(getUserDatabaseSettings().cache.semanticCacheMaxSize, 53);
          });
        }
      );
      await t.test(
        "failed writes do not advance generation or publish into the prepared scope",
        async () => {
          await database.transaction((tx) =>
            tx.query(`CREATE FUNCTION omniroute.reject_l37_settings() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'isolated settings write rejection'; END $$`)
          );
          await database.transaction((tx) =>
            tx.query(`CREATE TRIGGER reject_l37_settings BEFORE INSERT OR UPDATE ON omniroute.key_value
        FOR EACH ROW WHEN (NEW.namespace='databaseSettings') EXECUTE FUNCTION omniroute.reject_l37_settings()`)
          );
          try {
            await selected(async () => {
              const before = catalogGeneration();
              await assert.rejects(
                updateDatabaseSettingsAsync({ cache: { semanticCacheMaxSize: 59 } }),
                PostgresError
              );
              assert.equal(catalogGeneration(), before);
              assert.equal(getUserDatabaseSettings().cache.semanticCacheMaxSize, 53);
              await assertPostgresCatalogSnapshotCurrent();
            });
          } finally {
            await database.transaction(async (tx) => {
              await tx.query("DROP TRIGGER reject_l37_settings ON omniroute.key_value");
              await tx.query("DROP FUNCTION omniroute.reject_l37_settings()");
            });
          }
        }
      );
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
    } finally {
      await contender.close();
      await database.close();
      for (const [name, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
