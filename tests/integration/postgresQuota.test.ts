import assert from "node:assert/strict";
import test from "node:test";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import {
  getQuotaStore,
  getQuotaStoreSync,
  resetQuotaStoreSingleton,
} from "../../src/lib/quota/storeFactory.ts";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { PostgresQuotaRepository } from "../../src/lib/db/postgres/quotaRepository.ts";
import { PostgresQuotaStore } from "../../src/lib/quota/postgresQuotaStore.ts";
import type { ScheduleInput } from "../../src/lib/db/quotaSchedules.ts";
import { readApiKeyPolicyEpoch } from "../../src/lib/db/postgres/apiKeyPolicyState.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

function schedule(id: string, priority = 0, enabled = true): ScheduleInput {
  return {
    id,
    days: [1, 2],
    startMinute: 0,
    endMinute: 1440,
    timezone: "UTC",
    mode: "allow",
    reserves: [{ window: "weekly", percent: 30 }],
    budgetWindow: "daily",
    budgetValue: 100,
    budgetUnit: "tokens",
    priority,
    enabled,
  };
}

test(
  "PostgreSQL pooled quota metadata and counters",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const database = await createTestPostgresDatabase({
      maxConnections: 32,
      lockTimeoutMs: 10_000,
      statementTimeoutMs: 12_000,
      queryTimeoutMs: 13_000,
      transactionTimeoutMs: 30_000,
    });
    const quota = new PostgresQuotaRepository(database);
    const store = new PostgresQuotaStore(database);
    try {
      await migratePostgresSchema(database);
      const prefix = `quota-${crypto.randomUUID()}`;
      const connectionA = `${prefix}-a`;
      const connectionB = `${prefix}-b`;
      const connectionOther = `${prefix}-other`;
      await database.transaction(async (transaction) => {
        for (const [id, provider] of [
          [connectionA, "codex"],
          [connectionB, "codex"],
          [connectionOther, "anthropic"],
        ]) {
          await transaction.query(
            "INSERT INTO omniroute.provider_connections(id,provider,created_at,updated_at) VALUES($1,$2,$3,$3)",
            [id, provider, new Date().toISOString()]
          );
        }
      });
      const groupA = await quota.createGroup(`${prefix}-group-a`);
      const groupB = await quota.createGroup(`${prefix}-group-b`);
      const allocation = { apiKeyId: `${prefix}-key`, weight: 50, policy: "hard" as const };
      const pool = await quota.createPool({
        connectionId: connectionA,
        connectionIds: [connectionB, connectionA],
        name: `${prefix}-pool`,
        groupId: groupA.id,
        allocations: [allocation],
      });
      const sibling = await quota.createPool({
        connectionId: connectionA,
        name: `${prefix}-sibling`,
        groupId: groupA.id,
      });
      const isolated = await quota.createPool({
        connectionId: connectionA,
        name: `${prefix}-isolated`,
        groupId: groupB.id,
      });

      await t.test(
        "selected PostgreSQL quota store remains native when Redis configuration is absent",
        async () => {
          const previousEncryptionKey = process.env.STORAGE_ENCRYPTION_KEY;
          const previousJwtSecret = process.env.JWT_SECRET;
          const previousApiSecret = process.env.API_KEY_SECRET;
          const previousDriver = process.env.QUOTA_STORE_DRIVER;
          const previousRedisUrl = process.env.QUOTA_STORE_REDIS_URL;
          process.env.STORAGE_ENCRYPTION_KEY = "quota-integration-test-key-000000";
          process.env.JWT_SECRET = "quota-scope-test-jwt";
          process.env.API_KEY_SECRET = "quota-scope-test-api";
          process.env.QUOTA_STORE_DRIVER = "redis";
          delete process.env.QUOTA_STORE_REDIS_URL;
          resetQuotaStoreSingleton();
          let controlPlane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
          try {
            controlPlane = await initializePostgresControlPlane({
              connection: await testPostgresOptions(),
              logger: { warn() {} },
              runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
            });
            await controlPlane.run(async (repositories) => {
              await repositories.settings.update({ quotaStore: { driver: "redis", redisUrl: "" } });
              const selected = await getQuotaStore();
              assert(selected instanceof PostgresQuotaStore);
              assert.equal(getQuotaStoreSync(), selected);
              const dimension = {
                poolId: `${prefix}-scope`,
                unit: "requests" as const,
                window: "hourly" as const,
              };
              assert.equal(await selected.consume(allocation.apiKeyId, dimension, 1), 1);
            });
          } finally {
            await controlPlane?.close();
            resetQuotaStoreSingleton();
            if (previousEncryptionKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
            else process.env.STORAGE_ENCRYPTION_KEY = previousEncryptionKey;
            if (previousJwtSecret === undefined) delete process.env.JWT_SECRET;
            else process.env.JWT_SECRET = previousJwtSecret;
            if (previousApiSecret === undefined) delete process.env.API_KEY_SECRET;
            else process.env.API_KEY_SECRET = previousApiSecret;
            if (previousDriver === undefined) delete process.env.QUOTA_STORE_DRIVER;
            else process.env.QUOTA_STORE_DRIVER = previousDriver;
            if (previousRedisUrl === undefined) delete process.env.QUOTA_STORE_REDIS_URL;
            else process.env.QUOTA_STORE_REDIS_URL = previousRedisUrl;
          }
        }
      );

      await t.test("ordered members, protected groups and mixed-provider validation", async () => {
        assert.equal(pool.connectionId, connectionB);
        assert.deepEqual(pool.connectionIds, [connectionB, connectionA]);
        await assert.rejects(quota.deleteGroup("group-demo"), /protected seed/);
        await assert.rejects(quota.deleteGroup(groupA.id), /has pools/);
        await assert.rejects(
          quota.createPool({
            connectionId: connectionA,
            connectionIds: [connectionA, connectionOther],
            name: `${prefix}-mixed`,
          }),
          /single provider/
        );
        assert.equal(
          (await quota.listPools()).items.filter((item) => item.name === `${prefix}-mixed`).length,
          0
        );
      });

      await t.test(
        "concurrent ensure creates one pool and publishes a durable policy epoch",
        async () => {
          const before = await database.transaction(readApiKeyPolicyEpoch);
          const input = { connectionId: connectionA, name: `${prefix}-ensure`, groupId: groupB.id };
          const results = await Promise.all([quota.ensurePool(input), quota.ensurePool(input)]);
          assert.equal(new Set(results.map((result) => result.pool.id)).size, 1);
          assert.equal(results.filter((result) => result.created).length, 1);
          assert(BigInt(await database.transaction(readApiKeyPolicyEpoch)) > BigInt(before));
        }
      );

      await t.test("replacement rollback preserves membership, schedules and name", async () => {
        await quota.replaceSchedules(pool.id, [schedule(`${prefix}-schedule`)]);
        await assert.rejects(
          quota.updatePool(pool.id, {
            name: "uncommitted",
            connectionIds: [connectionA],
            allocations: [allocation, allocation],
          }),
          (error: unknown) => error instanceof PostgresError && error.sqlState === "23505"
        );
        const after = await quota.getPool(pool.id);
        assert.equal(after?.name, pool.name);
        assert.deepEqual(after?.connectionIds, pool.connectionIds);
        assert.equal((await quota.listSchedules(pool.id)).length, 1);
      });

      await t.test(
        "allocation propagation normalizes zero weights and isolates other groups",
        async () => {
          const zero = { ...allocation, weight: 0 };
          await quota.upsertAllocations(pool.id, [zero, { ...zero, apiKeyId: `${prefix}-key2` }]);
          for (const id of [pool.id, sibling.id]) {
            assert.deepEqual(
              (await quota.getPool(id))?.allocations.map((item) => item.weight),
              [50, 50]
            );
          }
          assert.deepEqual((await quota.getPool(isolated.id))?.allocations, []);
        }
      );

      await t.test(
        "schedule ordering and failed replacement retain the previous complete set",
        async () => {
          const schedules = [
            schedule(`${prefix}-low`),
            schedule(`${prefix}-high`, 10),
            schedule(`${prefix}-disabled`, 20, false),
          ];
          const saved = await quota.replaceSchedules(pool.id, schedules);
          assert.deepEqual(
            saved.map((item) => item.id),
            [schedules[2].id, schedules[1].id, schedules[0].id]
          );
          assert.equal(saved[0].enabled, false);
          await assert.rejects(
            quota.replaceSchedules(pool.id, [schedule("duplicate"), schedule("duplicate")])
          );
          assert.deepEqual(
            (await quota.listSchedules(pool.id)).map((item) => item.id),
            saved.map((item) => item.id)
          );
        }
      );

      await t.test(
        "parallel increments have no lost updates and preserve key/model/window isolation",
        async () => {
          const dim = { poolId: pool.id, unit: "tokens" as const, window: "hourly" as const };
          const values = await Promise.all(
            Array.from({ length: 20 }, () => store.consume(allocation.apiKeyId, dim, 1))
          );
          assert.deepEqual(
            [...values].sort((a, b) => a - b),
            Array.from({ length: 20 }, (_, index) => index + 1)
          );
          assert.equal(await store.peek(allocation.apiKeyId, dim), 20);
          assert.equal(await store.peek(`${prefix}-key2`, dim), 0);
          assert.equal(await store.peek(allocation.apiKeyId, { ...dim, window: "daily" }), 0);
          assert.equal(
            await store.peek(allocation.apiKeyId, { ...dim, poolId: `${pool.id}:model:other` }),
            0
          );
          await store.consume(`${prefix}-unallocated-key`, dim, 5);
          assert.equal(await store.poolConsumedTotal(pool.id, dim), 25);
          const snapshot = await store.poolUsageWithDimensions(pool.id, [
            { unit: "tokens", window: "hourly", limit: 100 },
          ]);
          assert.equal(snapshot.dimensions[0].consumedTotal, 20);
          assert.equal(snapshot.dimensions[0].perKey[0].fairShare, 50);
        }
      );

      await t.test(
        "clear zeros both current and previous buckets without affecting another counter",
        async () => {
          const dim = {
            poolId: `${pool.id}:model:clear`,
            unit: "requests" as const,
            window: "hourly" as const,
          };
          const now = Date.now();
          const bucket = Math.floor(now / 3_600_000);
          const key = `${dim.poolId}:requests:hourly`;
          await quota.incrementBucket(allocation.apiKeyId, key, bucket - 1, 10, now);
          await store.consume(allocation.apiKeyId, dim, 5);
          await store.consume(`${prefix}-key2`, dim, 7);
          await store.clear(allocation.apiKeyId, dim);
          assert.equal(await store.peek(allocation.apiKeyId, dim), 0);
          assert.equal(await store.peek(`${prefix}-key2`, dim), 7);
          assert.equal(await store.consume(allocation.apiKeyId, dim, 1), 1);
        }
      );

      await t.test(
        "clear retains bigint bucket precision and only changes its two scoped rows",
        async () => {
          const apiKeyId = `${prefix}-clear-bigint-key`;
          const otherApiKeyId = `${prefix}-clear-bigint-other-key`;
          const dimensionKey = `${prefix}:clear-bigint:hourly`;
          const otherDimensionKey = `${prefix}:clear-bigint:daily`;
          const currentBucket = 2 ** 31 + 17;
          const nowMs = Date.now();
          for (const [bucket, amount] of [
            [currentBucket - 2, 11],
            [currentBucket - 1, 13],
            [currentBucket, 17],
            [currentBucket + 1, 19],
          ]) {
            await quota.incrementBucket(apiKeyId, dimensionKey, bucket, amount, nowMs);
          }
          await quota.incrementBucket(otherApiKeyId, dimensionKey, currentBucket, 23, nowMs);
          await quota.incrementBucket(apiKeyId, otherDimensionKey, currentBucket, 29, nowMs);
          await quota.clear(apiKeyId, dimensionKey, currentBucket);
          const actual = await database.transaction(async (transaction) => {
            const result = await transaction.query<{
              api_key_id: string;
              dimension_key: string;
              bucket_index: string;
              consumed: number;
            }>(
              `SELECT api_key_id,dimension_key,bucket_index,consumed FROM omniroute.quota_consumption
              WHERE api_key_id=ANY($1::text[]) AND dimension_key=ANY($2::text[])`,
              [
                [apiKeyId, otherApiKeyId],
                [dimensionKey, otherDimensionKey],
              ]
            );
            return result.rows.map((row) => [
              row.api_key_id,
              row.dimension_key,
              Number(row.bucket_index),
              row.consumed,
            ]);
          });
          const expected = [
            [apiKeyId, dimensionKey, currentBucket - 2, 11],
            [apiKeyId, dimensionKey, currentBucket - 1, 0],
            [apiKeyId, dimensionKey, currentBucket, 0],
            [apiKeyId, dimensionKey, currentBucket + 1, 19],
            [otherApiKeyId, dimensionKey, currentBucket, 23],
            [apiKeyId, otherDimensionKey, currentBucket, 29],
          ];
          const canonical = (rows: unknown[][]) => rows.map((row) => JSON.stringify(row)).sort();
          assert.deepEqual(canonical(actual), canonical(expected));
          const absentKey = `${prefix}-clear-absent`;
          await quota.clear(absentKey, dimensionKey, currentBucket);
          const absentRows = await database.transaction((transaction) =>
            transaction.query("SELECT 1 FROM omniroute.quota_consumption WHERE api_key_id=$1", [
              absentKey,
            ])
          );
          assert.equal(absentRows.rowCount, 0);
        }
      );

      await t.test("literal pool log prefixes and strict GC boundary", async () => {
        const literal = `${prefix}%_`;
        const now = Date.now();
        await quota.incrementBucket("log-key", `${literal}:tokens:daily`, 10, 5, now);
        await quota.incrementBucket("log-key", `${prefix}OTHER:tokens:daily`, 10, 6, now);
        const events = await quota.listConsumptionForPool(literal, 500);
        assert.equal(events.length, 1);
        assert.equal(events[0].consumed, 5);
        const strictDimension = `${prefix}:gc:hourly`;
        await quota.incrementBucket("gc-key", strictDimension, 0, 1, 10);
        await quota.incrementBucket("gc-key", strictDimension, 1, 1, 11);
        await quota.gcOlderThan(11);
        assert.equal(await quota.getBucket("gc-key", strictDimension, 0), 0);
        assert.equal(await quota.getBucket("gc-key", strictDimension, 1), 1);
      });

      await t.test(
        "provider plans and model caps retain exact connection and model scope",
        async () => {
          await quota.upsertPlan(
            connectionB,
            "codex",
            [{ unit: "tokens", window: "daily", limit: 500 }],
            "manual"
          );
          assert.equal((await quota.getPlan(connectionB))?.dimensions[0].limit, 500);
          assert.equal(await quota.getPlan(connectionA), null);
          await quota.setModelCap({
            poolId: pool.id,
            apiKeyId: allocation.apiKeyId,
            model: "model-a",
            capValue: 10,
            capUnit: "tokens",
          });
          assert.equal(
            (await quota.getModelCap(pool.id, allocation.apiKeyId, "model-a"))?.capValue,
            10
          );
          assert.equal(await quota.getModelCap(pool.id, allocation.apiKeyId, "model-b"), null);
        }
      );

      await t.test(
        "pool deletion atomically prunes allowed quotas and owned schedule/model rows",
        async () => {
          const key = `${prefix}-permission-key`;
          await database.transaction((transaction) =>
            transaction.query(
              "INSERT INTO omniroute.api_keys(id,name,key,created_at,allowed_quotas) VALUES($1,$1,$1,$2,$3)",
              [key, new Date().toISOString(), JSON.stringify([pool.id, isolated.id])]
            )
          );
          assert.equal(await quota.deletePool(pool.id), true);
          assert.equal(await quota.getPool(pool.id), null);
          assert.deepEqual(await quota.listSchedules(pool.id), []);
          assert.equal(await quota.getModelCap(pool.id, allocation.apiKeyId, "model-a"), null);
          const permissions = await database.transaction((transaction) =>
            transaction.query<{ allowed_quotas: string }>(
              "SELECT allowed_quotas FROM omniroute.api_keys WHERE id=$1",
              [key]
            )
          );
          assert.deepEqual(JSON.parse(permissions.rows[0].allowed_quotas), [isolated.id]);
          assert.equal(await quota.deletePool(pool.id), false);
        }
      );
    } finally {
      await database.close();
    }
  }
);
