import assert from "node:assert/strict";
import test from "node:test";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import { PostgresQuotaRepository } from "../../src/lib/db/postgres/quotaRepository.ts";
import { resetQuotaStoreSingleton } from "../../src/lib/quota/storeFactory.ts";
import { scheduleBucketPoolId, resolveActiveSchedule } from "../../src/lib/quota/schedules.ts";
import { scheduleStreamingQuotaShareConsumptionAsync } from "../../open-sse/handlers/chatCore/streamingQuotaShare.ts";
import { scheduleQuotaShareConsumptionAsync } from "../../open-sse/handlers/chatCore/quotaShareConsumption.ts";
import {
  testPostgresOptions,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "native completion accounting keeps captured reset and schedule identity across deferred cost",
  { skip: !postgresTestsEnabled },
  async () => {
    const originalClock = Date.now;
    const previous = {
      encryption: process.env.STORAGE_ENCRYPTION_KEY,
      jwt: process.env.JWT_SECRET,
      apiKey: process.env.API_KEY_SECRET,
    };
    process.env.STORAGE_ENCRYPTION_KEY = "quota-integration-test-key-000000";
    process.env.JWT_SECRET = "quota-event-time-jwt";
    process.env.API_KEY_SECRET = "quota-event-time-api";
    resetQuotaStoreSingleton();
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    try {
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      await plane.run(async (repositories) => {
        const database = repositories.database;
        assert(database);
        const quota = new PostgresQuotaRepository(database);
        const prefix = `event-time-${crypto.randomUUID()}`;
        const connectionId = `${prefix}-connection`;
        const apiKeyId = `${prefix}-key`;
        await database.transaction((transaction) =>
          transaction.query(
            "INSERT INTO omniroute.provider_connections(id,provider,created_at,updated_at) VALUES($1,'codex',$2,$2)",
            [connectionId, new Date().toISOString()]
          )
        );
        const group = await quota.createGroup(prefix);
        const pool = await quota.createPool({
          connectionId,
          groupId: group.id,
          name: prefix,
          allocations: [{ apiKeyId, weight: 100, policy: "hard" }],
        });
        await quota.upsertPlan(
          connectionId,
          "codex",
          [
            { unit: "tokens", window: "hourly", limit: 100 },
            { unit: "requests", window: "hourly", limit: 100 },
            { unit: "usd", window: "daily", limit: 100 },
          ],
          "manual"
        );
        await quota.setModelCap({
          poolId: pool.id,
          apiKeyId,
          model: "capped-model",
          capValue: 100,
          capUnit: "tokens",
        });
        const schedules = await quota.replaceSchedules(pool.id, [
          {
            id: `${prefix}-before`,
            days: [1],
            startMinute: 10 * 60,
            endMinute: 11 * 60,
            timezone: "UTC",
            mode: "allow",
            reserves: [],
            budgetValue: 100,
            budgetUnit: "requests",
            budgetWindow: "hourly",
            priority: 0,
            enabled: true,
          },
          {
            id: `${prefix}-after`,
            days: [1],
            startMinute: 11 * 60,
            endMinute: 12 * 60,
            timezone: "UTC",
            mode: "allow",
            reserves: [],
            budgetValue: 100,
            budgetUnit: "requests",
            budgetWindow: "hourly",
            priority: 0,
            enabled: true,
          },
        ]);
        await repositories.settings.update({ quotaStore: { driver: "postgres" } });
        const completedAtMs = Date.UTC(2026, 8, 21, 10, 59, 59, 900);
        const afterResetMs = Date.UTC(2026, 8, 21, 11, 0, 0, 100);
        const before = resolveActiveSchedule(schedules, completedAtMs);
        const after = resolveActiveSchedule(schedules, afterResetMs);
        assert(before.kind === "active" && after.kind === "active");
        assert.notEqual(before.schedule.id, after.schedule.id);
        assert.notEqual(
          Math.floor(completedAtMs / 3_600_000),
          Math.floor(afterResetMs / 3_600_000)
        );
        let entered!: () => void;
        const pricingEntered = new Promise<void>((resolve) => {
          entered = resolve;
        });
        let release!: () => void;
        const pricingGate = new Promise<void>((resolve) => {
          release = resolve;
        });
        const args = {
          apiKeyId,
          connectionId,
          provider: "codex",
          model: "capped-model",
          streamUsage: { prompt_tokens: 3, completion_tokens: 5 },
          streamStatus: 200,
          completedAtMs,
          async calculateCost() {
            entered();
            await pricingGate;
            return 0.125;
          },
        };
        const pending = scheduleStreamingQuotaShareConsumptionAsync(args);
        await Promise.race([
          pricingEntered,
          pending.then(() =>
            assert.fail("native hook settled before reaching its controlled cost owner")
          ),
        ]);
        const initial = await database.transaction((transaction) =>
          transaction.query("SELECT 1 FROM omniroute.quota_consumption WHERE api_key_id=$1", [
            apiKeyId,
          ])
        );
        assert.equal(initial.rowCount, 0);
        release();
        await pending;
        const rows = await database.transaction((transaction) =>
          transaction.query<{
            dimension_key: string;
            bucket_index: string;
            consumed: number;
            updated_at: string;
          }>(
            "SELECT dimension_key,bucket_index,consumed,updated_at FROM omniroute.quota_consumption WHERE api_key_id=$1",
            [apiKeyId]
          )
        );
        const hourBucket = Math.floor(completedAtMs / 3_600_000);
        const dayBucket = Math.floor(completedAtMs / 86_400_000);
        const canonical = (values: unknown[][]) =>
          values.map((value) => JSON.stringify(value)).sort();
        assert.deepEqual(
          canonical(
            rows.rows.map((row) => [
              row.dimension_key,
              Number(row.bucket_index),
              row.consumed,
              Number(row.updated_at),
            ])
          ),
          canonical([
            [`${pool.id}:tokens:hourly`, hourBucket, 8, completedAtMs],
            [`${pool.id}:requests:hourly`, hourBucket, 1, completedAtMs],
            [`${pool.id}:usd:daily`, dayBucket, 0.125, completedAtMs],
            [`${pool.id}:model:capped-model:tokens:hourly`, hourBucket, 8, completedAtMs],
            [
              `${scheduleBucketPoolId(pool.id, before.schedule.id)}:requests:hourly`,
              hourBucket,
              1,
              completedAtMs,
            ],
          ])
        );
        await scheduleStreamingQuotaShareConsumptionAsync({
          ...args,
          model: "uncapped-model",
          calculateCost: async () => 0.125,
        });
        const uncapped = await database.transaction((transaction) =>
          transaction.query(
            "SELECT 1 FROM omniroute.quota_consumption WHERE api_key_id=$1 AND starts_with(dimension_key,$2)",
            [apiKeyId, `${pool.id}:model:uncapped-model:`]
          )
        );
        assert.equal(uncapped.rowCount, 0);
        await quota.setModelCap({
          poolId: pool.id,
          apiKeyId,
          model: "placeholder-model",
          capValue: Number.EPSILON,
          capUnit: "tokens",
        });
        await scheduleQuotaShareConsumptionAsync({
          apiKeyId,
          connectionId,
          provider: "codex",
          model: "placeholder-model",
          usage: { prompt_tokens: "2", completion_tokens: 3 },
          estimatedCost: 0.25,
          completedAtMs,
        });
        const ignored = await database.transaction((transaction) =>
          transaction.query(
            "SELECT 1 FROM omniroute.quota_consumption WHERE api_key_id=$1 AND starts_with(dimension_key,$2)",
            [apiKeyId, `${pool.id}:model:placeholder-model:`]
          )
        );
        assert.equal(ignored.rowCount, 0);
        for (const invalid of [
          NaN,
          -1,
          0.5,
          Infinity,
          8_640_000_000_000_001,
          null as unknown as number,
        ]) {
          await scheduleStreamingQuotaShareConsumptionAsync({
            ...args,
            completedAtMs: invalid,
            calculateCost: async () => {
              assert.fail("invalid time must not enter a deferred owner");
            },
          });
        }
        const afterInvalid = await database.transaction((transaction) =>
          transaction.query<{ consumed: number }>(
            "SELECT consumed FROM omniroute.quota_consumption WHERE api_key_id=$1 AND dimension_key=$2 AND bucket_index=$3",
            [apiKeyId, `${pool.id}:requests:hourly`, hourBucket]
          )
        );
        assert.equal(afterInvalid.rows[0].consumed, 3);
        assert.equal(Date.now, originalClock);
      });
    } finally {
      await plane?.close();
      resetQuotaStoreSingleton();
      for (const [name, value] of [
        ["STORAGE_ENCRYPTION_KEY", previous.encryption],
        ["JWT_SECRET", previous.jwt],
        ["API_KEY_SECRET", previous.apiKey],
      ] as const) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    }
  }
);
