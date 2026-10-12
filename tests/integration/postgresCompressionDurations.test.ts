import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  writeCompressionAnalytics,
  writeCompressionSkip,
} from "../../open-sse/handlers/chatCore/compressionAnalyticsWrite.ts";
import type { CompressionStats } from "../../open-sse/services/compression/types.ts";
import {
  getCompressionAnalyticsSummaryPostgres,
  insertCompressionAnalyticsRowPostgres,
} from "../../src/lib/db/postgres/compressionAnalyticsRepository.ts";
import { createPostgresCompressionTelemetryRepository } from "../../src/lib/db/postgres/compressionTelemetryRepository.ts";
import type { PostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import {
  isPostgresErrorKind,
  type PostgresDatabase,
  type PostgresTransaction,
} from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

function row(requestId: string, duration: number | null) {
  return {
    timestamp: new Date().toISOString(),
    request_id: requestId,
    mode: "stacked",
    original_tokens: 100,
    compressed_tokens: 70,
    tokens_saved: 30,
    duration_ms: duration,
  };
}

async function durations(database: PostgresDatabase) {
  return database.transaction(async (tx) => {
    const aggregate = await tx.query(
      "SELECT request_id,duration_ms FROM omniroute.compression_analytics ORDER BY request_id"
    );
    const breakdown = await tx.query(
      "SELECT request_id,engine,duration_ms FROM omniroute.compression_engine_breakdown ORDER BY request_id,engine"
    );
    return { aggregate: aggregate.rows, breakdown: breakdown.rows };
  });
}

test(
  "native compression producers preserve fractional aggregate, breakdown and skip durations",
  { skip: !postgresTestsEnabled },
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "omniroute-pg-compression-duration-"));
    const saved = {
      DATA_DIR: process.env.DATA_DIR,
      STORAGE_ENCRYPTION_KEY: process.env.STORAGE_ENCRYPTION_KEY,
      JWT_SECRET: process.env.JWT_SECRET,
      API_KEY_SECRET: process.env.API_KEY_SECRET,
    };
    process.env.DATA_DIR = directory;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-compression-duration-key";
    process.env.JWT_SECRET = "isolated-compression-duration-jwt";
    process.env.API_KEY_SECRET = "isolated-compression-duration-api";
    const database = await createTestPostgresDatabase();
    let plane: PostgresControlPlane | undefined;
    try {
      await database.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      const { initializePostgresControlPlane } =
        await import("../../src/lib/db/postgres/controlPlane.ts");
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: {
          applyRuntimeSettings() {},
          scheduleCatalogRevalidationAfterInit() {},
        },
      });
      const warnings: unknown[][] = [];
      const stats: CompressionStats = {
        originalTokens: 100,
        compressedTokens: 70,
        savingsPercent: 30,
        techniquesUsed: [],
        mode: "stacked",
        timestamp: Date.now(),
        durationMs: 1.23,
        engineBreakdown: [
          {
            engine: "lite",
            originalTokens: 100,
            compressedTokens: 70,
            savingsPercent: 30,
            techniquesUsed: [],
            durationMs: 0.45,
          },
        ],
      };
      const options = {
        stats,
        provider: "fixture",
        effectiveModel: "fixture",
        effectiveServiceTier: undefined,
        comboName: null,
        mode: "stacked",
        compressionComboId: null,
        skillRequestId: "fractional-success",
        cavemanOutputModeApplied: false,
        cavemanOutputModeIntensity: null,
        log: { warn: (...args: unknown[]) => warnings.push(args) },
      };
      await plane.run(async () => {
        await writeCompressionAnalytics(options, { calculateCost: async () => 0 });
        await writeCompressionSkip(
          {
            ...options,
            stats: { ...stats, durationMs: 0.01, compressedTokens: 100 },
            skillRequestId: "fractional-skip",
          },
          "no_savings"
        );
      });
      assert.deepEqual(warnings, [], "normal fractional producers must not skip telemetry");
      assert.deepEqual(await durations(database), {
        aggregate: [
          { request_id: "fractional-skip", duration_ms: 0.01 },
          { request_id: "fractional-success", duration_ms: 1.23 },
        ],
        breakdown: [{ request_id: "fractional-success", engine: "lite", duration_ms: 0.45 }],
      });
      const summary = await getCompressionAnalyticsSummaryPostgres(database);
      assert.equal(summary.totalRequests, 1);
      assert.equal(summary.totalSkipped, 1);
      assert.equal(summary.avgDurationMs, 1, "the existing rounded summary display is preserved");
    } finally {
      await plane?.close();
      await database.close();
      for (const [name, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      await rm(directory, { recursive: true, force: true });
    }
  }
);

test(
  "duration upgrade preserves v5 history and data, and rolls back both columns on failure",
  { skip: !postgresTestsEnabled },
  async () => {
    const database = await createTestPostgresDatabase();
    try {
      await database.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      const stopAtVersion5 = new Error("Test migration prefix complete");
      let prefixReached = false;
      const prefix = new Proxy(database, {
        get(target, property, receiver) {
          if (property !== "transaction") return Reflect.get(target, property, receiver);
          return <T>(body: (tx: PostgresTransaction) => Promise<T>) =>
            target.transaction(async (tx) => {
              try {
                return await body({
                  async query<Row>(sql: string, values?: unknown[]) {
                    const result = await tx.query<Row>(sql, values);
                    if (
                      sql ===
                        "INSERT INTO omniroute.schema_migrations(version,name,checksum) VALUES($1,$2,$3)" &&
                      values?.[0] === 5
                    ) {
                      prefixReached = true;
                      throw stopAtVersion5;
                    }
                    return result;
                  },
                });
              } catch (error) {
                if (error !== stopAtVersion5) throw error;
                return undefined as T;
              }
            });
        },
      });
      await migratePostgresSchema(prefix);
      assert.equal(prefixReached, true);
      const history = async () =>
        (
          await database.transaction((tx) =>
            tx.query(
              "SELECT version,name,checksum,applied_at::text AS applied_at FROM omniroute.schema_migrations ORDER BY version"
            )
          )
        ).rows;
      const columnTypes = async () =>
        (
          await database.transaction((tx) =>
            tx.query(
              "SELECT table_name,data_type FROM information_schema.columns WHERE table_schema='omniroute' AND table_name IN ('compression_analytics','compression_engine_breakdown') AND column_name='duration_ms' ORDER BY table_name"
            )
          )
        ).rows;
      const oldHistory = await history();
      assert.deepEqual(
        oldHistory.map((entry) => entry.version),
        [1, 2, 3, 4, 5]
      );
      const oldTypes = await columnTypes();
      assert.deepEqual(
        oldTypes.map((entry) => entry.data_type),
        ["integer", "integer"]
      );
      const telemetry = createPostgresCompressionTelemetryRepository(database);
      await insertCompressionAnalyticsRowPostgres(database, row("legacy-integer", 2));
      await insertCompressionAnalyticsRowPostgres(database, row("legacy-null", null));
      await telemetry.insertBreakdown([{ ...row("legacy-integer", 2), engine: "lite" }]);
      const oldData = await durations(database);
      const integerInputError = (error: unknown) =>
        isPostgresErrorKind(error, "query_failed") && error.sqlState === "22P02";
      await assert.rejects(
        insertCompressionAnalyticsRowPostgres(database, row("fractional-upgrade", 1.23)),
        integerInputError,
        "the genuine v5 extended-protocol integer binding rejects a normal fractional value"
      );
      await assert.rejects(
        telemetry.insertBreakdown([{ ...row("fractional-upgrade", 0.45), engine: "lite" }]),
        integerInputError
      );
      assert.deepEqual(await durations(database), oldData);
      const failedUpgrade = new Error("Injected second duration ALTER failure");
      const interrupted = new Proxy(database, {
        get(target, property, receiver) {
          if (property !== "transaction") return Reflect.get(target, property, receiver);
          return <T>(body: (tx: PostgresTransaction) => Promise<T>) =>
            target.transaction((tx) =>
              body({
                query<Row>(sql: string, values?: unknown[]) {
                  if (sql.startsWith("ALTER TABLE omniroute.compression_engine_breakdown")) {
                    return Promise.reject(failedUpgrade);
                  }
                  return tx.query<Row>(sql, values);
                },
              })
            );
        },
      });
      await assert.rejects(migratePostgresSchema(interrupted), (error) => error === failedUpgrade);
      assert.deepEqual(await history(), oldHistory);
      assert.deepEqual(await columnTypes(), oldTypes);
      assert.deepEqual(await durations(database), oldData);
      await migratePostgresSchema(database);
      const upgradedHistory = await history();
      assert.deepEqual(upgradedHistory.slice(0, 5), oldHistory);
      assert.equal(upgradedHistory[5].name, "fractional_compression_durations");
      assert.deepEqual(await columnTypes(), [
        { table_name: "compression_analytics", data_type: "double precision" },
        { table_name: "compression_engine_breakdown", data_type: "double precision" },
      ]);
      assert.deepEqual(await durations(database), oldData);
      await insertCompressionAnalyticsRowPostgres(database, row("fractional-upgrade", 1.23));
      await telemetry.insertBreakdown([{ ...row("fractional-upgrade", 0.45), engine: "lite" }]);
      const upgradedData = await durations(database);
      assert.equal(upgradedData.aggregate[0].duration_ms, 1.23);
      assert.equal(upgradedData.breakdown[0].duration_ms, 0.45);
      await migratePostgresSchema(database);
      assert.deepEqual(await history(), upgradedHistory);
      assert.deepEqual(await durations(database), upgradedData);
    } finally {
      await database.close();
    }
  }
);
