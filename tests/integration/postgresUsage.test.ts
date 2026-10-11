import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { PostgresUsageHistoryRepository } from "../../src/lib/db/postgres/usageHistoryRepository.ts";
import {
  PostgresUsageAggregationRepository,
  UsageArchiveChangedError,
} from "../../src/lib/db/postgres/usageAggregationRepository.ts";
import { PostgresCallLogRepository } from "../../src/lib/db/postgres/callLogRepository.ts";
import { PostgresProxyLogStore } from "../../src/lib/db/postgres/proxyLogStore.ts";
import { PostgresDetailedLogRepository } from "../../src/lib/db/postgres/detailedLogRepository.ts";
import {
  getUsageSummary,
  getDailyUsage,
  getAccountUsageRows,
  getWeeklyPatternRows,
  getEndpointUsageRows,
  getPresetCostModelRows,
} from "../../src/lib/db/postgres/usageAnalyticsRepository.ts";
import {
  getProviderCallStats,
  getModelCallStats,
} from "../../src/lib/db/postgres/providerStatsRepository.ts";
import {
  getAutoRoutingTotalCount,
  getAutoRoutingVariantBreakdown,
  getAutoRoutingTopProviders,
} from "../../src/lib/db/postgres/usageLogsRepository.ts";
import {
  getRecentEgressIpForConnection,
  updateAttemptTiming,
} from "../../src/lib/db/postgres/proxyLogsRepository.ts";
import {
  buildUnifiedSource,
  buildPresetUnifiedSource,
} from "../../src/lib/db/postgres/usageSources.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

async function reset(database: PostgresDatabase): Promise<void> {
  await database.transaction(async (transaction) => {
    await transaction.query(`TRUNCATE omniroute.usage_history,omniroute.call_logs,omniroute.proxy_logs,
   omniroute.request_detail_logs,omniroute.native_usage_archive,omniroute.daily_usage_summary,
   omniroute.hourly_usage_summary,omniroute.artifact_deletion_queue RESTART IDENTITY`);
  });
}
function metadata(id: string, timestamp = "2026-10-11T10:00:00.000Z"): Record<string, unknown> {
  return {
    id,
    timestamp,
    provider: "openai",
    model: "test-model",
    status: 200,
    duration: 1.5,
    tokensIn: 10,
    tokensOut: 5,
    detailState: "none",
    hasRequestBody: 0,
    hasResponseBody: 0,
    hasPipelineDetails: 0,
    videoContentRemoved: 0,
    method: "POST",
    path: "/v1/chat/completions",
    account: "account",
    cacheSource: "upstream",
  };
}

test(
  "PostgreSQL usage, logging, export and retention owners",
  { skip: !postgresTestsEnabled },
  async (context) => {
    const database = await createTestPostgresDatabase({ maxConnections: 8 });
    try {
      await migratePostgresSchema(database);
      const history = new PostgresUsageHistoryRepository(database);
      const logs = new PostgresCallLogRepository(database);
      await context.test(
        "concurrent duplicate saves retain one stable receipt and backfill metadata",
        async () => {
          await reset(database);
          const entry = {
            provider: "openai",
            model: "test-model",
            timestamp: "2026-10-11T10:00:00.000Z",
            tokens: { input: 10, output: 5 },
            latencyMs: 1.5,
            timeToFirstTokenMs: 0.5,
          };
          const receipts = await Promise.all(Array.from({ length: 6 }, () => history.save(entry)));
          assert.equal(receipts.filter((receipt) => receipt.inserted).length, 1);
          assert.equal(new Set(receipts.map((receipt) => receipt.id)).size, 1);
          const filled = await history.save({
            ...entry,
            endpoint: "/v1/responses",
            cpaAuthIndex: "opaque-index",
          });
          assert.equal(filled.inserted, false);
          assert.equal(filled.id, receipts[0].id);
          const row = (await history.history())[0];
          assert.equal(row.endpoint, "/v1/responses");
          assert.equal(row.cpa_auth_index, "opaque-index");
          assert.equal(row.latency_ms, 1.5);
          assert.equal(row.ttft_ms, 0.5);
        }
      );
      await context.test(
        "keyset history pagination retains rows with identical timestamps",
        async () => {
          await reset(database);
          for (const input of [1, 2, 3])
            await history.save({
              provider: "openai",
              model: "test-model",
              timestamp: "2026-10-11T10:00:00.000Z",
              tokens: { input, output: 0 },
            });
          const first = await history.page(null, 2);
          assert.equal(first.rows.length, 2);
          assert.ok(first.nextCursor);
          const second = await history.page(null, 2, first.nextCursor);
          assert.equal(second.rows.length, 1);
          assert.equal(new Set([...first.rows, ...second.rows].map((row) => row.id)).size, 3);
        }
      );
      await context.test(
        "partial-day cleanup is atomic, additive and repeatable without double counting",
        async () => {
          await reset(database);
          for (const [hour, input] of [
            [10, 10],
            [11, 20],
            [12, 30],
          ])
            await history.save({
              provider: "openai",
              model: "test-model",
              timestamp: `2026-10-11T${hour}:00:00.000Z`,
              tokens: { input, output: 0 },
            });
          const archive = new PostgresUsageAggregationRepository(database);
          assert.equal((await archive.prune("2026-10-11T10:30:00.000Z", async () => 1)).deleted, 1);
          assert.equal((await archive.prune("2026-10-11T11:30:00.000Z", async () => 1)).deleted, 1);
          assert.equal((await archive.prune("2026-10-11T11:30:00.000Z", async () => 1)).deleted, 0);
          const source = buildUnifiedSource({
            sinceIso: "2026-10-11T00:00:00.000Z",
            untilIso: null,
            rawCutoffDate: "2025-10-11",
            apiKeyWhere: "",
            apiKeyParams: {},
          });
          const summary = await getUsageSummary(
            database,
            source.unifiedSource,
            source.unifiedParams
          );
          assert.equal(summary.totalRequests, 3);
          assert.equal(summary.totalTokens, 60);
          assert.equal(
            (await getDailyUsage(database, source.unifiedSource, source.unifiedParams))[0].requests,
            3
          );
          assert.equal((await history.history()).length, 1);
        }
      );
      await context.test(
        "native archive supersedes a legacy day after the last raw row is pruned",
        async () => {
          await reset(database);
          await history.save({
            provider: "openai",
            model: "test-model",
            timestamp: "2026-01-01T10:00:00.000Z",
            tokens: { input: 10, output: 5 },
          });
          await database.transaction((transaction) =>
            transaction.query(
              "INSERT INTO omniroute.daily_usage_summary(provider,model,date,total_requests,total_input_tokens,total_output_tokens,total_cost) VALUES('openai','test-model','2026-01-01',1,10,5,1)"
            )
          );
          const archive = new PostgresUsageAggregationRepository(database);
          await archive.prune("2026-02-01", async () => 1);
          const options = {
            sinceIso: null,
            untilIso: null,
            rawCutoffDate: "2026-02-01",
            apiKeyWhere: "",
            apiKeyParams: {},
          };
          const unified = buildUnifiedSource(options);
          const summary = await getUsageSummary(
            database,
            unified.unifiedSource,
            unified.unifiedParams
          );
          assert.equal(summary.totalRequests, 1);
          assert.equal(summary.totalTokens, 15);
          const preset = buildPresetUnifiedSource(options);
          const costs = await getPresetCostModelRows(
            database,
            preset.unifiedSource,
            preset.unifiedParams
          );
          assert.equal(costs.length, 1);
          assert.equal(costs[0].promptTokens, 10);
          assert.equal(costs[0].storedCost, 1);
        }
      );
      await context.test(
        "surviving old raw rows remain visible after a later archive batch fails",
        async () => {
          await reset(database);
          await database.transaction((transaction) =>
            transaction.query(`
    INSERT INTO omniroute.usage_history(provider,model,timestamp,tokens_input,tokens_output,service_tier)
    SELECT 'openai','test-model','2026-01-01T10:00:00.000Z',1,0,'standard' FROM generate_series(1,501)`)
          );
          const archive = new PostgresUsageAggregationRepository(database);
          let priced = 0;
          await assert.rejects(
            archive.prune("2026-02-01", async () => {
              if (++priced === 501) throw new Error("price unavailable");
              return 0;
            })
          );
          assert.equal((await history.history()).length, 1);
          const source = buildUnifiedSource({
            sinceIso: "2026-01-01T00:00:00.000Z",
            untilIso: null,
            rawCutoffDate: "2026-02-01",
            apiKeyWhere: "",
            apiKeyParams: {},
          });
          const summary = await getUsageSummary(
            database,
            source.unifiedSource,
            source.unifiedParams
          );
          assert.equal(summary.totalRequests, 501);
          assert.equal(summary.totalTokens, 501);
        }
      );
      await context.test("archive UTC day bounds exclude adjacent days", async () => {
        await reset(database);
        await history.save({
          provider: "openai",
          model: "test-model",
          timestamp: "2026-01-02T00:00:00+01:00",
          tokens: { input: 1, output: 0 },
        });
        await history.save({
          provider: "openai",
          model: "test-model",
          timestamp: "2026-01-02T00:00:00.000Z",
          tokens: { input: 2, output: 0 },
        });
        await history.save({
          provider: "openai",
          model: "test-model",
          timestamp: "2026-01-03T00:00:00.000Z",
          tokens: { input: 3, output: 0 },
        });
        await new PostgresUsageAggregationRepository(database).prune("2026-02-01", async () => 0);
        const options = {
          sinceIso: "2026-01-02T00:00:00.000Z",
          untilIso: "2026-01-02T23:59:59.999Z",
          rawCutoffDate: "2026-02-01",
          apiKeyWhere: "",
          apiKeyParams: {},
        };
        const source = buildUnifiedSource(options);
        const summary = await getUsageSummary(database, source.unifiedSource, source.unifiedParams);
        assert.equal(summary.totalRequests, 1);
        assert.equal(summary.totalTokens, 2);
        const preset = buildPresetUnifiedSource(options);
        const costs = await getPresetCostModelRows(
          database,
          preset.unifiedSource,
          preset.unifiedParams
        );
        assert.equal(costs.length, 1);
        assert.equal(costs[0].promptTokens, 2);
      });
      await context.test("pricing failure and changed snapshot delete no usage rows", async () => {
        await reset(database);
        await history.save({
          provider: "openai",
          model: "test-model",
          timestamp: "2026-01-01T00:00:00.000Z",
          tokens: { input: 10, output: 0 },
        });
        const archive = new PostgresUsageAggregationRepository(database);
        await assert.rejects(
          archive.prune("2026-02-01", async () => {
            throw new Error("price unavailable");
          })
        );
        assert.equal((await history.history()).length, 1);
        await assert.rejects(
          archive.prune("2026-02-01", async () => {
            await database.transaction((transaction) =>
              transaction.query("UPDATE omniroute.usage_history SET tokens_input=11")
            );
            return 1;
          }),
          UsageArchiveChangedError
        );
        assert.equal((await history.history()).length, 1);
        const rows = await database.transaction((transaction) =>
          transaction.query("SELECT * FROM omniroute.native_usage_archive")
        );
        assert.equal(rows.rows.length, 0);
      });
      await context.test(
        "native grouped projections retain case-sensitive camel fields and UTC buckets",
        async () => {
          await reset(database);
          await history.save({
            provider: "openai",
            model: "test-model",
            timestamp: "2026-10-11T01:00:00+02:00",
            apiKeyId: "usage-key",
            tokens: { input: 10, output: 5 },
            endpoint: "/v1/responses",
          });
          const source = buildUnifiedSource({
            sinceIso: "2026-10-10T00:00:00.000Z",
            untilIso: null,
            rawCutoffDate: "2025-01-01",
            apiKeyWhere: "",
            apiKeyParams: {},
          });
          assert.equal(
            (await getDailyUsage(database, source.unifiedSource, source.unifiedParams))[0].date,
            "2026-10-10"
          );
          const summary = await getUsageSummary(
            database,
            source.unifiedSource,
            source.unifiedParams
          );
          assert.equal(summary.uniqueApiKeys, 1);
          assert.equal(typeof summary.promptTokens, "number");
          assert.equal(
            (await getWeeklyPatternRows(database, source.unifiedSource, source.unifiedParams))[0]
              .dayOfWeek,
            "6"
          );
          assert.equal((await getAccountUsageRows(database, "", {}))[0].requests, 1);
          assert.equal(
            (await getEndpointUsageRows(database, { sinceIso: "2026-10-10" }))[0].endpoint,
            "/v1/responses"
          );
        }
      );
      await context.test("metadata privacy is rechecked at the write fence", async () => {
        await reset(database);
        await database.transaction(async (transaction) => {
          await transaction.query("DELETE FROM omniroute.api_keys WHERE id='usage-private'");
          await transaction.query(
            "INSERT INTO omniroute.api_keys(id,name,key,created_at,no_log) VALUES('usage-private','private','usage-test-key','now',1)"
          );
        });
        const result = await logs.insert({
          ...metadata("private-log"),
          apiKeyId: "usage-private",
          requestSummary: "payload",
          artifactRelPath: "attempt-owned.json",
          hasRequestBody: 1,
          detailState: "ready",
        });
        assert.equal(result.inserted, true);
        assert.equal(result.payloadAccepted, false);
        const row = await logs.byId("private-log");
        assert.ok(row);
        assert.equal(row.artifact_relpath, null);
        assert.equal(row.request_summary, null);
        assert.equal(row.has_request_body, 0);
        const queue = await database.transaction((transaction) =>
          transaction.query("SELECT relpath FROM omniroute.artifact_deletion_queue")
        );
        assert.deepEqual(queue.rows, [{ relpath: "attempt-owned.json" }]);
      });
      await context.test(
        "duplicate call ids report a known no-op and preserve the first row",
        async () => {
          await reset(database);
          assert.equal((await logs.insert(metadata("same"))).inserted, true);
          assert.equal((await logs.insert({ ...metadata("same"), tokensIn: 20 })).inserted, false);
          assert.equal((await logs.byId("same"))!.tokens_in, 10);
        }
      );
      await context.test(
        "export cursor cannot advance past a lower uncommitted insertion",
        async () => {
          await reset(database);
          let admitted!: () => void, release!: () => void;
          const waiting = new Promise<void>((resolve) => {
              admitted = resolve;
            }),
            released = new Promise<void>((resolve) => {
              release = resolve;
            });
          const first = database.transaction(async (transaction) => {
            await transaction.query("SELECT pg_advisory_xact_lock(18765,8)");
            await transaction.query(
              "INSERT INTO omniroute.call_logs(id,timestamp,video_content_removed) VALUES('first','2026-10-11',0)"
            );
            admitted();
            await released;
          });
          await waiting;
          const second = logs.insert(metadata("second"));
          try {
            await delay(25);
            assert.equal(await logs.maxCursor(), 0);
          } finally {
            release();
          }
          await first;
          await second;
          const rows = await logs.exportAfter(0, 100);
          assert.deepEqual(
            rows.map((row) => row.id),
            ["first", "second"]
          );
          assert.equal(await logs.countAfter(0), 2);
        }
      );
      await context.test(
        "retention queues a shared imported artifact only after its last reference",
        async () => {
          await reset(database);
          await logs.insert({
            ...metadata("older", "2026-01-01T00:00:00.000Z"),
            artifactRelPath: "shared-legacy.json",
          });
          await logs.insert({
            ...metadata("newer", "2026-02-01T00:00:00.000Z"),
            artifactRelPath: "shared-legacy.json",
          });
          assert.equal((await logs.deleteBefore("2026-01-02", 500)).paths.length, 0);
          assert.deepEqual((await logs.deleteBefore("2026-03-01", 500)).paths, [
            "shared-legacy.json",
          ]);
          const rows = await database.transaction((transaction) =>
            transaction.query("SELECT relpath FROM omniroute.artifact_deletion_queue")
          );
          assert.deepEqual(rows.rows, [{ relpath: "shared-legacy.json" }]);
        }
      );
      await context.test("provider and auto-routing analytics use native groups", async () => {
        await reset(database);
        await database.transaction(async (transaction) => {
          await transaction.query("DELETE FROM omniroute.provider_nodes WHERE id='usage-node'");
          await transaction.query(
            "INSERT INTO omniroute.provider_nodes(id,type,name,prefix,api_type,base_url,created_at,updated_at) VALUES('usage-node','openai-compatible','Node','node','chat','https://example.test','now','now')"
          );
        });
        await logs.insert({
          ...metadata("auto-a"),
          provider: "usage-node",
          requestedModel: "auto/fast",
        });
        await logs.insert({
          ...metadata("auto-b"),
          provider: "usage-node",
          requestedModel: "auto",
          status: 500,
        });
        assert.equal((await getProviderCallStats(database))[0].nodeName, "Node");
        assert.equal((await getModelCallStats(database))[0].requests, 2);
        assert.equal((await getAutoRoutingTotalCount(database)).count, 2);
        assert.deepEqual(
          (await getAutoRoutingVariantBreakdown(database)).map((row) => row.variant).sort(),
          ["default", "fast"]
        );
        assert.equal((await getAutoRoutingTopProviders(database))[0].provider, "usage-node");
      });
      await context.test("proxy batch ids and delayed timing updates remain stable", async () => {
        await reset(database);
        const store = new PostgresProxyLogStore(database);
        const entry = {
          id: "proxy-one",
          timestamp: "2026-10-11T00:00:00.000Z",
          status: "success",
          proxy: null,
          level: "direct",
          levelId: null,
          provider: "openai",
          targetUrl: null,
          clientIp: null,
          egressIp: "203.0.113.1",
          latencyMs: 10,
          error: null,
          connectionId: "connection",
          comboId: null,
          account: null,
          rotationAccount: null,
          correlationId: null,
          tlsFingerprint: false,
          upstreamStatus: 200,
          attemptNumber: 1,
          attemptIssue: "served" as const,
          headersMs: 2,
          firstChunkMs: null,
        };
        await store.append([entry]);
        await store.append([entry]);
        assert.equal((await store.list()).length, 1);
        assert.equal(await updateAttemptTiming(database, entry.id, { firstChunkMs: 9 }), true);
        assert.equal((await store.list())[0].first_chunk_ms, 9);
        assert.deepEqual(
          await getRecentEgressIpForConnection(database, "connection", "2026-10-10"),
          { egressIp: "203.0.113.1", at: entry.timestamp }
        );
      });
      await context.test("historical detailed-log ring is bounded at 500 entries", async () => {
        await reset(database);
        const detailed = new PostgresDetailedLogRepository(database);
        for (let index = 0; index < 501; index++)
          await detailed.save([
            String(index),
            null,
            `2026-10-11T00:00:${String(index % 60).padStart(2, "0")}.000Z`,
            null,
            null,
            null,
            null,
            "openai",
            "test-model",
            null,
            null,
            0,
          ]);
        assert.equal(await detailed.count(), 500);
      });
    } finally {
      await database.close();
    }
  }
);
