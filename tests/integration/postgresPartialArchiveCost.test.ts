import assert from "node:assert/strict";
import test from "node:test";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import { withPostgresCatalogSnapshot } from "../../src/lib/db/postgres/catalogSnapshot.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { PostgresUsageAggregationRepository } from "../../src/lib/db/postgres/usageAggregationRepository.ts";
import {
  getDailyCostRows,
  getModelUsageRows,
  getProviderCostRows,
  getServiceTierUsageRows,
  getPresetCostModelRows,
} from "../../src/lib/db/postgres/usageAnalyticsRepository.ts";
import {
  buildUnifiedSource,
  buildPresetUnifiedSource,
} from "../../src/lib/db/postgres/usageSources.ts";
import { queryNamedUsageRows } from "../../src/lib/db/postgres/usageQueries.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

interface Analytics {
  summary: { totalCost: number; totalRequests: number; totalTokens: number };
  dailyTrend: Array<{ cost: number }>;
  byModel: Array<{
    cost: number;
    requests: number;
    avgLatencyMs: number;
    totalTokens: number;
    provider: string;
  }>;
  byProvider: Array<{ cost: number }>;
  byServiceTier: Array<{ cost: number; serviceTier: string; requests: number }>;
  presetSummaries: Record<string, { totalCost: number }>;
}

test(
  "partial archives preserve independent cost operands and existing weighted consumers",
  {
    skip: !postgresTestsEnabled,
    timeout: 90_000,
  },
  async (t) => {
    const database = await createTestPostgresDatabase({ maxConnections: 8 });
    const savedKey = process.env.STORAGE_ENCRYPTION_KEY;
    const savedTtl = process.env.OMNIROUTE_ANALYTICS_CACHE_TTL_MS;
    process.env.STORAGE_ENCRYPTION_KEY = "partial-archive-isolated-test";
    process.env.OMNIROUTE_ANALYTICS_CACHE_TTL_MS = "0";
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    const today = new Date().toISOString().slice(0, 10);
    const yesterday = new Date(Date.now() - 86_400_000).toISOString().slice(0, 10);
    const twoDaysAgo = new Date(Date.now() - 2 * 86_400_000).toISOString().slice(0, 10);
    const sourceOptions = {
      sinceIso: null,
      untilIso: null,
      rawCutoffDate: today,
      apiKeyWhere: "",
      apiKeyParams: {},
    };
    const reset = () =>
      database.transaction((tx) =>
        tx.query(
          "TRUNCATE omniroute.usage_history,omniroute.native_usage_archive,omniroute.daily_usage_summary,omniroute.hourly_usage_summary,omniroute.call_logs RESTART IDENTITY"
        )
      );
    const raw = (
      day: string,
      input: number,
      latency: number | null,
      tier = "standard",
      provider = "openai"
    ) =>
      database.transaction((tx) =>
        tx.query(
          "INSERT INTO omniroute.usage_history(provider,model,timestamp,tokens_input,latency_ms,service_tier,api_key_id) VALUES($1,'partial-cost-model',$2,$3,$4,$5,'partial-key')",
          [provider, `${day}T10:00:00.000Z`, input, latency, tier]
        )
      );
    const archived = (
      day: string,
      requests: number,
      input: number,
      cost: number,
      provider = "openai"
    ) =>
      database.transaction((tx) =>
        tx.query(
          "INSERT INTO omniroute.native_usage_archive(provider,model,date,total_requests,total_input_tokens,total_cost) VALUES($1,'partial-cost-model',$2,$3,$4,$5)",
          [provider, day, requests, input, cost]
        )
      );
    const pricing = (input: number, provider = "openai") =>
      plane!.run(async (binding) => {
        const { updatePricing } = await import("../../src/lib/db/settings.ts");
        await withPostgresCatalogSnapshot(binding.database!, () =>
          updatePricing({
            [provider]: { "partial-cost-model": { input, output: 0 } },
          })
        );
      });
    const analytics = async (extra = ""): Promise<Analytics> =>
      plane!.run(async (binding) => {
        return withPostgresCatalogSnapshot(binding.database!, async () => {
          const route = await import("../../src/app/api/usage/analytics/route.ts");
          const response = await route.GET(
            new Request(`http://localhost/api/usage/analytics?range=all&presets=7d${extra}`)
          );
          const body: unknown = await response.json();
          assert.equal(response.status, 200, JSON.stringify(body));
          return body as Analytics;
        });
      });
    const assertCosts = (body: Analytics, expected: number) => {
      assert.equal(body.summary.totalCost, expected);
      for (const rows of [body.dailyTrend, body.byModel, body.byProvider, body.byServiceTier])
        assert.equal(
          rows.reduce((sum, row) => sum + row.cost, 0),
          expected
        );
      assert.equal(body.presetSummaries["7d"].totalCost, expected);
    };

    try {
      await migratePostgresSchema(database);
      await reset();
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions({ maxConnections: 8 }),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      await plane.run((binding) => binding.settings.update({ requireLogin: false }));
      await pricing(1_000_000);

      await t.test(
        "a committed 500-row prune batch plus one raw row costs 5 + 7 in every projection",
        async () => {
          await reset();
          await database.transaction((tx) =>
            tx.query(
              `INSERT INTO omniroute.usage_history(provider,model,timestamp,tokens_input,latency_ms,api_key_id)
         SELECT 'openai','partial-cost-model',$1,CASE WHEN n=501 THEN 7 ELSE 1 END,80,'partial-key'
         FROM generate_series(1,501) AS n`,
              [`${yesterday}T10:00:00.000Z`]
            )
          );
          let priced = 0;
          await assert.rejects(
            new PostgresUsageAggregationRepository(database).prune(
              `${today}T00:00:00.000Z`,
              async () => {
                priced++;
                if (priced === 501) throw new Error("controlled second-batch pricing failure");
                return priced === 1 ? 5 : 0;
              }
            ),
            /controlled second-batch/
          );
          const retained = await database.transaction((tx) =>
            tx.query<{ raw: string; archived: string; cost: number }>(
              `SELECT (SELECT COUNT(*) FROM omniroute.usage_history) AS raw,
           (SELECT SUM(total_requests) FROM omniroute.native_usage_archive) AS archived,
           (SELECT SUM(total_cost) FROM omniroute.native_usage_archive) AS cost`
            )
          );
          assert.equal(Number(retained.rows[0].raw), 1);
          assert.equal(Number(retained.rows[0].archived), 500);
          assert.equal(retained.rows[0].cost, 5);
          const unified = buildUnifiedSource(sourceOptions),
            preset = buildPresetUnifiedSource(sourceOptions);
          const projections = await Promise.all([
            getDailyCostRows(database, unified.unifiedSource, unified.unifiedParams),
            getModelUsageRows(database, unified.unifiedSource, unified.unifiedParams),
            getProviderCostRows(database, unified.unifiedSource, unified.unifiedParams),
            getServiceTierUsageRows(database, unified.unifiedSource, unified.unifiedParams),
            getPresetCostModelRows(database, preset.unifiedSource, preset.unifiedParams),
          ]);
          for (const rows of projections) {
            assert.equal(rows.length, 2);
            assert.deepEqual(rows.map((row) => row.isAggregated).sort(), [0, 1]);
            assert.equal(rows.find((row) => row.isAggregated === 1)?.storedCost, 5);
            assert.equal(rows.find((row) => row.isAggregated === 0)?.promptTokens, 7);
            assert.equal(
              rows.reduce((sum, row) => sum + row.promptTokens, 0),
              507
            );
          }
          const body = await analytics();
          assertCosts(body, 12);
          assert.equal(body.summary.totalRequests, 501);
          assert.equal(body.summary.totalTokens, 507);
          assert.equal(body.byModel[0].requests, 501);
          assert.equal(body.byModel[0].avgLatencyMs, 80);

          await pricing(2_000_000);
          assertCosts(await analytics(), 19 /* archive remains exactly 5 */);
          assertCosts(await analytics("&apiKeyIds=partial-key"), 14);
          await pricing(1_000_000);
        }
      );

      await t.test(
        "raw-only, archive-only, zero-cost fallback and flat-rate controls",
        async () => {
          await reset();
          await raw(yesterday, 7, 80);
          assertCosts(await analytics(), 7);
          await reset();
          await archived(yesterday, 5, 500, 5);
          assertCosts(await analytics(), 5);
          assert.equal((await analytics()).byModel[0].avgLatencyMs, 0);

          await reset();
          await database.transaction((tx) =>
            tx.query(
              "INSERT INTO omniroute.daily_usage_summary(provider,model,date,total_requests,total_input_tokens,total_cost) VALUES('openai','partial-cost-model',$1,2,3,0)",
              [twoDaysAgo]
            )
          );
          assertCosts(await analytics(), 3 /* legacy zero cost retains pricing fallback */);

          await reset();
          await archived(yesterday, 5, 500, 5, "claude");
          await raw(yesterday, 7, 80, "standard", "claude");
          await pricing(1_000_000, "claude");
          assertCosts(await analytics(), 0);
          assertCosts(await analytics("&includeFlatRateEstimates=true"), 12);
        }
      );

      await t.test(
        "latency cohorts preserve day, tier, NULL observations and request weighting",
        async () => {
          await reset();
          await raw(yesterday, 1, 80, "");
          await raw(yesterday, 1, 120);
          await raw(yesterday, 1, null);
          await archived(yesterday, 500, 500, 5);
          await raw(twoDaysAgo, 1, 300);
          await archived(twoDaysAgo, 4, 4, 2);
          await raw(yesterday, 1, 600, "flex");
          await archived(today, 10, 10, 1);
          const source = buildUnifiedSource(sourceOptions);
          const rows = await getModelUsageRows(
            database,
            source.unifiedSource,
            source.unifiedParams
          );
          // Former grouping is an independent SQL oracle for the consumer's average.
          const oldGroups = await queryNamedUsageRows<{ requests: number; latency: number }>(
            database,
            `SELECT SUM(requests) AS requests,COALESCE(AVG(latency_ms),0) AS latency
         FROM ${source.unifiedSource} AS original
         GROUP BY to_char(timestamp::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD'),
           LOWER(model),LOWER(provider),COALESCE(NULLIF(service_tier,''),'standard')`,
            source.unifiedParams,
            ["requests", "latency"]
          );
          assert.ok(oldGroups.length > 0);
          const weighted = rows.reduce((sum, row) => sum + row.avgLatencyMs * row.requests, 0);
          const requests = rows.reduce((sum, row) => sum + row.requests, 0);
          assert.equal(requests, 519);
          assert.equal(weighted, 100 * 503 + 300 * 5 + 600);
          assert.equal(
            oldGroups.reduce((sum, row) => sum + Number(row.requests) * row.latency, 0),
            weighted
          );
          assert.equal(rows.filter((row) => row.requests === 500)[0].avgLatencyMs, 100);
          assert.equal(rows.filter((row) => row.requests === 10)[0].avgLatencyMs, 0);
          assert.equal(rows.filter((row) => row.serviceTier === "flex")[0].avgLatencyMs, 600);
          assert.equal(rows[0].requests, 500);
          assert.equal(rows[1].requests, 3, "operands retain their original group's rank");
          const body = await analytics();
          assert.equal(body.byModel[0].avgLatencyMs, Math.round(weighted / requests));
          assert.equal(body.byModel[0].requests, requests);
          assert.equal(body.byServiceTier.find((row) => row.serviceTier === "flex")?.requests, 1);
          await reset();
          assert.deepEqual(
            await getModelUsageRows(database, source.unifiedSource, source.unifiedParams),
            []
          );
        }
      );
      await t.test(
        "splitting operands preserves the representative provider for a merged model",
        async () => {
          await reset();
          await archived(yesterday, 6, 6, 5);
          for (let i = 0; i < 4; i++) await raw(yesterday, 1, 100);
          for (let i = 0; i < 8; i++) await raw(yesterday, 1, 100, "standard", "anthropic");
          await pricing(1_000_000, "anthropic");
          const source = buildUnifiedSource(sourceOptions);
          const rows = await getModelUsageRows(
            database,
            source.unifiedSource,
            source.unifiedParams
          );
          assert.equal(
            rows[0].provider,
            "openai",
            "original group of ten outranks the group of eight"
          );
          assert.equal(rows[1].provider, "openai");
          assert.equal(rows[2].provider, "anthropic");
          const body = await analytics();
          assert.equal(body.byModel[0].provider, "openai");
          assert.equal(body.byModel[0].requests, 18);
          assertCosts(body, 17);
        }
      );
    } finally {
      await plane?.close();
      await database.close();
      if (savedKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = savedKey;
      if (savedTtl === undefined) delete process.env.OMNIROUTE_ANALYTICS_CACHE_TTL_MS;
      else process.env.OMNIROUTE_ANALYTICS_CACHE_TTL_MS = savedTtl;
    }
  }
);
