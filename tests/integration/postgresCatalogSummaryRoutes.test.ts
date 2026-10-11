import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import { withPostgresCatalogSnapshot } from "../../src/lib/db/postgres/catalogSnapshot.ts";
import { lockPostgresKeyValueMutation } from "../../src/lib/db/postgres/keyValueRepository.ts";
import { lockApiKeyPolicy } from "../../src/lib/db/postgres/apiKeyPolicyState.ts";
import { lockServices } from "../../src/lib/db/postgres/serviceQuery.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

type SummaryBody = {
  catalogSource: "baseline" | "radar-overlay";
  catalogUpdatedAt: string;
  steadyRecurringTokens: number;
  usedThisMonth: number | null;
  remaining: number | null;
  modelCount: number;
  perModel: Array<{ provider: string; modelId: string }>;
};

test(
  "native combo and free-tier summary GETs preserve pagination, entitlement and local state",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async (t) => {
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "API_KEY_SECRET",
      "INITIAL_PASSWORD",
      "RADAR_ENABLED",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.INITIAL_PASSWORD = "native-catalog-routes-test-password";
    delete process.env.RADAR_ENABLED;
    const plane = await initializePostgresControlPlane({
      connection: await testPostgresOptions(),
      runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      logger: { warn() {} },
    });
    process.env.API_KEY_SECRET = plane.bootstrapSecrets.apiKeySecret;
    const database = await createTestPostgresDatabase();
    const keyIds: string[] = [];
    const comboIds: string[] = [];
    const prefix = `native-catalog-routes-${randomUUID()}`;
    const prepared = <T>(body: () => T | Promise<T>) =>
      plane.run((binding) => withPostgresCatalogSnapshot(binding.database!, body));
    const { FREE_CATALOG_CURATED_AT, FREE_MODEL_BUDGETS } =
      await import("../../open-sse/config/freeModelCatalog.data.ts");
    const { computeFreeModelTotals } = await import("../../open-sse/config/freeModelCatalog.ts");
    const target = FREE_MODEL_BUDGETS[0];
    const original = await database.transaction(async (tx) => ({
      values: (
        await tx.query<{ namespace: string; key: string; value: string }>(
          "SELECT namespace,key,value FROM omniroute.key_value WHERE (namespace='settings' AND key='requireLogin') OR (namespace='feature_flags' AND key='RADAR_ENABLED')"
        )
      ).rows,
      cache: (
        await tx.query<{
          version: string;
          generated_at: string | null;
          tier: string;
          payload: string;
          signature: string;
          fetched_at: string;
        }>("SELECT * FROM omniroute.radar_feed_cache WHERE id=1")
      ).rows[0],
      local: (
        await tx.query<{
          display_name: string | null;
          enabled: number | null;
          tombstoned: number;
          updated_at: string;
        }>("SELECT * FROM omniroute.radar_local_model_state WHERE provider=$1 AND model_id=$2", [
          target.provider,
          target.modelId,
        ])
      ).rows[0],
    }));
    try {
      const combosRoute = await import("../../src/app/api/combos/route.ts");
      const summaryRoute = await import("../../src/app/api/free-tier/summary/route.ts");
      const radar = await import("../../src/lib/db/radar.ts");
      const { setFeatureFlagOverrideAsync } = await import("../../src/lib/db/featureFlags.ts");
      const { sumUsageTokensThisMonthAsync } = await import("../../src/lib/db/usageSummary.ts");
      const manager = await plane.run(async (binding) => {
        await binding.settings.update({ requireLogin: true });
        const key = await binding.apiKeys.createApiKey(`${prefix}-manager`, prefix, ["manage"]);
        keyIds.push(key.id);
        return key;
      });
      const client = await plane.run(async (binding) => {
        const key = await binding.apiKeys.createApiKey(`${prefix}-client`, prefix);
        keyIds.push(key.id);
        return key;
      });
      const initialCount = await plane.run((binding) => binding.combos!.count());
      for (let index = 0; index < 3; index++) {
        const combo = await plane.run((binding) =>
          binding.combos!.create({
            name: `${prefix}-${index}`,
            models: [{ provider: "openai", model: "gpt-4.1" }],
            context_length: 12345 + index,
          })
        );
        comboIds.push(String(combo.id));
      }
      const request = (path: string, key?: string) =>
        new Request(`http://localhost${path}`, {
          headers: key ? { Authorization: `Bearer ${key}` } : {},
        });
      const getSummary = async (key?: string) => {
        const response = await prepared(() =>
          summaryRoute.GET(request("/api/free-tier/summary", key))
        );
        assert.equal(response.status, 200);
        assert.equal(response.headers.get("Access-Control-Allow-Origin"), "*");
        return response.json() as Promise<SummaryBody>;
      };

      await t.test("combo auth still denies anonymous and inference-only callers", async () => {
        assert.equal((await prepared(() => combosRoute.GET(request("/api/combos")))).status, 401);
        assert.equal(
          (await prepared(() => combosRoute.GET(request("/api/combos", client.key)))).status,
          403
        );
      });
      await t.test(
        "authorized combo pages await native totals and retain computed context",
        async () => {
          const allResponse = await prepared(() =>
            combosRoute.GET(request("/api/combos", manager.key))
          );
          assert.equal(allResponse.status, 200);
          const all = (await allResponse.json()) as {
            total: number;
            combos: Array<{ id: string; computed_context_length?: number }>;
          };
          assert.equal(all.total, initialCount + 3);
          assert.equal(all.combos.length, all.total);
          comboIds.forEach((id, index) =>
            assert.equal(
              all.combos.find((combo) => combo.id === id)?.computed_context_length,
              12345 + index
            )
          );
          const pageResponse = await prepared(() =>
            combosRoute.GET(request("/api/combos?limit=1&offset=1", manager.key))
          );
          assert.equal(pageResponse.status, 200);
          const page = (await pageResponse.json()) as typeof all;
          assert.equal(page.total, all.total);
          assert.deepEqual(
            page.combos.map((combo) => combo.id),
            all.combos.slice(1, 2).map((combo) => combo.id)
          );
          const empty = await prepared(() =>
            combosRoute.GET(request(`/api/combos?limit=1&offset=${all.total + 1}`, manager.key))
          );
          assert.deepEqual(await empty.json(), { combos: [], total: all.total });
          const invalid = await prepared(() =>
            combosRoute.GET(request("/api/combos?offset=-1", manager.key))
          );
          assert.equal(invalid.status, 400);
          assert.ok(!JSON.stringify(await invalid.json()).includes("at /"));
        }
      );

      const generatedAt = `${FREE_CATALOG_CURATED_AT}T12:00:00.000Z`;
      const overlayTokens = 1234567;
      const modelId = `${prefix}-overlay`;
      const seedFeed = (tier: "community" | "live", date = generatedAt) =>
        prepared(() =>
          radar.setRadarCacheAsync({
            version: "2026.08.25.1",
            generatedAt: date,
            tier,
            payload: JSON.stringify({
              feed: "omniroute-radar",
              schemaVersion: 1,
              version: "2026.08.25.1",
              generatedAt: date,
              tier,
              counts: { providers: 1, models: 1 },
              providers: [{ id: prefix, name: "Native route test" }],
              models: [
                {
                  provider: prefix,
                  modelId,
                  displayName: "Native overlay model",
                  familyId: null,
                  freeType: "recurring-monthly",
                  budget: { kind: "per_model", tokensPerMonth: overlayTokens },
                  limits: { rpm: null, rpd: null, tpm: null, tpd: null },
                  contextWindow: null,
                  capabilities: { tools: false, vision: false, thinking: false },
                  trainsOnPrompts: null,
                  tosRisk: "ok",
                  setup: null,
                  enabled: true,
                },
              ],
              quirks: [],
              totals: { dedupedTokensPerMonth: overlayTokens, modelCount: 1, poolCount: 0 },
            }),
            signature: "read-validation-test-signature",
            fetchedAt: generatedAt,
          })
        );
      await prepared(() => setFeatureFlagOverrideAsync("RADAR_ENABLED", "true"));
      await t.test(
        "community overlay stays public while operator usage stays private",
        async () => {
          await seedFeed("community");
          const publicBody = await getSummary();
          assert.equal(publicBody.catalogSource, "radar-overlay");
          assert.equal(publicBody.catalogUpdatedAt, generatedAt);
          assert.ok(publicBody.perModel.some((entry) => entry.modelId === modelId));
          assert.equal(publicBody.usedThisMonth, null);
          assert.equal(publicBody.remaining, null);
          const used = await prepared(() => sumUsageTokensThisMonthAsync());
          const authenticated = await getSummary(manager.key);
          assert.equal(authenticated.catalogSource, "radar-overlay");
          assert.equal(authenticated.usedThisMonth, used);
          assert.equal(
            authenticated.remaining,
            Math.max(0, authenticated.steadyRecurringTokens - used)
          );
        }
      );
      await t.test(
        "live overlay remains authenticated; anonymous callers receive the shipped baseline",
        async () => {
          await seedFeed("live");
          const anonymous = await getSummary();
          assert.equal(anonymous.catalogSource, "baseline");
          assert.equal(
            anonymous.steadyRecurringTokens,
            computeFreeModelTotals().steadyRecurringTokens
          );
          assert.ok(!anonymous.perModel.some((entry) => entry.modelId === modelId));
          assert.equal(anonymous.usedThisMonth, null);
          const authenticated = await getSummary(manager.key);
          assert.equal(authenticated.catalogSource, "radar-overlay");
          assert.ok(authenticated.perModel.some((entry) => entry.modelId === modelId));
        }
      );
      await t.test("stale overlay fallback honors durable local tombstones", async () => {
        const stale = new Date(`${FREE_CATALOG_CURATED_AT}T00:00:00.000Z`);
        stale.setUTCDate(stale.getUTCDate() - 1);
        await seedFeed("community", stale.toISOString());
        await prepared(async () => {
          await radar.setRadarModelTombstoneAsync(target.provider, target.modelId, false);
          await radar.clearRadarLocalModelOverrideAsync(target.provider, target.modelId);
        });
        const before = await getSummary();
        assert.ok(
          before.perModel.some(
            (entry) => entry.provider === target.provider && entry.modelId === target.modelId
          )
        );
        await prepared(() =>
          radar.setRadarModelTombstoneAsync(target.provider, target.modelId, true)
        );
        const after = await getSummary();
        assert.equal(after.catalogSource, "baseline");
        assert.equal(after.catalogUpdatedAt, FREE_CATALOG_CURATED_AT);
        assert.ok(
          !after.perModel.some(
            (entry) => entry.provider === target.provider && entry.modelId === target.modelId
          )
        );
        assert.ok(!after.perModel.some((entry) => entry.modelId === modelId));
        assert.equal(after.modelCount, before.modelCount - 1);
        assert.equal(after.usedThisMonth, null);
      });
    } finally {
      try {
        await plane.run(async (binding) => {
          for (const id of comboIds) await binding.combos!.deleteById(id);
          for (const id of keyIds) await binding.apiKeys.deleteApiKey(id);
        });
        await database.transaction(async (tx) => {
          await tx.query("SELECT pg_advisory_xact_lock(18765,3)");
          await lockApiKeyPolicy(tx);
          await lockPostgresKeyValueMutation(tx);
          await tx.query(
            "DELETE FROM omniroute.key_value WHERE (namespace='settings' AND key='requireLogin') OR (namespace='feature_flags' AND key='RADAR_ENABLED')"
          );
          for (const row of original.values)
            await tx.query(
              "INSERT INTO omniroute.key_value(namespace,key,value) VALUES($1,$2,$3)",
              [row.namespace, row.key, row.value]
            );
        });
        await database.transaction(async (tx) => {
          await lockServices(tx, "radar");
          await tx.query("DELETE FROM omniroute.radar_feed_cache WHERE id=1");
          if (original.cache) {
            const row = original.cache;
            await tx.query(
              "INSERT INTO omniroute.radar_feed_cache(id,version,generated_at,tier,payload,signature,fetched_at) VALUES(1,$1,$2,$3,$4,$5,$6)",
              [row.version, row.generated_at, row.tier, row.payload, row.signature, row.fetched_at]
            );
          }
          await tx.query(
            "DELETE FROM omniroute.radar_local_model_state WHERE provider=$1 AND model_id=$2",
            [target.provider, target.modelId]
          );
          if (original.local) {
            const row = original.local;
            await tx.query(
              "INSERT INTO omniroute.radar_local_model_state(provider,model_id,display_name,enabled,tombstoned,updated_at) VALUES($1,$2,$3,$4,$5,$6)",
              [
                target.provider,
                target.modelId,
                row.display_name,
                row.enabled,
                row.tombstoned,
                row.updated_at,
              ]
            );
          }
        });
      } finally {
        await Promise.allSettled([plane.close(), database.close()]);
        for (const [name, value] of previous) {
          if (value === undefined) delete process.env[name];
          else process.env[name] = value;
        }
      }
    }
  }
);
