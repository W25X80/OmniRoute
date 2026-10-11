import assert from "node:assert/strict";
import test from "node:test";
import {
  initializePostgresControlPlane,
  type PostgresControlPlane,
} from "../../src/lib/db/postgres/controlPlane.ts";
import { withPostgresCatalogSnapshot } from "../../src/lib/db/postgres/catalogSnapshot.ts";
import { getSelectedPostgresDatabase } from "../../src/lib/db/controlPlaneContext.ts";
import {
  writePostgresKeyValue,
  withPostgresKeyValueMutation,
} from "../../src/lib/db/postgres/keyValueRepository.ts";
import {
  setFeatureFlagOverrideAsync,
  removeFeatureFlagOverrideAsync,
} from "../../src/lib/db/featureFlags.ts";
import {
  resolveFeatureFlag,
  resolveFeatureFlagAsync,
} from "../../src/shared/utils/featureFlags.ts";
import {
  getPricingWithSources,
  updatePricing,
  resetPricing,
} from "../../src/lib/db/settings/pricing.ts";
import {
  setLKGP,
  getLKGP,
  deleteLKGPByComboName,
  deleteLKGPByConnectionIds,
} from "../../src/lib/db/settings/lkgp.ts";
import { getCacheMetrics, getCacheTrend } from "../../src/lib/db/settings/cacheMetrics.ts";
import {
  updateChatAdmissionSettings,
  getEffectiveChatAdmissionSettingsAsync,
  getChatAdmissionSettingsSourceAsync,
} from "../../src/lib/db/chatAdmissionSettings.ts";
import { getApiKeyDisplayNamesAsync } from "../../src/lib/db/apiKeys/displayNames.ts";
import { getRootLayoutSettings } from "../../src/lib/db/rootLayoutSettings.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "PostgreSQL settings auxiliary owners use native storage and prepared request authority",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const saved = {
      encryption: process.env.STORAGE_ENCRYPTION_KEY,
      flag: process.env.REQUIRE_API_KEY,
      admission: process.env.OMNIROUTE_CHAT_ADMISSION_HEALTHY_HEADROOM,
      password: process.env.INITIAL_PASSWORD,
    };
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-postgres-settings-extras-key";
    delete process.env.INITIAL_PASSWORD;
    let plane: PostgresControlPlane | undefined;
    try {
      const setup = await createTestPostgresDatabase();
      try {
        await setup.transaction((transaction) =>
          transaction.query("DROP SCHEMA IF EXISTS omniroute CASCADE")
        );
      } finally {
        await setup.close();
      }
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      await plane.run(async () => {
        const database = getSelectedPostgresDatabase()!;
        await database.transaction(async (transaction) => {
          await transaction.query(
            "DELETE FROM omniroute.key_value WHERE namespace = ANY($1::text[])",
            [["feature_flags", "pricing", "pricing_synced", "models_dev_pricing", "lkgp"]]
          );
          await transaction.query("DELETE FROM omniroute.usage_history");
        });
        await t.test(
          "DB override wins, prepared environment stays stable and removal reveals env",
          async () => {
            process.env.REQUIRE_API_KEY = "true";
            await setFeatureFlagOverrideAsync("REQUIRE_API_KEY", "false");
            assert.equal(await resolveFeatureFlagAsync("REQUIRE_API_KEY"), "false");
            await withPostgresCatalogSnapshot(database, async () => {
              assert.equal(resolveFeatureFlag("REQUIRE_API_KEY"), "false");
              await removeFeatureFlagOverrideAsync("REQUIRE_API_KEY");
              process.env.REQUIRE_API_KEY = "false";
              assert.equal(resolveFeatureFlag("REQUIRE_API_KEY"), "true");
              assert.equal(await resolveFeatureFlagAsync("REQUIRE_API_KEY"), "true");
            });
            assert.equal(await resolveFeatureFlagAsync("REQUIRE_API_KEY"), "false");
          }
        );
        await t.test(
          "pricing namespaces preserve precedence, corruption tolerance and strict write decoding",
          async () => {
            await withPostgresKeyValueMutation(database, async (transaction) => {
              await writePostgresKeyValue(
                transaction,
                "pricing_synced",
                "fixture",
                JSON.stringify({ model: { input: 1, output: 2 } })
              );
              await writePostgresKeyValue(
                transaction,
                "models_dev_pricing",
                "fixture",
                JSON.stringify({ model: { input: 3, output: 4 } })
              );
              await writePostgresKeyValue(transaction, "pricing", "corrupt", "not-json");
            });
            assert.deepEqual((await getPricingWithSources()).pricing.fixture.model, {
              input: 3,
              output: 4,
            });
            assert.equal((await getPricingWithSources()).pricing.corrupt, undefined);
            await assert.rejects(updatePricing({ fixture: { model: { input: 5, output: 6 } } }));
            await database.transaction((transaction) =>
              transaction.query(
                "DELETE FROM omniroute.key_value WHERE namespace = 'pricing' AND key = 'corrupt'"
              )
            );
            await updatePricing({ fixture: { model: { input: 5, output: 6 } } });
            await withPostgresCatalogSnapshot(database, async () => {
              assert.deepEqual((await getPricingWithSources()).pricing.fixture.model, {
                input: 5,
                output: 6,
              });
              await resetPricing("fixture", "model");
              assert.equal((await getPricingWithSources()).sourceMap.fixture.model, "modelsDev");
            });
          }
        );
        await t.test(
          "LKGP literal wildcard combo deletion and connection cleanup preserve legacy pins",
          async () => {
            await setLKGP("combo_%", "m", "p", "gone");
            await setLKGP("combo-other", "m", "p", "kept");
            assert.equal(await deleteLKGPByComboName("combo_%"), 1);
            assert.equal(await getLKGP("combo_%", "m"), null);
            assert.equal((await getLKGP("combo-other", "m"))?.connectionId, "kept");
            await withPostgresKeyValueMutation(database, (transaction) =>
              writePostgresKeyValue(transaction, "lkgp", "legacy:m", "legacy-provider")
            );
            assert.equal(await deleteLKGPByConnectionIds(["kept"]), 1);
            assert.deepEqual(await getLKGP("legacy", "m"), { provider: "legacy-provider" });
          }
        );
        await t.test(
          "admission env precedence, layout parsing and sparse key names avoid SQLite",
          async () => {
            process.env.OMNIROUTE_CHAT_ADMISSION_HEALTHY_HEADROOM = "0";
            await updateChatAdmissionSettings({
              chatMaxHeavyInFlight: 3,
              chatAdmissionHeapShedRatio: 0.5,
              chatAdmissionHealthyHeadroom: 4,
            });
            assert.equal(
              (await getEffectiveChatAdmissionSettingsAsync()).chatAdmissionHealthyHeadroom,
              0
            );
            assert.equal(
              (await getChatAdmissionSettingsSourceAsync()).chatAdmissionHealthyHeadroom,
              "env"
            );
            await withPostgresKeyValueMutation(database, (transaction) =>
              writePostgresKeyValue(
                transaction,
                "settings",
                "instanceName",
                JSON.stringify("Native fixture")
              )
            );
            assert.equal((await getRootLayoutSettings()).instanceName, "Native fixture");
            await database.transaction(async (transaction) => {
              await transaction.query(
                `INSERT INTO omniroute.api_keys(id,name,key,created_at) VALUES
                ('settings-name-visible','Visible label','fixture-key-visible',$1),
                ('settings-name-blank','  ','fixture-key-blank',$1),
                ('settings-name-deleted','Deleted label','fixture-key-deleted',$1)`,
                [new Date().toISOString()]
              );
              await transaction.query(
                "DELETE FROM omniroute.api_keys WHERE id = 'settings-name-deleted'"
              );
            });
            assert.deepEqual(
              await getApiKeyDisplayNamesAsync([
                "settings-name-visible",
                "settings-name-blank",
                "settings-name-deleted",
                "unknown-deleted-id",
                "",
                "settings-name-visible",
              ]),
              new Map([["settings-name-visible", "Visible label"]])
            );
            await database.transaction((transaction) =>
              transaction.query("DELETE FROM omniroute.api_keys WHERE id = ANY($1::text[])", [
                ["settings-name-visible", "settings-name-blank"],
              ])
            );
          }
        );
        await t.test(
          "native cache aggregate SQL and UTC trend decode counts and token sums",
          async () => {
            await database.transaction(async (transaction) => {
              await transaction.query(
                `INSERT INTO omniroute.usage_history(provider, model, timestamp, tokens_input, tokens_cache_read, tokens_cache_creation, combo_strategy)
            VALUES ('fixture','m',$1,100,1000000,0,NULL), ('fixture','m',$1,10,0,50,'priority'), ('no-cache','m',$1,5,0,0,'direct')`,
                [new Date().toISOString()]
              );
            });
            const metrics = await getCacheMetrics();
            assert.equal(metrics.totalRequests, 3);
            assert.equal(metrics.requestsWithCacheControl, 2);
            assert.equal(metrics.totalInputTokens, 110);
            assert.equal(metrics.estimatedCostSaved, 2.7);
            assert.equal(metrics.byProvider.fixture.cachedRequests, 2);
            assert.equal(metrics.byProvider["no-cache"], undefined);
            const trend = await getCacheTrend(1);
            assert.equal(
              trend.reduce((sum, point) => sum + point.requests, 0),
              3
            );
            assert.equal(
              trend.reduce((sum, point) => sum + point.cachedTokens, 0),
              1000000
            );
          }
        );
      });
    } finally {
      await plane?.close();
      for (const [key, value] of Object.entries({
        STORAGE_ENCRYPTION_KEY: saved.encryption,
        REQUIRE_API_KEY: saved.flag,
        OMNIROUTE_CHAT_ADMISSION_HEALTHY_HEADROOM: saved.admission,
        INITIAL_PASSWORD: saved.password,
      })) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  }
);
