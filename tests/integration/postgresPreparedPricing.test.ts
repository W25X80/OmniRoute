import assert from "node:assert/strict";
import test from "node:test";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import {
  withPostgresCatalogSnapshot,
  mutatePostgresCatalog,
  putCatalogValue,
  deleteCatalogMatchingKeys,
  catalogSnapshotReadIdentity,
} from "../../src/lib/db/postgres/catalogSnapshot.ts";
import { readPreparedPostgresTierPricing } from "../../src/lib/db/postgres/pricingSnapshot.ts";
import { updatePricing, resetPricing } from "../../src/lib/db/settings/pricing.ts";
import { readUserPricingMemoized, readUserPricingSync } from "../../src/lib/catalogUserPricing.ts";
import { enrichCatalogModelEntry } from "../../src/lib/modelMetadataRegistry.ts";
import {
  classifyTier,
  classifyTierAsync,
  clearTierCache,
  getTierStats,
  setTierConfig,
} from "../../open-sse/services/tierResolver.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

function binding(database: PostgresDatabase): PostgresControlPlaneBinding {
  return {
    database,
    databaseIdentity: Object.freeze({}),
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
}

test(
  "prepared native prices remain scoped, fresh and source-shaped",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const database = await createTestPostgresDatabase();
    const otherDatabase = await createTestPostgresDatabase();
    const current = binding(database);
    const foreign = binding(otherDatabase);
    const prefix = `prepared-pricing-${crypto.randomUUID()}`;
    const freeModel = `${prefix}-free`;
    const premiumModel = `${prefix}-premium`;
    const partialModel = `${prefix}-partial`;
    const selected = <T>(body: () => T | Promise<T>, owner = current) =>
      withPostgresControlPlane(owner, () => withPostgresCatalogSnapshot(owner.database!, body));
    try {
      await migratePostgresSchema(database);
      setTierConfig({});
      await selected(() =>
        updatePricing({
          openai: {
            [freeModel]: { input: 0, output: 0 },
            [premiumModel]: { input: 7, output: 21 },
          },
        })
      );

      await t.test(
        "zero price is FREE, premium price is retained, namespace decode is reused",
        () =>
          selected(async () => {
            assert.deepEqual(getTierStats(), { free: 0, cheap: 0, premium: 0 });
            const first = readPreparedPostgresTierPricing();
            assert.equal(readPreparedPostgresTierPricing(), first);
            assert.equal(classifyTier("openai", freeModel).tier, "free");
            assert.equal(classifyTier("OPENAI", freeModel.toUpperCase()).costPer1MInput, 0);
            assert.equal(classifyTier("openai", premiumModel).tier, "premium");
            assert.equal(classifyTier("openai", premiumModel).costPer1MOutput, 21);
            assert.equal((await classifyTierAsync("openai", freeModel)).tier, "free");
            assert.deepEqual(getTierStats(), { free: 2, cheap: 0, premium: 1 });
            const user = readUserPricingMemoized();
            assert.equal(readUserPricingSync(), user);
            assert.equal(readUserPricingMemoized(), user);
          })
      );

      await t.test(
        "acknowledged price edits replace the token within the same prepared scope",
        () =>
          selected(async () => {
            const identity = catalogSnapshotReadIdentity();
            const old = readUserPricingMemoized();
            assert.equal(classifyTier("openai", freeModel).tier, "free");
            await updatePricing({ openai: { [freeModel]: { input: 9, output: 27 } } });
            assert.notEqual(catalogSnapshotReadIdentity(), identity);
            assert.notEqual(readUserPricingMemoized(), old);
            assert.equal(classifyTier("openai", freeModel).tier, "premium");
            assert.equal(classifyTier("openai", freeModel).costPer1MInput, 9);
            setTierConfig({ providerOverrides: [{ provider: "openai", tier: "cheap" }] });
            assert.deepEqual(getTierStats(), { free: 0, cheap: 0, premium: 0 });
            assert.equal(classifyTier("openai", freeModel).tier, "cheap");
            setTierConfig({});
            assert.equal(classifyTier("openai", freeModel).tier, "premium");
            clearTierCache();
            assert.deepEqual(getTierStats(), { free: 0, cheap: 0, premium: 0 });
          })
      );

      await t.test(
        "foreign authority cannot consume an inherited snapshot or poison the parent",
        () =>
          selected(async () => {
            const price = classifyTier("openai", freeModel);
            assert.equal(price.costPer1MInput, 9);
            await withPostgresControlPlane(foreign, async () => {
              assert.throws(() => classifyTier("openai", freeModel), /snapshot was not prepared/);
              assert.throws(() => readUserPricingMemoized(), /snapshot was not prepared/);
              await withPostgresCatalogSnapshot(otherDatabase, async () => {
                assert.deepEqual(getTierStats(), { free: 0, cheap: 0, premium: 0 });
                await updatePricing({ openai: { [freeModel]: { input: 0, output: 0 } } });
                assert.equal(classifyTier("openai", freeModel).tier, "free");
              });
            });
            assert.equal(classifyTier("openai", freeModel), price);
            assert.equal(readUserPricingMemoized().openai[freeModel].input, 9);
          })
      );
      await selected(() => assert.equal(classifyTier("openai", freeModel).tier, "free"));

      await t.test(
        "catalog override stays per-field and existing entry prices stay untouched",
        () =>
          selected(async () => {
            await updatePricing({
              openai: { [partialModel]: { input: 9, output: "ignored", cached: 0.01 } },
            });
            const entry: {
              id: string;
              owned_by: string;
              root: string;
              pricing?: Record<string, number>;
            } = { id: `openai/${partialModel}`, owned_by: "openai", root: partialModel };
            const snapshot = {
              modelsDevPricing: {
                openai: {
                  [partialModel]: { input: 1, output: 2, cached: 0.5, cache_creation: 0.2 },
                },
              },
            };
            const implicit = enrichCatalogModelEntry(entry, undefined, snapshot);
            assert.deepEqual(implicit.pricing, {
              input: 9,
              output: 2,
              cached: 0.01,
              cache_creation: 0.2,
            });
            const explicit = enrichCatalogModelEntry(entry, undefined, {
              ...snapshot,
              userPricing: readUserPricingMemoized(),
            });
            assert.deepEqual(explicit.pricing, implicit.pricing);
            const preset = enrichCatalogModelEntry(
              { ...entry, pricing: { input: 123, output: 456 } },
              undefined,
              snapshot
            );
            assert.deepEqual(preset.pricing, { input: 123, output: 456 });
            await updatePricing({ openai: { [partialModel]: { input: 11 } } });
            assert.equal(
              (enrichCatalogModelEntry(entry, undefined, snapshot).pricing as { input: number })
                .input,
              11
            );
          })
      );

      await t.test(
        "empty native user layer is authoritative and never falls through to SQLite memo",
        () =>
          selected(async () => {
            const saved = await database.transaction((transaction) =>
              transaction.query<{ key: string; value: string }>(
                "SELECT key,value FROM omniroute.key_value WHERE namespace='pricing'"
              )
            );
            try {
              await mutatePostgresCatalog(database, () =>
                deleteCatalogMatchingKeys("pricing", () => true)
              );
              assert.deepEqual(readUserPricingMemoized(), {});
              assert.equal(readUserPricingSync(), readUserPricingMemoized());
              assert.notEqual(classifyTier("openai", freeModel).tier, "free");
            } finally {
              await mutatePostgresCatalog(database, async () => {
                for (const row of saved.rows) await putCatalogValue("pricing", row.key, row.value);
              });
            }
          })
      );
    } finally {
      try {
        await selected(async () => {
          for (const model of [freeModel, premiumModel, partialModel])
            await resetPricing("openai", model);
        });
      } finally {
        setTierConfig({});
        await Promise.all([database.close(), otherDatabase.close()]);
      }
    }
  }
);
