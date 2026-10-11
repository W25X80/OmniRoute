import assert from "node:assert/strict";
import test from "node:test";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import {
  CATALOG_TABLES,
  withPostgresCatalogSnapshot,
} from "../../src/lib/db/postgres/catalogSnapshot.ts";
import { readPreparedPostgresTierPricing } from "../../src/lib/db/postgres/pricingSnapshot.ts";
import { readUserPricingMemoized, readUserPricingSync } from "../../src/lib/catalogUserPricing.ts";
import {
  classifyTier,
  getTierStats,
  setTierConfig,
  setTierPricingSnapshot,
} from "../../open-sse/services/tierResolver.ts";

function fixture(input: number) {
  let reads = 0;
  const rows = {
    generation: "1",
    authority: "unit-fixture",
    values: [
      {
        namespace: "pricing",
        key: "openai",
        value: JSON.stringify({ "gpt-4o": { input, output: input * 3 } }),
      },
    ],
    tables: Object.fromEntries(CATALOG_TABLES.map((table) => [table, []])),
  };
  const database = {
    async transaction(body: (transaction: { query(): Promise<unknown> }) => Promise<unknown>) {
      return body({
        async query() {
          reads++;
          return { rows: [structuredClone(rows)] };
        },
      });
    },
  } as unknown as PostgresDatabase;
  const binding = {
    database,
    databaseIdentity: Object.freeze({}),
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
  return { database, binding, reads: () => reads };
}

test("native prepared price and assignment reads are pure, identity-scoped and retain SQLite cache", async () => {
  setTierConfig({});
  setTierPricingSnapshot({ openai: { "gpt-4o": { input: 0, output: 0 } } });
  const sqlite = classifyTier("openai", "gpt-4o");
  const sqliteStats = getTierStats();
  const a = fixture(0);
  const b = fixture(9);
  try {
    await withPostgresControlPlane(a.binding, () =>
      withPostgresCatalogSnapshot(a.database, async () => {
        assert.deepEqual(getTierStats(), { free: 0, cheap: 0, premium: 0 });
        const reads = a.reads();
        const prices = readPreparedPostgresTierPricing();
        const user = readUserPricingMemoized();
        for (let i = 0; i < 20; i++) {
          assert.equal(classifyTier("openai", "gpt-4o").tier, "free");
          assert.equal(readPreparedPostgresTierPricing(), prices);
          assert.equal(readUserPricingSync(), user);
        }
        assert.equal(a.reads(), reads, "pure reads must not issue SQL or open SQLite");
        const parent = classifyTier("openai", "gpt-4o");
        await withPostgresControlPlane(b.binding, async () => {
          assert.throws(() => classifyTier("openai", "gpt-4o"), /snapshot was not prepared/);
          assert.throws(() => readUserPricingMemoized(), /snapshot was not prepared/);
          await withPostgresCatalogSnapshot(b.database, () => {
            assert.deepEqual(getTierStats(), { free: 0, cheap: 0, premium: 0 });
            assert.equal(classifyTier("openai", "gpt-4o").tier, "premium");
            assert.equal(readUserPricingMemoized().openai["gpt-4o"].input, 9);
          });
        });
        assert.equal(classifyTier("openai", "gpt-4o"), parent);
        assert.equal(readUserPricingMemoized(), user);
        assert.deepEqual(getTierStats(), { free: 1, cheap: 0, premium: 0 });
      })
    );
    assert.equal(classifyTier("openai", "gpt-4o"), sqlite);
    assert.deepEqual(getTierStats(), sqliteStats);
  } finally {
    setTierPricingSnapshot(null);
    setTierConfig({});
  }
});
