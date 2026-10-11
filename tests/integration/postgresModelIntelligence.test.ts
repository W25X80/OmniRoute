import { UnsupportedPersistenceDomainError } from "../../src/lib/db/controlPlaneContext.ts";
import assert from "node:assert/strict";
import test from "node:test";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  withPostgresCatalogSnapshot,
  catalogGeneration,
} from "../../src/lib/db/postgres/catalogSnapshot.ts";
import * as intelligence from "../../src/lib/db/modelIntelligence.ts";
import { getTaskFitnessWithSource } from "../../open-sse/services/autoCombo/taskFitness.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

let database: PostgresDatabase;
let other: PostgresDatabase;
let binding: PostgresControlPlaneBinding;
const select = (database: PostgresDatabase): PostgresControlPlaneBinding =>
  ({
    database,
    databaseIdentity: Object.freeze({}),
    assertReady() {},
  }) as unknown as PostgresControlPlaneBinding;
const selected = <T>(body: () => T | Promise<T>, current = binding): Promise<T> =>
  withPostgresControlPlane(current, () => withPostgresCatalogSnapshot(current.database!, body));
const entry = (
  model: string,
  source: string,
  category: string,
  score: number,
  expiresAt: string | null = null
): Omit<intelligence.ModelIntelligenceEntry, "syncedAt"> => ({
  model,
  source,
  category,
  score,
  eloRaw: 11.25,
  confidence: "measured",
  expiresAt,
});

if (!postgresTestsEnabled)
  test("native model intelligence requires the isolated TLS runner", { skip: true }, () => {});
else {
  test.before(async () => {
    database = await createTestPostgresDatabase({ maxConnections: 1 });
    other = await createTestPostgresDatabase();
    await migratePostgresSchema(database);
    binding = select(database);
  });
  test.beforeEach(async () => {
    await database.transaction(async (tx) => {
      await tx.query("SELECT pg_advisory_xact_lock(18765,18)");
      await tx.query("SELECT pg_advisory_xact_lock(18765,20)");
      await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
      await tx.query("DELETE FROM omniroute.model_intelligence");
      await tx.query(
        "UPDATE omniroute.api_key_policy_epoch SET generation=generation+1 WHERE singleton"
      );
    });
  });
  test.after(async () => {
    await other?.close();
    await database?.close();
  });

  test("native precedence, corrupt expiry and source numeric affinity preserve source reads", async () => {
    await selected(async () => {
      await intelligence.bulkUpsertModelIntelligenceAsync([
        entry("model", "models_dev_tier", "coding", 0.4),
        entry("model", "arena_elo", "coding", 0.7),
        entry("model", "user_override", "coding", 0.9),
        entry("model", "unknown_source", "coding", 1),
        entry("old", "arena_elo", "coding", 0.8, "2020-01-01"),
        entry("invalid", "arena_elo", "coding", 0.8, "not-a-date"),
        entry("equal", "arena_elo", "coding", 0.8, "now"),
      ]);
      assert.equal(intelligence.getResolvedTaskFitness("model", "coding"), 0.9);
      assert.equal(
        intelligence.getModelIntelligenceBySource("model", "arena_elo", "coding")?.eloRaw,
        11.25
      );
      assert.equal(intelligence.getModelIntelligence("old", "coding"), null);
      assert.equal(intelligence.getModelIntelligence("invalid", "coding"), null);
      assert.equal(intelligence.getModelIntelligence("equal", "coding"), null);
      assert.equal(await intelligence.deleteExpiredIntelligenceAsync(), 1);
      assert.equal(
        intelligence.listModelIntelligence().some((row) => row.model === "invalid"),
        true
      );
      assert.equal(
        intelligence.listModelIntelligence().some((row) => row.model === "equal"),
        true
      );
      assert.throws(
        () => intelligence.upsertModelIntelligence(entry("guard", "arena_elo", "coding", 0.5)),
        UnsupportedPersistenceDomainError
      );
    });
  });

  test("bulk duplicates and composite refresh prune remain atomic", async () => {
    await selected(async () => {
      await intelligence.bulkUpsertModelIntelligenceAsync([
        entry("model", "arena_elo", "coding", 0.6),
        entry("model", "arena_elo", "default", 0.3),
        entry("model", "user_override", "default", 0.9),
      ]);
      const epoch = catalogGeneration();
      assert.deepEqual(
        await intelligence.applyArenaEloRefreshAsync([
          entry("model", "arena_elo", "coding", 0.7),
          entry("model", "arena_elo", "coding", 0.8),
        ]),
        { upserted: 2, pruned: 1 }
      );
      assert.equal(BigInt(catalogGeneration()) - BigInt(epoch), 1n);
      assert.equal(intelligence.getModelIntelligence("model", "coding")?.score, 0.8);
      assert.equal(
        intelligence.getModelIntelligenceBySource("model", "user_override", "default")?.score,
        0.9
      );
      const stable = intelligence.listModelIntelligence();
      await assert.rejects(
        intelligence.applyArenaEloRefreshAsync([entry("bad", "arena_elo", "coding", NaN)])
      );
      assert.deepEqual(intelligence.listModelIntelligence(), stable);
    });
  });

  test("task fitness bypasses process cache while each request keeps a coherent authority", async () => {
    const second = select(other);
    await selected(() => intelligence.setUserFitnessOverrideEntryAsync("gpt-4o", "coding", 0.91));
    await selected(async () => {
      assert.deepEqual(getTaskFitnessWithSource("gpt-4o", "coding"), {
        score: 0.91,
        source: "user_override",
      });
      await selected(
        () => intelligence.setUserFitnessOverrideEntryAsync("gpt-4o", "coding", 0.42),
        second
      );
      assert.equal(getTaskFitnessWithSource("gpt-4o", "coding").score, 0.91);
    });
    assert.equal(await selected(() => getTaskFitnessWithSource("gpt-4o", "coding").score), 0.42);
  });
}
