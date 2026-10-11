import assert from "node:assert/strict";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { createPostgresMemoryRepository } from "../../src/lib/db/postgres/memoryRepository.ts";
import { PostgresMemoryVectorStore } from "../../src/lib/db/postgres/memoryVectorRepository.ts";
import {
  createPostgresReasoningCacheRepository,
  createPostgresSemanticCacheRepository,
} from "../../src/lib/db/postgres/cacheRepository.ts";
import { createPostgresCompressionRepository } from "../../src/lib/db/postgres/compressionRepository.ts";
import { createPostgresCompressionTelemetryRepository } from "../../src/lib/db/postgres/compressionTelemetryRepository.ts";
import {
  getCompressionAnalyticsSummaryPostgres,
  insertCompressionAnalyticsRowPostgres,
  attachCompressionUsageReceiptPostgres,
  getPerEngineAnalyticsPostgres,
} from "../../src/lib/db/postgres/compressionAnalyticsRepository.ts";
import { MemoryType } from "../../src/lib/memory/types.ts";
import type { EmbeddingResolution } from "../../src/lib/memory/embedding/types.ts";

const resolution: EmbeddingResolution = {
  source: "static",
  model: "fixture",
  dimensions: 2,
  signature: "static:fixture:2",
  reason: "test fixture",
};

function input(apiKeyId: string, key: string, content: string) {
  return {
    apiKeyId,
    key,
    content,
    sessionId: "session-a",
    type: MemoryType.FACTUAL,
    metadata: { category: "work" },
    expiresAt: null,
  };
}

test(
  "PostgreSQL memory preserves identity, owner filters, lexical updates and exact-vector races",
  { skip: !postgresTestsEnabled },
  async () => {
    const database = await createTestPostgresDatabase();
    try {
      await migratePostgresSchema(database);
      await database.transaction((tx) => tx.query("TRUNCATE omniroute.memories CASCADE"));
      const memory = createPostgresMemoryRepository(database);
      const candidates = await Promise.all([
        memory.create(input("owner-a", "same-key", "zebra legacy")),
        memory.create(input("owner-a", "same-key", "zebra current")),
      ]);
      assert.equal(candidates[0].id, candidates[1].id, "concurrent upserts retain one identity");
      assert.equal((await memory.list({ apiKeyId: "owner-a" })).total, 1);
      const first = await memory.findById(candidates[0].id);
      assert.ok(first);
      assert.deepEqual(first.metadata, { category: "work" });
      await memory.create(input("owner-b", "private", "zebra secret"));
      const lexical = await memory.candidates({
        apiKeyId: "owner-a",
        query: "zebra",
        lexical: true,
      });
      assert.equal(lexical.length, 1);
      assert.equal(lexical[0].apiKeyId, "owner-a");
      const vector = new PostgresMemoryVectorStore(database);
      assert.equal((await vector.ensureReady(resolution)).ready, true);
      await vector.upsertVector(first.id, new Float32Array([1, 0]), first.content);
      assert.equal(
        (await vector.searchVector(new Float32Array([1, 0]), 20, "owner-a"))[0].memoryId,
        first.id
      );
      assert.deepEqual(await vector.searchVector(new Float32Array([1, 0]), 20, "owner-b"), []);
      await assert.rejects(
        vector.upsertVector(first.id, new Float32Array([NaN, 0]), first.content)
      );
      await memory.update(first.id, { content: "okapi replacement" });
      await assert.rejects(
        vector.upsertVector(first.id, new Float32Array([1, 0]), first.content),
        /content changed/
      );
      assert.equal(
        (await memory.candidates({ apiKeyId: "owner-a", query: "zebra", lexical: true })).length,
        0
      );
      assert.equal(
        (await memory.candidates({ apiKeyId: "owner-a", query: "okapi", lexical: true })).length,
        1
      );
      const second = new PostgresMemoryVectorStore(database);
      await second.ensureReady({ ...resolution, signature: "static:other:2" });
      await assert.rejects(
        vector.upsertVector(first.id, new Float32Array([1, 0]), "okapi replacement"),
        /signature changed/
      );
      await second.upsertVector(first.id, new Float32Array([0, 1]), "okapi replacement");
      const hybrid = await second.searchHybrid(new Float32Array([0, 1]), "okapi", 20, "owner-a");
      assert.equal(hybrid[0].memoryId, first.id);
      assert.equal(hybrid[0].vecRank, 1);
      assert.equal(hybrid[0].ftsRank, 1);
      assert.equal(await memory.deleteById(first.id), true);
      assert.deepEqual(await second.searchVector(new Float32Array([0, 1]), 20, "owner-a"), []);
    } finally {
      await database.close();
    }
  }
);

test(
  "PostgreSQL caches enforce expiration, invalidation, placeholder rejection and durable replay",
  { skip: !postgresTestsEnabled },
  async () => {
    const database = await createTestPostgresDatabase();
    try {
      await migratePostgresSchema(database);
      const semantic = createPostgresSemanticCacheRepository(database);
      const reasoning = createPostgresReasoningCacheRepository(database);
      await semantic.clear();
      await reasoning.clear();
      await semantic.set(
        "owner-a.signature",
        "fixture",
        { choices: [{ message: { content: "cached" } }] },
        25,
        60000
      );
      assert.deepEqual(await semantic.get("owner-b.signature"), null);
      assert.ok(await semantic.get("owner-a.signature"));
      assert.equal((await semantic.stats()).tokensSaved, 25);
      await semantic.set("expired", "fixture", {}, 1, -1);
      assert.equal(await semantic.get("expired"), null);
      await semantic.set("owner-a.signature", "fixture", { changed: true }, 3, 60000);
      assert.deepEqual(await semantic.get("owner-a.signature"), { changed: true });
      assert.equal(await semantic.deleteByModel("fixture"), 2);
      await reasoning.set("call-a", "deepseek", "fixture", "retained reasoning", 60000);
      await reasoning.set(
        "placeholder",
        "deepseek",
        "fixture",
        "(prior reasoning summary unavailable)"
      );
      await reasoning.set("expired", "deepseek", "fixture", "expired reasoning", -1000);
      assert.equal(await reasoning.get("placeholder"), null);
      assert.equal(await reasoning.get("expired"), null);
      assert.equal((await reasoning.getMany(["call-a", "expired", "missing"])).length, 1);
      assert.equal((await reasoning.stats()).totalEntries, 1);
      assert.equal((await reasoning.list())[0].toolCallId, "call-a");
      const other = await createTestPostgresDatabase();
      try {
        assert.equal(
          (await createPostgresReasoningCacheRepository(other).get("call-a"))?.reasoning,
          "retained reasoning"
        );
      } finally {
        await other.close();
      }
      assert.equal(await reasoning.cleanup(), 1);
    } finally {
      await database.close();
    }
  }
);

test(
  "PostgreSQL compression retains default uniqueness, disabled engines and receipt accounting",
  { skip: !postgresTestsEnabled },
  async () => {
    const database = await createTestPostgresDatabase();
    try {
      await migratePostgresSchema(database);
      const compression = createPostgresCompressionRepository(database);
      const telemetry = createPostgresCompressionTelemetryRepository(database);
      const a = await compression.create({ name: "A", isDefault: true });
      const b = await compression.create({ name: "B", isDefault: true });
      assert.equal((await compression.list()).filter((c) => c.isDefault).length, 1);
      assert.equal((await compression.findDefault())?.id, b.id);
      await compression.setEngine("session-dedup", false);
      await compression.setEngine("lite", false);
      assert.deepEqual((await compression.findDefault())?.pipeline, []);
      await compression.assign(a.id, "routing-a");
      await compression.assign(b.id, "routing-a");
      assert.equal((await compression.forRoutingCombo("routing-a"))?.id, b.id);
      await compression.updateAssignments(a.id, ["routing-a", "routing-a", "routing-b"]);
      assert.equal((await compression.assignments(a.id)).length, 2);
      await database.transaction((tx) =>
        tx.query(
          "TRUNCATE omniroute.compression_analytics,omniroute.compression_engine_breakdown,omniroute.compression_run_telemetry"
        )
      );
      const row = {
        timestamp: new Date().toISOString(),
        mode: "stacked",
        original_tokens: 100,
        compressed_tokens: 60,
        tokens_saved: 40,
        request_id: "request-a",
      };
      await insertCompressionAnalyticsRowPostgres(database, row);
      await insertCompressionAnalyticsRowPostgres(database, {
        ...row,
        request_id: "skip-a",
        tokens_saved: 0,
        skip_reason: "no_savings",
      });
      await telemetry.insertBreakdown([{ ...row, engine: "lite" }]);
      await attachCompressionUsageReceiptPostgres(
        database,
        "request-a",
        { prompt_tokens: 60, completion_tokens: 10, total_tokens: 70 },
        "provider"
      );
      const summary = await getCompressionAnalyticsSummaryPostgres(database);
      assert.equal(summary.totalRequests, 1);
      assert.equal(summary.totalSkipped, 1);
      assert.equal(summary.totalTokensSaved, 40);
      assert.equal(summary.realUsage.totalTokens, 70);
      const engine = await getPerEngineAnalyticsPostgres(database, "lite");
      assert.equal(engine.runs, 1);
      assert.equal(engine.tokensSaved, 40);
      await telemetry.insertRun({
        requestId: "run-a",
        model: "fixture",
        provider: "fixture",
        source: "fixture",
        tokensBefore: 20,
        tokensAfter: 10,
        ratio: 0.5,
        outputStyles: [{ id: "concise", level: "lite" }],
        outputTokens: 5,
      });
      assert.equal((await telemetry.runSummary()).appliedStyleCounts.concise, 1);
    } finally {
      await database.close();
    }
  }
);
