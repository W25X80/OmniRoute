import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import type { CompressionAnalyticsRow } from "../../src/lib/db/compressionAnalytics.ts";
import {
  getCompressionAnalyticsSummaryPostgres,
  insertCompressionAnalyticsRowPostgres,
  attachCompressionUsageReceiptPostgres,
} from "../../src/lib/db/postgres/compressionAnalyticsRepository.ts";
import { createPostgresGamificationRepository } from "../../src/lib/db/postgres/gamificationRepository.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

/** Delay delivery of a real query result until a second real transaction commits. */
function afterFirstRead(
  database: PostgresDatabase,
  matches: (sql: string) => boolean,
  write: () => Promise<unknown>
) {
  let committed = false;
  const scheduled = new Proxy(database, {
    get(target, property, receiver) {
      if (property !== "transaction") return Reflect.get(target, property, receiver);
      return async <T>(body: (tx: PostgresTransaction) => Promise<T>) => {
        // Capture the independent request before entering the reader's SQL scope.
        // Its real write commits while delivery of the reader's result is paused.
        const writerRequest = new AsyncResource("compound-read-independent-writer");
        try {
          return await target.transaction((tx) =>
            body({
              query: async <Row = Record<string, unknown>>(sql: string, values?: unknown[]) => {
                const result = await tx.query<Row>(sql, values);
                if (!committed && matches(sql)) {
                  await writerRequest.runInAsyncScope(write);
                  committed = true;
                }
                return result;
              },
            })
          );
        } finally {
          writerRequest.emitDestroy();
        }
      };
    },
  });
  return { database: scheduled, wasCommitted: () => committed };
}

function compressionRow(requestId: string): CompressionAnalyticsRow {
  return {
    timestamp: new Date().toISOString(),
    request_id: requestId,
    mode: "lite",
    engine: "lite",
    provider: "fixture",
    original_tokens: 100,
    compressed_tokens: 70,
    tokens_saved: 30,
    duration_ms: 7,
  };
}

type Summary = Awaited<ReturnType<typeof getCompressionAnalyticsSummaryPostgres>>;
function assertConservation(summary: Summary): void {
  for (const groups of [summary.byMode, summary.byEngine, summary.byProvider]) {
    assert.equal(
      Object.values(groups).reduce((total, group) => total + group.count, 0),
      summary.totalRequests
    );
    assert.equal(
      Object.values(groups).reduce((total, group) => total + group.tokensSaved, 0),
      summary.totalTokensSaved
    );
  }
  assert.equal(
    Object.values(summary.byMode).reduce((total, group) => total + group.skipped, 0),
    summary.totalSkipped
  );
}

test(
  "compound native reads retain one database state while ordinary writers commit",
  { skip: !postgresTestsEnabled, timeout: 60_000 },
  async (t) => {
    const database = await createTestPostgresDatabase({ maxConnections: 4 });
    try {
      await migratePostgresSchema(database);
      const writer = createPostgresGamificationRepository(database);

      await t.test(
        "compression totals and every breakdown retain the pre-insert state",
        async () => {
          await database.transaction((tx) =>
            tx.query("DELETE FROM omniroute.compression_analytics")
          );
          const scheduled = afterFirstRead(
            database,
            (sql) => sql.includes('COUNT(*) AS "total"'),
            () =>
              insertCompressionAnalyticsRowPostgres(database, compressionRow("concurrent-insert"))
          );
          const before = await getCompressionAnalyticsSummaryPostgres(scheduled.database, "24h");
          assert.equal(
            scheduled.wasCommitted(),
            true,
            "the independent writer committed during the read"
          );
          assert.equal(before.totalRequests, 0);
          assert.deepEqual(before.byMode, {});
          assert.deepEqual(before.byEngine, {});
          assert.deepEqual(before.byProvider, {});
          assert.ok(before.last24h.every((hour) => hour.count === 0));
          assertConservation(before);

          const after = await getCompressionAnalyticsSummaryPostgres(database, "24h");
          assert.equal(after.totalRequests, 1);
          assert.equal(after.byMode.lite.count, 1);
          assert.equal(after.totalTokensSaved, 30);
          assertConservation(after);
        }
      );

      await t.test(
        "late receipts appear in the next summary, not partway through this one",
        async () => {
          const scheduled = afterFirstRead(
            database,
            (sql) => sql.includes('COUNT(*) AS "total"'),
            () =>
              attachCompressionUsageReceiptPostgres(database, "concurrent-insert", {
                prompt_tokens: 70,
                completion_tokens: 10,
                total_tokens: 80,
              })
          );
          const before = await getCompressionAnalyticsSummaryPostgres(scheduled.database);
          assert.equal(scheduled.wasCommitted(), true);
          assert.equal(before.totalRequests, 1);
          assert.equal(before.realUsage.requestsWithReceipts, 0);
          const after = await getCompressionAnalyticsSummaryPostgres(database);
          assert.equal(after.realUsage.requestsWithReceipts, 1);
          assert.equal(after.realUsage.totalTokens, 80);
          assertConservation(after);
        }
      );

      await t.test("score changes cannot return the user as their own neighbor", async () => {
        await writer.updateScore("moving-user", "global", 100);
        await writer.updateScore("lower-user", "global", 50);
        const scheduled = afterFirstRead(
          database,
          (sql) => sql.includes("FROM omniroute.leaderboard"),
          () => writer.updateScore("moving-user", "global", 100)
        );
        const before = await createPostgresGamificationRepository(
          scheduled.database
        ).getLeaderboardNeighbors("moving-user", "global", 1);
        assert.equal(scheduled.wasCommitted(), true);
        assert.deepEqual(before, { above: [], below: [{ apiKeyId: "lower-user", score: 50 }] });
        assert.equal((await writer.getTopN("global", 1))[0].score, 200);
        assert.deepEqual(await writer.getLeaderboardNeighbors("moving-user", "global", 1), before);
      });

      await t.test(
        "one score cannot move from above to below within one neighbor result",
        async () => {
          await writer.updateScore("anchor", "crossing", 100);
          await writer.updateScore("moving-peer", "crossing", 150);
          const scheduled = afterFirstRead(
            database,
            // Old implementation: after the upper side; one-statement implementation: after both.
            (sql) => sql.includes("FROM omniroute.leaderboard") && sql.includes("score>"),
            () => writer.updateScore("moving-peer", "crossing", -100)
          );
          const before = await createPostgresGamificationRepository(
            scheduled.database
          ).getLeaderboardNeighbors("anchor", "crossing", 1);
          assert.equal(scheduled.wasCommitted(), true);
          assert.deepEqual(before, { above: [{ apiKeyId: "moving-peer", score: 150 }], below: [] });
          assert.deepEqual(await writer.getLeaderboardNeighbors("anchor", "crossing", 1), {
            above: [],
            below: [{ apiKeyId: "moving-peer", score: 50 }],
          });
        }
      );

      await t.test(
        "summary projections and bounded neighbors preserve SQLite behavior",
        async () => {
          const directory = await mkdtemp(join(tmpdir(), "omniroute-compound-read-sqlite-"));
          const names = ["DATA_DIR", "OMNIROUTE_DATABASE_BACKEND"] as const;
          const previous = new Map(names.map((name) => [name, process.env[name]]));
          process.env.DATA_DIR = directory;
          process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
          let core: typeof import("../../src/lib/db/core.ts") | undefined;
          try {
            core = await import("../../src/lib/db/core.ts");
            const sqliteAnalytics = await import("../../src/lib/db/compressionAnalytics.ts");
            const sqliteGames = await import("../../src/lib/db/gamification.ts");
            await database.transaction((tx) =>
              tx.query("DELETE FROM omniroute.compression_analytics")
            );
            const hour = Math.floor(Date.now() / 3_600_000) * 3_600_000;
            const fixture: CompressionAnalyticsRow[] = [
              {
                ...compressionRow("receipt"),
                timestamp: new Date(hour - 2 * 3_600_000).toISOString(),
                compression_combo_id: "fixture-combo",
                receipt_source: "provider",
                actual_prompt_tokens: 70,
                actual_completion_tokens: 10,
                actual_total_tokens: 80,
                actual_cache_read_tokens: 3,
                actual_cache_write_tokens: 2,
                estimated_usd_saved: 0.01,
                mcp_description_tokens_saved: 5,
                validation_fallback: true,
              },
              ...[0, 3, 14, 40].map((days) => ({
                ...compressionRow(`age-${days}`),
                timestamp: new Date(hour - (days * 24 + 2) * 3_600_000).toISOString(),
                mode: "stacked",
                engine: "stacked",
                provider: null,
                duration_ms: null,
                tokens_saved: 10,
                original_tokens: 50,
                compressed_tokens: 40,
              })),
              ...["lite", "skip-only"].map((mode) => ({
                ...compressionRow(`skip-${mode}`),
                timestamp: new Date(hour - 2 * 3_600_000).toISOString(),
                mode,
                tokens_saved: 0,
                compressed_tokens: 100,
                skip_reason: "no_savings",
                receipt_source: "estimated" as const,
                actual_total_tokens: 999,
              })),
            ];
            for (const row of fixture) {
              sqliteAnalytics.insertCompressionAnalyticsRow(row);
              await insertCompressionAnalyticsRowPostgres(database, row);
            }
            // Empty padding can roll to a new UTC hour between the two calls; compare
            // the actual hourly observations and independently require 24 padded buckets.
            const observed = (summary: Summary) => ({
              ...summary,
              last24h: summary.last24h.filter((point) => point.count > 0),
            });
            for (const since of [undefined, "24h", "7d", "30d"]) {
              const native = await getCompressionAnalyticsSummaryPostgres(database, since);
              const sqlite = sqliteAnalytics.getCompressionAnalyticsSummary(since);
              assert.deepEqual(
                observed(native),
                observed(sqlite),
                `summary window ${since ?? "all"}`
              );
              assert.equal(native.last24h.length, 24);
              assertConservation(native);
            }

            const entries = [
              ["low", 10],
              ["middle", 50],
              ["anchor", 100],
              ["tie", 100],
              ["high", 150],
              ["top", 200],
            ] as const;
            for (const [key, score] of entries) {
              await writer.updateScore(key, "parity", score);
              sqliteGames.updateScore(key, "parity", score);
            }
            await writer.updateScore("other-scope", "excluded", 125);
            sqliteGames.updateScore("other-scope", "excluded", 125);
            for (const key of ["anchor", "missing"]) {
              for (const radius of [0, 1, 2, -1]) {
                assert.deepEqual(
                  await writer.getLeaderboardNeighbors(key, "parity", radius),
                  sqliteGames.getLeaderboardNeighbors(key, "parity", radius)
                );
              }
            }
            // Preserve the native owner's deterministic ordering for equal-score peers.
            await writer.updateScore("high-a", "parity", 150);
            await writer.updateScore("high-z", "parity", 150);
            assert.deepEqual(await writer.getLeaderboardNeighbors("anchor", "parity", 2), {
              above: [
                { apiKeyId: "high-a", score: 150 },
                { apiKeyId: "high-z", score: 150 },
              ],
              below: [
                { apiKeyId: "middle", score: 50 },
                { apiKeyId: "low", score: 10 },
              ],
            });
          } finally {
            core?.resetDbInstance();
            for (const [name, value] of previous) {
              if (value === undefined) delete process.env[name];
              else process.env[name] = value;
            }
            await rm(directory, { recursive: true, force: true });
          }
        }
      );

      const isolation = await database.transaction((tx) =>
        tx.query<{ transaction_isolation: string }>("SHOW transaction_isolation")
      );
      assert.equal(isolation.rows[0].transaction_isolation, "read committed");
    } finally {
      await database.close();
    }
  }
);
