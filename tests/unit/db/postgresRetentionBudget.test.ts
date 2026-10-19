import assert from "node:assert/strict";
import test from "node:test";
import { createPostgresCleanupRepository } from "../../../src/lib/db/postgres/cleanupRepository.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";

const target = { table: "conversation_turn_nodes", column: "last_seen_at", cutoff: "iso" } as const;

function fixture(onDelete: () => void = () => {}) {
  let batches = 0;
  let commits = 0;
  let projectedNodeCount: number | null = 100_000;
  const database = {
    async transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
      const result = await body({
        async query<Row>(sql: string, values: unknown[] = []) {
          let rows: unknown[] = [];
          let rowCount = 0;
          let command = "SELECT";
          if (sql.startsWith("SELECT to_regclass")) {
            rows = [{ present: true }];
            rowCount = 1;
          } else if (sql.includes("pg_advisory_xact_lock_shared")) {
            assert.deepEqual(values, [21]);
          } else if (sql.includes("SELECT a.id FROM omniroute.agentic_conversations")) {
            assert.match(sql, /FOR UPDATE OF a SKIP LOCKED/);
            rows = [{ id: "root" }];
            rowCount = 1;
          } else if (sql.includes("RETURNING conversation_id")) {
            assert.match(sql, /FOR UPDATE SKIP LOCKED/);
            assert.deepEqual(values, [["root"], "2026-10-01T00:00:00Z"]);
            batches++;
            onDelete();
            rows = Array.from({ length: 1000 }, () => ({ conversation_id: "root" }));
            rowCount = 1000;
            command = "DELETE";
          } else if (sql.startsWith("WITH locked AS MATERIALIZED")) {
            assert.match(sql, /ORDER BY conversation_id COLLATE "C" FOR UPDATE/);
            assert.match(sql, /c\.node_count>=d\.removed/);
            assert.deepEqual(values, [["root"], [1000]]);
            if (projectedNodeCount !== null && projectedNodeCount >= 1000) {
              projectedNodeCount -= 1000;
              rowCount = 1;
              command = "UPDATE";
            }
          } else if (sql.startsWith("DELETE FROM omniroute.conversation_node_counts")) {
            assert.match(sql, /node_count=0/);
            assert.deepEqual(values, [["root"]]);
            if (projectedNodeCount === 0) {
              projectedNodeCount = null;
              rowCount = 1;
              command = "DELETE";
            }
          } else if (sql.startsWith("UPDATE omniroute.agentic_conversations")) {
            assert.match(sql, /history_revision=history_revision\+1/);
            assert.deepEqual(values, [["root"]]);
            rowCount = 1;
            command = "UPDATE";
          } else {
            assert.match(sql, /SELECT DISTINCT conversation_id/);
          }
          return { rows: rows as Row[], rowCount, command };
        },
      });
      commits++;
      return result;
    },
  } as PostgresDatabase;
  return { database, batches: () => batches, commits: () => commits };
}

test("retention stops admitting batches at the pass deadline and resumes on the next call", async (t) => {
  let now = 0;
  t.mock.method(performance, "now", () => now);
  const f = fixture(() => {
    now += 1000;
  });
  const { database } = f;
  const cleanup = createPostgresCleanupRepository(database);
  assert.equal(await cleanup.deleteBefore(target, "2026-10-01T00:00:00Z"), 1000);
  assert.equal(f.batches(), 1);
  assert.equal(f.commits(), 1);
  assert.equal(await cleanup.deleteBefore(target, "2026-10-01T00:00:00Z"), 1000);
  assert.equal(f.batches(), 2);
  assert.equal(f.commits(), 2);
});

test("a fast backlog is bounded by the batch cap even before the deadline", async (t) => {
  t.mock.method(performance, "now", () => 0);
  const f = fixture();
  assert.equal(
    await createPostgresCleanupRepository(f.database).deleteBefore(target, "2026-10-01T00:00:00Z"),
    100_000
  );
  assert.equal(f.batches(), 100);
  assert.equal(f.commits(), 100);
});
