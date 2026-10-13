import assert from "node:assert/strict";
import test from "node:test";
import { createPostgresCleanupRepository } from "../../../src/lib/db/postgres/cleanupRepository.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";

const target = { table: "conversation_turn_nodes", column: "last_seen_at", cutoff: "iso" } as const;

test("retention stops admitting batches at the pass deadline and resumes on the next call", async (t) => {
  let now = 0;
  let batches = 0;
  let commits = 0;
  t.mock.method(performance, "now", () => now);
  const database = {
    async transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
      const result = await body({
        async query<Row>(sql: string) {
          if (sql.startsWith("SELECT to_regclass"))
            return { rows: [{ present: true }] as Row[], rowCount: 1, command: "SELECT" };
          batches++;
          now += 1000;
          return { rows: [] as Row[], rowCount: 1000, command: "DELETE" };
        },
      });
      commits++;
      return result;
    },
  } as PostgresDatabase;
  const cleanup = createPostgresCleanupRepository(database);
  assert.equal(await cleanup.deleteBefore(target, "2026-10-01T00:00:00Z"), 1000);
  assert.equal(batches, 1);
  assert.equal(commits, 1);
  assert.equal(await cleanup.deleteBefore(target, "2026-10-01T00:00:00Z"), 1000);
  assert.equal(batches, 2);
  assert.equal(commits, 2);
});

test("a fast backlog is bounded by the batch cap even before the deadline", async (t) => {
  t.mock.method(performance, "now", () => 0);
  let commits = 0;
  const database = {
    async transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
      const result = await body({
        async query<Row>(sql: string) {
          return sql.startsWith("SELECT to_regclass")
            ? { rows: [{ present: true }] as Row[], rowCount: 1, command: "SELECT" }
            : { rows: [] as Row[], rowCount: 1000, command: "DELETE" };
        },
      });
      commits++;
      return result;
    },
  } as PostgresDatabase;
  assert.equal(
    await createPostgresCleanupRepository(database).deleteBefore(target, "2026-10-01T00:00:00Z"),
    100_000
  );
  assert.equal(commits, 100);
});
