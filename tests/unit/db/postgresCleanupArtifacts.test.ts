import assert from "node:assert/strict";
import test from "node:test";
import { createPostgresCleanupRepository } from "../../../src/lib/db/postgres/cleanupRepository.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";
import type { DeleteByPeriodTarget } from "../../../src/lib/db/cleanup/usagePurge.ts";

const target = {
  table: "call_logs",
  column: "timestamp",
  cutoff: "iso",
  resultKey: "logs",
} as const;

function databaseFixture(deleted: Array<string | null>, unreferenced: string[], failQueue = false) {
  const statements: Array<{ sql: string; values?: unknown[] }> = [];
  let committed = false;
  const tx: PostgresTransaction = {
    async query<Row>(sql: string, values?: unknown[]) {
      statements.push({ sql, values });
      let rows: unknown[] = [];
      let rowCount = 0;
      if (sql.startsWith("SELECT to_regclass")) rows = [{ present: true }];
      else if (sql.includes("DELETE FROM omniroute")) {
        rows = deleted.map((reference) => ({ artifact_relpath: reference }));
        rowCount = deleted.length;
      } else if (sql.startsWith("SELECT DISTINCT relpath")) {
        assert.match(sql, /NOT EXISTS\(SELECT 1 FROM omniroute\.call_logs/);
        rows = unreferenced.map((relpath) => ({ relpath }));
      } else if (sql.startsWith("INSERT INTO omniroute.artifact_deletion_queue") && failQueue) {
        throw new Error("queue rejected");
      }
      return { rows: rows as Row[], rowCount, command: sql.split(" ")[0] };
    },
  };
  const database = {
    async transaction<T>(body: (transaction: PostgresTransaction) => Promise<T>) {
      const result = await body(tx);
      committed = true;
      return result;
    },
  } as unknown as PostgresDatabase;
  return { database, statements, committed: () => committed };
}

test("retention returns/queues classified last references, not every removed row", async () => {
  const fixture = databaseFixture(["shared", "retired", "retired", null], ["retired"]);
  const result = await createPostgresCleanupRepository(fixture.database).deleteCallLogs(
    "2026-01-01"
  );
  assert.deepEqual(result, { deleted: 4, artifacts: ["retired"] });
  assert.equal(fixture.statements[0].sql, "SELECT pg_advisory_xact_lock(18765,8)");
  const classification = fixture.statements.findIndex((statement) =>
    statement.sql.startsWith("SELECT DISTINCT relpath")
  );
  const deletion = fixture.statements.findIndex((statement) =>
    statement.sql.includes("DELETE FROM omniroute.call_logs")
  );
  const queue = fixture.statements.findIndex((statement) =>
    statement.sql.startsWith("INSERT INTO omniroute.artifact_deletion_queue")
  );
  assert.ok(deletion < classification && classification < queue);
  assert.deepEqual(fixture.statements[queue].values, [["retired"]]);
  assert.equal(fixture.committed(), true);
});

test("partial reset retains reset fencing and adds history fencing before call-log deletion", async () => {
  const fixture = databaseFixture(["shared", "retired"], ["retired"]);
  const result = await createPostgresCleanupRepository(fixture.database).reset(
    [target],
    "2026-01-01"
  );
  assert.deepEqual(result, { counts: { logs: 2 }, artifacts: ["retired"] });
  assert.equal(fixture.statements[0].sql, "SELECT pg_advisory_xact_lock(18765, 7)");
  assert.equal(fixture.statements[1].sql, "SELECT pg_advisory_xact_lock(18765,8)");
  const queue = fixture.statements.find((statement) =>
    statement.sql.startsWith("INSERT INTO omniroute.artifact_deletion_queue")
  );
  assert.deepEqual(queue?.values, [["retired"]]);
});

test("metadata-only and skipped allOnly resets do not acquire the new history fence", async () => {
  for (const targets of [
    [{ table: "quota_snapshots", column: "created_at", cutoff: "iso", resultKey: "quota" }],
    [{ ...target, allOnly: true }],
  ] as Array<Array<DeleteByPeriodTarget & { resultKey: string; allOnly?: boolean }>>) {
    const fixture = databaseFixture([], []);
    await createPostgresCleanupRepository(fixture.database).reset(targets, "2026-01-01");
    assert.equal(
      fixture.statements.some(
        (statement) => statement.sql === "SELECT pg_advisory_xact_lock(18765,8)"
      ),
      false
    );
    assert.equal(
      fixture.statements.some((statement) =>
        statement.sql.startsWith("INSERT INTO omniroute.artifact_deletion_queue")
      ),
      false
    );
  }
});

test("a rejected queue write rejects the same transaction's deletion result", async () => {
  for (const reset of [false, true]) {
    const fixture = databaseFixture(["retired"], ["retired"], true);
    const owner = createPostgresCleanupRepository(fixture.database);
    await assert.rejects(reset ? owner.reset([target]) : owner.deleteCallLogs(), /queue rejected/);
    assert.equal(fixture.committed(), false);
  }
});
