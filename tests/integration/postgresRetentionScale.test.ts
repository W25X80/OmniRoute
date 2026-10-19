import {
  insertConversationFixture,
  deleteConversationFixture,
} from "../helpers/persistence/conversationHistoryFixture.ts";
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { createPostgresCleanupRepository } from "../../src/lib/db/postgres/cleanupRepository.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { POSTGRES_RETENTION_SCHEMA_STATEMENTS } from "../../src/lib/db/postgres/retentionSchema.ts";
import {
  capturePostgresApplicationSnapshot,
  importApplicationSnapshot,
  verifyApplicationImport,
} from "../../src/lib/db/postgres/applicationImport.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

const target = { table: "conversation_turn_nodes", column: "last_seen_at", cutoff: "iso" } as const;
const cutoff = "2026-10-01T00:00:00.000Z";
const fresh = "2026-10-13T00:00:00.000Z";
const old = "2026-09-01T00:00:00.000Z";
const insert = `INSERT INTO omniroute.conversation_turn_nodes
  (id,conversation_id,role,first_seen_at,last_seen_at) VALUES($1,'retention','user',$2,$2)`;

type Plan = {
  "Node Type": string;
  "Relation Name"?: string;
  "Index Name"?: string;
  Plans?: Plan[];
};
function flatten(plan: Plan): Plan[] {
  return [plan, ...(plan.Plans ?? []).flatMap(flatten)];
}

describe(
  "indexed native history retention",
  { skip: !postgresTestsEnabled, concurrency: false },
  () => {
    let database: PostgresDatabase;
    before(async () => {
      database = await createTestPostgresDatabase({
        // Offline scale migration budgets; interactive defaults are unchanged.
        statementTimeoutMs: 240_000,
        queryTimeoutMs: 250_000,
        transactionTimeoutMs: 600_000,
      });
      await migratePostgresSchema(database);
    });
    after(async () => database?.close());

    it("preserves offset, UTC text and strict microsecond cutoff semantics on insert and update", async () => {
      const samples = [
        ["before", "2026-09-30T23:59:59.999999Z"],
        ["equal", cutoff],
        ["offset-equal", "2026-10-01T02:00:00+02:00"],
        ["offset-before", "2026-10-01T01:59:59.999999+02:00"],
        ["utc-text", "2026-10-01 00:00:00"],
        ["after", "2026-10-01T00:00:00.000001Z"],
      ];
      await database.transaction(async (tx) => {
        await tx.query(
          "TRUNCATE omniroute.conversation_turn_nodes,omniroute.conversation_node_counts"
        );
        await tx.query("SET LOCAL TIME ZONE 'America/Los_Angeles'");
        for (const [id, timestamp] of samples)
          await insertConversationFixture(tx, insert, [id, timestamp]);
        await tx.query("SET LOCAL TIME ZONE 'UTC'");
        const rows = await tx.query<{ id: string; last_seen_at: string; matches: boolean }>(
          `SELECT id,last_seen_at,last_seen_at_retention=last_seen_at::timestamptz AS matches
         FROM omniroute.conversation_turn_nodes ORDER BY id`
        );
        assert.deepEqual(
          rows.rows.map((row) => [row.id, row.last_seen_at]),
          [...samples].sort()
        );
        assert.ok(rows.rows.every((row) => row.matches));
        await tx.query(
          "UPDATE omniroute.conversation_turn_nodes SET last_seen_at=$1 WHERE id='after'",
          [old]
        );
        await tx.query(
          "UPDATE omniroute.conversation_turn_nodes SET last_seen_at_retention='1900-01-01' WHERE id='equal'"
        );
      });
      assert.equal(await createPostgresCleanupRepository(database).deleteBefore(target, cutoff), 3);
      const left = await database.transaction((tx) =>
        tx.query<{ id: string }>("SELECT id FROM omniroute.conversation_turn_nodes ORDER BY id")
      );
      assert.deepEqual(
        left.rows.map((row) => row.id),
        ["equal", "offset-equal", "utc-text"]
      );
      for (const invalid of [
        "not-a-date",
        "2026-02-30T00:00:00Z",
        "now",
        "today",
        "yesterday",
        "tomorrow",
      ]) {
        await assert.rejects(
          database.transaction((tx) => insertConversationFixture(tx, insert, ["invalid", invalid]))
        );
        await assert.rejects(
          database.transaction((tx) =>
            tx.query(
              "UPDATE omniroute.conversation_turn_nodes SET last_seen_at=$1 WHERE id='equal'",
              [invalid]
            )
          )
        );
      }
      await assert.rejects(
        createPostgresCleanupRepository(database).deleteBefore(target, "not-a-date")
      );
    });

    it("uses the retention index for an empty eligible set without scanning retained history", async () => {
      const scale = process.env.RUN_PG_RETENTION_SCALE === "1" ? 4_800_000 : 120_000;
      const queryOnly = process.env.RUN_PG_RETENTION_QUERY_ONLY === "1";
      if (queryOnly) {
        // Separates runtime query qualification from the offline migration budget.
        // Committed fixture batches avoid imposing one enormous INSERT on either.
        await database.transaction((tx) =>
          tx.query("TRUNCATE omniroute.conversation_turn_nodes,omniroute.conversation_node_counts")
        );
        for (let start = 0; start < scale; start += 100_000) {
          await database.transaction((tx) =>
            insertConversationFixture(
              tx,
              `INSERT INTO omniroute.conversation_turn_nodes(id,conversation_id,role,first_seen_at,last_seen_at)
             SELECT lpad(n::text,64,'0'),'scale','user',$3,$3
             FROM generate_series($1::integer,$2::integer) n`,
              [start + 1, Math.min(start + 100_000, scale), fresh]
            )
          );
          if ((start + 100_000) % 1_000_000 === 0 || start + 100_000 >= scale)
            console.log(
              JSON.stringify({
                scenario: "query-only-seed",
                retentionRows: Math.min(start + 100_000, scale),
              })
            );
        }
        await database.transaction((tx) => tx.query("ANALYZE omniroute.conversation_turn_nodes"));
      } else
        await database.transaction(async (tx) => {
          await tx.query(
            "TRUNCATE omniroute.conversation_turn_nodes,omniroute.conversation_node_counts"
          );
          await tx.query(
            "ALTER TABLE omniroute.conversation_turn_nodes DROP COLUMN last_seen_at_retention CASCADE"
          );
          await tx.query("DROP FUNCTION omniroute.set_turn_node_retention_timestamp() CASCADE");
          await insertConversationFixture(
            tx,
            `INSERT INTO omniroute.conversation_turn_nodes(id,conversation_id,role,first_seen_at,last_seen_at)
        SELECT lpad(n::text,64,'0'),'scale','user',$2,$2 FROM generate_series(1,$1::integer) n`,
            [scale, fresh]
          );
          const start = performance.now();
          for (const sql of POSTGRES_RETENTION_SCHEMA_STATEMENTS) await tx.query(sql);
          console.log(
            JSON.stringify({
              retentionRows: scale,
              scenario: "legacy-backfill",
              elapsedMs: performance.now() - start,
            })
          );
          await tx.query("ANALYZE omniroute.conversation_turn_nodes");
        });
      const plans: Plan[] = [];
      const observed = {
        transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
          return database.transaction((tx) =>
            body({
              async query<Row>(sql: string, values?: unknown[]) {
                // An empty candidate set now ends after the parent/orphan lookups,
                // before DELETE. Observe their real plans as well as any deletion.
                if (sql.includes("omniroute.conversation_turn_nodes")) {
                  const explain = await tx.query<{
                    "QUERY PLAN": Array<{ Plan: Plan; "Execution Time": number }>;
                  }>(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`, values);
                  const report = explain.rows[0]["QUERY PLAN"][0];
                  plans.push(report.Plan);
                  console.log(
                    JSON.stringify({
                      retentionRows: scale,
                      scenario: "zero-eligible",
                      executionMs: report["Execution Time"],
                      plan: report.Plan,
                    })
                  );
                }
                return tx.query<Row>(sql, values);
              },
            })
          );
        },
      } as PostgresDatabase;
      assert.equal(await createPostgresCleanupRepository(observed).deleteBefore(target, cutoff), 0);
      assert.ok(plans.length > 0);
      const nodes = plans.flatMap(flatten);
      assert.ok(nodes.some((node) => node["Index Name"] === "conversation_turn_nodes_retention"));
      assert.ok(
        !nodes.some(
          (node) =>
            node["Relation Name"] === "conversation_turn_nodes" &&
            /Seq Scan/.test(node["Node Type"])
        )
      );

      // The same retained population becomes mostly eligible at a later cutoff.
      // A writer refreshes its oldest node while the bounded pass makes progress.
      let unlock!: () => void;
      let locked!: () => void;
      const barrier = new Promise<void>((resolve) => {
        locked = resolve;
      });
      const released = new Promise<void>((resolve) => {
        unlock = resolve;
      });
      const future = "2026-10-20T00:00:00Z";
      const hold = database.transaction(async (tx) => {
        await tx.query(
          "UPDATE omniroute.conversation_turn_nodes SET last_seen_at='2026-10-21T00:00:00Z' WHERE id=$1",
          ["1".padStart(64, "0")]
        );
        locked();
        await released;
      });
      await barrier;
      const batchCounts: number[] = [];
      const eligiblePlans: Plan[] = [];
      const bounded = {
        transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
          return database.transaction((tx) =>
            body({
              async query<Row>(sql: string, values?: unknown[]) {
                if (sql.includes("WITH expired") && eligiblePlans.length === 0) {
                  const explain = await tx.query<{ "QUERY PLAN": Array<{ Plan: Plan }> }>(
                    `EXPLAIN (FORMAT JSON) ${sql}`,
                    values
                  );
                  eligiblePlans.push(explain.rows[0]["QUERY PLAN"][0].Plan);
                  console.log(
                    JSON.stringify({
                      retentionRows: scale,
                      scenario: "mostly-eligible-plan",
                      plan: explain.rows[0]["QUERY PLAN"][0].Plan,
                    })
                  );
                }
                const result = await tx.query<Row>(sql, values);
                if (sql.includes("WITH expired")) batchCounts.push(result.rowCount ?? 0);
                return result;
              },
            })
          );
        },
      } as PostgresDatabase;
      try {
        const start = performance.now();
        const deleted = await createPostgresCleanupRepository(bounded).deleteBefore(target, future);
        assert.ok(deleted > 0 && deleted <= 100_000);
        assert.ok(batchCounts.every((count) => count <= 1000));
        assert.equal(
          batchCounts.reduce((sum, count) => sum + count, 0),
          deleted
        );
        assert.ok(
          eligiblePlans
            .flatMap(flatten)
            .some((node) => node["Index Name"] === "conversation_turn_nodes_retention")
        );
        assert.ok(
          !eligiblePlans
            .flatMap(flatten)
            .some(
              (node) =>
                node["Relation Name"] === "conversation_turn_nodes" &&
                /Seq Scan/.test(node["Node Type"])
            ),
          "the deletion lookup must also avoid scanning the full history"
        );
        assert.ok(
          eligiblePlans.flatMap(flatten).some((node) => node["Node Type"] === "Tid Scan"),
          "deletion fetches only the tuple addresses locked by this statement"
        );
        console.log(
          JSON.stringify({
            retentionRows: scale,
            scenario: "mostly-eligible-active-writer",
            deleted,
            batches: batchCounts.length,
            elapsedMs: performance.now() - start,
          })
        );
      } finally {
        unlock();
        await hold;
      }
      const refreshed = await database.transaction((tx) =>
        tx.query<{ fresh: boolean }>(
          "SELECT last_seen_at_retention>$2::timestamptz AS fresh FROM omniroute.conversation_turn_nodes WHERE id=$1",
          ["1".padStart(64, "0"), future]
        )
      );
      assert.deepEqual(refreshed.rows, [{ fresh: true }]);
    });

    it("commits bounded batches, resumes the backlog and revisits a locked oldest row", async () => {
      await database.transaction(async (tx) => {
        await tx.query(
          "TRUNCATE omniroute.conversation_turn_nodes,omniroute.conversation_node_counts"
        );
        await insertConversationFixture(
          tx,
          `INSERT INTO omniroute.conversation_turn_nodes(id,conversation_id,role,first_seen_at,last_seen_at)
        SELECT lpad(n::text,64,'0'),'backlog','user',$1,$1 FROM generate_series(1,102001) n`,
          [old]
        );
        await tx.query("ANALYZE omniroute.conversation_turn_nodes");
      });
      let unlock!: () => void;
      let locked!: () => void;
      const barrier = new Promise<void>((resolve) => {
        locked = resolve;
      });
      const released = new Promise<void>((resolve) => {
        unlock = resolve;
      });
      const hold = database.transaction(async (tx) => {
        await tx.query("SET LOCAL idle_in_transaction_session_timeout='60s'");
        await tx.query(
          "SELECT id FROM omniroute.conversation_turn_nodes ORDER BY id LIMIT 1 FOR UPDATE"
        );
        locked();
        await released;
      });
      await barrier;
      const cleanup = createPostgresCleanupRepository(database);
      try {
        const first = await cleanup.deleteBefore(target, cutoff);
        assert.ok(first > 0 && first <= 100_000, `bounded pass: ${first}`);
        let total = first;
        for (let pass = 0; pass < 20 && total < 102000; pass++)
          total += await cleanup.deleteBefore(target, cutoff);
        assert.equal(total, 102000, "locked oldest row must not starve unlocked history");
        assert.equal(await cleanup.deleteBefore(target, cutoff), 0);
      } finally {
        unlock();
        await hold;
      }
      assert.equal(
        await cleanup.deleteBefore(target, cutoff),
        1,
        "next pass revisits the unlocked row"
      );
      await database.transaction((tx) => insertConversationFixture(tx, insert, ["late-old", old]));
      assert.equal(
        await cleanup.deleteBefore(target, cutoff),
        1,
        "backdated arrivals are not lost behind a cursor"
      );
    });

    it("retains a concurrently refreshed node and later expires it at the configured cutoff", async () => {
      await database.transaction((tx) =>
        insertConversationFixture(tx, insert, ["active-writer", old])
      );
      let release!: () => void;
      let updated!: () => void;
      const barrier = new Promise<void>((resolve) => {
        updated = resolve;
      });
      const released = new Promise<void>((resolve) => {
        release = resolve;
      });
      const write = database.transaction(async (tx) => {
        await tx.query(
          "UPDATE omniroute.conversation_turn_nodes SET last_seen_at=$1 WHERE id='active-writer'",
          [fresh]
        );
        updated();
        await released;
      });
      await barrier;
      const cleanup = createPostgresCleanupRepository(database);
      try {
        assert.equal(await cleanup.deleteBefore(target, cutoff), 0);
      } finally {
        release();
        await write;
      }
      assert.equal(await cleanup.deleteBefore(target, cutoff), 0);
      assert.equal(await cleanup.deleteBefore(target, "2026-10-14T00:00:00Z"), 1);
    });

    it("server cancellation rolls back a partially executed batch and the next pass resumes", async () => {
      await database.transaction(async (tx) => {
        await tx.query(
          "TRUNCATE omniroute.conversation_turn_nodes,omniroute.conversation_node_counts"
        );
        await insertConversationFixture(
          tx,
          `INSERT INTO omniroute.conversation_turn_nodes(id,conversation_id,role,first_seen_at,last_seen_at)
           SELECT 'cancel-'||lpad(n::text,4,'0'),'cancel','user',$1,$1 FROM generate_series(1,1200) n`,
          [old]
        );
        await insertConversationFixture(tx, insert, ["cancel-fresh", fresh]);
        // Sequence progress is visible outside the cancelled transaction, so the
        // oracle proves cancellation after some deletions, not before the batch.
        await tx.query("CREATE SEQUENCE omniroute.test_retention_delete_progress");
        await tx.query(`CREATE FUNCTION omniroute.test_retention_cancel() RETURNS trigger
          LANGUAGE plpgsql AS $$ BEGIN
            PERFORM nextval('omniroute.test_retention_delete_progress');
            IF OLD.id='cancel-0500' THEN PERFORM pg_advisory_xact_lock(18990,1); END IF;
            RETURN OLD;
          END $$`);
        await tx.query(`CREATE TRIGGER test_retention_cancel BEFORE DELETE
          ON omniroute.conversation_turn_nodes FOR EACH ROW
          EXECUTE FUNCTION omniroute.test_retention_cancel()`);
      });
      let unlock!: () => void;
      let locked!: () => void;
      const barrier = new Promise<void>((resolve) => {
        locked = resolve;
      });
      const released = new Promise<void>((resolve) => {
        unlock = resolve;
      });
      const hold = database.transaction(async (tx) => {
        await tx.query("SET LOCAL idle_in_transaction_session_timeout='30s'");
        await tx.query("SELECT pg_advisory_xact_lock(18990,1)");
        locked();
        await released;
      });
      await barrier;
      const cancellable = {
        transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
          return database.transaction(async (tx) => {
            await tx.query("SET LOCAL lock_timeout='20s'");
            return body(tx);
          });
        },
      } as PostgresDatabase;
      const outcome = createPostgresCleanupRepository(cancellable)
        .deleteBefore(target, cutoff)
        .then(
          (deleted) => ({ deleted, error: undefined }),
          (error: { sqlState?: string }) => ({ deleted: undefined, error })
        );
      try {
        let pid: number | undefined;
        const deadline = performance.now() + 10_000;
        while (pid === undefined && performance.now() < deadline) {
          pid = (
            await database.transaction((tx) =>
              tx.query<{ pid: number }>(
                "SELECT pid FROM pg_locks WHERE locktype='advisory' AND classid=18990 AND objid=1 AND NOT granted"
              )
            )
          ).rows[0]?.pid;
          if (pid === undefined) await delay(10);
        }
        assert.ok(pid, "deletion must reach the deterministic cancellation barrier");
        const progress = await database.transaction((tx) =>
          tx.query<{ last_value: string }>(
            "SELECT last_value FROM omniroute.test_retention_delete_progress"
          )
        );
        assert.ok(
          Number(progress.rows[0].last_value) > 1,
          "prior tuple deletions executed before cancellation"
        );
        const cancelled = await database.transaction((tx) =>
          tx.query<{ cancelled: boolean }>("SELECT pg_cancel_backend($1) AS cancelled", [pid])
        );
        assert.equal(cancelled.rows[0].cancelled, true);
        assert.equal((await outcome).error?.sqlState, "57014");
      } finally {
        unlock();
        await hold;
        await outcome;
        await database.transaction(async (tx) => {
          await tx.query("DROP TRIGGER test_retention_cancel ON omniroute.conversation_turn_nodes");
          await tx.query("DROP FUNCTION omniroute.test_retention_cancel()");
          await tx.query("DROP SEQUENCE omniroute.test_retention_delete_progress");
        });
      }
      const restored = await database.transaction((tx) =>
        tx.query<{ count: string }>(
          "SELECT count(*) AS count FROM omniroute.conversation_turn_nodes"
        )
      );
      assert.equal(
        restored.rows[0].count,
        "1201",
        "cancelled batch leaves all original rows intact"
      );
      assert.equal(
        await createPostgresCleanupRepository(database).deleteBefore(target, cutoff),
        1200
      );
      const surviving = await database.transaction((tx) =>
        tx.query<{ id: string }>("SELECT id FROM omniroute.conversation_turn_nodes")
      );
      assert.deepEqual(surviving.rows, [{ id: "cancel-fresh" }]);
    });

    it("preflights invalid historical timestamps atomically and backfills old public text verbatim", async () => {
      await database.transaction(async (tx) => {
        await tx.query(
          "TRUNCATE omniroute.conversation_turn_nodes,omniroute.conversation_node_counts"
        );
        await tx.query(
          "ALTER TABLE omniroute.conversation_turn_nodes DROP COLUMN last_seen_at_retention CASCADE"
        );
        await tx.query("DROP FUNCTION omniroute.set_turn_node_retention_timestamp() CASCADE");
        await insertConversationFixture(tx, insert, [
          "old-offset",
          "2026-10-01T01:00:00.123456+02:00",
        ]);
        await insertConversationFixture(tx, insert, ["old-utc", "2026-10-01 01:00:00"]);
        await insertConversationFixture(tx, insert, ["invalid", "not-a-date"]);
      });
      const migrate = () =>
        database.transaction(async (tx) => {
          await tx.query("SET LOCAL TIME ZONE 'Pacific/Auckland'");
          for (const sql of POSTGRES_RETENTION_SCHEMA_STATEMENTS) await tx.query(sql);
        });
      await assert.rejects(migrate(), (error: { sqlState?: string }) => error.sqlState === "22007");
      await database.transaction(async (tx) => {
        const columns = await tx.query(
          "SELECT 1 FROM information_schema.columns WHERE table_schema='omniroute' AND table_name='conversation_turn_nodes' AND column_name='last_seen_at_retention'"
        );
        assert.equal(columns.rowCount, 0, "failed preflight leaves no partial schema");
        await deleteConversationFixture(
          tx,
          "DELETE FROM omniroute.conversation_turn_nodes WHERE id='invalid'"
        );
      });
      for (const relative of ["now", "today", "yesterday", "tomorrow"]) {
        await database.transaction((tx) =>
          insertConversationFixture(tx, insert, ["relative", relative])
        );
        await assert.rejects(
          migrate(),
          (error: { sqlState?: string }) => error.sqlState === "22007"
        );
        await database.transaction(async (tx) => {
          const columns = await tx.query(
            "SELECT 1 FROM information_schema.columns WHERE table_schema='omniroute' AND table_name='conversation_turn_nodes' AND column_name='last_seen_at_retention'"
          );
          assert.equal(columns.rowCount, 0);
          await deleteConversationFixture(
            tx,
            "DELETE FROM omniroute.conversation_turn_nodes WHERE id='relative'"
          );
        });
      }
      const beforeRewrite = await database.transaction((tx) =>
        tx.query<{ node: number }>(
          "SELECT pg_relation_filenode('omniroute.conversation_turn_nodes') AS node"
        )
      );
      await migrate();
      const afterRewrite = await database.transaction((tx) =>
        tx.query<{ node: number }>(
          "SELECT pg_relation_filenode('omniroute.conversation_turn_nodes') AS node"
        )
      );
      assert.notEqual(
        afterRewrite.rows[0].node,
        beforeRewrite.rows[0].node,
        "legacy backfill rewrites the heap instead of maintaining indexes per updated row"
      );
      const rows = await database.transaction((tx) =>
        tx.query<{ id: string; last_seen_at: string; agrees: boolean }>(
          "SELECT id,last_seen_at,last_seen_at_retention=last_seen_at::timestamptz AS agrees FROM omniroute.conversation_turn_nodes ORDER BY id"
        )
      );
      assert.deepEqual(rows.rows, [
        { id: "old-offset", last_seen_at: "2026-10-01T01:00:00.123456+02:00", agrees: true },
        { id: "old-utc", last_seen_at: "2026-10-01 01:00:00", agrees: true },
      ]);
      assert.equal(await createPostgresCleanupRepository(database).deleteBefore(target, cutoff), 1);
    });

    it("restores derived timestamps from original text without exposing them as snapshot fields", async () => {
      const previousKey = process.env.STORAGE_ENCRYPTION_KEY;
      process.env.STORAGE_ENCRYPTION_KEY = "isolated-retention-encryption-key";
      try {
        await database.transaction(async (tx) => {
          await tx.query(
            "TRUNCATE omniroute.conversation_turn_nodes,omniroute.conversation_node_counts"
          );
          await insertConversationFixture(tx, insert, [
            "microsecond",
            "2026-10-01T00:00:00.123456+02:00",
          ]);
          await insertConversationFixture(tx, insert, ["unbounded", "infinity"]);
        });
        const snapshot = await capturePostgresApplicationSnapshot(database);
        const history = snapshot.tables.find((table) => table.name === target.table)!;
        assert.ok(!history.columns.includes("last_seen_at_retention"));
        await database.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
        await migratePostgresSchema(database);
        await importApplicationSnapshot(database, snapshot);
        await verifyApplicationImport(database, snapshot);
        const rows = await database.transaction((tx) =>
          tx.query<{ agrees: boolean }>(
            "SELECT last_seen_at_retention=last_seen_at::timestamptz AS agrees FROM omniroute.conversation_turn_nodes"
          )
        );
        assert.equal(rows.rows.length, 2);
        assert.ok(rows.rows.every((row) => row.agrees));
      } finally {
        if (previousKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
        else process.env.STORAGE_ENCRYPTION_KEY = previousKey;
      }
    });
  }
);
