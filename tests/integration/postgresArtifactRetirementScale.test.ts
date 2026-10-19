import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { PostgresCallLogRepository } from "../../src/lib/db/postgres/callLogRepository.ts";
import {
  attachArtifactRecovery,
  claimArtifactRecoveryDispatch,
} from "../../src/lib/db/postgres/artifactRecoveryRepository.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

type Plan = {
  "Node Type": string;
  "Relation Name"?: string;
  "Index Name"?: string;
  Plans?: Plan[];
};
function nodes(plan: Plan): Plan[] {
  return [plan, ...(plan.Plans ?? []).flatMap(nodes)];
}
function barrier() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

function stopBeforeMigrationStatement(database: PostgresDatabase, statementPrefix: string) {
  const stopped = new Error(`controlled migration boundary: ${statementPrefix}`);
  let markerHits = 0;
  const bounded = {
    transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
      return database.transaction(async (tx) => {
        try {
          return await body({
            async query<Row>(sql: string, values?: unknown[]) {
              if (sql.startsWith(statementPrefix)) {
                markerHits += 1;
                throw stopped;
              }
              return tx.query<Row>(sql, values);
            },
          });
        } catch (error) {
          if (error !== stopped) throw error;
          return undefined as T;
        }
      });
    },
  } as PostgresDatabase;
  return { database: bounded, markerHits: () => markerHits };
}

test(
  "indexed retirement preserves all states and admits concurrent history and artifact writers",
  { skip: !postgresTestsEnabled, timeout: 120000 },
  async () => {
    // Fixture loading/migration is offline; competing production operations use defaults.
    const fixture = await createTestPostgresDatabase({
      statementTimeoutMs: 60000,
      queryTimeoutMs: 65000,
      transactionTimeoutMs: 90000,
    });
    const database = await createTestPostgresDatabase();
    const monitor = await createTestPostgresDatabase();
    const held = barrier(),
      release = barrier();
    const rowCount = 100100;
    const checksum = createHash("sha256").update("payload").digest("hex");
    const intent = {
      artifact: {
        reference: `s3/${randomUUID()}_${checksum}.json`,
        checksum,
        sizeBytes: 7,
        storageScope: "a".repeat(64),
        singleAttempt: true as const,
      },
      owner: randomUUID(),
      ciphertext: "enc:v1:retirement-fixture",
    };
    let revoke: string;
    const explain = (sql: string, values: unknown[]) =>
      fixture.transaction(
        async (tx) =>
          (
            await tx.query<{ "QUERY PLAN": Array<{ Plan: Plan }> }>(
              `EXPLAIN (FORMAT JSON) ${sql}`,
              values
            )
          ).rows[0]["QUERY PLAN"][0].Plan
      );
    let trimming: Promise<unknown> | undefined;
    let writing: Promise<unknown> | undefined;
    let attaching: Promise<unknown> | undefined;
    try {
      await fixture.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      const beforeVersion10 = stopBeforeMigrationStatement(
        fixture,
        "CREATE INDEX artifact_recovery_retirement_owner_idx"
      );
      await migratePostgresSchema(beforeVersion10.database);
      assert.equal(beforeVersion10.markerHits(), 1, "migration 10 boundary must be reached once");
      const historyV9 = await fixture.transaction((tx) =>
        tx.query<{ version: number; name: string; checksum: string; applied_at: string }>(
          "SELECT version,name,checksum,applied_at::text AS applied_at FROM omniroute.schema_migrations ORDER BY version"
        )
      );
      assert.deepEqual(
        historyV9.rows.map((row) => row.version),
        [1, 2, 3, 4, 5, 6, 7, 8, 9]
      );
      assert.deepEqual(
        historyV9.rows.map((row) => row.name),
        [
          "routing_repositories",
          "control_plane",
          "server_persistence",
          "file_content_chunks",
          "artifact_publication_ownership",
          "fractional_compression_durations",
          "indexed_conversation_retention",
          "durable_artifact_recovery",
          "conversation_concurrency",
        ]
      );
      for (const row of historyV9.rows) {
        assert.match(row.checksum, /^[0-9a-f]{64}$/);
        assert.notEqual(row.applied_at, "");
      }
      const assertMigrationBoundaryObjects = async (retirementIndexExpected: boolean) => {
        const objects = await fixture.transaction((tx) =>
          tx.query<{ retirement_index: string | null; node_counts: string | null }>(
            `SELECT to_regclass('omniroute.artifact_recovery_retirement_owner_idx') AS retirement_index,
              to_regclass('omniroute.conversation_node_counts') AS node_counts`
          )
        );
        assert.equal(Boolean(objects.rows[0]?.retirement_index), retirementIndexExpected);
        assert.equal(objects.rows[0]?.node_counts ?? null, null);
        const authColumns = await fixture.transaction((tx) =>
          tx.query<{ column_name: string }>(
            `SELECT column_name FROM information_schema.columns
             WHERE table_schema='omniroute' AND table_name='sync_tokens'
               AND column_name=ANY($1::text[]) ORDER BY column_name`,
            [["issued_by", "expires_at", "auth_generation"]]
          )
        );
        assert.deepEqual(authColumns.rows, []);
        const authGeneration = await fixture.transaction((tx) =>
          tx.query(
            "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key='managementAuthGeneration'"
          )
        );
        assert.deepEqual(authGeneration.rows, []);
      };
      await assertMigrationBoundaryObjects(false);
      const trigger = await fixture.transaction((tx) =>
        tx.query<{ prosrc: string }>(
          "SELECT prosrc FROM pg_proc WHERE oid='omniroute.revoke_call_artifact_intent()'::regprocedure"
        )
      );
      // Explain the actual stored trigger statement, including its complete predicate.
      revoke = trigger.rows[0].prosrc
        .match(/UPDATE[\s\S]*?;/)![0]
        .replaceAll("OLD.id", "$1")
        .replaceAll("OLD.insertion_order", "$2");
      await fixture.transaction(async (tx) => {
        await tx.query(
          `INSERT INTO omniroute.call_logs(id,timestamp,insertion_order,artifact_relpath)
          SELECT 'retire-'||n,to_char('2026-08-01'::timestamp+n*interval '1 second','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),n,
            CASE WHEN n%3=0 THEN 'retire-object-'||n ELSE NULL END
          FROM generate_series(1,$1::integer) n`,
          [rowCount]
        );
        await tx.query(`INSERT INTO omniroute.artifact_deletion_queue
          (relpath,call_log_id,call_log_order,publication_state,upload_state,staged_payload,staged_bytes)
          SELECT 'retire-object-'||n,'retire-'||n,n,
            CASE n%3 WHEN 0 THEN 'attached' WHEN 1 THEN 'publishing' ELSE 'delete_pending' END,
            CASE WHEN n%3=1 THEN 'not_sent' ELSE 'acknowledged' END,
            CASE WHEN n%3=1 THEN 'enc:v1:fixture' END,
            CASE WHEN n%3=1 THEN octet_length('enc:v1:fixture') ELSE 0 END
          FROM generate_series(1,100000) n`);
        await tx.query(
          "SELECT setval(pg_get_serial_sequence('omniroute.call_logs','insertion_order'),$1)",
          [rowCount]
        );
        await tx.query("ANALYZE omniroute.call_logs");
        await tx.query("ANALYZE omniroute.artifact_deletion_queue");
      });
      const before = nodes(await explain(revoke, ["absent-owner", "1"]));
      assert.ok(
        before.some((n) => n["Node Type"] === "Seq Scan"),
        "the schema-9 trigger lookup scans the queue despite the active-owner index"
      );
      const beforeVersion11 = stopBeforeMigrationStatement(
        fixture,
        "CREATE TABLE omniroute.conversation_node_counts"
      );
      await migratePostgresSchema(beforeVersion11.database);
      assert.equal(beforeVersion11.markerHits(), 1, "migration 11 boundary must be reached");
      const historyV10 = await fixture.transaction((tx) =>
        tx.query<{ version: number; name: string; checksum: string; applied_at: string }>(
          "SELECT version,name,checksum,applied_at::text AS applied_at FROM omniroute.schema_migrations ORDER BY version"
        )
      );
      assert.deepEqual(
        historyV10.rows.map((row) => row.version),
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      );
      assert.deepEqual(historyV10.rows.slice(0, 9), historyV9.rows);
      assert.equal(historyV10.rows[9].name, "indexed_artifact_recovery_retirement");
      assert.match(historyV10.rows[9].checksum, /^[0-9a-f]{64}$/);
      assert.notEqual(historyV10.rows[9].applied_at, "");
      await assertMigrationBoundaryObjects(true);
      const indexAfterUpgrade = await fixture.transaction((tx) =>
        tx.query<{ retirement_index: string | null }>(
          "SELECT to_regclass('omniroute.artifact_recovery_retirement_owner_idx') AS retirement_index"
        )
      );
      assert.ok(indexAfterUpgrade.rows[0]?.retirement_index);
      await migratePostgresSchema(beforeVersion11.database);
      assert.equal(
        beforeVersion11.markerHits(),
        2,
        "repeated bounded upgrade must validate version 10 and reach migration 11 again"
      );
      const historyAfterRepeat = await fixture.transaction((tx) =>
        tx.query<{ version: number; name: string; checksum: string; applied_at: string }>(
          "SELECT version,name,checksum,applied_at::text AS applied_at FROM omniroute.schema_migrations ORDER BY version"
        )
      );
      assert.deepEqual(historyAfterRepeat.rows, historyV10.rows);
      await assertMigrationBoundaryObjects(true);
      for (const owner of ["retire-1", "retire-2", "retire-3", "absent-owner"]) {
        const plan = nodes(
          await explain(revoke, [owner, owner === "absent-owner" ? "1" : owner.slice(7)])
        );
        assert.ok(plan.some((n) => n["Index Name"] === "artifact_recovery_retirement_owner_idx"));
        assert.ok(
          !plan.some(
            (n) => n["Node Type"] === "Seq Scan" && n["Relation Name"] === "artifact_deletion_queue"
          )
        );
        // OLD.id comes from a C-collated column; an untyped client parameter may
        // use the database default instead. Both must avoid a queue-sized scan.
        for (const collation of ["", ' COLLATE "C"']) {
          const report = await fixture.transaction(
            async (tx) =>
              (
                await tx.query<{
                  "QUERY PLAN": Array<{
                    Plan: Plan & { "Shared Hit Blocks": number; "Shared Read Blocks": number };
                  }>;
                }>(
                  `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT 1
                FROM omniroute.artifact_deletion_queue
                WHERE call_log_id=$1${collation} AND call_log_order=$2`,
                  [owner, owner === "absent-owner" ? "1" : owner.slice(7)]
                )
              ).rows[0]["QUERY PLAN"][0].Plan
          );
          assert.ok(
            nodes(report).some(
              (node) => node["Index Name"] === "artifact_recovery_retirement_owner_idx"
            )
          );
          assert.ok(
            report["Shared Hit Blocks"] + report["Shared Read Blocks"] < 32,
            "owner lookup must touch a bounded index path, not scan its retained entries"
          );
        }
      }

      const repository = new PostgresCallLogRepository(database);
      const admitted = await repository.insert(
        {
          id: "attach-during-retention",
          timestamp: "2026-10-19T00:00:00.000Z",
          detailState: "missing",
          detailReason: "publication_pending",
          videoContentRemoved: 0,
        },
        intent
      );
      assert.equal(admitted.intentAccepted, true);
      assert.equal(
        await claimArtifactRecoveryDispatch(
          database,
          intent.artifact.reference,
          intent.owner,
          randomUUID()
        ),
        true
      );
      let holderPid = 0;
      const observed = {
        transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
          return database.transaction((tx) =>
            body({
              async query<Row>(sql: string, values?: unknown[]) {
                const result = await tx.query<Row>(sql, values);
                if (sql === "SELECT pg_advisory_xact_lock(18765,$1)") {
                  holderPid = (await tx.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))
                    .rows[0].pid;
                  held.release();
                  await release.promise;
                }
                return result;
              },
            })
          );
        },
      } as PostgresDatabase;
      const retirement = new PostgresCallLogRepository(observed).trim(rowCount - 99, 100);
      trimming = retirement;
      void retirement.catch(() => {});
      await Promise.race([held.promise, retirement]);
      writing = repository.insert({
        id: "admit-during-retention",
        timestamp: "2026-10-19T00:00:01.000Z",
        videoContentRemoved: 0,
      });
      attaching = attachArtifactRecovery(database, intent.artifact.reference, intent.owner);
      void writing.catch(() => {});
      void attaching.catch(() => {});
      // Observe real lock contention, rather than trusting a sleep or promise ordering.
      let waiting = 0;
      const deadline = performance.now() + 700;
      do {
        waiting = Number(
          (
            await monitor.transaction((tx) =>
              tx.query<{ count: string }>(
                "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND wait_event='advisory' AND $1=ANY(pg_blocking_pids(pid))",
                [holderPid]
              )
            )
          ).rows[0].count
        );
        if (waiting >= 2) break;
        await delay(5);
      } while (performance.now() < deadline);
      assert.equal(waiting, 2);
      const started = performance.now();
      release.release();
      const [removed, inserted, attached] = await Promise.all([retirement, writing, attaching]);
      assert.equal(removed.deletedRows, 100);
      assert.equal((inserted as { inserted: boolean }).inserted, true);
      assert.equal(attached, true);
      console.log(
        JSON.stringify({
          scenario: "indexed-artifact-retirement",
          queueRows: 100000,
          retired: removed.deletedRows,
          concurrentWriters: 2,
          elapsedMs: performance.now() - started,
        })
      );
      const states = await database.transaction((tx) =>
        tx.query<{
          retired: string;
          cleared: string;
          attached: string;
          staged: string;
        }>(`SELECT count(*) FILTER(WHERE call_log_order<=100 AND publication_state='delete_pending') AS retired,
        count(*) FILTER(WHERE call_log_order<=100 AND staged_payload IS NULL AND staged_bytes=0) AS cleared,
        count(*) FILTER(WHERE call_log_order=102 AND publication_state='attached') AS attached,
        count(*) FILTER(WHERE call_log_order=103 AND publication_state='publishing' AND staged_payload IS NOT NULL) AS staged
        FROM omniroute.artifact_deletion_queue WHERE relpath LIKE 'retire-object-%'`)
      );
      assert.deepEqual(states.rows, [
        { retired: "100", cleared: "100", attached: "1", staged: "1" },
      ]);
      assert.equal(removed.paths.length, 33);
    } finally {
      release.release();
      await Promise.allSettled([trimming, writing, attaching].filter(Boolean));
      await database.close();
      await monitor.close();
      await fixture.close();
    }
  }
);
