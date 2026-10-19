import assert from "node:assert/strict";
import test from "node:test";
import { createPostgresAgenticConversationsRepository } from "../../src/lib/db/postgres/agenticConversationsRepository.ts";
import { createPostgresCleanupRepository } from "../../src/lib/db/postgres/cleanupRepository.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "production-sized native history permits dashboard reads alongside unrelated writers",
  {
    skip: !postgresTestsEnabled || process.env.RUN_PG_HISTORY_SCALE !== "1",
    timeout: 1200000,
  },
  async () => {
    const roots = 55000,
      nodes = 12500000,
      logs = 90000;
    let migrationMs = 0;
    // Bulk preparation and ANALYZE use an offline budget, including waits for maintenance.
    // The measured application below gets a separate client with the original defaults.
    const setup = await createTestPostgresDatabase({
      maxConnections: 1,
      lockTimeoutMs: 30_000,
      statementTimeoutMs: 240_000,
      queryTimeoutMs: 250_000,
      transactionTimeoutMs: 600_000,
    });
    try {
      const stopped = new Error("controlled migration boundary");
      let reached = false;
      const prefix = {
        transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
          return setup.transaction(async (tx) => {
            try {
              return await body({
                query<Row>(sql: string, values?: unknown[]) {
                  if (sql.startsWith("CREATE TABLE omniroute.conversation_node_counts")) {
                    reached = true;
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
      await migratePostgresSchema(prefix);
      assert.equal(reached, true);
      const historyBefore = await setup.transaction((tx) =>
        tx.query(
          "SELECT version,name,checksum,applied_at::text FROM omniroute.schema_migrations ORDER BY version"
        )
      );
      assert.equal(historyBefore.rows.length, 10);
      const countsBefore = await setup.transaction((tx) =>
        tx.query("SELECT to_regclass('omniroute.conversation_node_counts') AS counts")
      );
      assert.deepEqual(countsBefore.rows, [{ counts: null }]);
      await setup.transaction((tx) =>
        tx.query(
          `INSERT INTO omniroute.agentic_conversations(id,api_key_id,fingerprint_hash,last_message_count,last_messages_hash,first_seen_at,last_seen_at)
       SELECT 'scale-root-'||n,NULL,'scale',0,'','2026-10-15T00:00:00Z','2026-10-15T00:00:00Z' FROM generate_series(1,$1) n`,
          [roots]
        )
      );
      for (let start = 1; start <= nodes; start += 10000) {
        await setup.transaction((tx) =>
          tx.query(
            `INSERT INTO omniroute.conversation_turn_nodes(id,conversation_id,parent_id,role,content_hash,first_seen_at,last_seen_at)
         SELECT 'scale-node-'||n,'scale-root-'||(n%$3+1),NULL,'user',repeat('a',64),'2026-10-15T00:00:00Z','2026-10-15T00:00:00Z'
         FROM generate_series($1::int,$2::int) n`,
            [start, Math.min(nodes, start + 9999), roots]
          )
        );
      }
      for (let start = 1; start <= logs; start += 10000)
        await setup.transaction((tx) =>
          tx.query(
            `INSERT INTO omniroute.call_logs(id,timestamp,session_tag)
       SELECT 'scale-log-'||n,'2026-10-15T00:00:00Z','scale-root-'||(n%$3+1) FROM generate_series($1::int,$2::int) n`,
            [start, Math.min(logs, start + 9999), roots]
          )
        );
      for (const table of ["agentic_conversations", "conversation_turn_nodes", "call_logs"])
        await setup.transaction((tx) => tx.query(`ANALYZE omniroute.${table}`));
      const migrationStarted = performance.now();
      await migratePostgresSchema(setup);
      migrationMs = performance.now() - migrationStarted;
      const historyAfter = await setup.transaction((tx) =>
        tx.query(
          "SELECT version,name,checksum,applied_at::text FROM omniroute.schema_migrations ORDER BY version"
        )
      );
      assert.equal(historyAfter.rows.length, 12);
      assert.deepEqual(historyAfter.rows.slice(0, 10), historyBefore.rows);
      assert.deepEqual(
        historyAfter.rows.map((row) => row.version),
        Array.from({ length: 12 }, (_, index) => index + 1)
      );
      assert.deepEqual(
        historyAfter.rows.slice(10).map((row) => [row.version, row.name]),
        [
          [11, "conversation_node_counts"],
          [12, "management_auth_lifecycle"],
        ]
      );
      const projected = await setup.transaction((tx) =>
        tx.query<{ groups: string; nodes: string }>(
          "SELECT count(*)::text AS groups,COALESCE(sum(node_count),0)::text AS nodes FROM omniroute.conversation_node_counts"
        )
      );
      assert.deepEqual(projected.rows, [{ groups: String(roots), nodes: String(nodes) }]);
      await setup.transaction((tx) => tx.query("ANALYZE omniroute.conversation_node_counts"));
      console.info(
        "OMNIROUTE_HISTORY_SCALE_MIGRATION " +
          JSON.stringify({ status: "passed", roots, nodes, logs, migrationMs })
      );
    } finally {
      await setup.close();
    }
    const database = await createTestPostgresDatabase({ maxConnections: 4 });
    try {
      const timeouts = await database.transaction((tx) =>
        tx.query(
          "SELECT current_setting('lock_timeout') AS lock_timeout,current_setting('statement_timeout') AS statement_timeout"
        )
      );
      assert.deepEqual(timeouts.rows, [{ lock_timeout: "1s", statement_timeout: "5s" }]);
      console.info("OMNIROUTE_HISTORY_SCALE_FIXTURE_READY");
      const repository = createPostgresAgenticConversationsRepository(database);
      const readMs: number[] = [],
        writeMs: number[] = [];
      for (let sample = 0; sample < 10; sample++)
        await Promise.all([
          (async () => {
            const started = performance.now();
            const page = await repository.listMultiTurnConversations({ limit: 50 });
            readMs.push(performance.now() - started);
            assert.equal(page.total, roots);
            assert.equal(page.rows.length, 50);
          })(),
          (async () => {
            const started = performance.now();
            await repository.touchOrCreateExternalConversation("scale-independent-writer", {
              apiKeyId: null,
            });
            writeMs.push(performance.now() - started);
          })(),
        ]);
      const cleanupStarted = performance.now();
      assert.equal(
        await createPostgresCleanupRepository(database).deleteBefore(
          { table: "conversation_turn_nodes", column: "last_seen_at", cutoff: "iso" },
          "2026-10-14T00:00:00Z"
        ),
        0
      );
      assert.ok(
        writeMs.every((ms) => ms < 1000),
        "independent writes stay within the old lock-wait budget"
      );
      console.info(
        "OMNIROUTE_HISTORY_SCALE_RESULT " +
          JSON.stringify({
            status: "passed",
            roots,
            nodes,
            logs,
            readMs,
            writeMs,
            migrationMs,
            cleanupMs: performance.now() - cleanupStarted,
          })
      );
    } finally {
      await database.close();
    }
  }
);
