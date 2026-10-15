import assert from "node:assert/strict";
import test from "node:test";
import { createPostgresAgenticConversationsRepository } from "../../src/lib/db/postgres/agenticConversationsRepository.ts";
import { createPostgresCleanupRepository } from "../../src/lib/db/postgres/cleanupRepository.ts";
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
    const database = await createTestPostgresDatabase({ maxConnections: 4 });
    try {
      await migratePostgresSchema(database);
      const roots = 55000,
        nodes = 12500000,
        logs = 90000;
      await database.transaction((tx) =>
        tx.query(
          `INSERT INTO omniroute.agentic_conversations(id,api_key_id,fingerprint_hash,last_message_count,last_messages_hash,first_seen_at,last_seen_at)
       SELECT 'scale-root-'||n,NULL,'scale',0,'','2026-10-15T00:00:00Z','2026-10-15T00:00:00Z' FROM generate_series(1,$1) n`,
          [roots]
        )
      );
      for (let start = 1; start <= nodes; start += 10000) {
        await database.transaction((tx) =>
          tx.query(
            `INSERT INTO omniroute.conversation_turn_nodes(id,conversation_id,parent_id,role,content_hash,first_seen_at,last_seen_at)
         SELECT 'scale-node-'||n,'scale-root-'||(n%$3+1),NULL,'user',repeat('a',64),'2026-10-15T00:00:00Z','2026-10-15T00:00:00Z'
         FROM generate_series($1::int,$2::int) n`,
            [start, Math.min(nodes, start + 9999), roots]
          )
        );
      }
      for (let start = 1; start <= logs; start += 10000)
        await database.transaction((tx) =>
          tx.query(
            `INSERT INTO omniroute.call_logs(id,timestamp,session_tag)
       SELECT 'scale-log-'||n,'2026-10-15T00:00:00Z','scale-root-'||(n%$3+1) FROM generate_series($1::int,$2::int) n`,
            [start, Math.min(logs, start + 9999), roots]
          )
        );
      for (const table of ["agentic_conversations", "conversation_turn_nodes", "call_logs"])
        await database.transaction((tx) => tx.query(`ANALYZE omniroute.${table}`));
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
            cleanupMs: performance.now() - cleanupStarted,
          })
      );
    } finally {
      await database.close();
    }
  }
);
