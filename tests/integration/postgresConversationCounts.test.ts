import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, beforeEach, describe, it } from "node:test";
import Database from "better-sqlite3";
import { createBetterSqliteAdapter } from "../../src/lib/db/adapters/betterSqliteAdapter.ts";
import { createPostgresAgenticConversationsRepository } from "../../src/lib/db/postgres/agenticConversationsRepository.ts";
import { createPostgresConversationResolutionRepository } from "../../src/lib/db/postgres/conversationResolutionRepository.ts";
import { createPostgresCleanupRepository } from "../../src/lib/db/postgres/cleanupRepository.ts";
import {
  prepareConversationChunks,
  insertConversationChunks,
  lockConversationRoots,
  lockConversationWrite,
} from "../../src/lib/db/postgres/conversationHistory.ts";
import { verifyConversationNodeCounts } from "../../src/lib/db/postgres/conversationNodeCounts.ts";
import { captureSqliteApplicationSnapshot } from "../../src/lib/db/postgres/applicationSnapshot.ts";
import {
  importApplicationSnapshot,
  verifyApplicationImport,
  capturePostgresApplicationSnapshot,
} from "../../src/lib/db/postgres/applicationImport.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

describe(
  "native history cardinality projection",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  () => {
    let database: PostgresDatabase;
    let history: ReturnType<typeof createPostgresAgenticConversationsRepository>;
    let resolution: ReturnType<typeof createPostgresConversationResolutionRepository>;
    let cleanup: ReturnType<typeof createPostgresCleanupRepository>;
    const previousKey = process.env.STORAGE_ENCRYPTION_KEY;
    before(async () => {
      process.env.STORAGE_ENCRYPTION_KEY = "isolated-conversation-count-test-key";
      database = await createTestPostgresDatabase({ maxConnections: 8 });
      await migratePostgresSchema(database);
      history = createPostgresAgenticConversationsRepository(database);
      resolution = createPostgresConversationResolutionRepository(database);
      cleanup = createPostgresCleanupRepository(database);
    });
    beforeEach(async () => {
      await cleanup.reset([
        {
          table: "conversation_turn_nodes",
          column: "last_seen_at",
          cutoff: "iso",
          resultKey: "nodes",
        },
        {
          table: "agentic_conversations",
          column: "last_seen_at",
          cutoff: "iso",
          resultKey: "roots",
        },
      ]);
    });
    after(async () => {
      await database?.close();
      if (previousKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = previousKey;
    });
    const nodes = (count: number) =>
      Array.from({ length: count }, () => {
        const id = randomUUID();
        return { id, parentId: null, role: "user", contentHash: id };
      });
    async function seed(count: number) {
      const id = randomUUID(),
        turns = nodes(count);
      await history.createAgenticConversation({ id, apiKeyId: null, fingerprintHash: id });
      await history.insertConversationTurnNodes(id, null, turns);
      return { id, turns };
    }
    async function count(id: string) {
      return database.transaction(
        async (tx) =>
          (
            await tx.query<{ count: string }>(
              "SELECT node_count::text AS count FROM omniroute.conversation_node_counts WHERE conversation_id=$1",
              [id]
            )
          ).rows[0]?.count ?? "0"
      );
    }
    const verify = () =>
      database.transaction(verifyConversationNodeCounts, { readOnlySnapshot: true });
    async function expire(ids: string[]) {
      await database.transaction((tx) =>
        tx.query(
          "UPDATE omniroute.conversation_turn_nodes SET last_seen_at='0001-01-01T00:00:00Z' WHERE id=ANY($1::text[])",
          [ids]
        )
      );
    }

    it("counts actual inserts once across chunks, conflicts and both append owners", async () => {
      const { id, turns } = await seed(300);
      await history.insertConversationTurnNodes(id, null, turns);
      assert.equal(await count(id), "300");
      const snapshot = await resolution.snapshot(id);
      assert.ok(snapshot?.version);
      assert.equal(
        await resolution.commit({
          kind: "existing",
          id,
          expected: snapshot.version,
          nodes: [...turns, ...nodes(2)],
          correlationId: null,
          turnCount: 2,
        }),
        true
      );
      assert.equal(await count(id), "302");
      const newId = randomUUID();
      assert.equal(
        await resolution.commit({
          kind: "create",
          id: newId,
          apiKeyId: null,
          fingerprintHash: "new",
          correlationId: null,
          nodes: nodes(2),
        }),
        true
      );
      assert.equal(await count(newId), "2");
      await seed(1);
      const sql: string[] = [];
      const observed = {
        transaction<T>(
          body: (tx: PostgresTransaction) => Promise<T>,
          options: Parameters<PostgresDatabase["transaction"]>[1]
        ) {
          return database.transaction(
            (tx) =>
              body({
                query<Row>(query: string, values?: unknown[]) {
                  sql.push(query);
                  return tx.query<Row>(query, values);
                },
              }),
            options
          );
        },
      } as PostgresDatabase;
      const page = await createPostgresAgenticConversationsRepository(
        observed
      ).listMultiTurnConversations({ limit: 1 });
      assert.equal(page.total, 2);
      assert.equal(page.rows.length, 1);
      assert.ok(sql.length >= 2);
      assert.ok(
        sql.every((query) => !query.includes("conversation_turn_nodes")),
        "list qualification must not visit child history"
      );
      await verify();
    });

    it("retains orphan counts across root deletion/recreation and retires orphan nodes", async () => {
      const { id, turns } = await seed(2);
      await cleanup.deleteAll("agentic_conversations");
      assert.equal(await count(id), "2");
      assert.equal((await history.listMultiTurnConversations()).total, 0);
      await history.touchOrCreateExternalConversation(id, { apiKeyId: null });
      assert.equal((await history.listMultiTurnConversations()).total, 1);
      await cleanup.deleteAll("agentic_conversations");
      await expire(turns.map((turn) => turn.id));
      assert.equal(
        await cleanup.deleteBefore(
          { table: "conversation_turn_nodes", column: "last_seen_at", cutoff: "iso" },
          "0002-01-01T00:00:00Z"
        ),
        2
      );
      assert.equal(await count(id), "0");
      await verify();
    });

    it("root retention and generic partial reset apply only actual deleted deltas", async () => {
      const first = await seed(3),
        second = await seed(2);
      await expire([first.turns[0].id, second.turns[0].id]);
      assert.equal(
        await cleanup.deleteBefore(
          { table: "conversation_turn_nodes", column: "last_seen_at", cutoff: "iso" },
          "0002-01-01T00:00:00Z"
        ),
        2
      );
      assert.equal(await count(first.id), "2");
      assert.equal(await count(second.id), "1");
      await expire([first.turns[1].id]);
      const result = await cleanup.reset(
        [
          {
            table: "conversation_turn_nodes",
            column: "last_seen_at",
            cutoff: "iso",
            resultKey: "nodes",
          },
        ],
        "0002-01-01T00:00:00Z"
      );
      assert.equal(result.counts.nodes, 1);
      assert.equal(await count(first.id), "1");
      assert.equal((await history.listMultiTurnConversations()).total, 0);
      await verify();
      assert.equal(await cleanup.deleteAll("conversation_turn_nodes"), 2);
      await verify();
    });

    it("rolls back source and projection together and refuses drift without repair", async () => {
      const { id, turns } = await seed(2);
      const failure = new Error("controlled rollback");
      await assert.rejects(
        database.transaction(async (tx) => {
          await lockConversationWrite(tx);
          await lockConversationRoots(tx, [id]);
          await insertConversationChunks(tx, id, null, prepareConversationChunks(nodes(2)));
          throw failure;
        }),
        (error) => error === failure
      );
      assert.equal(await count(id), "2");
      await verify();
      await database.transaction((tx) =>
        tx.query("DELETE FROM omniroute.conversation_node_counts WHERE conversation_id=$1", [id])
      );
      await expire(turns.map((turn) => turn.id));
      await assert.rejects(
        cleanup.deleteBefore(
          { table: "conversation_turn_nodes", column: "last_seen_at", cutoff: "iso" },
          "0002-01-01T00:00:00Z"
        )
      );
      assert.equal((await history.getConversationTurnIndex(id)).nodeIds.size, 2);
      await assert.rejects(verify(), /integrity failure/);
      assert.equal(await count(id), "0", "verification must not silently repair missing state");
    });

    it("reads counts and eligibility from one repeatable-read snapshot", async () => {
      const { id } = await seed(1);
      const entered = Promise.withResolvers<void>(),
        release = Promise.withResolvers<void>();
      const observed = {
        transaction<T>(
          body: (tx: PostgresTransaction) => Promise<T>,
          options: Parameters<PostgresDatabase["transaction"]>[1]
        ) {
          return database.transaction(
            (tx) =>
              body({
                async query<Row>(sql: string, values?: unknown[]) {
                  const result = await tx.query<Row>(sql, values);
                  if (sql.startsWith("SELECT count(*) AS c")) {
                    entered.resolve();
                    await release.promise;
                  }
                  return result;
                },
              }),
            options
          );
        },
      } as PostgresDatabase;
      const reading =
        createPostgresAgenticConversationsRepository(observed).listMultiTurnConversations();
      await entered.promise;
      try {
        await history.insertConversationTurnNodes(id, null, nodes(1));
      } finally {
        release.resolve();
      }
      const page = await reading;
      assert.equal(page.total, 0);
      assert.deepEqual(page.rows, [], "a concurrent append cannot enter an older count snapshot");
      assert.equal((await history.listMultiTurnConversations()).total, 1);
      await verify();
    });

    it("reconstructs imports and native restores, excludes derived rows and rejects tampering", async () => {
      const raw = new Database(":memory:");
      try {
        raw.exec(`CREATE TABLE conversation_turn_nodes(id TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,role TEXT NOT NULL,first_seen_at TEXT NOT NULL,last_seen_at TEXT NOT NULL);
        INSERT INTO conversation_turn_nodes VALUES('n1','orphan','user','2026-10-19','2026-10-19'),('n2','orphan','assistant','2026-10-19','2026-10-19');`);
        raw.pragma("query_only=ON");
        const snapshot = captureSqliteApplicationSnapshot(createBetterSqliteAdapter(raw));
        await importApplicationSnapshot(database, snapshot);
        assert.equal(await count("orphan"), "2");
        await verifyApplicationImport(database, snapshot);
        const native = await capturePostgresApplicationSnapshot(database);
        assert.ok(native.excluded.some((table) => table.name === "conversation_node_counts"));
        assert.ok(native.tables.every((table) => table.name !== "conversation_node_counts"));
        await database.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
        await migratePostgresSchema(database);
        await importApplicationSnapshot(database, native);
        assert.equal(await count("orphan"), "2");
        await verifyApplicationImport(database, native);
        await database.transaction((tx) =>
          tx.query(
            "UPDATE omniroute.conversation_node_counts SET node_count=3 WHERE conversation_id='orphan'"
          )
        );
        await assert.rejects(verifyApplicationImport(database, native), /integrity failure/);
        await assert.rejects(capturePostgresApplicationSnapshot(database), /integrity failure/);
        assert.equal(await count("orphan"), "3");
      } finally {
        raw.close();
      }
    });
    it("backfills legacy orphans atomically while preserving migration history", async () => {
      await database.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
      const stopped = new Error("controlled migration boundary");
      let reached = false;
      const prefix = {
        transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
          return database.transaction(async (tx) => {
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
      const historyBefore = await database.transaction((tx) =>
        tx.query(
          "SELECT version,name,checksum,applied_at::text FROM omniroute.schema_migrations ORDER BY version"
        )
      );
      assert.equal(historyBefore.rows.length, 10);
      await database.transaction((tx) =>
        tx.query(
          `INSERT INTO omniroute.conversation_turn_nodes(id,conversation_id,role,first_seen_at,last_seen_at)
         SELECT 'legacy-'||n,'orphan','user','2026-10-19','2026-10-19' FROM generate_series(1,3) n`
        )
      );
      const rejected = {
        transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
          return database.transaction((tx) =>
            body({
              query<Row>(sql: string, values?: unknown[]) {
                if (sql.startsWith("INSERT INTO omniroute.conversation_node_counts")) throw stopped;
                return tx.query<Row>(sql, values);
              },
            })
          );
        },
      } as PostgresDatabase;
      await assert.rejects(migratePostgresSchema(rejected), (error) => error === stopped);
      const absent = await database.transaction((tx) =>
        tx.query("SELECT to_regclass('omniroute.conversation_node_counts') AS counts")
      );
      assert.deepEqual(absent.rows, [{ counts: null }]);
      await migratePostgresSchema(database);
      assert.equal(await count("orphan"), "3");
      const historyAfter = await database.transaction((tx) =>
        tx.query(
          "SELECT version,name,checksum,applied_at::text FROM omniroute.schema_migrations WHERE version<=10 ORDER BY version"
        )
      );
      assert.deepEqual(historyAfter.rows, historyBefore.rows);
      await verify();
      await history.touchOrCreateExternalConversation("orphan", { apiKeyId: null });
      assert.equal((await history.listMultiTurnConversations()).total, 1);
    });
  }
);
