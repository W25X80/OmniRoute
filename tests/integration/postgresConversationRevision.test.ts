import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, describe, it } from "node:test";
import { createPostgresAgenticConversationsRepository } from "../../src/lib/db/postgres/agenticConversationsRepository.ts";
import { createPostgresConversationResolutionRepository } from "../../src/lib/db/postgres/conversationResolutionRepository.ts";
import { createPostgresCleanupRepository } from "../../src/lib/db/postgres/cleanupRepository.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

describe(
  "native conversation revision protocol",
  { skip: !postgresTestsEnabled, timeout: 30000 },
  () => {
    let database: PostgresDatabase;
    let history: ReturnType<typeof createPostgresAgenticConversationsRepository>;
    let resolution: ReturnType<typeof createPostgresConversationResolutionRepository>;
    let cleanup: ReturnType<typeof createPostgresCleanupRepository>;
    before(async () => {
      database = await createTestPostgresDatabase({ maxConnections: 8 });
      await migratePostgresSchema(database);
      history = createPostgresAgenticConversationsRepository(database);
      resolution = createPostgresConversationResolutionRepository(database);
      cleanup = createPostgresCleanupRepository(database);
    });
    after(async () => {
      await database?.close();
    });
    async function seed(id = randomUUID()) {
      await history.createAgenticConversation({ id, apiKeyId: null, fingerprintHash: id });
      const node = randomUUID();
      await history.insertConversationTurnNodes(id, null, [
        { id: node, parentId: id, role: "user", contentHash: node },
      ]);
      return { id, node };
    }
    function command(
      id: string,
      expected: NonNullable<Awaited<ReturnType<typeof resolution.snapshot>>>["version"]
    ) {
      return {
        kind: "existing" as const,
        id,
        expected,
        nodes: [],
        correlationId: null,
        turnCount: 2,
      };
    }

    it("reads history while the legacy global exclusive fence is held", async () => {
      const { id } = await seed();
      const entered = Promise.withResolvers<void>(),
        release = Promise.withResolvers<void>();
      const held = database.transaction(async (tx) => {
        await tx.query("SELECT pg_advisory_xact_lock(18765,21)");
        entered.resolve();
        await release.promise;
      });
      await entered.promise;
      try {
        assert.ok(await resolution.snapshot(id));
        assert.equal((await history.getConversationTurnIndex(id)).nodeIds.size, 1);
        await history.listMultiTurnConversations();
      } finally {
        release.resolve();
        await held;
      }
    });

    it("a locked root does not block a different external conversation", async () => {
      const { id } = await seed();
      const entered = Promise.withResolvers<void>(),
        release = Promise.withResolvers<void>();
      const held = database.transaction(async (tx) => {
        await tx.query("SELECT id FROM omniroute.agentic_conversations WHERE id=$1 FOR UPDATE", [
          id,
        ]);
        entered.resolve();
        await release.promise;
      });

      await entered.promise;
      try {
        await history.touchOrCreateExternalConversation(randomUUID(), { apiKeyId: null });
      } finally {
        release.resolve();
        await held;
      }
    });

    it("the transaction owner supplies a consistent read-only compound snapshot", async () => {
      const { id, node } = await seed();
      const entered = Promise.withResolvers<void>(),
        release = Promise.withResolvers<void>();
      const read = database.transaction(
        async (tx) => {
          const before = await tx.query<{ history_revision: string }>(
            "SELECT history_revision FROM omniroute.agentic_conversations WHERE id=$1",
            [id]
          );
          entered.resolve();
          await release.promise;
          const nodes = await tx.query(
            "SELECT id FROM omniroute.conversation_turn_nodes WHERE conversation_id=$1",
            [id]
          );
          return { version: before.rows[0].history_revision, count: nodes.rowCount };
        },
        { readOnlySnapshot: true }
      );
      await entered.promise;
      try {
        const child = randomUUID();
        await history.insertConversationTurnNodes(id, null, [
          { id: child, parentId: node, role: "user", contentHash: child },
        ]);
      } finally {
        release.resolve();
      }
      assert.deepEqual(await read, { version: "1", count: 1 });
      await assert.rejects(
        database.transaction(
          (tx) =>
            tx.query("UPDATE omniroute.agentic_conversations SET turn_count=99 WHERE id=$1", [id]),
          { readOnlySnapshot: true }
        )
      );
    });
    it("stale snapshots cannot append after another committed node writer", async () => {
      const { id, node } = await seed();
      const snapshot = await resolution.snapshot(id);
      const child = randomUUID();
      await history.insertConversationTurnNodes(id, null, [
        { id: child, parentId: node, role: "user", contentHash: child },
      ]);
      assert.equal(await resolution.commit(command(id, snapshot!.version)), false);
      assert.equal((await history.getMultiTurnConversationById(id))!.turnCount, 1);
      assert.equal(
        await resolution.commit(command(id, (await resolution.snapshot(id))!.version)),
        true
      );
      assert.equal((await history.getMultiTurnConversationById(id))!.turnCount, 2);
    });

    it("reset advances versions even when it leaves parent metadata intact", async () => {
      const { id } = await seed();
      const snapshot = await resolution.snapshot(id);
      await cleanup.reset([
        {
          table: "conversation_turn_nodes",
          column: "last_seen_at",
          cutoff: "iso",
          resultKey: "nodes",
        },
      ]);
      assert.equal(await resolution.commit(command(id, snapshot!.version)), false);
      assert.equal((await resolution.snapshot(id))!.index.nodeIds.size, 0);
    });

    it("deletion and recreation of the same ID cannot reuse an old incarnation", async () => {
      const { id } = await seed();
      const snapshot = await resolution.snapshot(id);
      await cleanup.deleteAll("agentic_conversations");
      await history.createAgenticConversation({ id, apiKeyId: null, fingerprintHash: "recreated" });
      const fresh = await resolution.snapshot(id);
      assert.notEqual(fresh!.version!.incarnation, snapshot!.version!.incarnation);
      assert.equal(await resolution.commit(command(id, snapshot!.version)), false);
    });

    it("root creation and node insertion roll back together on a rejected statement", async () => {
      const id = randomUUID();
      await assert.rejects(
        resolution.commit({
          kind: "create",
          id,
          apiKeyId: null,
          fingerprintHash: "atomic",
          correlationId: null,
          nodes: [
            { id: randomUUID(), parentId: id, role: null as unknown as string, contentHash: "x" },
          ],
        })
      );
      assert.equal(await resolution.snapshot(id), null);
    });

    it("retention skips a busy first root and invalidates another root's snapshot", async () => {
      const first = await seed("000-busy-" + randomUUID());
      const other = await seed("zzz-free-" + randomUUID());
      const snapshot = await resolution.snapshot(other.id);
      await database.transaction((tx) =>
        tx.query(
          "UPDATE omniroute.conversation_turn_nodes SET last_seen_at=$1 WHERE conversation_id=ANY($2::text[])",
          ["0001-01-01T00:00:00Z", [first.id, other.id]]
        )
      );
      const entered = Promise.withResolvers<void>(),
        release = Promise.withResolvers<void>();
      const held = database.transaction(async (tx) => {
        await tx.query("SELECT id FROM omniroute.agentic_conversations WHERE id=$1 FOR UPDATE", [
          first.id,
        ]);
        entered.resolve();
        await release.promise;
      });
      await entered.promise;
      try {
        assert.ok(
          (await cleanup.deleteBefore(
            { table: "conversation_turn_nodes", column: "last_seen_at", cutoff: "iso" },
            "0002-01-01T00:00:00Z"
          )) >= 1
        );
        assert.equal((await history.getConversationTurnIndex(first.id)).nodeIds.size, 1);
        assert.equal((await history.getConversationTurnIndex(other.id)).nodeIds.size, 0);
        assert.equal(await resolution.commit(command(other.id, snapshot!.version)), false);
      } finally {
        release.resolve();
        await held;
      }
    });

    it("latest-log ties preserve one root per page with the most recently stored call", async () => {
      const { id, node } = await seed();
      const child = randomUUID();
      await history.insertConversationTurnNodes(id, null, [
        { id: child, parentId: node, role: "user", contentHash: child },
      ]);
      const logIds = [randomUUID(), randomUUID()];
      await database.transaction(async (tx) => {
        for (let i = 0; i < 2; i++)
          await tx.query(
            "INSERT INTO omniroute.call_logs(id,timestamp,session_tag) VALUES($1,$2,$3)",
            [logIds[i], "2099-01-01T00:00:00Z", id]
          );
      });
      const page = await history.listMultiTurnConversations({ limit: 200 });
      assert.equal(page.rows.filter((row) => row.id === id).length, 1);
      assert.equal(page.rows.find((row) => row.id === id)!.lastCallLogId, logIds[1]);
      assert.equal((await history.getMultiTurnConversationById(id))!.lastCallLogId, logIds[1]);
      assert.equal(page.total, 1);
      assert.equal(
        (await history.listMultiTurnConversations({ limit: 1, offset: 1 })).rows.length,
        0
      );
    });
  }
);
