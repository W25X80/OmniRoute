import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, describe, it } from "node:test";
import {
  resolveConversationId,
  hashTurnContent,
} from "../../open-sse/services/conversationTracker.ts";
import {
  getConversationTurnPageAsync,
  getMultiTurnConversationByIdAsync,
} from "../../src/lib/db/agenticConversations.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  CONVERSATION_INDEX_MAX_ROWS,
  readConversationIndex,
} from "../../src/lib/db/postgres/conversationHistory.ts";
import { addConversationNodeCount } from "../../src/lib/db/postgres/conversationNodeCounts.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

describe("native implicit conversation concurrency", { skip: !postgresTestsEnabled }, () => {
  let database: PostgresDatabase;
  let binding: PostgresControlPlaneBinding;
  const owner = randomUUID();

  before(async () => {
    database = await createTestPostgresDatabase({ maxConnections: 8 });
    await migratePostgresSchema(database);
    // The resolver only uses the selected database and its stable identity.
    binding = {
      database,
      databaseIdentity: {},
      assertReady() {},
    } as unknown as PostgresControlPlaneBinding;
    await database.transaction((tx) =>
      tx.query("INSERT INTO omniroute.api_keys(id,name,key,created_at) VALUES($1,$1,$2,$3)", [
        owner,
        randomUUID(),
        new Date().toISOString(),
      ])
    );
  });

  after(async () => {
    await database?.close();
  });

  function resolve(model: string, texts: string[], header: string | null = null) {
    return withPostgresControlPlane(binding, () =>
      resolveConversationId({
        body: { messages: texts.map((content) => ({ role: "user", content })) },
        model,
        apiKeyId: owner,
        clientSessionIdHeader: header,
        correlationId: randomUUID(),
      })
    );
  }

  async function assertTranscript(id: string, texts: string[], count: number) {
    const page = await withPostgresControlPlane(binding, () =>
      getConversationTurnPageAsync(id, { limit: 500 })
    );
    const parents = page.nodes.map((node) => node.parentId).filter((parent) => parent !== null);
    assert.equal(new Set(parents).size, parents.length, "implicit chains cannot fork");
    assert.deepEqual(
      page.nodes.map((node) => node.contentHash),
      texts.map((text) =>
        hashTurnContent({ role: "user", text, blockKind: "text", toolName: null })
      )
    );
    const row = await withPostgresControlPlane(binding, () =>
      getMultiTurnConversationByIdAsync(id)
    );
    assert.equal(row?.turnCount, count);
  }

  it("reclassifies a concurrent divergent tail from committed nodes", async () => {
    const seed = await resolve("divergent", ["a", "b"]);
    const [left, right] = await Promise.all([
      resolve("divergent", ["a", "b", "c"]),
      resolve("divergent", ["a", "b", "c-prime"]),
    ]);
    assert.notEqual(left.conversationId, right.conversationId);
    assert.ok([left.conversationId, right.conversationId].includes(seed.conversationId));
    await assertTranscript(left.conversationId!, ["a", "b", "c"], left.isNewConversation ? 1 : 2);
    await assertTranscript(
      right.conversationId!,
      ["a", "b", "c-prime"],
      right.isNewConversation ? 1 : 2
    );
    const retry = await resolve("divergent", ["a", "b", "c-prime"]);
    assert.equal(retry.conversationId, right.conversationId);
  });

  it("counts concurrent exact retries without duplicate nodes", async () => {
    const seed = await resolve("retry", ["a", "b"]);
    const results = await Promise.all(
      Array.from({ length: 6 }, () => resolve("retry", ["a", "b"]))
    );
    assert.ok(results.every((result) => result.conversationId === seed.conversationId));
    await assertTranscript(seed.conversationId!, ["a", "b"], 7);
  });

  it("reuses a concurrently created implicit root", async () => {
    const results = await Promise.all([resolve("first", ["a", "b"]), resolve("first", ["a", "b"])]);
    assert.equal(results[0].conversationId, results[1].conversationId);
    assert.equal(results.filter((result) => result.isNewConversation).length, 1);
    await assertTranscript(results[0].conversationId!, ["a", "b"], 2);
  });

  it("retains the old prefix across a compacted reconnect and concurrent full retry", async () => {
    const seed = await resolve("compacted", ["a", "b", "c"]);
    const results = await Promise.all([
      resolve("compacted", ["summary", "b", "c", "d"]),
      resolve("compacted", ["a", "b", "c", "d"]),
    ]);
    assert.ok(results.every((result) => result.conversationId === seed.conversationId));
    await assertTranscript(seed.conversationId!, ["a", "b", "c", "d"], 3);
  });

  it("prefers the longest verified run when repeated content ends later", async () => {
    const original = ["start", "a1", "ok", "poll", "ok", "poll"];
    const continued = [...original, "ok", "brand new"];
    const seed = await resolve("repeated-content", original);
    const result = await resolve("repeated-content", continued);
    assert.equal(result.conversationId, seed.conversationId);
    assert.equal(result.isNewConversation, false);
    await assertTranscript(seed.conversationId!, continued, 2);
  });

  it("keeps explicit headers authoritative over divergent histories", async () => {
    const header = randomUUID();
    const results = await Promise.all([
      resolve("one", ["a"], header),
      resolve("two", ["b"], header),
    ]);
    assert.ok(results.every((result) => result.conversationId === header));
    await assertTranscript(header, [], 2);
  });

  it("refuses an oversized index without truncating or mutating the stored history", async () => {
    const seed = await resolve("oversized-index", ["seed"]);
    const id = seed.conversationId!;
    await database.transaction(async (tx) => {
      await tx.query(
        `INSERT INTO omniroute.conversation_turn_nodes
        (id,conversation_id,parent_id,role,content_hash,first_seen_at,last_seen_at)
        SELECT $1||'-'||n,$1,NULL,'user','hash-'||n,$2,$2
        FROM generate_series(1,$3::integer) AS n`,
        [id, new Date().toISOString(), CONVERSATION_INDEX_MAX_ROWS]
      );
      await addConversationNodeCount(tx, id, CONVERSATION_INDEX_MAX_ROWS);
    });
    await assert.rejects(
      database.transaction((tx) => readConversationIndex(tx, id)),
      /Conversation history exceeds tracking capacity/
    );
    const { rows } = await database.transaction((tx) =>
      tx.query<{ count: string; derived: string }>(
        `SELECT (SELECT count(*)::text FROM omniroute.conversation_turn_nodes WHERE conversation_id=$1) AS count,
       (SELECT node_count::text FROM omniroute.conversation_node_counts WHERE conversation_id=$1) AS derived`,
        [id]
      )
    );
    assert.equal(rows[0].count, String(CONVERSATION_INDEX_MAX_ROWS + 1));
    assert.equal(rows[0].derived, rows[0].count);
  });
});
