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

  it("keeps explicit headers authoritative over divergent histories", async () => {
    const header = randomUUID();
    const results = await Promise.all([
      resolve("one", ["a"], header),
      resolve("two", ["b"], header),
    ]);
    assert.ok(results.every((result) => result.conversationId === header));
    await assertTranscript(header, [], 2);
  });
});
