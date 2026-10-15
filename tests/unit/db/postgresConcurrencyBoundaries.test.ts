import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createPostgresCompressionRepository } from "../../../src/lib/db/postgres/compressionRepository.ts";
import {
  CONVERSATION_INSERT_CHUNK_BYTES,
  CONVERSATION_INSERT_CHUNK_ROWS,
  createPostgresAgenticConversationsRepository,
} from "../../../src/lib/db/postgres/agenticConversationsRepository.ts";
import { acquirePostgresLock, POSTGRES_LOCK_OWNERS } from "../../../src/lib/db/postgres/locks.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";

function fixture(query: (sql: string, values: unknown[]) => unknown[]) {
  let active = false;
  const calls: { sql: string; values: unknown[] }[] = [];
  const database = {
    async transaction<T>(body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
      assert.equal(active, false);
      active = true;
      try {
        return await body({
          async query<Row>(sql: string, values: unknown[] = []) {
            calls.push({ sql, values });
            return { rows: query(sql, values) as Row[], rowCount: 0, command: "SELECT" };
          },
        });
      } finally {
        active = false;
      }
    },
  } as unknown as PostgresDatabase;
  return { database, calls, active: () => active };
}

describe("PostgreSQL concurrency boundaries", () => {
  it("reads compression config/default in one snapshot and projects after release", async () => {
    const f = fixture((sql) => {
      assert.match(sql, /SELECT/);
      assert.match(sql, /omniroute.key_value/);
      assert.match(sql, /omniroute.compression_combos/);
      assert.doesNotMatch(sql, /pg_advisory|FOR UPDATE|UPDATE |INSERT /);
      return [
        {
          config_rows: [{ key: "enabled", value: "true" }],
          combo: {
            id: "custom",
            get pipeline() {
              assert.equal(f.active(), false, "pipeline parsing must not retain a SQL client");
              return '[{"engine":"lite"}]';
            },
          },
        },
      ];
    });
    const snapshot = await createPostgresCompressionRepository(f.database).readConfigSnapshot();
    assert.equal(f.calls.length, 1);
    assert.equal(snapshot.combo?.id, "custom");
    assert.deepEqual(snapshot.rows, [{ key: "enabled", value: "true" }]);
  });

  it("bounds SQL payloads, preserves duplicate input and projects indexes after release", async () => {
    const input = Array.from({ length: CONVERSATION_INSERT_CHUNK_ROWS * 2 + 1 }, (_, i) => ({
      id: `node-${i}`,
      parentId: i ? `node-${i - 1}` : null,
      role: "user",
      contentHash: `hash-${i}`,
    }));
    input[1] = { ...input[0], contentHash: "conflicting-retry" };
    const f = fixture((sql) => {
      if (sql.includes("AS incarnation,history_revision::text AS revision")) {
        assert.match(sql, /FOR UPDATE/);
        return [{ id: "conversation", incarnation: "1", revision: "0" }];
      }
      return sql.includes("SELECT id,parent_id")
        ? [
            {
              get id() {
                assert.equal(f.active(), false);
                return "node-0";
              },
              parent_id: null,
              content_hash: "hash-0",
            },
          ]
        : [];
    });
    const repo = createPostgresAgenticConversationsRepository(f.database);
    await repo.insertConversationTurnNodes("conversation", "correlation", input);
    const inserts = f.calls.filter(({ sql }) => sql.includes("INSERT INTO"));
    assert.equal(inserts.length, 3);
    for (const call of inserts) {
      assert.match(call.sql, /WITH ORDINALITY/);
      assert.match(call.sql, /ORDER BY ordinal ON CONFLICT\(id\) DO NOTHING/);
      assert.ok(Buffer.byteLength(String(call.values[3])) <= CONVERSATION_INSERT_CHUNK_BYTES);
    }
    assert.deepEqual(
      inserts.flatMap(({ values }) => JSON.parse(String(values[3]))),
      input.map((node) => [node.id, node.parentId, node.role, node.contentHash])
    );
    assert.ok((await repo.getConversationTurnIndex("conversation")).nodeIds.has("node-0"));
  });

  it("rejects a single oversized node before leasing a transaction", () => {
    const f = fixture(() => []);
    assert.throws(
      () =>
        createPostgresAgenticConversationsRepository(f.database).insertConversationTurnNodes(
          "id",
          null,
          [
            {
              id: "n",
              parentId: null,
              role: "user",
              contentHash: "x".repeat(CONVERSATION_INSERT_CHUNK_BYTES),
            },
          ]
        ),
      RangeError
    );
    assert.equal(f.calls.length, 0);
  });

  it("assigns distinct domains and preserves shared/exclusive key identity", async () => {
    const values = Object.values(POSTGRES_LOCK_OWNERS);
    assert.equal(new Set(values).size, values.length);
    const f = fixture(() => []);
    await f.database.transaction(async (tx) => {
      await acquirePostgresLock(tx, "skills", "shared");
      await acquirePostgresLock(tx, "skills");
    });
    assert.deepEqual(
      f.calls.map(({ values }) => values),
      [[22], [22]]
    );
    assert.match(f.calls[0].sql, /lock_shared/);
    assert.doesNotMatch(f.calls[1].sql, /lock_shared/);
  });
});
