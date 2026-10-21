import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import type {
  PostgresDatabase,
  PostgresResult,
  PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";
import { acquirePostgresEntityLock } from "../../../src/lib/db/postgres/locks.ts";
import { createPostgresSessionRepository } from "../../../src/lib/db/postgres/sessionRepository.ts";

function fixture() {
  const queries: Array<{ text: string; values?: unknown[] }> = [];
  const locks: Array<{ owner: string; mode: string }> = [];
  const tx: PostgresTransaction = {
    noteAdvisoryLock(owner, mode) {
      locks.push({ owner, mode });
    },
    async query<Row>(text: string, values?: unknown[]): Promise<PostgresResult<Row>> {
      queries.push({ text, values });
      const rows = text.startsWith("SELECT value FROM")
        ? [
            {
              value: JSON.stringify({
                connectionId: "connection-a",
                createdAt: "1970-01-01T00:00:01.000Z",
                lastUsedAt: "1970-01-01T00:00:01.000Z",
                expiresAt: "1970-01-01T00:00:03.000Z",
              }),
            },
          ]
        : text.startsWith("SELECT to_char")
          ? [{ now: "1970-01-01T00:00:02.000Z" }]
          : [];
      return { command: text.split(" ")[0], rowCount: rows.length, rows: rows as Row[] };
    },
  };
  const database = {
    transaction<T>(body: (transaction: PostgresTransaction) => Promise<T>) {
      return body(tx);
    },
  } as PostgresDatabase;
  return { repository: createPostgresSessionRepository(database), queries, locks, tx };
}

test("every affinity operation fences the same hashed pin behind a shared session gate", async () => {
  const session = "private-session-sentinel";
  const provider = "openai";
  const expectedKey = `${provider}:${createHash("sha256").update(`${provider}:${session}`).digest("hex")}`;
  const operations = [
    (repository: ReturnType<typeof createPostgresSessionRepository>) =>
      repository.getSessionAccountAffinity(session, provider, 1000, 2000),
    (repository: ReturnType<typeof createPostgresSessionRepository>) =>
      repository.upsertSessionAccountAffinity(session, provider, "connection-b", 2000, 1000),
    (repository: ReturnType<typeof createPostgresSessionRepository>) =>
      repository.touchSessionAccountAffinity(session, provider, 2000, 1000),
    (repository: ReturnType<typeof createPostgresSessionRepository>) =>
      repository.deleteSessionAccountAffinity(session, provider),
    (repository: ReturnType<typeof createPostgresSessionRepository>) =>
      repository.evictSessionAccountAffinityForConnection(session, provider, "connection-a"),
  ];
  for (const operation of operations) {
    const f = fixture();
    await operation(f.repository);
    assert.deepEqual(f.locks, [
      { owner: "sessions", mode: "shared" },
      { owner: "sessionAffinity", mode: "exclusive" },
    ]);
    assert.match(f.queries[0].text, /pg_advisory_xact_lock_shared/);
    assert.deepEqual(f.queries[0].values, [13]);
    assert.deepEqual(f.queries[1].values, [18768, JSON.stringify([expectedKey])]);
    assert.doesNotMatch(JSON.stringify(f.locks), /private-session-sentinel|connection-a/);
    assert.doesNotMatch(JSON.stringify(f.queries.slice(0, 2)), /private-session-sentinel/);
  }
});

test("different session/provider pins retain distinct entity identities", async () => {
  const identities: unknown[] = [];
  for (const [session, provider] of [
    ["session-a", "openai"],
    ["session-b", "openai"],
    ["session-a", "anthropic"],
  ]) {
    const f = fixture();
    await f.repository.deleteSessionAccountAffinity(session, provider);
    identities.push(f.queries[1]?.values?.[1]);
  }
  assert.equal(new Set(identities).size, 3);
});

test("affinity cleanup remains exclusive and history appends share the session gate", async () => {
  const cleanup = fixture();
  await cleanup.repository.cleanupStaleSessionAccountAffinities(undefined, 2000);
  assert.deepEqual(cleanup.locks, [{ owner: "sessions", mode: "exclusive" }]);
  assert.deepEqual(cleanup.queries[0].values, [13]);

  const history = fixture();
  await history.repository.recordSessionModelUsage("session", "combo", "model", "openai");
  assert.deepEqual(history.locks, [{ owner: "sessions", mode: "shared" }]);
  assert.deepEqual(history.queries[0].values, [13]);
});

test("session affinity does not renumber existing entity-lock namespaces", async () => {
  const f = fixture();
  await acquirePostgresEntityLock(f.tx, "quotaCounter", ["key", "dimension"]);
  await acquirePostgresEntityLock(f.tx, "providerQuota", ["connection", "model"]);
  assert.deepEqual(
    f.queries.map((query) => query.values?.[0]),
    [18766, 18767]
  );
});
