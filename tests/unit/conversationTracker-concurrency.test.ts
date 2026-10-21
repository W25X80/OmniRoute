import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setImmediate } from "node:timers/promises";
import test from "node:test";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database";

const directory = mkdtempSync(join(tmpdir(), "omniroute-conversation-concurrency-"));
process.env.DATA_DIR = directory;
process.env.API_KEY_SECRET = "conversation-concurrency-test-secret";
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
delete process.env.OMNIROUTE_DISABLE_CONVERSATION_TRACKING;

const { resolveConversationId, hashTurnContent } =
  await import("../../open-sse/services/conversationTracker.ts");
const { getConversationTurnPage, getMultiTurnConversationById } =
  await import("../../src/lib/db/agenticConversations.ts");
const { resetDbInstance } = await import("../../src/lib/db/core.ts");
const { withPostgresControlPlane } = await import("../../src/lib/db/controlPlaneContext.ts");
const { PostgresError } = await import("../../src/lib/db/postgres/database.ts");
const { withConversationFingerprint } =
  await import("../../open-sse/services/conversationTrackingFence.ts");
const { computeFingerprintHash } = await import("../../open-sse/services/conversationTracker.ts");

test("a cancelled fingerprint waiter never materializes canonical history", async () => {
  const identity = {};
  const owner = "waiting-preparation";
  const fingerprint = computeFingerprintHash({
    apiKeyId: owner,
    model: "test-model",
    toolNames: [],
  });
  const gate = Promise.withResolvers<void>();
  const held = withConversationFingerprint(identity, fingerprint, () => gate.promise, {
    deadline: Date.now() + 10000,
  });
  const controller = new AbortController();
  let historyReads = 0;
  const body = {
    get messages() {
      historyReads++;
      return [{ role: "user", content: "large history" }];
    },
  };
  const binding = {
    databaseIdentity: identity,
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
  const waiting = withPostgresControlPlane(binding, () =>
    resolveConversationId({ ...input(owner, []), body, signal: controller.signal })
  );
  const rejected = assert.rejects(waiting, /cancelled/);
  try {
    await setImmediate();
    assert.equal(historyReads, 0);
  } finally {
    controller.abort();
    gate.resolve();
    await Promise.all([held, rejected]);
  }
});

test.after(() => {
  resetDbInstance();
  rmSync(directory, { recursive: true, force: true });
});

function input(owner: string, texts: string[], header: string | null = null) {
  return {
    body: { messages: texts.map((content) => ({ role: "user", content })) },
    model: "test-model",
    apiKeyId: owner,
    clientSessionIdHeader: header,
    correlationId: owner + texts.join("/"),
  };
}

function transcript(id: string) {
  const nodes = getConversationTurnPage(id, { limit: 500 }).nodes;
  const parents = nodes.map((node) => node.parentId).filter((parent) => parent !== null);
  assert.equal(new Set(parents).size, parents.length, "implicit chains cannot have two children");
  return nodes.map((node) => node.contentHash);
}

function hashes(texts: string[]) {
  return texts.map((text) =>
    hashTurnContent({ role: "user", text, blockKind: "text", toolName: null })
  );
}

test("concurrent divergent implicit tails become independent straight transcripts", async () => {
  const seed = await resolveConversationId(input("divergent", ["a", "b"]));
  const [left, right] = await Promise.all([
    resolveConversationId(input("divergent", ["a", "b", "c"])),
    resolveConversationId(input("divergent", ["a", "b", "c-prime"])),
  ]);
  assert.notEqual(left.conversationId, right.conversationId);
  assert.ok([left.conversationId, right.conversationId].includes(seed.conversationId));
  assert.deepEqual(transcript(left.conversationId!), hashes(["a", "b", "c"]));
  assert.deepEqual(transcript(right.conversationId!), hashes(["a", "b", "c-prime"]));
});

test("concurrent exact retries reuse one root and count every accepted request", async () => {
  const seed = await resolveConversationId(input("retry", ["a", "b"]));
  const requests = Array.from({ length: 6 }, () =>
    resolveConversationId(input("retry", ["a", "b"]))
  );
  const results = await Promise.all(requests);
  assert.ok(results.every((result) => result.conversationId === seed.conversationId));
  assert.equal(getMultiTurnConversationById(seed.conversationId!)!.turnCount, 7);
  assert.deepEqual(transcript(seed.conversationId!), hashes(["a", "b"]));
});

test("concurrent first requests create one implicit root for identical history", async () => {
  const results = await Promise.all([
    resolveConversationId(input("first", ["a", "b"])),
    resolveConversationId(input("first", ["a", "b"])),
  ]);
  assert.equal(results[0].conversationId, results[1].conversationId);
  assert.equal(results.filter((result) => result.isNewConversation).length, 1);
  assert.equal(getMultiTurnConversationById(results[0].conversationId!)!.turnCount, 2);
});

test("compacted reconnect and its concurrent full retry keep the recorded prefix", async () => {
  const seed = await resolveConversationId(input("compacted", ["a", "b", "c"]));
  const results = await Promise.all([
    resolveConversationId(input("compacted", ["summary", "b", "c", "d"])),
    resolveConversationId(input("compacted", ["a", "b", "c", "d"])),
  ]);
  assert.ok(results.every((result) => result.conversationId === seed.conversationId));
  assert.deepEqual(transcript(seed.conversationId!), hashes(["a", "b", "c", "d"]));
  assert.equal(getMultiTurnConversationById(seed.conversationId!)!.turnCount, 3);
});

test("explicit header remains authoritative for divergent concurrent inputs", async () => {
  const results = await Promise.all([
    resolveConversationId(input("header-one", ["a"], "explicit-session")),
    resolveConversationId(input("header-two", ["unrelated"], "explicit-session")),
  ]);
  assert.ok(results.every((result) => result.conversationId === "explicit-session"));
  assert.equal(getMultiTurnConversationById("explicit-session")!.turnCount, 2);
});

test("native fence shares module copies, isolates authority/fingerprint and releases after rejection", async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let entered = 0;
  const expected = new PostgresError("commit_unknown");
  // A transaction-entry barrier exercises the real resolver and native facade.
  // It models no SQL/COMMIT semantics and provides no PostgreSQL-engine evidence.
  const database = {
    async transaction() {
      entered++;
      await gate;
      throw expected;
    },
  } as unknown as PostgresDatabase;
  const binding = (identity: object) =>
    ({
      database,
      databaseIdentity: identity,
      assertReady() {},
    }) as unknown as PostgresControlPlaneBinding;
  const first = binding({});
  const second = binding({});
  const copy = (await import(
    new URL("../../open-sse/services/conversationTracker.ts?concurrency-copy", import.meta.url).href
  )) as { resolveConversationId: typeof resolveConversationId };
  const start = (
    scope: PostgresControlPlaneBinding,
    owner: string,
    resolve = resolveConversationId
  ) =>
    withPostgresControlPlane(scope, () => resolve(input(owner, ["a"]))).then(
      () => null,
      (error: unknown) => error
    );
  const jobs = [
    start(first, "same"),
    start(first, "same"),
    start(first, "same", copy.resolveConversationId),
    start(first, "other"),
    start(second, "same"),
  ];
  try {
    await setImmediate();
    assert.equal(entered, 3, "only the same authority and fingerprint must wait");
  } finally {
    release();
    assert.deepEqual(await Promise.all(jobs), [expected, expected, expected, expected, expected]);
  }
  assert.equal(entered, 5, "a rejected predecessor must release its queued successors");
  assert.equal(await start(first, "same"), expected);
  assert.equal(entered, 6, "a completed queue must not retain its failed promise");
});
