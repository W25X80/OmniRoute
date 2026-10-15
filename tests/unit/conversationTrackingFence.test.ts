import assert from "node:assert/strict";
import test from "node:test";
import {
  withConversationFingerprint,
  ConversationTrackingUnavailableError,
} from "../../open-sse/services/conversationTrackingFence.ts";

function barrier() {
  return Promise.withResolvers<void>();
}
const options = () => ({ deadline: Date.now() + 10000 });

test("cancelled middle waiter cannot release the live predecessor", async () => {
  const identity = {},
    entered = barrier(),
    release = barrier();
  const order: string[] = [];
  const a = withConversationFingerprint(
    identity,
    "f",
    async () => {
      order.push("a");
      entered.resolve();
      await release.promise;
    },
    options()
  );
  await entered.promise;
  const controller = new AbortController();
  const b = withConversationFingerprint(
    identity,
    "f",
    async () => {
      order.push("b");
    },
    {
      ...options(),
      signal: controller.signal,
    }
  );
  const rejected = assert.rejects(
    b,
    (error: unknown) =>
      error instanceof ConversationTrackingUnavailableError && error.reason === "cancelled"
  );
  const c = withConversationFingerprint(
    identity,
    "f",
    async () => {
      order.push("c");
    },
    options()
  );
  controller.abort();
  await rejected;
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.deepEqual(order, ["a"]);
  release.resolve();
  await Promise.all([a, c]);
  assert.deepEqual(order, ["a", "c"]);
});

test("a holder error releases exactly the next accepted waiter", async () => {
  const identity = {},
    gate = barrier(),
    entered = barrier();
  const expected = new Error("classification failed");
  const first = withConversationFingerprint(
    identity,
    "f",
    async () => {
      entered.resolve();
      await gate.promise;
      throw expected;
    },
    options()
  );
  const rejected = assert.rejects(first, (error) => error === expected);
  await entered.promise;
  const next = withConversationFingerprint(identity, "f", async () => "next", options());
  gate.resolve();
  await rejected;
  assert.equal(await next, "next");
});

test("fingerprint and database identities isolate independent work", async () => {
  const identity = {},
    gate = barrier(),
    entered = barrier();
  const held = withConversationFingerprint(
    identity,
    "f",
    async () => {
      entered.resolve();
      await gate.promise;
    },
    options()
  );
  await entered.promise;
  assert.equal(await withConversationFingerprint(identity, "other", async () => 1, options()), 1);
  assert.equal(await withConversationFingerprint({}, "f", async () => 2, options()), 2);
  gate.resolve();
  await held;
});

test("bounded admission refuses overflow and cancelled waiters return their capacity", async () => {
  const identity = {},
    gate = barrier(),
    entered = barrier();
  const held = withConversationFingerprint(
    identity,
    "f",
    async () => {
      entered.resolve();
      await gate.promise;
    },
    options()
  );
  await entered.promise;
  const controller = new AbortController();
  const waiters = Array.from({ length: 64 }, () =>
    withConversationFingerprint(identity, "f", async () => {}, {
      ...options(),
      signal: controller.signal,
    }).then(
      () => "granted",
      (error: ConversationTrackingUnavailableError) => error.reason
    )
  );
  await assert.rejects(
    withConversationFingerprint(identity, "f", async () => {}, options()),
    (error: unknown) =>
      error instanceof ConversationTrackingUnavailableError && error.reason === "capacity"
  );
  controller.abort();
  assert.ok((await Promise.all(waiters)).every((reason) => reason === "cancelled"));
  const next = withConversationFingerprint(identity, "f", async () => "accepted", options());
  gate.resolve();
  await held;
  assert.equal(await next, "accepted");
});

test("expired work is refused before executing user code", async () => {
  let calls = 0;
  await assert.rejects(
    withConversationFingerprint(
      {},
      "f",
      async () => {
        calls++;
      },
      {
        deadline: Date.now() - 1,
      }
    ),
    (error: unknown) =>
      error instanceof ConversationTrackingUnavailableError && error.reason === "deadline"
  );
  assert.equal(calls, 0);
});
