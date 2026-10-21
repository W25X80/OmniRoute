import assert from "node:assert/strict";
import { setImmediate } from "node:timers/promises";
import test from "node:test";
import {
  findReconnectMatch,
  findReconnectMatchCooperatively,
  hashTurnContent,
} from "../../open-sse/services/conversationTracker.ts";

function workload() {
  const turns = ["repeated", "different"].map((text) => ({
    role: "user" as const,
    text,
    blockKind: "text" as const,
    toolName: null,
  }));
  const index = {
    nodeIds: new Set<string>(),
    byContentHash: new Map([
      [hashTurnContent(turns[0]), Array.from({ length: 2000 }, (_, n) => `anchor-${n}`)],
    ]),
    parentsWithChildren: new Set<string>(),
  };
  return { turns, index };
}

test("cooperative reconnect preserves exact election and step budget while dispatching other work", async () => {
  const { turns, index } = workload();
  const expected = findReconnectMatch(turns, index, { maxSteps: 1500 });
  let anotherRequestRan = false;
  const other = setImmediate().then(() => {
    anotherRequestRan = true;
  });
  const actual = await findReconnectMatchCooperatively(
    turns,
    index,
    { maxSteps: 1500 },
    { deadline: Date.now() + 10000 }
  );
  assert.deepEqual(actual, expected);
  assert.equal(anotherRequestRan, true, "a long history walk cannot monopolize the event loop");
  await other;
});

test("a cancelled cooperative walk cannot return a partial election", async () => {
  const { turns, index } = workload();
  const controller = new AbortController();
  const cancelled = setImmediate().then(() => controller.abort());
  await assert.rejects(
    findReconnectMatchCooperatively(
      turns,
      index,
      { maxSteps: 1500 },
      { deadline: Date.now() + 10000, signal: controller.signal }
    ),
    /cancelled/
  );
  await cancelled;
});

test("an anchor-free scan yields without charging or changing the proof budget", async () => {
  const { turns, index } = workload();
  const absent = Array.from({ length: 4096 }, () => turns[1]);
  let yielded = false;
  const other = setImmediate().then(() => {
    yielded = true;
  });
  const result = await findReconnectMatchCooperatively(
    absent,
    index,
    { maxSteps: 1500 },
    { deadline: Date.now() + 10000 }
  );
  assert.deepEqual(result, { match: null, stepsUsed: 0 });
  assert.equal(yielded, true);
  await other;
});
