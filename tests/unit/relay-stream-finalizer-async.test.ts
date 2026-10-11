import assert from "node:assert/strict";
import test from "node:test";
import { finalizeReadableStream } from "../../src/app/api/v1/relay/chat/completions/streamFinalizer.ts";
test("relay stream completion waits for asynchronous accounting exactly once", async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let calls = 0;
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new Uint8Array([1]));
      controller.close();
    },
  });
  const stream = finalizeReadableStream(source, async () => {
    calls++;
    await gate;
  });
  const reader = stream.getReader();
  assert.deepEqual(await reader.read(), { done: false, value: new Uint8Array([1]) });
  const done = reader.read();
  let settled = false;
  void done.then(() => {
    settled = true;
  });
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(calls, 1);
  assert.equal(settled, false);
  release();
  assert.deepEqual(await done, { done: true, value: undefined });
  assert.equal(calls, 1);
});

test("relay cancellation awaits asynchronous accounting and preserves reason", async () => {
  let cancelReason: unknown,
    finalReason: unknown,
    calls = 0;
  const source = new ReadableStream<Uint8Array>({
    cancel(reason) {
      cancelReason = reason;
    },
  });
  const stream = finalizeReadableStream(source, async (error) => {
    await Promise.resolve();
    calls++;
    finalReason = error;
  });
  await stream.cancel("client closed");
  assert.equal(cancelReason, "client closed");
  assert.equal(finalReason, "client closed");
  assert.equal(calls, 1);
});

test("failed accounting rejects the stream without repeating the callback", async () => {
  const failure = new Error("accounting unavailable");
  let calls = 0;
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.close();
    },
  });
  const stream = finalizeReadableStream(source, async () => {
    calls++;
    throw failure;
  });
  await assert.rejects(stream.getReader().read(), (error) => error === failure);
  assert.equal(calls, 1);
});
