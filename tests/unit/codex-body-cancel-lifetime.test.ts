import assert from "node:assert/strict";
import test from "node:test";
import {
  readCodexPeekChunk,
  buildCodexTimeoutSafePassthroughBody,
} from "../../open-sse/executors/codex/bodyTimeout.ts";

async function bounded<T>(work: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Cancellation blocked the read deadline")), 100);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

for (const mode of ["peek", "passthrough"] as const) {
  test(`Codex ${mode} timeout does not wait for upstream cancellation acknowledgement`, async () => {
    const cancel = Promise.withResolvers<void>();
    let cancellations = 0;
    const source = new ReadableStream<Uint8Array>({
      cancel() {
        cancellations++;
        return cancel.promise;
      },
    });
    const reader = source.getReader();
    const work =
      mode === "peek"
        ? readCodexPeekChunk(reader, 5)
        : new Response(buildCodexTimeoutSafePassthroughBody([], reader, 5)).text();
    try {
      if (mode === "peek") {
        assert.deepEqual(await bounded(work), { done: true, timedOut: true });
      } else {
        await assert.rejects(bounded(work), { name: "BodyTimeoutError" });
      }
      assert.equal(cancellations, 1);
    } finally {
      cancel.resolve();
      await work.catch(() => undefined);
      reader.releaseLock();
    }
  });
}

test("downstream cancellation wins over an already-read chunk awaiting handoff", async (t) => {
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const acknowledgement = Promise.withResolvers<void>();
  let cancellations = 0;
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new TextEncoder().encode("late chunk"));
    },
    cancel() {
      cancellations++;
      return acknowledgement.promise;
    },
  });
  const reader = source.getReader();
  const read = reader.read.bind(reader);
  t.mock.method(reader, "read", async () => {
    const chunk = await read();
    entered.resolve();
    await release.promise;
    return chunk;
  });
  const unlock = t.mock.method(reader, "releaseLock");
  const body = buildCodexTimeoutSafePassthroughBody([], reader, 1000).getReader();
  const pending = body.read();
  try {
    await bounded(entered.promise);
    await bounded(body.cancel("client disconnected"));
    assert.deepEqual(await pending, { done: true, value: undefined });
    release.resolve();
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(cancellations, 1);
    assert.equal(source.locked, false);
    assert.equal(unlock.mock.callCount(), 1);
  } finally {
    release.resolve();
    acknowledgement.resolve();
    await pending.catch(() => undefined);
    body.releaseLock();
  }
});
