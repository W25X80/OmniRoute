import assert from "node:assert/strict";
import test from "node:test";
import { setImmediate } from "node:timers/promises";

import { FORMATS } from "../../open-sse/translator/formats.ts";
import {
  createDisconnectAwareStream,
  createNoopAbortWritable,
  createStreamController,
} from "../../open-sse/utils/streamHandler.ts";
import { getUpstreamTimeoutConfig } from "../../src/shared/utils/runtimeTimeouts.ts";

const encoder = new TextEncoder();

function sse(type: string, fields: Record<string, unknown> = {}): string {
  return `event: ${type}\ndata: ${JSON.stringify({ type, ...fields })}\n\n`;
}

test("pending handoff read preserves the producer after outer cancel and a trailing chunk", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let upstream!: ReadableStreamDefaultController<Uint8Array>;
  let ended = false;
  let cancellations = 0;
  let flushes = 0;
  let resolveFlush!: () => void;
  const flushed = new Promise<void>((resolve) => {
    resolveFlush = resolve;
  });
  const handoff = encoder.encode(
    sse("response.function_call_arguments.done", {
      item_id: "fc_1",
      output_index: 0,
      arguments: "{}",
    }) +
      sse("response.output_item.done", {
        output_index: 0,
        item: {
          id: "fc_1",
          type: "function_call",
          call_id: "call_1",
          name: "lookup",
          arguments: "{}",
          status: "completed",
        },
      })
  );
  const provider = new ReadableStream<Uint8Array>({
    start(controller) {
      upstream = controller;
      controller.enqueue(handoff);
    },
    cancel() {
      ended = true;
      cancellations++;
    },
  });
  const transform = new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      controller.enqueue(chunk);
    },
    flush() {
      flushes++;
      resolveFlush();
    },
  });
  const pipeline = createStreamController({
    clientResponseFormat: FORMATS.OPENAI_RESPONSES,
    allowCompletedToolHandoffGrace: true,
    clientDisconnectGracePeriodMs: getUpstreamTimeoutConfig().streamDisconnectGracePeriodMs,
  });
  const completion = t.mock.method(pipeline, "handleComplete");
  const reader = createDisconnectAwareStream(
    { readable: provider.pipeThrough(transform), writable: createNoopAbortWritable() },
    pipeline,
    { highWaterMark: 0 }
  ).getReader();
  try {
    assert.deepEqual((await reader.read()).value, handoff);
    assert.equal(pipeline.shouldDeferCompletedToolHandoff(), true);
    let settled = false;
    const pending = reader.read();
    void pending.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      }
    );
    await setImmediate();
    assert.equal(settled, false, "the second read must be waiting before outer cancellation");
    await reader.cancel("request_signal_aborted");
    assert.equal((await pending).done, true);
    assert.equal(pipeline.isConnected(), false);
    assert.equal(pipeline.signal.aborted, false);

    // This resolves the original pending inner read after the output has closed.
    // EOF remains withheld, so canceling a healthy handoff cannot look like a drain.
    upstream.enqueue(encoder.encode(sse("response.in_progress")));
    await setImmediate();
    assert.equal(cancellations, 0, "the closed output must not cancel a healthy handoff producer");
    assert.equal(flushes, 0, "normal transform completion must wait for upstream EOF");
    assert.equal(completion.mock.callCount(), 0, "the handoff drain must still own completion");
    assert.equal(pipeline.signal.aborted, false);

    upstream.enqueue(
      encoder.encode(sse("response.completed", { response: { status: "completed" } }))
    );
    ended = true;
    upstream.close();
    await flushed;
    await setImmediate();
    assert.equal(flushes, 1);
    assert.equal(completion.mock.callCount(), 1);
    assert.equal(cancellations, 0);
    assert.equal(pipeline.signal.aborted, false);
  } finally {
    if (!ended) {
      ended = true;
      upstream.close();
    }
    await reader.cancel("test_cleanup");
    await setImmediate();
    pipeline.abort();
    reader.releaseLock();
    t.mock.timers.reset();
  }
});
