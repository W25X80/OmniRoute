import assert from "node:assert/strict";
import test from "node:test";
import { setImmediate } from "node:timers/promises";

import { FORMATS } from "../../open-sse/translator/formats.ts";
import { createStreamController, pipeWithDisconnect } from "../../open-sse/utils/streamHandler.ts";
import { getUpstreamTimeoutConfig } from "../../src/shared/utils/runtimeTimeouts.ts";

const encoder = new TextEncoder();
const completed =
  'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}}\n\n';
const configuredTimeouts = getUpstreamTimeoutConfig();
const watchdogLifetime =
  Math.max(configuredTimeouts.streamIdleTimeoutMs, configuredTimeouts.streamActiveTimeoutMs) + 1;

for (const [format, terminal] of [
  [FORMATS.OPENAI_RESPONSES, completed],
  [
    FORMATS.OPENAI,
    'data: {"choices":[{"delta":{"content":"answer"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n',
  ],
] as const) {
  test(`terminal ${format} request-signal disconnect releases the upstream and watchdogs`, async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    let upstreamCancelled = 0;
    let disconnects = 0;
    let providerErrors = 0;
    const client = new AbortController();
    const source = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(terminal));
      },
      cancel() {
        upstreamCancelled++;
      },
    });
    const pipeline = createStreamController({
      clientResponseFormat: format,
      clientAbortSignal: client.signal,
      onDisconnect() {
        disconnects++;
      },
      onError() {
        providerErrors++;
      },
    });
    const errors = t.mock.method(pipeline, "handleError");
    const stream = pipeWithDisconnect(new Response(source), new TransformStream(), pipeline, {
      highWaterMark: 0,
    });
    const reader = stream.getReader();
    try {
      assert.equal(new TextDecoder().decode((await reader.read()).value), terminal);
      client.abort("request_signal_aborted");
      assert.equal(pipeline.isConnected(), false);
      assert.equal(pipeline.signal.aborted, false);
      assert.equal((await reader.read()).done, true);
      await setImmediate();
      assert.equal(upstreamCancelled, 1, "the producer must be cancelled before upstream EOF");
      assert.equal(disconnects, 0);
      assert.equal(providerErrors, 0);
      t.mock.timers.tick(watchdogLifetime);
      await setImmediate();
      assert.equal(
        errors.mock.callCount(),
        0,
        "completed disconnect must not leave live watchdogs"
      );
      assert.equal(pipeline.signal.aborted, false);
      assert.equal(upstreamCancelled, 1);
    } finally {
      await reader.cancel("test_cleanup");
      reader.releaseLock();
      pipeline.abort();
      t.mock.timers.reset();
    }
  });
}

test("normal upstream EOF completes without cancellation or a stale watchdog", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let upstreamCancelled = 0;
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode(completed));
      controller.close();
    },
    cancel() {
      upstreamCancelled++;
    },
  });
  const pipeline = createStreamController({ clientResponseFormat: FORMATS.OPENAI_RESPONSES });
  const errors = t.mock.method(pipeline, "handleError");
  const reader = pipeWithDisconnect(new Response(source), new TransformStream(), pipeline, {
    highWaterMark: 0,
  }).getReader();
  try {
    assert.equal(new TextDecoder().decode((await reader.read()).value), completed);
    assert.equal((await reader.read()).done, true);
    await setImmediate();
    assert.equal(upstreamCancelled, 0);
    assert.equal(pipeline.signal.aborted, false);
    t.mock.timers.tick(watchdogLifetime);
    await setImmediate();
    assert.equal(errors.mock.callCount(), 0);
  } finally {
    await reader.cancel("test_cleanup");
    reader.releaseLock();
    pipeline.abort();
    t.mock.timers.reset();
  }
});

test("pre-terminal request-signal disconnect still aborts and releases the upstream", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let upstreamCancelled = 0;
  let disconnects = 0;
  let providerErrors = 0;
  const client = new AbortController();
  const created = 'event: response.created\ndata: {"type":"response.created"}\n\n';
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode(created));
    },
    cancel() {
      upstreamCancelled++;
    },
  });
  const pipeline = createStreamController({
    clientResponseFormat: FORMATS.OPENAI_RESPONSES,
    clientAbortSignal: client.signal,
    onDisconnect() {
      disconnects++;
    },
    onError() {
      providerErrors++;
    },
  });
  const errors = t.mock.method(pipeline, "handleError");
  const reader = pipeWithDisconnect(new Response(source), new TransformStream(), pipeline, {
    highWaterMark: 0,
  }).getReader();
  try {
    assert.equal(new TextDecoder().decode((await reader.read()).value), created);
    client.abort("request_signal_aborted");
    assert.equal(pipeline.signal.aborted, true);
    assert.equal((await reader.read()).done, true);
    await setImmediate();
    assert.equal(upstreamCancelled, 1);
    assert.equal(disconnects, 1);
    assert.equal(providerErrors, 0);
    t.mock.timers.tick(watchdogLifetime);
    await setImmediate();
    assert.equal(errors.mock.callCount(), 0);
  } finally {
    await reader.cancel("test_cleanup");
    reader.releaseLock();
    pipeline.abort();
    t.mock.timers.reset();
  }
});
