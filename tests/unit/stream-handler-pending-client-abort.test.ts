import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import test from "node:test";
import { setImmediate } from "node:timers/promises";

import { FORMATS } from "../../open-sse/translator/formats.ts";
import { createStreamController, pipeWithDisconnect } from "../../open-sse/utils/streamHandler.ts";
import { getUpstreamTimeoutConfig } from "../../src/shared/utils/runtimeTimeouts.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const completed =
  'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}}\n\n';
const created = 'event: response.created\ndata: {"type":"response.created"}\n\n';
const timeouts = getUpstreamTimeoutConfig();
const watchdogLifetime = Math.max(timeouts.streamIdleTimeoutMs, timeouts.streamActiveTimeoutMs) + 1;

/** The producer stays open until the test explicitly releases EOF, including failed assertions. */
function heldProvider(first: string) {
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  let ended = false;
  let cancellations = 0;
  const source = new ReadableStream<Uint8Array>({
    start(value) {
      controller = value;
      controller.enqueue(encoder.encode(first));
    },
    cancel() {
      ended = true;
      cancellations++;
    },
  });
  return {
    source,
    get cancellations() {
      return cancellations;
    },
    send(text: string) {
      if (!ended) controller.enqueue(encoder.encode(text));
    },
    close() {
      if (!ended) {
        ended = true;
        controller.close();
      }
    },
    fail(error: unknown) {
      if (!ended) {
        ended = true;
        controller.error(error);
      }
    },
  };
}

for (const [format, terminal] of [
  [FORMATS.OPENAI_RESPONSES, completed],
  [
    FORMATS.OPENAI,
    'data: {"choices":[{"delta":{"content":"answer"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n',
  ],
] as const) {
  test(`pending terminal ${format} read releases the producer on request abort before EOF`, async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const client = new AbortController();
    const provider = heldProvider(terminal);
    let disconnects = 0;
    let providerErrors = 0;
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
    const reader = pipeWithDisconnect(
      new Response(provider.source),
      new TransformStream(),
      pipeline,
      {
        highWaterMark: 0,
      }
    ).getReader();
    try {
      assert.equal(decoder.decode((await reader.read()).value), terminal);
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
      assert.equal(settled, false, "the second read must already be waiting without provider EOF");
      client.abort("request_signal_aborted");
      assert.equal(pipeline.isConnected(), false);
      assert.equal(pipeline.signal.aborted, false);
      await setImmediate();
      assert.equal(provider.cancellations, 1, "a pending read must not retain the producer");
      assert.equal((await pending).done, true);
      assert.equal(disconnects, 0);
      assert.equal(providerErrors, 0);
      t.mock.timers.tick(watchdogLifetime);
      await setImmediate();
      assert.equal(errors.mock.callCount(), 0);
      assert.equal(pipeline.signal.aborted, false);
      assert.equal(provider.cancellations, 1);
    } finally {
      provider.close();
      pipeline.abort();
      await reader.cancel("test_cleanup");
      reader.releaseLock();
      t.mock.timers.reset();
    }
  });
}

test("pending pre-terminal client abort closes without a provider EOF error", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const client = new AbortController();
  const provider = heldProvider(created);
  let disconnects = 0;
  let providerErrors = 0;
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
  const reader = pipeWithDisconnect(
    new Response(provider.source),
    new TransformStream(),
    pipeline,
    {
      highWaterMark: 0,
    }
  ).getReader();
  try {
    assert.equal(decoder.decode((await reader.read()).value), created);
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
    assert.equal(settled, false);
    client.abort("request_signal_aborted");
    assert.equal(pipeline.signal.aborted, true);
    await setImmediate();
    assert.equal(provider.cancellations, 1);
    assert.equal((await pending).done, true);
    assert.equal(disconnects, 1);
    assert.equal(providerErrors, 0);
    t.mock.timers.tick(watchdogLifetime);
    await setImmediate();
    assert.equal(errors.mock.callCount(), 0);
  } finally {
    provider.close();
    pipeline.abort();
    await reader.cancel("test_cleanup");
    reader.releaseLock();
    t.mock.timers.reset();
  }
});

test("pending deadline abort remains a 504 provider error rather than client completion", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const client = new AbortController();
  const provider = heldProvider(created);
  let disconnects = 0;
  const statuses: number[] = [];
  const pipeline = createStreamController({
    clientResponseFormat: FORMATS.OPENAI_RESPONSES,
    clientAbortSignal: client.signal,
    onDisconnect() {
      disconnects++;
    },
    onError(event) {
      statuses.push(event.statusCode);
      return true;
    },
  });
  pipeline.signal.addEventListener("abort", () => provider.fail(pipeline.signal.reason), {
    once: true,
  });
  const errors = t.mock.method(pipeline, "handleError");
  const reader = pipeWithDisconnect(
    new Response(provider.source),
    new TransformStream(),
    pipeline,
    {
      highWaterMark: 0,
    }
  ).getReader();
  try {
    assert.equal(decoder.decode((await reader.read()).value), created);
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
    assert.equal(settled, false);
    const deadline = new Error("Model test deadline exceeded after 60000ms");
    deadline.name = "TimeoutError";
    client.abort(deadline);
    const chunk = await pending;
    assert.equal(chunk.done, false);
    const data = decoder
      .decode(chunk.value)
      .split("\n")
      .find((line) => line.startsWith("data:"));
    assert.ok(data);
    const failed = JSON.parse(data.slice(5)) as {
      type: unknown;
      response: { status: unknown; error: { code: unknown } };
    };
    assert.equal(failed.type, "response.failed");
    assert.equal(failed.response.status, "failed");
    assert.equal(failed.response.error.code, "server_error");
    assert.equal((await reader.read()).done, true);
    assert.deepEqual(statuses, [504]);
    assert.equal(disconnects, 0);
    assert.equal(pipeline.signal.aborted, true);
    assert.equal(provider.cancellations, 0);
    const errorCalls = errors.mock.callCount();
    t.mock.timers.tick(watchdogLifetime);
    await setImmediate();
    assert.equal(errors.mock.callCount(), errorCalls);
  } finally {
    provider.close();
    pipeline.abort();
    await reader.cancel("test_cleanup");
    reader.releaseLock();
    t.mock.timers.reset();
  }
});

test("pending completed-tool handoff still drains the trailing response and EOF", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const handoff =
    'event: response.function_call_arguments.done\ndata: {"type":"response.function_call_arguments.done","item_id":"fc_1","output_index":0,"arguments":"{}"}\n\n' +
    'event: response.output_item.done\ndata: {"type":"response.output_item.done","output_index":0,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}","status":"completed"}}\n\n';
  const client = new AbortController();
  const provider = heldProvider(handoff);
  let flushed = 0;
  const pipeline = createStreamController({
    clientResponseFormat: FORMATS.OPENAI_RESPONSES,
    clientAbortSignal: client.signal,
    allowCompletedToolHandoffGrace: true,
    clientDisconnectGracePeriodMs: timeouts.streamDisconnectGracePeriodMs,
  });
  const errors = t.mock.method(pipeline, "handleError");
  const reader = pipeWithDisconnect(
    new Response(provider.source),
    new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        controller.enqueue(chunk);
      },
      flush() {
        flushed++;
      },
    }),
    pipeline,
    { highWaterMark: 0 }
  ).getReader();
  try {
    assert.equal(decoder.decode((await reader.read()).value), handoff);
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
    assert.equal(settled, false);
    client.abort("request_signal_aborted");
    await setImmediate();
    assert.equal(provider.cancellations, 0);
    assert.equal(pipeline.signal.aborted, false);
    provider.send(completed);
    provider.close();
    assert.equal((await pending).done, true);
    await setImmediate();
    assert.equal(flushed, 1);
    assert.equal(provider.cancellations, 0);
    assert.equal(pipeline.signal.aborted, false);
    t.mock.timers.tick(watchdogLifetime);
    await setImmediate();
    assert.equal(errors.mock.callCount(), 0);
  } finally {
    provider.close();
    pipeline.abort();
    await reader.cancel("test_cleanup");
    reader.releaseLock();
    t.mock.timers.reset();
  }
});

test("an already-aborted client signal keeps the constructor decision ahead of reader cleanup", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const client = new AbortController();
  client.abort("request_signal_aborted");
  const provider = heldProvider(created);
  let disconnects = 0;
  const pipeline = createStreamController({
    clientResponseFormat: FORMATS.OPENAI_RESPONSES,
    clientAbortSignal: client.signal,
    onDisconnect() {
      disconnects++;
    },
  });
  const reader = pipeWithDisconnect(
    new Response(provider.source),
    new TransformStream(),
    pipeline,
    {
      highWaterMark: 0,
    }
  ).getReader();
  try {
    assert.equal((await reader.read()).done, true);
    await setImmediate();
    assert.equal(pipeline.signal.aborted, true);
    assert.equal(disconnects, 1);
    assert.equal(provider.cancellations, 1);
    assert.equal(getEventListeners(client.signal, "abort").length, 0);
  } finally {
    provider.close();
    pipeline.abort();
    await reader.cancel("test_cleanup");
    reader.releaseLock();
    t.mock.timers.reset();
  }
});

test("normal EOF removes the client listener and leaves completed classification stable", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const client = new AbortController();
  const provider = heldProvider(completed);
  const pipeline = createStreamController({
    clientResponseFormat: FORMATS.OPENAI_RESPONSES,
    clientAbortSignal: client.signal,
  });
  const errors = t.mock.method(pipeline, "handleError");
  const reader = pipeWithDisconnect(
    new Response(provider.source),
    new TransformStream(),
    pipeline,
    {
      highWaterMark: 0,
    }
  ).getReader();
  try {
    assert.equal(decoder.decode((await reader.read()).value), completed);
    assert.ok(getEventListeners(client.signal, "abort").length > 0);
    provider.close();
    assert.equal((await reader.read()).done, true);
    assert.equal(getEventListeners(client.signal, "abort").length, 0);
    client.abort("late_client_abort");
    assert.equal(pipeline.signal.aborted, false);
    assert.equal(provider.cancellations, 0);
    t.mock.timers.tick(watchdogLifetime);
    await setImmediate();
    assert.equal(errors.mock.callCount(), 0);
  } finally {
    provider.close();
    pipeline.abort();
    await reader.cancel("test_cleanup");
    reader.releaseLock();
    t.mock.timers.reset();
  }
});
