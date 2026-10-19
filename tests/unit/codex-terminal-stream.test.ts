import assert from "node:assert/strict";
import test from "node:test";
import { BaseExecutor, type ExecuteInput } from "../../open-sse/executors/base.ts";
import {
  CodexExecutor,
  __setCodexWebSocketTransportForTesting,
} from "../../open-sse/executors/codex.ts";
import { hasCodexSsePeekProgress } from "../../open-sse/executors/codex/ssePeekProgress.ts";

const encoder = new TextEncoder();
const input: ExecuteInput = {
  model: "gpt-5.5",
  body: { input: [{ role: "user", content: "fixture" }] },
  stream: true,
  credentials: { accessToken: "fixture-only" },
};

async function bounded<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Terminal stream did not finish")), 300);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function ongoingResponse(text: string, cancelAcknowledgement?: Promise<void>) {
  let controller: ReadableStreamDefaultController<Uint8Array>;
  let ended = false;
  let cancellations = 0;
  const body = new ReadableStream<Uint8Array>({
    start(value) {
      controller = value;
      controller.enqueue(encoder.encode(text));
    },
    async pull(value) {
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
      if (!ended) value.enqueue(encoder.encode(": ping\n\n"));
    },
    cancel() {
      ended = true;
      cancellations++;
      return cancelAcknowledgement;
    },
  });
  return {
    response: new Response(body, { headers: { "content-type": "text/event-stream" } }),
    get cancellations() {
      return cancellations;
    },
    close() {
      if (ended) return;
      ended = true;
      controller.close();
    },
  };
}

test("unfiltered Codex failover does not wait for cancellation acknowledgement", async (t) => {
  const key = "OMNIROUTE_CODEX_DROP_NONSTANDARD_EVENTS";
  const previous = process.env[key];
  process.env[key] = "false";
  t.after(() => {
    if (previous === undefined) delete process.env[key];
    else process.env[key] = previous;
  });
  const cancel = Promise.withResolvers<void>();
  const source = ongoingResponse(
    'event: error\ndata: {"error":{"message":"Selected model is at capacity"}}\n\n',
    cancel.promise
  );
  t.mock.method(BaseExecutor.prototype, "execute", async () => ({ response: source.response }));
  const execution = new CodexExecutor().execute(input);
  try {
    assert.equal((await bounded(execution)).response.status, 503);
    assert.equal(source.cancellations, 1);
  } finally {
    cancel.resolve();
    source.close();
    const result = await execution;
    await result.response.body?.cancel().catch(() => undefined);
  }
});

for (const type of ["response.completed", "response.failed", "response.incomplete"]) {
  test(`Codex HTTP finishes ${type} while the upstream keeps pinging`, async (t) => {
    const terminal = `event: ${type}\ndata: ${JSON.stringify({
      type,
      response: { status: type.slice(9), usage: { input_tokens: 17, output_tokens: 5 } },
    })}\n\n`;
    const source = ongoingResponse(terminal);
    t.mock.method(BaseExecutor.prototype, "execute", async () => ({
      response: source.response,
      url: "https://fixture.invalid/responses",
      headers: {},
      transformedBody: input.body,
    }));
    const execution = new CodexExecutor().execute(input);
    let reading: Promise<string> | undefined;
    try {
      const result = await bounded(execution);
      reading = result.response.text();
      assert.equal(await bounded(reading), terminal);
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(source.cancellations, 1, "terminal completion cancels the upstream once");
    } finally {
      source.close();
      const result = await execution;
      if (reading) await reading.catch(() => undefined);
      else await result.response.body?.cancel().catch(() => undefined);
    }
  });
}

test("Codex HTTP leaves lazy non-SSE bodies untouched", async (t) => {
  const response = new Response('{"output":[]}', {
    headers: { "content-type": "application/json" },
  });
  let bodyReads = 0;
  Object.defineProperty(response, "body", {
    get() {
      bodyReads++;
      throw new Error("A lazy non-SSE body must not be inspected");
    },
  });
  t.mock.method(BaseExecutor.prototype, "execute", async () => ({
    response,
    url: "https://fixture.invalid/responses",
    headers: {},
    transformedBody: input.body,
  }));
  const result = await new CodexExecutor().execute(input);
  assert.equal(result.response, response);
  assert.equal(bodyReads, 0);
  assert.equal(await response.text(), '{"output":[]}');
});

for (const newline of ["\r", "\r\n"]) {
  for (const filter of ["true", "false"]) {
    test(`Codex HTTP handles multiline ${JSON.stringify(newline)} frames with filter=${filter}`, async (t) => {
      const key = "OMNIROUTE_CODEX_DROP_NONSTANDARD_EVENTS";
      const previous = process.env[key];
      process.env[key] = filter;
      t.after(() => {
        if (previous === undefined) delete process.env[key];
        else process.env[key] = previous;
      });
      const frame = [
        "event: response.completed",
        'data: {"type":',
        'data: "response.completed","response":{"status":"completed","usage":{"input_tokens":17,"output_tokens":5}}}',
        "",
        "",
      ].join(newline);
      const source = ongoingResponse(frame);
      t.mock.method(BaseExecutor.prototype, "execute", async () => ({
        response: source.response,
        transformedBody: input.body,
      }));
      const execution = new CodexExecutor().execute(input);
      let reading: Promise<string> | undefined;
      try {
        const result = await bounded(execution);
        reading = result.response.text();
        assert.equal(await bounded(reading), frame);
      } finally {
        source.close();
        const result = await execution;
        if (reading) await reading.catch(() => undefined);
        else await result.response.body?.cancel().catch(() => undefined);
      }
    });
  }
}

test("a single CRLF does not dispatch a peek frame", () => {
  const line = 'data: {"type":"response.completed"}\r\n';
  assert.equal(hasCodexSsePeekProgress(line), false);
  assert.equal(hasCodexSsePeekProgress(line + "\r\n"), true);
});

test("Codex WebSocket incomplete completion closes once and retains usage", async (t) => {
  let closes = 0;
  const payload = {
    type: "response.incomplete",
    response: { status: "incomplete", usage: { input_tokens: 17, output_tokens: 5 } },
  };
  const socket: {
    send(data: string): void;
    close(): void;
    onmessage: ((event: { data: unknown }) => void) | null;
    onclose: ((event: { code?: number }) => void) | null;
    onerror: ((event: { message?: string }) => void) | null;
  } = {
    send() {
      queueMicrotask(() => socket.onmessage?.({ data: JSON.stringify(payload) }));
    },
    close() {
      closes++;
    },
    onmessage: null,
    onclose: null,
    onerror: null,
  };
  __setCodexWebSocketTransportForTesting(async () => socket);
  t.after(() => __setCodexWebSocketTransportForTesting(undefined));
  const result = await new CodexExecutor().execute({
    ...input,
    credentials: {
      accessToken: "fixture-only",
      providerSpecificData: { codexTransport: "websocket" },
    },
  });
  const text = await bounded(result.response.text());
  socket.onclose?.({ code: 1000 });
  assert.equal(closes, 1);
  assert.equal((text.match(/data: \[DONE\]/g) ?? []).length, 1);
  assert.ok(text.includes(JSON.stringify(payload)));
});
