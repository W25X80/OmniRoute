import assert from "node:assert/strict";
import test, { mock } from "node:test";

assert.ok(process.env.DATA_DIR, "the isolated fixture requires DATA_DIR");
assert.equal(process.env.OMNIROUTE_DATABASE_BACKEND, "sqlite");

function latch() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

type HookGate = { entered: ReturnType<typeof latch>; release: ReturnType<typeof latch> };
let hookGate: HookGate | null = null;
const pluginUrl = new URL("../../open-sse/handlers/chatCore/pluginOnResponse.ts", import.meta.url);
const originalPlugin: typeof import("../../open-sse/handlers/chatCore/pluginOnResponse.ts") =
  await import(pluginUrl.href);
const pluginMock = mock.module(pluginUrl.href, {
  namedExports: {
    ...originalPlugin,
    async runPluginOnResponseHook(
      args: Parameters<typeof originalPlugin.runPluginOnResponseHook>[0]
    ) {
      const gate = hookGate;
      if (gate && args.response.streamed) {
        gate.entered.resolve();
        await gate.release.promise;
      }
      return originalPlugin.runPluginOnResponseHook(args);
    },
  },
});

const { handleChatCore } = await import("../../open-sse/handlers/chatCore.ts");
const core = await import("../../src/lib/db/core.ts");
const { getUsageHistory } = await import("../../src/lib/usage/usageHistory.ts");
const { waitForCallLogSaves } = await import("../../src/lib/usage/callLogs.ts");
const { closeCallLogArtifactWriter } = await import("../../src/lib/usage/callLogArtifactWriter.ts");
const originalFetch = globalThis.fetch;
const encoder = new TextEncoder();
const model = "gpt-4o-mini";
type Failure = { status: number; code?: string };

async function bounded<T>(work: Promise<T>, label: string, timeoutMs = 5_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Fixture timeout: ${label}`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function waitFor(predicate: () => boolean): Promise<boolean> {
  const until = Date.now() + 1_000;
  while (!predicate() && Date.now() < until) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  return predicate();
}

function chunk(content: string, finishReason: string | null = null): Uint8Array {
  return encoder.encode(
    `data: ${JSON.stringify({
      id: "early-finalizer-fixture",
      object: "chat.completion.chunk",
      created: 1,
      model,
      choices: [{ index: 0, delta: { content }, finish_reason: finishReason }],
    })}\n\n`
  );
}

async function startRequest(prompt: string) {
  const gate: HookGate = { entered: latch(), release: latch() };
  hookGate = gate;
  let upstream!: ReadableStreamDefaultController<Uint8Array>;
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      upstream = controller;
      controller.enqueue(chunk("ready prefix"));
    },
  });
  globalThis.fetch = async () =>
    new Response(source, {
      headers: { "Content-Type": "text/event-stream" },
    });
  const abort = new AbortController();
  const failures: Failure[] = [];
  let returned = false;
  const body = { model, stream: true, messages: [{ role: "user", content: prompt }] };
  const work = handleChatCore({
    body,
    modelInfo: { provider: "openai", model, extendedContext: false },
    credentials: { apiKey: "fixture-key", providerSpecificData: {} },
    log: { debug() {}, info() {}, warn() {}, error() {} },
    onStreamFailure: (failure: Failure) => {
      failures.push(failure);
    },
    clientRawRequest: {
      endpoint: "/v1/chat/completions",
      body: structuredClone(body),
      headers: new Headers({ accept: "text/event-stream" }),
      signal: abort.signal,
    },
    userAgent: "early-stream-finalizer-fixture",
    skipResourcePressureGuard: true,
  });
  void work.then(
    () => {
      returned = true;
    },
    () => {
      returned = true;
    }
  );
  await bounded(gate.entered.promise, "streamed onResponse hook must be reached");
  return { gate, upstream, abort, failures, work, returned: () => returned };
}

async function finishRequest(request: Awaited<ReturnType<typeof startRequest>>) {
  request.gate.release.resolve();
  const result = await bounded(request.work, "release blocked response hook");
  assert.equal(result.success, true, "the request passed readiness before the hook");
  const output = await bounded(
    result.response.text().then(
      (text) => ({ text, failed: false }),
      () => ({ text: "", failed: true })
    ),
    "drain terminal response"
  );
  hookGate = null;
  return output;
}

test.afterEach(() => {
  globalThis.fetch = originalFetch;
  hookGate?.release.resolve();
  hookGate = null;
});
test.after(async () => {
  pluginMock.restore();
  assert.equal(await waitForCallLogSaves(3_000), true);
  await closeCallLogArtifactWriter();
  core.resetDbInstance();
});

test("pipeline errors finalize before an awaited response hook returns", async () => {
  const startedAt = new Date(Date.now() - 1_000);
  const request = await startRequest("pipeline error during awaited response hook");
  try {
    request.upstream.error(
      Object.assign(new Error("fixture upstream disconnected"), { code: "ECONNRESET" })
    );
    assert.equal(
      await waitFor(() => request.failures.length > 0),
      true,
      "the error callback must already be installed while the response hook is pending"
    );
    assert.equal(request.returned(), false, "the dispatch must still be inside the response hook");
    assert.equal(request.failures.length, 1);
    assert.equal(request.failures[0].status, 502);
    assert.equal(request.failures[0].code, "stream_pipeline_error");
    const usage = await getUsageHistory({ provider: "openai", model, startDate: startedAt });
    assert.ok(
      usage.some(
        (row) =>
          row.success === false && row.status === "502" && row.errorCode === "stream_pipeline_error"
      ),
      "the early error must reach durable failure accounting"
    );
  } finally {
    await finishRequest(request);
  }
  assert.equal(request.failures.length, 1, "late handoff must not duplicate the same failure");
});

test("normal completion during an awaited response hook does not become a failure", async () => {
  const request = await startRequest("normal completion during awaited response hook");
  let output: Awaited<ReturnType<typeof finishRequest>> | undefined;
  try {
    request.upstream.enqueue(chunk("", "stop"));
    request.upstream.enqueue(encoder.encode("data: [DONE]\n\n"));
    request.upstream.close();
    assert.equal(request.returned(), false);
    assert.equal(request.failures.length, 0);
  } finally {
    output = await finishRequest(request);
  }
  assert.equal(request.failures.length, 0);
  assert.equal(output.failed, false);
  assert.match(output.text, /ready prefix/);
  assert.match(output.text, /data: \[DONE\]/);
});

test("client disconnect finalizes once while an awaited response hook is pending", async () => {
  const request = await startRequest("client disconnect during awaited response hook");
  try {
    request.abort.abort(new DOMException("Fixture client disconnected", "AbortError"));
    assert.equal(
      await waitFor(() => request.failures.length > 0),
      true,
      "the disconnect finalizer must already be installed before response handoff"
    );
    assert.equal(request.returned(), false);
    assert.equal(request.failures.length, 1);
    assert.equal(request.failures[0].status, 499);
    assert.equal(request.failures[0].code, "client_disconnected");
  } finally {
    await finishRequest(request);
  }
  assert.equal(
    request.failures.length,
    1,
    "the carried disconnect callback must remain idempotent"
  );
});
