import assert from "node:assert/strict";
import test from "node:test";
import "../_setup/isolateDataDir.ts";

import {
  adoptEmptyTurnPolicy,
  hasTrustedEmptyTurn,
  inheritEmptyTurnPolicy,
  markEmptyTurnExecution,
} from "../../open-sse/utils/emptyTurnPolicy.ts";
import { createRecoverableStream } from "../../open-sse/services/streamRecovery.ts";
import { createStreamController, pipeWithDisconnect } from "../../open-sse/utils/streamHandler.ts";
import { synthesizeOpenAiSseFromJson } from "../../open-sse/utils/jsonToSse.ts";
import { FORMATS } from "../../open-sse/translator/formats.ts";

const credentials = {
  connectionId: "accepted-connection",
  apiKey: "synthetic-key",
};
const official = "https://api.openai.com/v1/chat/completions";
const codex = "https://chatgpt.com/backend-api/codex/responses";
const unknown = "https://unknown.example.invalid/v1/chat/completions";
const emptyStop =
  'data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n';
const emptyResponses =
  'data: {"type":"response.completed","response":{"status":"completed","output":[]}}\n\n';

function observed(url: string, payload: string | ReadableStream<Uint8Array>): Response {
  return markEmptyTurnExecution(
    new Response(payload, { headers: { "content-type": "text/event-stream" } }),
    url,
    credentials
  );
}

async function recoveredTurn(retryUrl: string, payload: string, initialUrl = official) {
  let generations = 1;
  let finalized = 0;
  const initial = observed(
    initialUrl,
    new ReadableStream<Uint8Array>({
      start(controller) {
        controller.error(Object.assign(new Error("socket hang up"), { code: "ECONNRESET" }));
      },
    })
  );
  const source = createRecoverableStream(
    initial.body!,
    async () => {
      generations++;
      const accepted = observed(retryUrl, payload);
      adoptEmptyTurnPolicy(accepted, initial);
      return accepted.body;
    },
    {
      maxEarlyRetries: 1,
      finalize() {
        finalized++;
      },
    }
  );
  // Both wrappers exist before recovery replaces the attempt; rebinding only initial is wrong.
  const inherited = inheritEmptyTurnPolicy(
    initial,
    new Response(source, {
      headers: { "content-type": "text/event-stream" },
    })
  );
  const outer = inheritEmptyTurnPolicy(
    inherited,
    new Response(inherited.body, {
      headers: inherited.headers,
    })
  );
  const statuses: number[] = [];
  const controller = createStreamController({
    clientResponseFormat: retryUrl === codex ? FORMATS.OPENAI_RESPONSES : FORMATS.OPENAI,
    onError(event) {
      statuses.push(event.statusCode);
      return true;
    },
  });
  const client = pipeWithDisconnect(
    outer,
    new TransformStream<Uint8Array, Uint8Array>(),
    controller,
    {
      stallTimeoutMs: 0,
      activeTimeoutMs: 0,
      highWaterMark: 0,
    }
  );
  const text = await new Response(client).text();
  return {
    text,
    generations,
    finalized,
    statuses,
    trusted: hasTrustedEmptyTurn(outer),
  };
}

for (const [url, payload] of [
  [official, emptyStop],
  [codex, emptyResponses],
]) {
  test(`early transport cutoff followed by accepted ${url} empty completion succeeds`, async () => {
    const result = await recoveredTurn(url, payload);
    assert.equal(result.generations, 2);
    assert.equal(result.finalized, 1);
    assert.equal(result.trusted, true);
    assert.deepEqual(result.statuses, []);
    assert.doesNotMatch(result.text, /Provider returned empty content|response.failed/);
  });
}

test("a wrapper initially lacking trusted origin observes an accepted official retry", async () => {
  const result = await recoveredTurn(official, emptyStop, unknown);
  assert.equal(result.generations, 2);
  assert.equal(result.finalized, 1);
  assert.equal(result.trusted, true);
  assert.deepEqual(result.statuses, []);
});

for (const [url, payload] of [
  [unknown, emptyStop],
  [official, "data: [DONE]\n\n"],
]) {
  test(`accepted ${url} retry without native ordinary-stop proof remains rejected`, async () => {
    const result = await recoveredTurn(url, payload);
    assert.equal(result.generations, 2);
    assert.equal(result.finalized, 1);
    assert.equal(result.trusted, false);
    assert.deepEqual(result.statuses, [502]);
    assert.match(result.text, /Provider returned empty content/);
  });
}

test("unknown-origin adoption revokes old trust and late old frames cannot restore it", async () => {
  let writer!: ReadableStreamDefaultController<Uint8Array>;
  const old = observed(
    official,
    new ReadableStream<Uint8Array>({
      start(value) {
        writer = value;
      },
    })
  );
  const inherited = inheritEmptyTurnPolicy(old, new Response("translated shell"));
  const outer = inheritEmptyTurnPolicy(inherited, new Response("cloned shell"));
  const accepted = observed(unknown, emptyStop);
  adoptEmptyTurnPolicy(accepted, old);
  writer.enqueue(new TextEncoder().encode(emptyStop));
  writer.close();
  await old.text();
  await accepted.text();
  assert.equal(hasTrustedEmptyTurn(old), false);
  assert.equal(hasTrustedEmptyTurn(inherited), false);
  assert.equal(hasTrustedEmptyTurn(outer), false);
});

test("an accepted native error overrides its normal stop through every inherited wrapper", async () => {
  const old = observed(official, emptyStop);
  await old.text();
  const alias = inheritEmptyTurnPolicy(old, new Response("shell"));
  assert.equal(hasTrustedEmptyTurn(alias), true);
  const accepted = observed(
    official,
    emptyStop + 'data: {"error":{"type":"insufficient_quota"}}\n\n'
  );
  adoptEmptyTurnPolicy(accepted, old);
  assert.equal(hasTrustedEmptyTurn(alias), false, "the new attempt has not terminated yet");
  await accepted.text();
  assert.equal(hasTrustedEmptyTurn(alias), false);
});

test("JSON-to-SSE conversion and an explicit clone inherit the adopted attempt", async () => {
  const old = observed(official, "data: [DONE]\n\n");
  const alias = inheritEmptyTurnPolicy(old, new Response("shell"));
  const accepted = markEmptyTurnExecution(
    Response.json({
      choices: [
        {
          index: 0,
          message: { role: "assistant", content: "" },
          finish_reason: "stop",
        },
      ],
    }),
    official,
    credentials
  );
  adoptEmptyTurnPolicy(accepted, old);
  const text = await accepted.text();
  const converted = inheritEmptyTurnPolicy(alias, new Response(synthesizeOpenAiSseFromJson(text)));
  const cloned = inheritEmptyTurnPolicy(converted, converted.clone());
  assert.equal(hasTrustedEmptyTurn(alias), true);
  assert.equal(hasTrustedEmptyTurn(converted), true);
  assert.equal(hasTrustedEmptyTurn(cloned), true);
  assert.match(await cloned.text(), /"finish_reason":"stop"/);
  await converted.body?.cancel();
  await old.body?.cancel();
});

test("a redirect's actual unknown origin cannot inherit the trusted request URL", async () => {
  const old = observed(official, emptyStop);
  await old.text();
  const alias = inheritEmptyTurnPolicy(old, new Response("shell"));
  const redirected = new Response(emptyStop, {
    headers: {
      "content-type": "text/event-stream",
      "X-OmniRoute-Selected-Connection-Id": "accepted-connection",
    },
  });
  Object.defineProperty(redirected, "url", { value: unknown });
  const accepted = markEmptyTurnExecution(redirected, official, credentials);
  adoptEmptyTurnPolicy(accepted, old);
  await accepted.text();
  assert.equal(hasTrustedEmptyTurn(alias), false);
});

test("late failure from the replaced observer cannot revoke the accepted attempt", async () => {
  let writer!: ReadableStreamDefaultController<Uint8Array>;
  const old = observed(
    official,
    new ReadableStream<Uint8Array>({
      start(value) {
        writer = value;
      },
    })
  );
  const alias = inheritEmptyTurnPolicy(old, new Response("shell"));
  const accepted = observed(official, emptyStop);
  adoptEmptyTurnPolicy(accepted, old);
  await accepted.text();
  assert.equal(hasTrustedEmptyTurn(alias), true);
  writer.enqueue(new TextEncoder().encode('data: {"error":{"type":"insufficient_quota"}}\n\n'));
  writer.close();
  await old.text();
  assert.equal(hasTrustedEmptyTurn(alias), true);
});
