import assert from "node:assert/strict";
import test from "node:test";

import { openaiToClaudeResponse } from "../../open-sse/translator/response/openai-to-claude.ts";
import { FORMATS } from "../../open-sse/translator/formats.ts";
import { initState, translateResponse } from "../../open-sse/translator/index.ts";
import { CLAUDE_OAUTH_TOOL_PREFIX } from "../../open-sse/translator/request/openai-to-claude.ts";
import { createSSEStream } from "../../open-sse/utils/stream.ts";

type ClaudeEvent = {
  type: string;
  index?: number;
  content_block?: { type?: string; id?: string; name?: string };
  delta?: { type?: string; partial_json?: string; stop_reason?: string };
  usage?: Record<string, number>;
};

function chunk(toolCalls: unknown[], finishReason: string | null = null) {
  return {
    id: "chatcmpl-claude-lifecycle-causal",
    model: "test-model",
    choices: [{ index: 0, delta: { tool_calls: toolCalls }, finish_reason: finishReason }],
  };
}

function finish() {
  return {
    ...chunk([], "tool_calls"),
    usage: { prompt_tokens: 8, completion_tokens: 3, total_tokens: 11 },
  };
}

function collect(chunks: unknown[]): ClaudeEvent[] {
  const state = initState(FORMATS.CLAUDE);
  return [...chunks, null].flatMap(
    (part) => (openaiToClaudeResponse(part, state) ?? []) as ClaudeEvent[]
  );
}

function starts(events: ClaudeEvent[]) {
  return events.filter(
    (event) => event.type === "content_block_start" && event.content_block?.type === "tool_use"
  );
}

function argsFor(events: ClaudeEvent[], index: number | undefined): string {
  return events
    .filter(
      (event) =>
        event.type === "content_block_delta" &&
        event.index === index &&
        event.delta?.type === "input_json_delta"
    )
    .map((event) => event.delta?.partial_json ?? "")
    .join("");
}

function assertClosedOnce(events: ClaudeEvent[]) {
  for (const opened of starts(events)) {
    assert.equal(
      events.filter((event) => event.type === "content_block_stop" && event.index === opened.index)
        .length,
      1,
      `block ${opened.index} closes exactly once`
    );
  }
  assert.equal(events.filter((event) => event.type === "message_stop").length, 1);
  assert.equal(events.filter((event) => event.type === "error").length, 0);
}

test("a published synthetic Bash followed by real-ID Grep is a new call", () => {
  const events = collect([
    chunk([{ index: 0, function: { name: "Bash", arguments: '{"command":"ls"}' } }]),
    chunk([
      { index: 0, id: "call_grep", function: { name: "Grep", arguments: '{"pattern":"y"}' } },
    ]),
    finish(),
  ]);
  const opened = starts(events);
  assert.equal(opened.length, 2);
  assert.deepEqual(
    opened.map((event) => event.content_block?.name),
    ["Bash", "Grep"]
  );
  assert.notEqual(opened[0].content_block?.id, "call_grep");
  assert.equal(opened[1].content_block?.id, "call_grep");
  assert.deepEqual(JSON.parse(argsFor(events, opened[0].index)), { command: "ls" });
  assert.deepEqual(JSON.parse(argsFor(events, opened[1].index)), { pattern: "y" });
  assertClosedOnce(events);
});

test("a published idless zero-argument call survives a new real-ID call", () => {
  const events = collect([
    chunk([{ index: 0, function: { name: "Bash", arguments: "" } }]),
    chunk([{ index: 0, id: "call_grep", function: { name: "Grep", arguments: "{}" } }]),
    finish(),
  ]);
  const opened = starts(events);
  assert.equal(opened.length, 2);
  assert.deepEqual(
    opened.map((event) => event.content_block?.name),
    ["Bash", "Grep"]
  );
  assert.equal(argsFor(events, opened[0].index), "");
  assert.equal(argsFor(events, opened[1].index), "{}");
  assertClosedOnce(events);
});

test("same-call late ID keeps the published synthetic ID and learns upstream identity", () => {
  const state = initState(FORMATS.CLAUDE);
  const first = (openaiToClaudeResponse(
    chunk([{ index: 0, function: { name: "Bash", arguments: '{"command":' } }]),
    state
  ) ?? []) as ClaudeEvent[];
  const publishedId = starts(first)[0]?.content_block?.id;
  assert.ok(publishedId, "the idless named call publishes a stable synthetic ID");
  const events = [
    ...first,
    ...[
      chunk([{ index: 0, id: "call_bash", function: { name: "Bash" } }]),
      chunk([{ index: 0, id: "call_bash", function: { arguments: '"ls"}' } }]),
      chunk([
        { index: 0, id: "call_grep", function: { name: "Grep", arguments: '{"pattern":"z"}' } },
      ]),
      finish(),
      null,
    ].flatMap((part) => (openaiToClaudeResponse(part, state) ?? []) as ClaudeEvent[]),
  ];
  const opened = starts(events);
  assert.equal(opened.length, 2);
  assert.equal(opened[0].content_block?.id, publishedId);
  assert.notEqual(opened[0].content_block?.id, "call_bash");
  assert.equal(opened[1].content_block?.id, "call_grep");
  assert.deepEqual(JSON.parse(argsFor(events, opened[0].index)), { command: "ls" });
  assert.deepEqual(JSON.parse(argsFor(events, opened[1].index)), { pattern: "z" });
  assertClosedOnce(events);
});

test("arguments then ID then name do not publish an empty tool name", () => {
  const state = initState(FORMATS.CLAUDE);
  const events: ClaudeEvent[] = [];
  for (const part of [
    chunk([{ index: 0, function: { arguments: '{"command":' } }]),
    chunk([{ index: 0, id: "call_args_first" }]),
  ]) {
    events.push(...((openaiToClaudeResponse(part, state) ?? []) as ClaudeEvent[]));
    assert.equal(starts(events).length, 0, "name is still unknown; hold argument bytes");
  }
  for (const part of [
    chunk([{ index: 0, function: { name: "Bash" } }]),
    chunk([{ index: 0, function: { arguments: '"pwd"}' } }]),
    finish(),
    null,
  ]) {
    events.push(...((openaiToClaudeResponse(part, state) ?? []) as ClaudeEvent[]));
  }
  const opened = starts(events);
  assert.equal(opened.length, 1);
  assert.equal(opened[0].content_block?.name, "Bash");
  assert.equal(opened[0].content_block?.id, "call_args_first");
  assert.equal(argsFor(events, opened[0].index), '{"command":"pwd"}');
  assertClosedOnce(events);
});

function responseItem(id: string, name: string, args: string) {
  return { type: "function_call", id: `fc_${id}`, call_id: id, name, arguments: args };
}

test("actual Responses to Claude hub keeps parallel calls closed out of order", () => {
  const state = initState(FORMATS.CLAUDE);
  const a = responseItem("call_a", "Bash", '{"command":"ls"}');
  const b = responseItem("call_b", "Grep", '{"pattern":"needle"}');
  const events = [
    { type: "response.created", response: { id: "resp_hub", model: "test-model" } },
    { type: "response.output_item.added", output_index: 0, item: { ...a, arguments: "" } },
    { type: "response.output_item.added", output_index: 1, item: { ...b, arguments: "" } },
    { type: "response.function_call_arguments.delta", item_id: a.id, delta: '{"command":' },
    { type: "response.output_item.done", output_index: 1, item: b },
    { type: "response.function_call_arguments.delta", item_id: a.id, delta: '"ls"}' },
    { type: "response.output_item.done", output_index: 0, item: a },
    {
      type: "response.completed",
      response: { output: [a, b], usage: { input_tokens: 20, output_tokens: 5 } },
    },
    null,
  ].flatMap(
    (part) =>
      translateResponse(FORMATS.OPENAI_RESPONSES, FORMATS.CLAUDE, part, state) as ClaudeEvent[]
  );
  const opened = starts(events);
  assert.equal(opened.length, 2);
  assert.deepEqual(
    opened.map((event) => event.content_block?.id),
    ["call_a", "call_b"]
  );
  assert.deepEqual(JSON.parse(argsFor(events, opened[0].index)), { command: "ls" });
  assert.deepEqual(JSON.parse(argsFor(events, opened[1].index)), { pattern: "needle" });
  assert.equal(events.find((event) => event.type === "message_delta")?.usage?.output_tokens, 5);
  assertClosedOnce(events);
});

for (const [wireName, expectedName] of [
  ["Bash", "Bash"],
  ["provider_alias", "DeclaredTool"],
  [`${CLAUDE_OAUTH_TOOL_PREFIX}Bash`, "Bash"],
] as const) {
  test(`actual Responses hub retains done-only identity for ${wireName}`, () => {
    const state = {
      ...initState(FORMATS.CLAUDE),
      toolNameMap: new Map([["provider_alias", "DeclaredTool"]]),
    };
    const item = responseItem("call_done_only", wireName, '{"command":"pwd"}');
    const events = [
      { type: "response.created", response: { id: "resp_done_only", model: "test-model" } },
      { type: "response.output_item.done", output_index: 0, item },
      {
        type: "response.completed",
        response: { output: [item], usage: { input_tokens: 2, output_tokens: 3 } },
      },
      null,
    ].flatMap(
      (part) =>
        translateResponse(FORMATS.OPENAI_RESPONSES, FORMATS.CLAUDE, part, state) as ClaudeEvent[]
    );
    const opened = starts(events);
    assert.equal(opened.length, 1);
    assert.equal(opened[0].content_block?.id, "call_done_only");
    assert.equal(opened[0].content_block?.name, expectedName);
    assert.equal(argsFor(events, opened[0].index), '{"command":"pwd"}');
    assertClosedOnce(events);
  });
}

for (const withFinish of [false, true]) {
  test(`actual SSE ${withFinish ? "finish plus trailing usage succeeds" : "usage without finish fails"}`, async () => {
    const failures: Record<string, unknown>[] = [];
    const completions: Record<string, unknown>[] = [];
    const parts: Record<string, unknown>[] = [
      {
        ...chunk([]),
        choices: [{ index: 0, delta: { content: "Partial output" }, finish_reason: null }],
      },
    ];
    if (withFinish) parts.push(chunk([], "stop"));
    parts.push({
      choices: [],
      usage: { prompt_tokens: 11, completion_tokens: 7, total_tokens: 18 },
    });
    const source = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const part of parts) {
          controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(part)}\n\n`));
        }
        controller.close();
      },
    });
    const response = new Response(
      source.pipeThrough(
        createSSEStream({
          mode: "translate",
          targetFormat: FORMATS.OPENAI,
          sourceFormat: FORMATS.CLAUDE,
          provider: "test-provider",
          model: "test-model",
          body: { messages: [{ role: "user", content: "Test" }] },
          onFailure: (failure) => {
            failures.push(failure as Record<string, unknown>);
          },
          onComplete: (completion) => {
            completions.push(completion as unknown as Record<string, unknown>);
          },
        })
      )
    );
    const text = await response.text();
    assert.equal(completions.length, 1, "one actual stream completion owner");
    if (withFinish) {
      assert.equal(failures.length, 0);
      assert.equal(completions[0].status, 200);
      assert.equal((text.match(/event: message_stop\n/g) ?? []).length, 1);
      assert.doesNotMatch(text, /event: error/);
    } else {
      assert.equal(failures.length, 1);
      assert.equal(failures[0].code, "stream_early_eof");
      assert.equal(completions[0].status, 502);
      assert.match(text, /event: error/);
      assert.doesNotMatch(text, /event: message_stop/);
    }
    const usage = completions[0].usage as Record<string, unknown>;
    assert.equal(usage.input_tokens, 11);
    assert.equal(usage.output_tokens, 7);
  });
}
