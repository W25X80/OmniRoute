import { test } from "node:test";
import assert from "node:assert/strict";

import {
  translateNonStreamingClientResponse,
  type NonStreamingClientTranslateInput,
} from "../../open-sse/handlers/chatCore/nonStreamingClientTranslate.ts";
import { FORMATS } from "../../open-sse/translator/formats.ts";
import {
  buildAssistantMessageCacheKey,
  clearReasoningCacheAll,
  lookupReasoning,
} from "../../open-sse/services/reasoningCache.ts";
import { extractRequestToolIdentityMap } from "../../open-sse/handlers/chatCore/requestToolIdentity.ts";
import { collectCustomToolNamesForSourceFormat } from "../../open-sse/translator/request/openai-responses/additionalTools.ts";
import { openaiResponsesToOpenAIRequest } from "../../open-sse/translator/request/openai-responses.ts";
import { invalidateBufferTokensCache } from "../../open-sse/utils/usageTracking.ts";

/* ── helpers ─────────────────────────────────────────────────────────────── */

function baseInput(
  overrides: Partial<NonStreamingClientTranslateInput> = {}
): NonStreamingClientTranslateInput {
  return {
    responseBody: {
      id: "chatcmpl-test",
      object: "chat.completion",
      choices: [
        {
          index: 0,
          message: { role: "assistant", content: "Hello!" },
          finish_reason: "stop",
        },
      ],
      usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
    },
    responsePayloadFormat: "openai",
    clientResponseFormat: "openai",
    sourceFormat: "openai",
    provider: "openai",
    model: "gpt-4o",
    requestBody: { messages: [{ role: "user", content: "hi" }] },
    responseToolNameMap: null,
    requestToolIdentityMap: null,
    reasoningCacheScope: null,
    clientHeaders: null,
    isClaudeCodeCompatible: false,
    phase: "final",
    ...overrides,
  };
}

function convertResponsesToolMetadata(tools: Record<string, unknown>[]) {
  const input = [
    { type: "message", role: "user", content: [{ type: "input_text", text: "run the tool" }] },
  ];
  const sourceBody = { model: "openai/gpt-5.5", input, tools };
  const requestBody = openaiResponsesToOpenAIRequest(sourceBody.model, sourceBody, false, {
    provider: "openai",
  }) as Record<string, unknown>;
  return {
    requestBody,
    customToolNames: collectCustomToolNamesForSourceFormat(
      FORMATS.OPENAI_RESPONSES,
      FORMATS.OPENAI_RESPONSES,
      tools,
      input
    ),
    requestToolIdentityMap: extractRequestToolIdentityMap(requestBody),
  };
}

/* ── characterization tests ──────────────────────────────────────────────── */

test("basic translate: same-format passthrough returns responseBody", () => {
  const input = baseInput();
  const result = translateNonStreamingClientResponse(input);
  assert.equal(result.response.choices[0].message.content, "Hello!");
  assert.ok(result.responseForMemoryExtraction);
});

test("translate from claude to openai format", () => {
  const input = baseInput({
    responseBody: {
      id: "msg-123",
      content: [{ type: "text", text: "Hi there" }],
      stop_reason: "end_turn",
      usage: { input_tokens: 10, output_tokens: 5 },
    },
    responsePayloadFormat: "claude",
    clientResponseFormat: "openai",
    sourceFormat: "claude",
    provider: "anthropic",
    model: "claude-sonnet-4-20250514",
  });
  const result = translateNonStreamingClientResponse(input);
  const msg = result.response.choices?.[0]?.message;
  assert.ok(msg, "should have choices[0].message");
  assert.equal((msg as { content: string }).content, "Hi there");
});

test("claude source strips markdown code fence", () => {
  const input = baseInput({
    responseBody: {
      id: "msg-123",
      content: [
        {
          type: "text",
          text: '```json\n{"key": "value"}\n```',
        },
      ],
      stop_reason: "end_turn",
      usage: { input_tokens: 10, output_tokens: 5 },
    },
    responsePayloadFormat: "claude",
    clientResponseFormat: "openai",
    sourceFormat: "claude",
    provider: "anthropic",
    model: "claude-sonnet-4-20250514",
  });
  const result = translateNonStreamingClientResponse(input);
  const content = result.response.choices?.[0]?.message?.content;
  assert.ok(typeof content === "string");
  // After stripping, the content should not have the markdown fence wrapper
  assert.ok(!content.startsWith("```json"), "markdown fence should be stripped");
});

test("normalizeOpenAIToolFinishReasons: tool_calls present → finish_reason tool_calls", () => {
  const input = baseInput({
    responseBody: {
      id: "chatcmpl-test",
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            content: null,
            tool_calls: [
              {
                id: "call_1",
                type: "function",
                function: { name: "get_weather", arguments: "{}" },
              },
            ],
          },
          finish_reason: "stop",
        },
      ],
      usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
    },
  });
  const result = translateNonStreamingClientResponse(input);
  assert.equal(result.response.choices[0].finish_reason, "tool_calls");
});

test("reasoning replay: no-tool history comes from historyMessages, not requestBody.input", () => {
  clearReasoningCacheAll();
  const scope = "api-key:test:s...6a";
  const historyMessages = [{ role: "user", content: "hi from translatedBody" }];
  const assistantMessage = {
    role: "assistant",
    content: "thinking result",
    reasoning_content: "let me think...",
  };
  const input = baseInput({
    responseBody: {
      id: "chatcmpl-test",
      choices: [
        {
          index: 0,
          message: assistantMessage,
          finish_reason: "stop",
        },
      ],
      usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
    },
    // Responses-shaped finalBody: input, no messages. Parent used translatedBody.messages.
    requestBody: { input: [{ role: "user", content: "wrong body" }] },
    historyMessages,
    provider: "deepseek",
    model: "deepseek-v4-pro",
    reasoningCacheScope: scope,
    phase: "intermediate",
  });
  const result = translateNonStreamingClientResponse(input);
  assert.ok(result.response);
  const cacheKey = buildAssistantMessageCacheKey(
    scope,
    [...historyMessages, assistantMessage],
    historyMessages.length
  );
  assert.equal(
    lookupReasoning(cacheKey),
    "let me think...",
    "must cache against translatedBody.messages, not finalBody.input"
  );
});

test("transcript-observed non-streaming replies bypass reasoning cache without changing the client reply", () => {
  clearReasoningCacheAll();
  const sentinel = "PRIVATE_NONSTREAM_REASONING_TRANSCRIPT_SENTINEL";
  const observed = { videoTranscriptSensitive: true };
  const input = baseInput({
    provider: "deepseek",
    model: "deepseek-v4-pro",
    responseBody: {
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            content: "visible client reply",
            reasoning_content: sentinel,
            tool_calls: [
              {
                id: "call_video_nonstream",
                type: "function",
                function: { name: "f", arguments: "{}" },
              },
            ],
          },
          finish_reason: "tool_calls",
        },
      ],
    },
    ...observed,
  });
  const result = translateNonStreamingClientResponse(input);
  assert.equal(result.response.choices[0].message.reasoning_content, sentinel);
  assert.equal(lookupReasoning("call_video_nonstream"), null);
});

test("phase=final applies client usage buffer", () => {
  // Gemini format skips OpenAI/Responses sanitize, so extra usage fields
  // only disappear if applyClientUsageBuffer → filterUsageForFormat runs.
  const input = baseInput({
    phase: "final",
    clientResponseFormat: FORMATS.GEMINI,
    responsePayloadFormat: FORMATS.GEMINI,
    sourceFormat: FORMATS.GEMINI,
    responseBody: {
      id: "chatcmpl-test",
      choices: [
        {
          index: 0,
          message: { role: "assistant", content: "Hello!" },
          finish_reason: "stop",
        },
      ],
      usage: {
        prompt_tokens: 10,
        completion_tokens: 5,
        total_tokens: 15,
        x_provider_extra: 99,
      },
    },
  });
  const result = translateNonStreamingClientResponse(input);
  const usage = (result.response as { usage: Record<string, unknown> }).usage;
  assert.equal(usage.x_provider_extra, undefined, "final phase must filter extra usage fields");
  assert.equal(
    usage.prompt_tokens,
    undefined,
    "final Gemini filter must drop OpenAI-shaped prompt_tokens"
  );
});

test("phase=intermediate skips applyClientUsageBuffer", () => {
  const input = baseInput({
    phase: "intermediate",
    clientResponseFormat: FORMATS.GEMINI,
    responsePayloadFormat: FORMATS.GEMINI,
    sourceFormat: FORMATS.GEMINI,
    responseBody: {
      id: "chatcmpl-test",
      choices: [
        {
          index: 0,
          message: { role: "assistant", content: "partial" },
          finish_reason: null,
        },
      ],
      usage: {
        prompt_tokens: 10,
        completion_tokens: 5,
        total_tokens: 15,
        x_provider_extra: 99,
      },
    },
  });
  const result = translateNonStreamingClientResponse(input);
  const usage = (result.response as { usage: Record<string, unknown> }).usage;
  assert.equal(
    usage.x_provider_extra,
    99,
    "intermediate must keep raw extra usage fields (buffer not applied)"
  );
  assert.equal(usage.prompt_tokens, 10);
});

test("Responses API format: sanitizeResponsesApiResponse is applied", () => {
  const input = baseInput({
    clientResponseFormat: FORMATS.OPENAI_RESPONSES,
    responseBody: {
      id: "resp_123",
      object: "response",
      output: [
        {
          type: "function_call",
          name: "ns__get_weather",
          arguments: "{}",
        },
      ],
      usage: { input_tokens: 10, output_tokens: 5 },
      illegal_top_level: "drop-me",
    },
    requestToolIdentityMap: new Map([
      ["ns__get_weather", { namespace: "ns", name: "get_weather" }],
    ]),
  });
  const result = translateNonStreamingClientResponse(input);
  assert.equal(result.response.object, "response");
  assert.equal(result.response.illegal_top_level, undefined, "sanitizer must drop illegal fields");
  const output = result.response.output as Array<Record<string, unknown>>;
  assert.equal(output[0]?.type, "function_call");
  assert.equal(output[0]?.namespace, "ns", "#7936 restore namespace");
  assert.equal(output[0]?.name, "get_weather", "#7936 restore original name");
});

test("Responses API format: restores non-stream custom tool calls before namespace identity", () => {
  const input = baseInput({
    responsePayloadFormat: FORMATS.OPENAI_RESPONSES,
    clientResponseFormat: FORMATS.OPENAI_RESPONSES,
    sourceFormat: FORMATS.OPENAI_RESPONSES,
    responseBody: {
      id: "resp_custom",
      object: "response",
      status: "completed",
      output: [
        {
          id: "fc_call_1",
          type: "function_call",
          call_id: "call_1",
          name: "functions__exec",
          arguments: '{"input":"printf \'nonstream-ok\\\\n\'"}',
        },
      ],
      usage: { input_tokens: 10, output_tokens: 5, total_tokens: 15 },
    },
    customToolNames: new Set(["functions__exec"]),
    requestToolIdentityMap: new Map([
      ["functions__exec", { namespace: "functions", name: "exec" }],
    ]),
  });

  const result = translateNonStreamingClientResponse(input);
  const output = result.response.output as Array<Record<string, unknown>>;
  assert.deepEqual(output[0], {
    id: "fc_call_1",
    type: "custom_tool_call",
    call_id: "call_1",
    name: "exec",
    input: "printf 'nonstream-ok\\n'",
    status: "completed",
    namespace: "functions",
  });
});

test("Responses nonstream custom calls use converted metadata for wire, bare and dotted names", () => {
  const metadata = convertResponsesToolMetadata([
    {
      type: "namespace",
      name: "functions",
      tools: [
        {
          type: "custom",
          name: "exec",
          description: "Run a command",
          format: { type: "grammar", syntax: "lark", definition: "command: /.+/" },
        },
      ],
    },
  ]);
  assert.equal(metadata.customToolNames.has("functions__exec"), true);
  assert.equal(metadata.requestToolIdentityMap?.get("functions__exec")?.namespace, "functions");

  for (const [index, name] of ["functions__exec", "exec", "functions.exec"].entries()) {
    const input = baseInput({
      responsePayloadFormat: FORMATS.OPENAI_RESPONSES,
      clientResponseFormat: FORMATS.OPENAI_RESPONSES,
      sourceFormat: FORMATS.OPENAI_RESPONSES,
      provider: "grok-cli",
      model: "grok-4.6",
      requestBody: metadata.requestBody,
      customToolNames: metadata.customToolNames,
      requestToolIdentityMap: metadata.requestToolIdentityMap,
      responseBody: {
        id: `resp_custom_${index}`,
        object: "response",
        status: "completed",
        output: [
          {
            id: `fc_custom_${index}`,
            type: "function_call",
            call_id: `call_custom_${index}`,
            name,
            arguments: JSON.stringify({ input: "raw command" }),
            status: "completed",
          },
        ],
        usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 },
      },
    });
    const result = translateNonStreamingClientResponse(input);
    const item = (result.response.output as Record<string, unknown>[])[0];
    assert.equal(item.type, "custom_tool_call", `custom identity for provider name ${name}`);
    assert.equal(item.input, "raw command");
    assert.equal(item.arguments, undefined);
    assert.equal(item.namespace, "functions");
    assert.equal(item.name, "exec");
  }
});

test("Responses nonstream classifies actual hash-truncated custom aliases from the identity ledger", () => {
  const namespace = `mcp__${"n".repeat(50)}`;
  const leaf = `custom_${"x".repeat(16)}`;
  const metadata = convertResponsesToolMetadata([
    {
      type: "namespace",
      name: namespace,
      tools: [{ type: "custom", name: leaf, format: { type: "grammar", definition: ".*" } }],
    },
  ]);
  const wireName = [...metadata.customToolNames][0];
  assert.equal(namespace.length, 55);
  assert.equal(leaf.length, 23);
  assert.equal(wireName.length, 64);
  assert.deepEqual(metadata.requestToolIdentityMap?.get(wireName), { namespace, name: leaf });

  const aliases = [wireName, leaf, `${namespace}.${leaf}`];
  const output = [
    ...aliases.map((name, index) => ({
      id: `fc_hash_${index}`,
      type: "function_call",
      call_id: `call_hash_${index}`,
      name,
      arguments: JSON.stringify({ input: `raw-command-${index}` }),
      status: "completed",
    })),
    {
      id: "fc_unknown_native_prefix",
      type: "function_call",
      call_id: "call_unknown_native_prefix",
      name: `unrecognized.${leaf}`,
      arguments: JSON.stringify({ input: "leave-as-function" }),
      status: "completed",
    },
  ];
  const result = translateNonStreamingClientResponse(
    baseInput({
      responsePayloadFormat: FORMATS.OPENAI_RESPONSES,
      clientResponseFormat: FORMATS.OPENAI_RESPONSES,
      sourceFormat: FORMATS.OPENAI_RESPONSES,
      provider: "grok-cli",
      model: "grok-4.6",
      requestBody: metadata.requestBody,
      customToolNames: metadata.customToolNames,
      requestToolIdentityMap: metadata.requestToolIdentityMap,
      responseBody: {
        id: "resp_hash_custom_aliases",
        object: "response",
        status: "completed",
        output,
      },
    })
  );
  const items = result.response.output as Record<string, unknown>[];
  for (const [index, item] of items.slice(0, aliases.length).entries()) {
    assert.equal(item.type, "custom_tool_call", `custom type for ledger alias ${aliases[index]}`);
    assert.equal(item.input, `raw-command-${index}`);
    assert.equal(item.arguments, undefined);
    assert.equal(item.namespace, namespace);
    assert.equal(item.name, leaf);
  }
  const unknown = items[3];
  assert.equal(unknown.type, "function_call");
  assert.equal(unknown.name, `unrecognized.${leaf}`);
  assert.equal(unknown.arguments, JSON.stringify({ input: "leave-as-function" }));
  assert.equal(unknown.namespace, undefined);
});

test("Responses nonstream custom classification preserves flat and ambiguous function controls", () => {
  const flatMetadata = convertResponsesToolMetadata([
    { type: "function", name: "exec", parameters: { type: "object", properties: {} } },
    {
      type: "namespace",
      name: "functions",
      tools: [{ type: "custom", name: "exec", format: { type: "grammar", definition: ".*" } }],
    },
  ]);
  assert.equal(flatMetadata.customToolNames.has("functions__exec"), false);
  const flatResult = translateNonStreamingClientResponse(
    baseInput({
      responsePayloadFormat: FORMATS.OPENAI_RESPONSES,
      clientResponseFormat: FORMATS.OPENAI_RESPONSES,
      sourceFormat: FORMATS.OPENAI_RESPONSES,
      requestBody: flatMetadata.requestBody,
      customToolNames: flatMetadata.customToolNames,
      requestToolIdentityMap: flatMetadata.requestToolIdentityMap,
      responseBody: {
        id: "resp_flat",
        object: "response",
        status: "completed",
        output: [
          {
            id: "fc_flat",
            type: "function_call",
            call_id: "call_flat",
            name: "exec",
            arguments: '{"command":"ls"}',
            status: "completed",
          },
        ],
      },
    })
  );
  const flatItem = (flatResult.response.output as Record<string, unknown>[])[0];
  assert.equal(flatItem.type, "function_call");
  assert.equal(flatItem.name, "exec");
  assert.equal(flatItem.arguments, '{"command":"ls"}');
  assert.equal(flatItem.namespace, undefined);

  const ambiguousMetadata = convertResponsesToolMetadata([
    {
      type: "namespace",
      name: "functions",
      tools: [{ type: "custom", name: "exec", format: { type: "grammar", definition: ".*" } }],
    },
    {
      type: "namespace",
      name: "mcp__other",
      tools: [{ type: "custom", name: "exec", format: { type: "grammar", definition: ".*" } }],
    },
  ]);
  const ambiguousResult = translateNonStreamingClientResponse(
    baseInput({
      responsePayloadFormat: FORMATS.OPENAI_RESPONSES,
      clientResponseFormat: FORMATS.OPENAI_RESPONSES,
      sourceFormat: FORMATS.OPENAI_RESPONSES,
      requestBody: ambiguousMetadata.requestBody,
      customToolNames: ambiguousMetadata.customToolNames,
      requestToolIdentityMap: ambiguousMetadata.requestToolIdentityMap,
      responseBody: {
        id: "resp_ambiguous",
        object: "response",
        status: "completed",
        output: [
          {
            id: "fc_ambiguous",
            type: "function_call",
            call_id: "call_ambiguous",
            name: "exec",
            arguments: '{"input":"ambiguous"}',
            status: "completed",
          },
        ],
      },
    })
  );
  const ambiguousItem = (ambiguousResult.response.output as Record<string, unknown>[])[0];
  assert.equal(ambiguousItem.type, "function_call");
  assert.equal(ambiguousItem.name, "exec");
  assert.equal(ambiguousItem.arguments, '{"input":"ambiguous"}');
  assert.equal(ambiguousItem.namespace, undefined);

  const namespaceMetadata = convertResponsesToolMetadata([
    {
      type: "namespace",
      name: "functions",
      tools: [
        {
          type: "custom",
          name: "exec",
          format: { type: "grammar", definition: ".*" },
        },
      ],
    },
  ]);
  // A stale explicit-flat ledger/schema remains authoritative if the stream of
  // request conversion metadata ever contains both aliases at this boundary.
  const schemaGuardRequestBody = {
    ...namespaceMetadata.requestBody,
    tools: [
      ...(namespaceMetadata.requestBody.tools as Record<string, unknown>[]),
      {
        type: "function",
        function: { name: "exec", parameters: { type: "object", properties: {} } },
      },
    ],
  };
  const schemaGuardIdentityMap = new Map(namespaceMetadata.requestToolIdentityMap || []);
  schemaGuardIdentityMap.set("exec", { namespace: "", name: "exec" });
  const schemaGuardResult = translateNonStreamingClientResponse(
    baseInput({
      responsePayloadFormat: FORMATS.OPENAI_RESPONSES,
      clientResponseFormat: FORMATS.OPENAI_RESPONSES,
      sourceFormat: FORMATS.OPENAI_RESPONSES,
      requestBody: schemaGuardRequestBody,
      customToolNames: namespaceMetadata.customToolNames,
      requestToolIdentityMap: schemaGuardIdentityMap,
      responseBody: {
        id: "resp_schema_guard",
        object: "response",
        status: "completed",
        output: [
          {
            id: "fc_schema_guard",
            type: "function_call",
            call_id: "call_schema_guard",
            name: "exec",
            arguments: '{"command":"ls"}',
            status: "completed",
          },
        ],
      },
    })
  );
  const schemaGuardItem = (schemaGuardResult.response.output as Record<string, unknown>[])[0];
  assert.equal(schemaGuardItem.type, "function_call");
  assert.equal(schemaGuardItem.name, "exec");
  assert.equal(schemaGuardItem.arguments, '{"command":"ls"}');
  assert.equal(schemaGuardItem.namespace, undefined);
});

test("Responses API format: classifies custom calls synthesized from Kiro chat output", () => {
  const input = baseInput({
    responsePayloadFormat: "kiro",
    clientResponseFormat: FORMATS.OPENAI_RESPONSES,
    sourceFormat: FORMATS.OPENAI_RESPONSES,
    responseBody: {
      id: "chatcmpl_custom",
      object: "chat.completion",
      choices: [
        {
          index: 0,
          message: {
            role: "assistant",
            content: null,
            tool_calls: [
              {
                id: "call_kiro_1",
                type: "function",
                function: {
                  name: "functions__exec",
                  arguments: '{"input":"printf \'kiro-nonstream-ok\\\\n\'"}',
                },
              },
            ],
          },
          finish_reason: "tool_calls",
        },
      ],
      usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
    },
    customToolNames: new Set(["functions__exec"]),
    requestToolIdentityMap: new Map([
      ["functions__exec", { namespace: "functions", name: "exec" }],
    ]),
  });

  const result = translateNonStreamingClientResponse(input);
  const output = result.response.output as Array<Record<string, unknown>>;
  assert.deepEqual(output[0], {
    id: "fc_call_kiro_1",
    type: "custom_tool_call",
    call_id: "call_kiro_1",
    name: "exec",
    input: "printf 'kiro-nonstream-ok\\n'",
    status: "completed",
    namespace: "functions",
  });
});

test("#12370: alias-shaped requestToolIdentityMap must not blank out function_call name", () => {
  // extractRequestToolIdentityMap() falls back to the `_toolNameMap` side channel
  // when no namespace tools are present. For Gemini/Claude pivots that side
  // channel is a plain Map<string, string> alias table (wire name -> original
  // name), NOT the {namespace, name} identity shape the #7936 restore loop
  // expects. A plain function tool like Codex's "shell" round-trips through
  // this alias map as an identity mapping ("shell" -> "shell"): reproduces the
  // exact live-VPS shape (tool_choice: auto, one `shell` function tool,
  // gemini-3-flash-preview) where the non-streaming /v1/responses item lost
  // its `name` key entirely.
  const input = baseInput({
    responsePayloadFormat: FORMATS.GEMINI,
    clientResponseFormat: FORMATS.OPENAI_RESPONSES,
    sourceFormat: FORMATS.OPENAI_RESPONSES,
    provider: "gemini",
    model: "gemini-3-flash-preview",
    responseBody: {
      candidates: [
        {
          content: {
            role: "model",
            parts: [{ functionCall: { name: "shell", args: { command: ["ls", "memory-bank/"] } } }],
          },
          finishReason: "STOP",
          index: 0,
        },
      ],
    },
    // Alias-shaped map (string -> string), as published by the openai->gemini
    // pivot — not a NamespaceIdentity map.
    requestToolIdentityMap: new Map([["shell", "shell"]]) as unknown as Map<
      string,
      { namespace?: string; name: string }
    >,
  });
  const result = translateNonStreamingClientResponse(input);
  const output = result.response.output as Array<Record<string, unknown>>;
  const functionCall = output.find((item) => item.type === "function_call");
  assert.ok(functionCall, "expected a function_call output item");
  assert.equal(functionCall?.name, "shell", "name must survive the alias-map fallback");
  assert.equal("name" in (functionCall as object), true, "name key must be present, not stripped");
});

test("empty content response: passthrough without crash", () => {
  const input = baseInput({
    responseBody: {},
  });
  const result = translateNonStreamingClientResponse(input);
  assert.ok(result.response);
  assert.ok(result.responseForMemoryExtraction);
});

test("isClaudeCodeCompatible preserves context budget usage", () => {
  const saved = process.env.USAGE_TOKEN_BUFFER;
  process.env.USAGE_TOKEN_BUFFER = "2000";
  invalidateBufferTokensCache();
  try {
    const input = baseInput({
      isClaudeCodeCompatible: true,
      clientResponseFormat: FORMATS.OPENAI,
      phase: "final",
      responseBody: {
        id: "chatcmpl-test",
        object: "chat.completion",
        choices: [
          {
            index: 0,
            message: { role: "assistant", content: "test" },
            finish_reason: "stop",
          },
        ],
        usage: {
          prompt_tokens: 100,
          completion_tokens: 50,
          total_tokens: 150,
        },
      },
    });
    const result = translateNonStreamingClientResponse(input);
    const usage = (result.response as { usage: Record<string, unknown> }).usage;
    assert.equal(
      usage.prompt_tokens,
      2100,
      "Claude Code path must fold context_budget_prompt_tokens (100+2000) into visible prompt_tokens"
    );
    assert.equal(usage.total_tokens, 2150);
    assert.equal("context_budget_prompt_tokens" in usage, false);
  } finally {
    if (saved === undefined) delete process.env.USAGE_TOKEN_BUFFER;
    else process.env.USAGE_TOKEN_BUFFER = saved;
    invalidateBufferTokensCache();
  }
});
