import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-tool-name-identity-"));
process.env.DATA_DIR = DATA_DIR;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.NODE_ENV = "test";

const core = await import("../../src/lib/db/core.ts");
const { createSSETransformStreamWithLogger } = await import("../../open-sse/utils/stream.ts");
const { FORMATS } = await import("../../open-sse/translator/formats.ts");
const { openaiResponsesToOpenAIRequest } =
  await import("../../open-sse/translator/request/openai-responses.ts");

type Event = {
  event: string;
  data: Record<string, unknown> & { item?: Record<string, unknown>; delta?: unknown };
};

function toolChunk(name: string, argumentsDelta: string, id?: string) {
  return {
    id: "chatcmpl-tool-name-identity",
    model: "offline-probe",
    choices: [
      {
        index: 0,
        delta: {
          tool_calls: [
            {
              index: 0,
              ...(id ? { id, type: "function" } : {}),
              function: { name, arguments: argumentsDelta },
            },
          ],
        },
        finish_reason: null,
      },
    ],
  };
}

function parseEvents(sse: string): Event[] {
  return sse
    .split(/\r?\n\r?\n/)
    .filter((frame) => frame.startsWith("event: "))
    .flatMap((frame) => {
      const lines = frame.split(/\r?\n/);
      const dataLine = lines.find((line) => line.startsWith("data: "));
      if (!dataLine) return [];
      return [
        {
          event: lines[0].slice(7),
          data: JSON.parse(dataLine.slice(6)) as Record<string, unknown>,
        },
      ];
    });
}

async function translateStream(chunks: Record<string, unknown>[], declaredNames: string[]) {
  const transform = createSSETransformStreamWithLogger(
    FORMATS.OPENAI,
    FORMATS.OPENAI_RESPONSES,
    "openai-compatible",
    null,
    null,
    "offline-probe",
    "conn-tool-name-test",
    {
      messages: [{ role: "user", content: "call the tool" }],
      tools: declaredNames.map((name) => ({
        type: "function",
        function: { name, parameters: { type: "object", properties: {} } },
      })),
    }
  );
  const encoder = new TextEncoder();
  const input = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks)
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`));
      controller.enqueue(encoder.encode("data: [DONE]\n\n"));
      controller.close();
    },
  });
  return parseEvents(await new Response(input.pipeThrough(transform)).text());
}

test.after(async () => {
  core.resetDbInstance();
  fs.rmSync(DATA_DIR, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

test("generic stream accumulates split provider-prefixed name and buffered arguments", async () => {
  const events = await translateStream(
    [
      toolChunk("functions.", '{"cmd":', "call_generic"),
      toolChunk("exec", '"ls"}'),
      {
        id: "chatcmpl-tool-name-identity",
        model: "offline-probe",
        choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }],
      },
    ],
    ["exec"]
  );
  const added = events.filter((event) => event.event === "response.output_item.added");
  const done = events.filter((event) => event.event === "response.output_item.done");
  const argumentDeltas = events
    .filter((event) => event.event === "response.function_call_arguments.delta")
    .map((event) => event.data.delta);
  assert.equal(added.length, 1);
  assert.equal(added[0].data.item.name, "exec");
  assert.equal(done.length, 1);
  assert.equal(done[0].data.item.name, "exec");
  assert.equal(done[0].data.item.arguments, '{"cmd":"ls"}');
  assert.deepEqual(argumentDeltas, ['{"cmd":', '"ls"}']);
});

test("generic stream leaves unknown and undeclared names unchanged", async () => {
  for (const [name, declaredNames] of [
    ["functions.nope", ["exec"]],
    ["functions.exec", []],
  ] as const) {
    const events = await translateStream(
      [
        toolChunk(name, "{}", `call-${name}`),
        {
          id: "chatcmpl-tool-name-identity",
          model: "offline-probe",
          choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }],
        },
      ],
      [...declaredNames]
    );
    const item = events.find((event) => event.event === "response.output_item.added")?.data.item;
    assert.equal(item?.name, name);
  }
});

test("Responses namespaced forced choice resolves only to a declared flattened wire tool", () => {
  const result = openaiResponsesToOpenAIRequest(
    "openai/gpt-5.5",
    {
      model: "openai/gpt-5.5",
      input: "run exec",
      tools: [
        {
          type: "namespace",
          name: "functions",
          tools: [{ type: "function", name: "exec", parameters: { type: "object" } }],
        },
      ],
      tool_choice: { type: "function", name: "exec", namespace: "functions" },
    },
    false,
    { provider: "openai" }
  ) as Record<string, unknown>;
  assert.deepEqual(result.tool_choice, {
    type: "function",
    function: { name: "functions__exec" },
  });
  assert.equal(
    (result.tools as Array<{ function?: { name?: string } }>).some(
      (tool) => tool.function?.name === "functions__exec"
    ),
    true
  );
});

test("Responses namespaced forced choice falls back to the leaf when the wire name is undeclared", () => {
  const result = openaiResponsesToOpenAIRequest(
    "openai/gpt-5.5",
    {
      model: "openai/gpt-5.5",
      input: "run exec",
      tools: [{ type: "function", name: "exec", parameters: { type: "object" } }],
      tool_choice: { type: "function", name: "exec", namespace: "functions" },
    },
    false,
    { provider: "openai" }
  ) as Record<string, unknown>;
  assert.deepEqual(result.tool_choice, {
    type: "function",
    function: { name: "exec" },
  });
});
