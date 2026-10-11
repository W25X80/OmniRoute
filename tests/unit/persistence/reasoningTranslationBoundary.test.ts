import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

const previousData = process.env.DATA_DIR;
const previousReplay = process.env.REASONING_REPLAY_ENABLED;
const directory = mkdtempSync(join(tmpdir(), "omniroute-replay-boundary-"));
process.env.DATA_DIR = directory;
process.env.REASONING_REPLAY_ENABLED = "true";
const core = await import("../../../src/lib/db/core.ts");
const { setReasoningCache } = await import("../../../src/lib/db/reasoningCache.ts");
const { translateRequest, translateRequestAsync } =
  await import("../../../open-sse/translator/index.ts");
const { FORMATS } = await import("../../../open-sse/translator/formats.ts");

test.after(() => {
  core.resetDbInstance();
  rmSync(directory, { recursive: true, force: true });
  if (previousData === undefined) delete process.env.DATA_DIR;
  else process.env.DATA_DIR = previousData;
  if (previousReplay === undefined) delete process.env.REASONING_REPLAY_ENABLED;
  else process.env.REASONING_REPLAY_ENABLED = previousReplay;
});

for (const source of [FORMATS.OPENAI, FORMATS.CLAUDE]) {
  for (const target of [FORMATS.OPENAI, FORMATS.OPENAI_RESPONSES]) {
    test(`awaited translation preserves ${source} to ${target} tool history`, async () => {
      const callId = "call_" + randomUUID().replaceAll("-", "");
      const body =
        source === FORMATS.CLAUDE
          ? {
              model: "deepseek-chat",
              max_tokens: 100,
              tools: [{ name: "lookup", input_schema: { type: "object", properties: {} } }],
              messages: [
                { role: "user", content: "Look up the answer" },
                {
                  role: "assistant",
                  content: [{ type: "tool_use", id: callId, name: "lookup", input: {} }],
                },
                {
                  role: "user",
                  content: [{ type: "tool_result", tool_use_id: callId, content: "42" }],
                },
              ],
            }
          : {
              model: "deepseek-chat",
              tools: [
                {
                  type: "function",
                  function: { name: "lookup", parameters: { type: "object", properties: {} } },
                },
              ],
              messages: [
                { role: "user", content: "Look up the answer" },
                {
                  role: "assistant",
                  content: null,
                  tool_calls: [
                    { id: callId, type: "function", function: { name: "lookup", arguments: "{}" } },
                  ],
                },
                { role: "tool", tool_call_id: callId, content: "42" },
              ],
            };
      setReasoningCache(callId, "deepseek", "deepseek-chat", "retained prior reasoning", 60000);
      const sync = translateRequest(
        source,
        target,
        "deepseek-chat",
        structuredClone(body),
        false,
        null,
        "deepseek",
        null,
        { reasoningCacheScope: "fixture-scope" }
      );
      const awaited = await translateRequestAsync(
        source,
        target,
        "deepseek-chat",
        structuredClone(body),
        false,
        null,
        "deepseek",
        null,
        { reasoningCacheScope: "fixture-scope" }
      );
      assert.deepEqual(awaited, sync);
      assert.ok(JSON.stringify(awaited).includes("retained prior reasoning"));
    });
  }
}
