import assert from "node:assert/strict";
import { after, test } from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { setImmediate } from "node:timers/promises";
import type { PersistenceTailPermit } from "../../src/lib/db/persistenceRuntime.ts";
import { createStreamingCompletionCallback } from "../../open-sse/handlers/chatCore/streamingCompletion.ts";

const names = ["DATA_DIR", "OMNIROUTE_DATABASE_BACKEND", "APP_LOG_TO_FILE"] as const;
const previous = new Map(names.map((name) => [name, process.env[name]]));
const directory = mkdtempSync(join(tmpdir(), "omni-terminal-accounting-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.APP_LOG_TO_FILE = "false";
const { createSSEStream } = await import("../../open-sse/utils/stream.ts");
const { createStreamController, pipeWithDisconnect } =
  await import("../../open-sse/utils/streamHandler.ts");
const { resetDbInstance } = await import("../../src/lib/db/core.ts");
after(() => {
  resetDbInstance();
  rmSync(directory, { recursive: true, force: true });
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

const encoder = new TextEncoder();
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
async function deadline<T>(work: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Terminal accounting test timed out")), 3000);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
const event = (value: unknown) => `data: ${JSON.stringify(value)}\n\n`;
const cases = [
  {
    name: "responses passthrough",
    options: {
      mode: "passthrough",
      sourceFormat: "openai-responses",
      clientResponseFormat: "openai-responses",
    },
    terminal: "response.completed",
    wire:
      event({
        type: "response.output_text.delta",
        item_id: "message",
        output_index: 0,
        content_index: 0,
        delta: "answer",
      }) +
      event({
        type: "response.completed",
        response: {
          id: "resp_terminal_accounting",
          object: "response",
          status: "completed",
          output: [
            {
              type: "message",
              role: "assistant",
              content: [{ type: "output_text", text: "answer" }],
            },
          ],
          usage: { input_tokens: 7, output_tokens: 3, total_tokens: 10 },
        },
      }),
  },
  ...["passthrough", "translate"].map((mode) => ({
    name: `chat ${mode}`,
    options: {
      mode,
      sourceFormat: "openai",
      targetFormat: "openai",
      clientResponseFormat: "openai",
    },
    terminal: '"finish_reason":"stop"',
    wire:
      event({
        id: "chatcmpl_terminal_accounting",
        object: "chat.completion.chunk",
        choices: [{ index: 0, delta: { content: "answer" }, finish_reason: "stop" }],
        usage: { prompt_tokens: 7, completion_tokens: 3, total_tokens: 10 },
      }) + "data: [DONE]\n\n",
  })),
];

for (const fixture of cases) {
  for (const action of ["signal", "cancel", "error", "eof"] as const) {
    for (const held of [false, true]) {
      test(`${fixture.name}: ${action} preserves one completion (${held ? "held tail" : "SQLite"})`, async () => {
        const accounting = deferred();
        const settled = deferred();
        let tailReleased = false;
        const pending = new Set<Promise<unknown>>();
        const permit: PersistenceTailPermit | undefined = held
          ? {
              run<T>(body: () => Promise<T>): Promise<T> {
                const work = Promise.resolve().then(body);
                pending.add(work);
                void work.then(
                  () => pending.delete(work),
                  () => pending.delete(work)
                );
                return work;
              },
              async release() {
                while (pending.size) await Promise.allSettled([...pending]);
                tailReleased = true;
              },
            }
          : undefined;
        let claimed = false;
        const completions: Array<{ status: number; usage?: unknown; responseBody?: unknown }> = [];
        const complete = createStreamingCompletionCallback({
          permit,
          claim: (payload: { status: number; usage?: unknown; responseBody?: unknown }) => {
            if (claimed) return false;
            claimed = true;
            completions.push(payload);
            return true;
          },
          complete: (_, track) =>
            track(async () => {
              await accounting.promise;
              settled.resolve();
            }, "accounting"),
        });
        const client = new AbortController();
        let upstream!: ReadableStreamDefaultController<Uint8Array>;
        let cancelled = 0;
        const source = new ReadableStream<Uint8Array>({
          start(controller) {
            upstream = controller;
          },
          cancel() {
            cancelled++;
          },
        });
        let disconnects = 0;
        const controller = createStreamController({
          clientResponseFormat: fixture.options.clientResponseFormat,
          clientAbortSignal: client.signal,
          onDisconnect() {
            disconnects++;
          },
        });
        const transform = createSSEStream({
          ...fixture.options,
          provider: "openai",
          model: "terminal-accounting",
          onComplete: complete,
        });
        const reader = pipeWithDisconnect(new Response(source), transform, controller, {
          highWaterMark: 0,
          stallTimeoutMs: 0,
          activeTimeoutMs: 0,
        }).getReader();
        try {
          upstream.enqueue(encoder.encode(fixture.wire));
          let received = "";
          while (!received.includes(fixture.terminal)) {
            const value = await deadline(reader.read());
            assert.equal(value.done, false);
            received += new TextDecoder().decode(value.value);
          }
          assert.equal(completions.length, 0, "normal terminal publication does not preempt EOF");
          if (action === "signal") client.abort("client received terminal");
          if (action === "cancel") await deadline(reader.cancel("client received terminal"));
          if (action === "error") upstream.error(new Error("transport closed after terminal"));
          if (action === "eof") upstream.close();
          if (action === "signal" || action === "error") {
            assert.equal((await deadline(reader.read())).done, true);
          }
          // Normal EOF may emit final metadata before awaiting accounting.
          const drained =
            action === "eof"
              ? (async () => {
                  while (!(await reader.read()).done) {
                    /* drain metadata */
                  }
                })()
              : Promise.resolve();
          for (let i = 0; i < 20 && completions.length === 0; i++) await setImmediate();
          assert.equal(completions.length, 1);
          assert.equal(completions[0].status, 200);
          const usage = completions[0].usage as {
            prompt_tokens?: number;
            completion_tokens?: number;
            input_tokens?: number;
            output_tokens?: number;
          };
          assert.equal(usage.prompt_tokens ?? usage.input_tokens, 7);
          assert.equal(usage.completion_tokens ?? usage.output_tokens, 3);
          assert.match(JSON.stringify(completions[0].responseBody), /answer/);
          assert.equal(disconnects, 0, "completed output must not become a client failure");
          if (action === "signal" || action === "cancel") {
            await setImmediate();
            assert.equal(cancelled, 1, "cleanup must not wait for accounting or upstream EOF");
          }
          if (held) assert.equal(tailReleased, false);
          accounting.resolve();
          await deadline(settled.promise);
          await deadline(drained);
          for (let i = 0; held && !tailReleased && i < 20; i++) await setImmediate();
          if (held) assert.equal(tailReleased, true);
          client.abort();
          await deadline(reader.cancel());
          assert.equal(completions.length, 1, "late cancel must not repeat accounting");
        } finally {
          accounting.resolve();
          await reader.cancel().catch(() => {});
          reader.releaseLock();
          controller.abort();
        }
      });
    }
  }
}

test("pre-terminal signal abort does not promote partial content to success", async () => {
  const client = new AbortController();
  let successes = 0;
  let cancelled = 0;
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(
        encoder.encode(event({ choices: [{ delta: { content: "partial" }, finish_reason: null }] }))
      );
    },
    cancel() {
      cancelled++;
    },
  });
  const controller = createStreamController({
    clientResponseFormat: "openai",
    clientAbortSignal: client.signal,
  });
  const reader = pipeWithDisconnect(
    new Response(source),
    createSSEStream({
      mode: "passthrough",
      clientResponseFormat: "openai",
      provider: "openai",
      onComplete(payload) {
        if (payload.status === 200) successes++;
      },
    }),
    controller,
    { highWaterMark: 0, stallTimeoutMs: 0, activeTimeoutMs: 0 }
  ).getReader();
  try {
    await deadline(reader.read());
    client.abort();
    assert.equal((await deadline(reader.read())).done, true);
    await setImmediate();
    assert.equal(cancelled, 1);
    assert.equal(successes, 0);
    assert.equal(controller.signal.aborted, true);
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
    controller.abort();
  }
});
