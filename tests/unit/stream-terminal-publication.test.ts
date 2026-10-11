import assert from "node:assert/strict";
import { after, test } from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createStreamTerminalPublication } from "../../open-sse/utils/streamTerminalPublication.ts";

const directory = mkdtempSync(join(tmpdir(), "omni-terminal-unit-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.APP_LOG_TO_FILE = "false";
const { createSSEStream } = await import("../../open-sse/utils/stream.ts");
const { resetDbInstance } = await import("../../src/lib/db/core.ts");
after(() => {
  resetDbInstance();
  rmSync(directory, { recursive: true, force: true });
});

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
async function withDeadline<T>(work: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("Terminal publication oracle deadline exceeded")),
          3000
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
const encoder = new TextEncoder();
const responseId = "resp_terminal_unit";
const response = {
  id: responseId,
  status: "completed",
  output: [
    { type: "message", role: "assistant", content: [{ type: "output_text", text: "answer" }] },
  ],
  usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 },
};
const event = (data: Record<string, unknown>) => `data: ${JSON.stringify(data)}\n\n`;

test("publication preserves synchronous ordinary output and awaits one ordered terminal owner", async () => {
  const gate = barrier();
  const emitted: string[] = [];
  const publication = createStreamTerminalPublication(() => gate.promise);
  publication.publish(() => emitted.push("ordinary"));
  assert.deepEqual(emitted, ["ordinary"]);
  assert.equal(publication.settle(), undefined);
  publication.prepare({ kind: "done", clientResponse: {} });
  publication.publish(() => emitted.push("terminal"));
  publication.publish(() => emitted.push("after-terminal"));
  const settled = publication.settle()!;
  await Promise.resolve();
  assert.deepEqual(emitted, ["ordinary"]);
  gate.resolve();
  await withDeadline(settled);
  assert.deepEqual(emitted, ["ordinary", "terminal", "after-terminal"]);
  assert.equal(publication.settle(), undefined);
});

test("publication rejection never publishes terminal bytes and its settlement is observed", async () => {
  const emitted: string[] = [];
  const publication = createStreamTerminalPublication(() =>
    Promise.reject(new Error("privacy unavailable"))
  );
  publication.prepare({ kind: "done", clientResponse: {} });
  publication.publish(() => emitted.push("terminal"));
  await assert.rejects(publication.settle(), /privacy unavailable/);
  assert.deepEqual(emitted, []);
});

for (const terminalType of ["response.completed", "response.incomplete"] as const) {
  test(`${terminalType} is prepared before publication even before upstream EOF`, async () => {
    const prepare = barrier(),
      entered = barrier();
    let upstream!: ReadableStreamDefaultController<Uint8Array>;
    let completed = false;
    let payload: unknown;
    const source = new ReadableStream<Uint8Array>({
      start(controller) {
        upstream = controller;
      },
    });
    const stream = source.pipeThrough(
      createSSEStream({
        mode: "passthrough",
        provider: "openai",
        model: "terminal-unit",
        clientResponseFormat: "openai-responses",
        onBeforeTerminal(data) {
          payload = data.clientResponse;
          entered.resolve();
          return prepare.promise;
        },
        onComplete() {
          completed = true;
        },
      })
    );
    const reader = stream.getReader();
    try {
      upstream.enqueue(
        encoder.encode(
          event({
            type: "response.created",
            response: { id: responseId, status: "in_progress", output: [] },
          })
        )
      );
      const created = await reader.read();
      assert.match(new TextDecoder().decode(created.value), /response.created/);
      upstream.enqueue(
        encoder.encode(
          event({
            type: terminalType,
            response: {
              ...response,
              status: terminalType === "response.incomplete" ? "incomplete" : "completed",
            },
          })
        )
      );
      const terminal = reader.read();
      let published = false;
      void terminal.then(() => {
        published = true;
      });
      await withDeadline(entered.promise);
      assert.equal(published, false);
      assert.equal(completed, false);
      const summary = (payload as { summary: typeof response }).summary;
      assert.equal(summary.id, responseId);
      assert.equal(
        summary.status,
        terminalType === "response.incomplete" ? "incomplete" : "completed"
      );
      assert.deepEqual(summary.output, response.output);
      prepare.resolve();
      assert.match(
        new TextDecoder().decode((await withDeadline(terminal)).value),
        new RegExp(terminalType)
      );
      assert.equal(completed, false);
      upstream.close();
      while (!(await withDeadline(reader.read())).done) {
        /* drain metadata */
      }
      assert.equal(completed, true);
    } finally {
      prepare.resolve();
      try {
        upstream.close();
      } catch {
        /* already closed or cancelled */
      }
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  });
}

for (const terminalType of ["response.completed", "response.incomplete"] as const) {
  test(`${terminalType} EOF line still gates publication without a trailing newline`, async () => {
    const prepare = barrier(),
      entered = barrier();
    let completed = false;
    const source = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(
          encoder.encode(
            `data: ${JSON.stringify({
              type: terminalType,
              response: {
                ...response,
                status: terminalType === "response.incomplete" ? "incomplete" : "completed",
              },
            })}`
          )
        );
        controller.close();
      },
    });
    const work = new Response(
      source.pipeThrough(
        createSSEStream({
          mode: "passthrough",
          provider: "openai",
          model: "terminal-unit",
          clientResponseFormat: "openai-responses",
          onBeforeTerminal() {
            entered.resolve();
            return prepare.promise;
          },
          onComplete() {
            completed = true;
          },
        })
      )
    ).text();
    void work.catch(() => {});
    try {
      await withDeadline(entered.promise);
      assert.equal(completed, false);
      prepare.resolve();
      assert.match(await withDeadline(work), new RegExp(terminalType));
      assert.equal(completed, true);
    } finally {
      prepare.resolve();
      await work.catch(() => {});
    }
  });
}

test("translated partial terminal uses the actual client summary before publication", async () => {
  const prepare = barrier(),
    entered = barrier();
  let completed = false;
  const chunks = [
    {
      id: "chatcmpl_partial_unit",
      choices: [{ index: 0, delta: { content: "partial answer" }, finish_reason: null }],
    },
    {
      id: "chatcmpl_partial_unit",
      choices: [{ index: 0, delta: {}, finish_reason: "length" }],
      usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
    },
  ];
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(event(chunk)));
      controller.enqueue(encoder.encode("data: [DONE]\n\n"));
      controller.close();
    },
  });
  const work = new Response(
    source.pipeThrough(
      createSSEStream({
        mode: "translate",
        sourceFormat: "openai-responses",
        targetFormat: "openai",
        clientResponseFormat: "openai-responses",
        provider: "openai",
        model: "terminal-unit",
        body: { input: [{ type: "message", role: "user", content: "question" }] },
        onBeforeTerminal(data) {
          assert.equal(data.kind, "response.incomplete");
          const summary = (
            data.clientResponse as { summary: { status: string; output: unknown[] } }
          ).summary;
          assert.equal(summary.status, "incomplete");
          assert.ok(summary.output.length > 0);
          entered.resolve();
          return prepare.promise;
        },
        onComplete() {
          completed = true;
        },
      })
    )
  ).text();
  void work.catch(() => {});
  try {
    await withDeadline(entered.promise);
    assert.equal(completed, false);
    prepare.resolve();
    assert.match(await withDeadline(work), /response.incomplete/);
    assert.equal(completed, true);
  } finally {
    prepare.resolve();
    await work.catch(() => {});
  }
});

for (const mode of ["passthrough", "translate"] as const) {
  test(`synthesized DONE in ${mode} mode is gated without premature completion`, async () => {
    const prepare = barrier(),
      entered = barrier();
    let completed = false;
    const chunks = [
      {
        id: "chatcmpl_terminal_unit",
        object: "chat.completion.chunk",
        choices: [{ index: 0, delta: { content: "answer" }, finish_reason: null }],
      },
      {
        id: "chatcmpl_terminal_unit",
        object: "chat.completion.chunk",
        choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
        usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      },
    ];
    const source = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(encoder.encode(event(chunk)));
        controller.close();
      },
    });
    const work = new Response(
      source.pipeThrough(
        createSSEStream({
          mode,
          targetFormat: "openai",
          sourceFormat: "openai",
          clientResponseFormat: "openai",
          provider: "openai",
          model: "terminal-unit",
          onBeforeTerminal(data) {
            assert.equal(data.kind, "done");
            entered.resolve();
            return prepare.promise;
          },
          onComplete() {
            completed = true;
          },
        })
      )
    ).text();
    void work.catch(() => {});
    try {
      await withDeadline(entered.promise);
      assert.equal(completed, false);
      prepare.resolve();
      assert.match(await withDeadline(work), /data: \[DONE\]/);
      assert.equal(completed, true);
    } finally {
      prepare.resolve();
      await work.catch(() => {});
    }
  });
}

const {
  seedPendingContinuationState,
  seedOwnedPendingContinuationState,
  clearOwnedPendingContinuationState,
  clearPendingContinuationState,
  resolvePreviousResponseStateAsync,
} = await import("../../src/lib/db/responsesContinuationStore.ts");
const pipeline = (text: string) => ({
  clientRawRequest: { body: { input: [{ role: "user", content: "question" }] } },
  clientResponse: {
    output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text }] }],
  },
});

test("legacy seed remains synchronous void and owned clear cannot remove a newer nonce", async () => {
  const id = "resp_nonce_unit";
  try {
    assert.equal(
      seedPendingContinuationState(id, "key-unit", pipeline("legacy"), false),
      undefined
    );
    assert.ok(await resolvePreviousResponseStateAsync(id, "key-unit"));
    const old = seedOwnedPendingContinuationState(id, "key-unit", pipeline("old"), false)!;
    const current = seedOwnedPendingContinuationState(id, "key-unit", pipeline("current"), false)!;
    clearOwnedPendingContinuationState(old);
    assert.deepEqual(
      (await resolvePreviousResponseStateAsync(id, "key-unit"))?.output,
      pipeline("current").clientResponse.output
    );
    clearOwnedPendingContinuationState(current);
    assert.equal(await resolvePreviousResponseStateAsync(id, "key-unit"), null);
  } finally {
    clearPendingContinuationState(id);
  }
});

test("owned cancellation cannot delete the same response ID seeded for another key", async () => {
  const id = "resp_key_unit";
  try {
    const own = seedOwnedPendingContinuationState(id, "key-a", pipeline("a"), false)!;
    const other = seedOwnedPendingContinuationState(id, "key-b", pipeline("b"), false)!;
    clearOwnedPendingContinuationState(own);
    assert.equal(await resolvePreviousResponseStateAsync(id, "key-a"), null);
    assert.deepEqual(
      (await resolvePreviousResponseStateAsync(id, "key-b"))?.output,
      pipeline("b").clientResponse.output
    );
    clearOwnedPendingContinuationState(other);
  } finally {
    clearPendingContinuationState(id);
  }
});

test("owned projection preserves video, truncation and empty-output rejection", () => {
  const valid = pipeline("answer");
  assert.equal(seedOwnedPendingContinuationState("resp_video_unit", null, valid, true), undefined);
  assert.equal(
    seedOwnedPendingContinuationState(
      "resp_empty_unit",
      null,
      { ...valid, clientResponse: { output: [] } },
      false
    ),
    undefined
  );
  assert.equal(
    seedOwnedPendingContinuationState(
      "resp_truncated_unit",
      null,
      { ...valid, clientResponse: { ...valid.clientResponse, _truncated: true } },
      false
    ),
    undefined
  );
});

test("keyless seed preserves source logging context without granting anonymous continuation reads", async () => {
  const id = "resp_keyless_unit";
  const seed = seedOwnedPendingContinuationState(id, null, pipeline("keyless"), false)!;
  try {
    assert.equal(seed.apiKeyId, null);
    assert.equal(await resolvePreviousResponseStateAsync(id, null), null);
  } finally {
    clearOwnedPendingContinuationState(seed);
  }
});
