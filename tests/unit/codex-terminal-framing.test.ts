import assert from "node:assert/strict";
import test from "node:test";
import {
  closeCodexSseOnTerminal,
  createCodexTerminalTransform,
} from "../../open-sse/executors/codex/terminalStream.ts";

const encoder = new TextEncoder();
const terminal =
  'data: {"type":"response.completed","response":{"usage":{"input_tokens":17,"output_tokens":5}}}\n\n';

function chunksBody(chunks: Uint8Array[], cancel?: () => void | Promise<void>) {
  let offset = 0;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (offset < chunks.length) controller.enqueue(chunks[offset++]);
      else if (!cancel) controller.close();
    },
    cancel,
  });
}

async function collect(chunks: Uint8Array[]): Promise<Buffer> {
  return Buffer.from(
    await new Response(chunksBody(chunks).pipeThrough(createCodexTerminalTransform())).arrayBuffer()
  );
}

test("Codex terminal observation preserves UTF-8 and every LF chunk boundary", async () => {
  const prefix = 'data: {"type":"response.output_text.delta","delta":"hello 🌍"}\n\n';
  const expected = Buffer.from(prefix + terminal);
  const bytes = encoder.encode(prefix + terminal + ": ping\n\n");
  for (let at = 1; at < bytes.length; at++) {
    assert.deepEqual(await collect([bytes.subarray(0, at), bytes.subarray(at)]), expected);
  }
});

test("CRLF, bare CR and multiline data close only after a complete typed frame", async () => {
  for (const newline of ["\r\n", "\r", "\n"]) {
    const frame = [
      "event: response.output_text.delta",
      'data: {"type":',
      'data: "response.incomplete", "response":{"status":"incomplete"}}',
      "",
      "",
    ].join(newline);
    const bytes = encoder.encode(frame + ": ping" + newline + newline);
    for (let at = 1; at < bytes.length; at++) {
      const actual = await collect([bytes.subarray(0, at), bytes.subarray(at)]);
      // A CR is itself the SSE line ending. Its optional LF can be in a later,
      // discarded chunk; everything through that complete boundary is verbatim.
      const expected = Buffer.from(frame);
      assert.ok(
        actual.equals(expected) ||
          (newline === "\r\n" && actual.equals(expected.subarray(0, expected.length - 1))),
        `unexpected bytes for ${JSON.stringify(newline)} at ${at}`
      );
    }
  }
});

test("event labels, malformed data and incomplete frames do not terminate a response", async () => {
  const prefix = [
    "event: response.completed\ndata: null\n\n",
    'data: [{"type":"response.completed"}]\n\n',
    'data: {"type":"response.completed"\n\n',
    'event: response.completed\ndata: {"type":"response.created"}\n\n',
    'data: {"response":{"type":"response.completed"}}\n\n',
    "data: [DONE]\n\n",
    'data: {"type":"response.output_text.delta","delta":"response.completed"}\n\n',
  ].join("");
  assert.equal(
    (await collect([encoder.encode(prefix + terminal + ": ping\n\n")])).toString(),
    prefix + terminal
  );
  const unframed = terminal.trimEnd();
  assert.equal((await collect([encoder.encode(prefix + unframed)])).toString(), prefix + unframed);
});

test("terminal EOF and downstream flush do not await a pending or rejected cancel", async () => {
  for (const mode of ["pending", "rejected"] as const) {
    let cancellations = 0;
    let flushes = 0;
    const upstream = chunksBody([encoder.encode(terminal)], () => {
      cancellations++;
      return mode === "pending"
        ? new Promise<void>(() => {})
        : Promise.reject(new Error("cancel failed"));
    });
    const downstream = upstream.pipeThrough(createCodexTerminalTransform()).pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform(chunk, controller) {
          controller.enqueue(chunk);
        },
        flush(controller) {
          flushes++;
          controller.enqueue(encoder.encode("data: [DONE]\n\n"));
        },
      })
    );
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const text = await Promise.race([
        new Response(downstream).text(),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error("cancel blocked EOF")), 300);
        }),
      ]);
      assert.equal(text, terminal + "data: [DONE]\n\n");
      assert.equal(flushes, 1);
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(cancellations, 1);
    } finally {
      clearTimeout(timer);
    }
  }
});

test("observer preserves backpressure, cancellation and upstream errors", async () => {
  let pulls = 0;
  let cancellations = 0;
  const source = new ReadableStream<Uint8Array>({
    pull(controller) {
      pulls++;
      controller.enqueue(encoder.encode(": ping\n\n"));
    },
    cancel() {
      cancellations++;
    },
  });
  const body = source.pipeThrough(createCodexTerminalTransform());
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.ok(pulls <= 2, `undemanded stream pulled ${pulls} chunks`);
  await body.cancel();
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(cancellations, 1);

  const failure = new Error("upstream read failed");
  const errored = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.error(failure);
    },
  });
  await assert.rejects(
    new Response(errored.pipeThrough(createCodexTerminalTransform())).text(),
    (error) => error === failure
  );
});

test("truncating a successful SSE response removes its stale length only", async () => {
  const response = new Response(terminal + ": ping\n\n", {
    headers: {
      "content-type": "text/event-stream",
      "content-length": "999",
      "x-request-id": "fixture",
    },
  });
  const result = closeCodexSseOnTerminal(response);
  assert.equal(result.headers.get("content-length"), null);
  assert.equal(result.headers.get("x-request-id"), "fixture");
  assert.equal(await result.text(), terminal);
  const failure = new Response("rejected", {
    status: 429,
    headers: { "content-type": "text/event-stream" },
  });
  assert.equal(closeCodexSseOnTerminal(failure), failure);
});
