import assert from "node:assert/strict";
import { syncBuiltinESMExports } from "node:module";
import { setImmediate as nextTurn } from "node:timers/promises";
import zlib from "node:zlib";
import test, { type TestContext } from "node:test";
import { DevinDesktopExecutor } from "../../open-sse/executors/devin-desktop.ts";

const MAX_FRAME_BYTES = 16 * 1024 * 1024;
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function envelope(payload: Uint8Array, flags: number) {
  const result = Buffer.alloc(5 + payload.byteLength);
  result[0] = flags;
  result.writeUInt32BE(payload.byteLength, 1);
  result.set(payload, 5);
  return result;
}
// Independent fixed protobuf: GetChatMessageResponse field3 text, field5 stop reason.
const MESSAGE = Buffer.from("1a0568656c6c6f2801", "hex");
const AUTH = Buffer.from("0a096a77742d76616c7565", "hex");
const TRAILER = Buffer.from("{}");
async function execute(t: TestContext, frames: Uint8Array[], onCancel?: () => void) {
  let calls = 0;
  t.mock.method(globalThis, "fetch", async () => {
    calls++;
    if (calls === 1) return new Response(AUTH);
    assert.equal(calls, 2);
    return new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          for (const frame of frames) controller.enqueue(frame);
          if (!onCancel) controller.close();
        },
        cancel() {
          onCancel?.();
        },
      })
    );
  });
  return new DevinDesktopExecutor().execute({
    model: "model-x",
    body: { messages: [{ role: "user", content: "hello" }] },
    stream: true,
    credentials: { accessToken: "fixture-not-secret" },
  });
}

test("native compressed data and trailers preserve SQLite SSE bytes and ordered framing", async (t) => {
  const saved = process.env.OMNIROUTE_DATABASE_BACKEND;
  const data = envelope(zlib.gzipSync(MESSAGE), 0x01);
  const trailer = envelope(zlib.gzipSync(TRAILER), 0x03);
  t.mock.method(Date, "now", () => 1784457764961);
  try {
    const outputs: string[] = [];
    for (const backend of ["sqlite", "postgres"] as const) {
      process.env.OMNIROUTE_DATABASE_BACKEND = backend;
      const result = await execute(t, [data.subarray(0, 3), data.subarray(3), trailer]);
      outputs.push(await result.response.text());
    }
    assert.equal(outputs[1], outputs[0]);
    assert.match(outputs[1], /"content":"hello"/);
    assert.match(outputs[1], /"finish_reason":"stop"/);
    assert.match(outputs[1], /data: \[DONE\]/);
  } finally {
    if (saved === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = saved;
  }
});

test("native invalid/oversized gzip keeps bounded sanitized error SSE", async (t) => {
  const saved = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const oversized = zlib.gzipSync(Buffer.alloc(MAX_FRAME_BYTES + 1, 0x61));
  const wireOversized = Buffer.alloc(5);
  wireOversized.writeUInt32BE(MAX_FRAME_BYTES + 1, 1);
  try {
    for (const frame of [
      envelope(Buffer.from("invalid-gzip"), 0x01),
      envelope(oversized, 0x01),
      wireOversized,
      envelope(MESSAGE, 0x04),
    ]) {
      let cancelled = 0;
      const result = await execute(t, [frame], () => {
        cancelled++;
      });
      const output = await result.response.text();
      assert.match(output, /devin_desktop_error/);
      assert.match(output, /data: \[DONE\]/);
      assert.equal(cancelled, 1);
      assert.ok(output.length < 4096);
      assert.ok(!output.includes("at /"));
    }
  } finally {
    if (saved === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = saved;
  }
});

test("native inflate is awaited in order and keeps its captured profile across macrotasks", async (t) => {
  const saved = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const original = zlib.gunzip;
  const entered = deferred(),
    computed = deferred();
  let release: (() => void) | undefined;
  let inflates = 0;
  let pending = 0;
  let maximumPending = 0;
  t.mock.method(zlib, "gunzip", (...args: Parameters<typeof zlib.gunzip>) => {
    const [input, options, callback] = args;
    assert.equal(options.maxOutputLength, MAX_FRAME_BYTES);
    inflates++;
    pending++;
    maximumPending = Math.max(maximumPending, pending);
    entered.resolve();
    original(input, options, (error, output) => {
      if (inflates === 1) {
        release = () => {
          pending--;
          callback(error, output);
        };
        computed.resolve();
      } else {
        pending--;
        callback(error, output);
      }
    });
  });
  syncBuiltinESMExports();
  let text: Promise<string> | undefined;
  try {
    const result = await execute(t, [
      envelope(zlib.gzipSync(MESSAGE), 0x01),
      envelope(zlib.gzipSync(TRAILER), 0x03),
    ]);
    text = result.response.text();
    let settled = false;
    void text.then(() => {
      settled = true;
    });
    await nextTurn();
    assert.equal(
      inflates,
      1,
      "native stream must invoke asynchronous inflate before awaiting its completion"
    );
    await entered.promise;
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    assert.equal(settled, false);
    await computed.promise;
    release!();
    release = undefined;
    assert.match(await text, /"content":"hello"/);
    assert.equal(inflates, 2);
    assert.equal(maximumPending, 1);
  } finally {
    release?.();
    await text?.catch(() => {});
    t.mock.restoreAll();
    syncBuiltinESMExports();
    if (saved === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = saved;
  }
});

test("native cancellation during real inflate suppresses late emissions and completion errors", async (t) => {
  const saved = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const original = zlib.gunzip;
  const enqueue = ReadableStreamDefaultController.prototype.enqueue;
  const close = ReadableStreamDefaultController.prototype.close;
  let afterCancel = false;
  let lateEnqueues = 0,
    lateCloses = 0;
  const unhandled: unknown[] = [];
  const onUnhandled = (reason: unknown) => {
    unhandled.push(reason);
  };
  process.on("unhandledRejection", onUnhandled);
  t.mock.method(
    ReadableStreamDefaultController.prototype,
    "enqueue",
    function (this: ReadableStreamDefaultController<unknown>, chunk: unknown) {
      if (afterCancel) lateEnqueues++;
      return enqueue.call(this, chunk);
    }
  );
  t.mock.method(
    ReadableStreamDefaultController.prototype,
    "close",
    function (this: ReadableStreamDefaultController<unknown>) {
      if (afterCancel) lateCloses++;
      return close.call(this);
    }
  );
  try {
    for (const compressed of [zlib.gzipSync(MESSAGE), Buffer.from("invalid-gzip")]) {
      const computed = deferred();
      let release: (() => void) | undefined;
      let cancelled = 0;
      afterCancel = false;
      t.mock.method(zlib, "gunzip", (...args: Parameters<typeof zlib.gunzip>) => {
        const [input, options, callback] = args;
        original(input, options, (error, output) => {
          release = () => callback(error, output);
          computed.resolve();
        });
      });
      syncBuiltinESMExports();
      const result = await execute(t, [envelope(compressed, 0x01)], () => {
        cancelled++;
      });
      await computed.promise;
      const reader = result.response.body!.getReader();
      await reader.cancel("controlled-client-cancel");
      reader.releaseLock();
      afterCancel = true;
      release!();
      await nextTurn();
      await nextTurn();
      assert.equal(cancelled, 1);
    }
    assert.equal(lateEnqueues, 0);
    assert.equal(lateCloses, 0);
    assert.deepEqual(unhandled, []);
  } finally {
    process.off("unhandledRejection", onUnhandled);
    t.mock.restoreAll();
    syncBuiltinESMExports();
    if (saved === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = saved;
  }
});
