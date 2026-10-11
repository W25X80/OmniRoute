import assert from "node:assert/strict";
import test from "node:test";
import {
  readFormDataWithLimit,
  RequestBodyTooLargeError,
  createBodyTooLargeResponse,
  getBodySizeLimit,
  MAX_BODY_BYTES_FILE,
} from "../../../src/shared/middleware/bodySizeGuard";

function multipart(payload: string) {
  return new TextEncoder().encode(
    `--fixture\r\nContent-Disposition: form-data; name="purpose"\r\n\r\nbatch\r\n--fixture\r\nContent-Disposition: form-data; name="file"; filename="input.jsonl"\r\nContent-Type: application/jsonl\r\n\r\n${payload}\r\n--fixture--\r\n`
  );
}
function request(
  bytes: Uint8Array,
  options: { declared?: string; chunk?: number; signal?: AbortSignal } = {}
) {
  let offset = 0;
  let canceled = 0;
  const headers = new Headers({ "content-type": "multipart/form-data; boundary=fixture" });
  if (options.declared !== undefined) headers.set("content-length", options.declared);
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (offset >= bytes.length) {
        controller.close();
        return;
      }
      const end = Math.min(offset + (options.chunk ?? 11), bytes.length);
      controller.enqueue(bytes.subarray(offset, end));
      offset = end;
    },
    cancel() {
      canceled++;
    },
  });
  return {
    request: new Request("http://localhost/v1/files", {
      method: "POST",
      headers,
      body: stream,
      signal: options.signal,
      duplex: "half",
    } as RequestInit & { duplex: "half" }),
    canceled: () => canceled,
    readBytes: () => offset,
  };
}

test("exact actual multipart byte budget preserves file bytes and purpose without Content-Length", async () => {
  const bytes = multipart('{"value":1}\n');
  const input = request(bytes);
  const parsed = await readFormDataWithLimit(input.request, bytes.length);
  assert.equal(parsed.get("purpose"), "batch");
  const file = parsed.get("file") as File;
  assert.equal(file.name, "input.jsonl");
  assert.equal(await file.text(), '{"value":1}\n');
  assert.equal(input.canceled(), 0);
});
for (const declared of [undefined, "1", "invalid"]) {
  test(`actual byte overflow rejects and cancels ${declared === undefined ? "headerless" : "misdeclared"} multipart`, async () => {
    const bytes = multipart("large payload".repeat(50));
    const input = request(bytes, { declared, chunk: 17 });
    await assert.rejects(
      readFormDataWithLimit(input.request, Math.floor(bytes.length / 2)),
      RequestBodyTooLargeError
    );
    assert.equal(input.canceled(), 1);
    assert.ok(input.readBytes() < bytes.length);
  });
}
test("declared header overflow rejects before any body read and preserves the existing 413 body", async () => {
  const input = request(multipart("valid"), { declared: "1000" });
  await assert.rejects(readFormDataWithLimit(input.request, 100), RequestBodyTooLargeError);
  assert.equal(input.canceled(), 1);
  assert.equal(input.readBytes(), 0);
  const response = createBodyTooLargeResponse(100);
  assert.equal(response.status, 413);
  assert.equal((await response.json()).error.code, "PAYLOAD_TOO_LARGE");
});
test("malformed MIME retains the source parser failure for a finite body within budget", async () => {
  const create = () =>
    new Request("http://localhost/v1/files", {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: "not multipart",
    });
  await assert.rejects(create().formData(), TypeError);
  await assert.rejects(readFormDataWithLimit(create(), 100), TypeError);
});

test("request abort cancels the owned stream rather than leaving an unbounded parser read", async () => {
  const signal = new AbortController();
  let canceled = false;
  let started!: () => void;
  const entered = new Promise<void>((resolve) => {
    started = resolve;
  });
  const input = new Request("http://localhost/v1/files", {
    method: "POST",
    headers: { "content-type": "multipart/form-data; boundary=fixture" },
    body: new ReadableStream({
      pull() {
        started();
      },
      cancel() {
        canceled = true;
      },
    }),
    signal: signal.signal,
    duplex: "half",
  } as RequestInit & { duplex: "half" });
  const parsing = readFormDataWithLimit(input, 100);
  await entered;
  signal.abort();
  await assert.rejects(parsing);
  assert.equal(canceled, true);
});
test("file route keeps its existing canonical policy floor instead of introducing a 512 MiB request limit", () => {
  assert.equal(getBodySizeLimit("/api/v1/files", { maxBodySizeMb: 1 }), MAX_BODY_BYTES_FILE);
  assert.equal(MAX_BODY_BYTES_FILE, 500 * 1024 * 1024);
});
