import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import files from "node:fs/promises";
import os from "node:os";
import path from "node:path";
const testDirectory = await files.mkdtemp(path.join(os.tmpdir(), "native-file-route-"));
const oldEnvironment = new Map(
  ["DATA_DIR", "APP_LOG_TO_FILE", "OMNIROUTE_DATABASE_BACKEND"].map((name) => [
    name,
    process.env[name],
  ])
);
process.env.DATA_DIR = testDirectory;
process.env.APP_LOG_TO_FILE = "false";
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
const { fileRouteFixture } = await import("../../helpers/persistence/nativeFileRouteFixture");
const { CORS_HEADERS } = await import("../../../src/shared/utils/cors");
const fixture = new URL("../../helpers/persistence/nativeFileRouteFixture.ts", import.meta.url)
  .href;
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (
      context.parentURL?.endsWith("/src/app/api/v1/files/route.ts") &&
      [
        "@/lib/db/files",
        "@/lib/db/readCache",
        "@/shared/middleware/bodySizeGuard",
        "@/app/api/v1/_helpers/apiKeyScope",
        "@/shared/utils/apiKeyPolicy",
      ].includes(specifier)
    )
      return { url: fixture, shortCircuit: true };
    return next(specifier, context);
  },
});
const { POST } = await import("../../../src/app/api/v1/files/route");
hooks.deregister();
test.after(async () => {
  (await import("../../../src/lib/db/core")).resetDbInstance();
  await files.rm(testDirectory, { recursive: true, force: true });
  for (const [name, value] of oldEnvironment)
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
});
test.beforeEach(() => {
  fileRouteFixture.records.length = 0;
  fileRouteFixture.paths.length = 0;
  fileRouteFixture.policies.length = 0;
  fileRouteFixture.settingsCalls = 0;
});
function upload(text: string) {
  const boundary = "file-route-fixture";
  const bytes = new TextEncoder().encode(
    `--${boundary}\r\nContent-Disposition: form-data; name="purpose"\r\n\r\nbatch\r\n` +
      `--${boundary}\r\nContent-Disposition: form-data; name="expires_after[anchor]"\r\n\r\ncreated_at\r\n` +
      `--${boundary}\r\nContent-Disposition: form-data; name="expires_after[seconds]"\r\n\r\n60\r\n` +
      `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="input.jsonl"\r\nContent-Type: application/jsonl\r\n\r\n${text}\r\n--${boundary}--\r\n`
  );
  let offset = 0;
  return new Request("http://localhost/v1/files", {
    method: "POST",
    headers: { "content-type": `multipart/form-data; boundary=${boundary}` },
    body: new ReadableStream<Uint8Array>({
      pull(controller) {
        if (offset === bytes.length) return controller.close();
        const end = Math.min(offset + 97, bytes.length);
        controller.enqueue(bytes.subarray(offset, end));
        offset = end;
      },
    }),
    duplex: "half",
  } as RequestInit & { duplex: "half" });
}

test("native upload uses the canonical existing limit and retains file bytes/metadata/key attribution", async (t) => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  t.mock.method(Date, "now", () => 1700000000000);
  const request = upload('{"value":1}\n');
  const response = await POST(request);
  assert.equal(response.status, 200);
  assert.deepEqual(fileRouteFixture.paths, ["/api/v1/files"]);
  assert.equal(fileRouteFixture.settingsCalls, 1);
  assert.equal(fileRouteFixture.policies[0], request);
  const stored = fileRouteFixture.records[0];
  assert.equal(stored.content?.toString(), '{"value":1}\n');
  assert.equal(stored.filename, "input.jsonl");
  assert.equal(stored.mimeType, "application/jsonl");
  assert.equal(stored.apiKeyId, "scope-owner");
  assert.equal(stored.expiresAt, 1700000060);
});
test("native actual-byte overflow returns existing 413/CORS and never calls the file repository", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const response = await POST(upload("x".repeat(2048)));
  assert.equal(response.status, 413);
  assert.equal((await response.json()).error.code, "PAYLOAD_TOO_LARGE");
  for (const [name, value] of Object.entries(CORS_HEADERS))
    assert.equal(response.headers.get(name), value);
  assert.equal(fileRouteFixture.records.length, 0);
});
test("default SQLite upload retains its parser path and performs no native limit-settings lookup", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  const response = await POST(upload("x".repeat(2048)));
  assert.equal(response.status, 200);
  assert.equal(fileRouteFixture.records.length, 1);
  assert.equal(fileRouteFixture.settingsCalls, 0);
  assert.deepEqual(fileRouteFixture.paths, []);
});
test("native malformed MIME keeps source upload error status and sanitized body", async (t) => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  t.mock.method(console, "error", () => {});
  const response = await POST(
    new Request("http://localhost/v1/files", {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: "invalid multipart",
    })
  );
  assert.equal(response.status, 500);
  assert.deepEqual(await response.json(), {
    error: { message: "Upload failed", type: "server_error" },
  });
  assert.equal(fileRouteFixture.records.length, 0);
});
