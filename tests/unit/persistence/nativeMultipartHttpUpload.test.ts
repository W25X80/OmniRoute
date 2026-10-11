import assert from "node:assert/strict";
import { once } from "node:events";
import files from "node:fs/promises";
import http from "node:http";
import { registerHooks } from "node:module";
import type { Socket } from "node:net";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import test from "node:test";

const directory = await files.mkdtemp(path.join(os.tmpdir(), "native-file-http-"));
const environment = new Map(
  ["DATA_DIR", "APP_LOG_TO_FILE", "OMNIROUTE_DATABASE_BACKEND"].map((name) => [
    name,
    process.env[name],
  ])
);
process.env.DATA_DIR = directory;
process.env.APP_LOG_TO_FILE = "false";
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
const { fileRouteFixture } = await import("../../helpers/persistence/nativeFileRouteFixture");
const { CORS_HEADERS } = await import("../../../src/shared/utils/cors");
const { getBodySizeLimit, MAX_BODY_BYTES_FILE } =
  await import("../../../src/shared/middleware/bodySizeGuard");
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
  await files.rm(directory, { recursive: true, force: true });
  for (const [name, value] of environment)
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
});
function multipart(text: string) {
  return Buffer.from(
    '--fixture\r\nContent-Disposition: form-data; name="purpose"\r\n\r\nbatch\r\n' +
      '--fixture\r\nContent-Disposition: form-data; name="file"; filename="input.jsonl"\r\nContent-Type: application/jsonl\r\n\r\n' +
      text +
      "\r\n--fixture--\r\n"
  );
}
async function exchange(body: Buffer, delayed: boolean) {
  fileRouteFixture.records.length = 0;
  fileRouteFixture.paths.length = 0;
  fileRouteFixture.settingsCalls = 0;
  const sockets = new Set<Socket>();
  let handled!: () => void;
  const handlerDone = new Promise<void>((resolve) => {
    handled = resolve;
  });
  let failure: unknown;
  let retirement: { complete: boolean; readableEnded: boolean; destroyed: boolean } | undefined;
  const server = http.createServer(async (incoming, outgoing) => {
    try {
      const request = new Request("http://127.0.0.1/v1/files", {
        method: "POST",
        headers: { "content-type": String(incoming.headers["content-type"]) },
        body: Readable.toWeb(incoming),
        duplex: "half",
      } as RequestInit & { duplex: "half" });
      const response = await POST(request);
      retirement = {
        complete: incoming.complete,
        readableEnded: incoming.readableEnded,
        destroyed: incoming.destroyed,
      };
      outgoing.writeHead(response.status, Object.fromEntries(response.headers));
      outgoing.end(Buffer.from(await response.arrayBuffer()));
    } catch (error) {
      failure = error;
      outgoing.destroy();
    } finally {
      handled();
    }
  });
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  let client: http.ClientRequest | undefined;
  try {
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const response = await new Promise<{
      status: number;
      headers: http.IncomingHttpHeaders;
      text: string;
    }>((resolve, reject) => {
      client = http.request(
        {
          host: "127.0.0.1",
          port: address.port,
          path: "/v1/files",
          method: "POST",
          headers: {
            "content-type": "multipart/form-data; boundary=fixture",
            "transfer-encoding": "chunked",
          },
        },
        (reply) => {
          const chunks: Buffer[] = [];
          reply.on("data", (chunk) => chunks.push(chunk));
          reply.once("error", reject);
          reply.once("end", () =>
            resolve({
              status: reply.statusCode!,
              headers: reply.headers,
              text: Buffer.concat(chunks).toString(),
            })
          );
        }
      );
      client.once("error", reject);
      if (delayed) {
        const split = body.length - 15;
        client.write(body.subarray(0, split));
        void handlerDone.then(() => {
          if (!client!.destroyed) client!.end(body.subarray(split));
        });
      } else client.end(body);
    });
    await handlerDone;
    if (failure) throw failure;
    return { ...response, retirement };
  } finally {
    client?.destroy();
    if (server.listening) {
      const closed = new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve()))
      );
      server.closeAllConnections();
      for (const socket of sockets) socket.destroy();
      await closed;
    }
  }
}

test("native actual HTTP multipart upload preserves file bytes, metadata, and the source 500 MiB policy", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  assert.equal(getBodySizeLimit("/api/v1/files", { maxBodySizeMb: 1 }), MAX_BODY_BYTES_FILE);
  assert.equal(MAX_BODY_BYTES_FILE, 500 * 1024 * 1024);
  const response = await exchange(multipart('{"value":1}\n'), false);
  assert.equal(response.status, 200);
  assert.deepEqual(fileRouteFixture.paths, ["/api/v1/files"]);
  const stored = fileRouteFixture.records[0];
  assert.equal(stored.content?.toString(), '{"value":1}\n');
  assert.equal(stored.filename, "input.jsonl");
  assert.equal(stored.mimeType, "application/jsonl");
  assert.equal(stored.purpose, "batch");
  assert.equal(stored.apiKeyId, "scope-owner");
});

test("native delayed chunked HTTP overflow retires an incomplete input and returns existing 413/CORS without storage", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const response = await exchange(multipart("x".repeat(2048)), true);
  assert.equal(response.status, 413);
  assert.equal(JSON.parse(response.text).error.code, "PAYLOAD_TOO_LARGE");
  assert.deepEqual(response.retirement, { complete: false, readableEnded: false, destroyed: true });
  assert.equal(fileRouteFixture.records.length, 0);
  assert.deepEqual(fileRouteFixture.paths, ["/api/v1/files"]);
  for (const [name, value] of Object.entries(CORS_HEADERS))
    assert.equal(response.headers[name.toLowerCase()], value);
});
