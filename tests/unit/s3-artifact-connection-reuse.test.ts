import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, Agent } from "node:https";
import type { AddressInfo, Socket } from "node:net";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import selfsigned from "selfsigned";
import { ArtifactStorageError } from "../../src/domain/persistence/artifactStore.ts";
import { createS3ArtifactStore } from "../../src/lib/artifacts/s3ArtifactStore.ts";

const configuration = {
  bucket: "reuse-test",
  region: "eu-central-1",
  prefix: "test/",
  expectedBucketOwner: "123456789012",
};

for (const persisted of [false, true]) {
  test(`a reused TLS connection can reset immediately while its PUT outcome remains ${persisted ? "persisted" : "absent"}`, async () => {
    const certificate = await selfsigned.generate([{ name: "commonName", value: "localhost" }], {
      keySize: 2048,
      extensions: [{ name: "subjectAltName", altNames: [{ type: 7, ip: "127.0.0.1" }] }],
    });
    const sockets = new Set<Socket>();
    const requestSockets: Socket[] = [];
    const objects = new Map<string, Buffer>();
    let puts = 0;
    const server = createServer(
      { key: certificate.private, cert: certificate.cert },
      async (request, response) => {
        requestSockets.push(request.socket);
        const path = new URL(request.url!, "https://fixture.invalid").pathname;
        if (request.method === "PUT") {
          const chunks: Buffer[] = [];
          for await (const chunk of request) chunks.push(Buffer.from(chunk));
          const bytes = Buffer.concat(chunks);
          if (++puts === 2) {
            if (persisted) objects.set(path, bytes);
            // The entire request was read; a reset is not evidence that no write occurred.
            request.socket.destroy();
            return;
          }
          objects.set(path, bytes);
          response.writeHead(200, { "content-length": "0" });
          response.end();
          return;
        }
        assert.equal(request.method, "GET");
        const bytes = objects.get(path);
        if (bytes) {
          response.writeHead(200, { "content-length": String(bytes.byteLength) });
          response.end(bytes);
        } else {
          response.writeHead(404, { "content-type": "application/xml" });
          response.end("<Error><Code>NoSuchKey</Code></Error>");
        }
      }
    );
    server.on("connection", (socket) => {
      sockets.add(socket);
      socket.on("error", () => {});
      socket.once("close", () => sockets.delete(socket));
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const agent = new Agent({
      keepAlive: true,
      maxSockets: 1,
      maxTotalSockets: 1,
      ca: certificate.cert,
    });
    const options = {
      region: configuration.region,
      endpoint: `https://127.0.0.1:${(server.address() as AddressInfo).port}`,
      forcePathStyle: true,
      credentials: { accessKeyId: "REUSEFIXTURE", secretAccessKey: "synthetic-test-only" },
      requestHandler: {
        httpsAgent: agent,
        connectionTimeout: 1000,
        requestTimeout: 4000,
        socketTimeout: 4000,
        throwOnRequestTimeout: true,
      },
    };
    const writer = new S3Client({ ...options, maxAttempts: 1 });
    const reader = new S3Client({ ...options, maxAttempts: 3 });
    const store = createS3ArtifactStore(configuration, {
      loadClient: async () => ({
        send(command, options) {
          return (command instanceof PutObjectCommand ? writer : reader).send(command, options);
        },
        destroy() {
          writer.destroy();
          reader.destroy();
          agent.destroy();
        },
      }),
    });
    try {
      await store.put(Buffer.from("initial"));
      await delay(1);
      let reference = "";
      const started = performance.now();
      await assert.rejects(store.put(Buffer.from("uncertain")), (error) => {
        assert.ok(error instanceof ArtifactStorageError);
        assert.equal(error.kind, "put_unknown");
        assert.equal(error.diagnostic?.name, "TimeoutError");
        assert.equal(error.diagnostic?.code, "ECONNRESET");
        assert.equal(error.diagnostic?.timeoutSource, "transport");
        reference = error.reference!;
        return true;
      });
      assert.ok(
        performance.now() - started < 1000,
        "a reset does not require the 1s/4s timers to expire"
      );
      assert.equal(puts, 2, "the uncertain PUT is never replayed");
      assert.equal(requestSockets[0], requestSockets[1], "the connection was reused");
      assert.deepEqual(await store.get(reference), persisted ? Buffer.from("uncertain") : null);
    } finally {
      await store.close();
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve()))
      );
    }
  });
}

test("shared writer/reader handlers clear the previous request timer when reusing a TLS socket", async () => {
  const certificate = await selfsigned.generate([{ name: "commonName", value: "localhost" }], {
    keySize: 2048,
    extensions: [{ name: "subjectAltName", altNames: [{ type: 7, ip: "127.0.0.1" }] }],
  });
  const sockets = new Set<Socket>();
  const requestSockets: Socket[] = [];
  const server = createServer(
    { key: certificate.private, cert: certificate.cert },
    async (request, response) => {
      requestSockets.push(request.socket);
      for await (const _chunk of request) {
        /* Consume the complete SDK request. */
      }
      if (request.method === "GET") await delay(150);
      response.writeHead(200, { "content-length": "0" });
      response.end();
    }
  );
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("error", () => {});
    socket.once("close", () => sockets.delete(socket));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const agent = new Agent({
    keepAlive: true,
    maxSockets: 1,
    maxTotalSockets: 1,
    ca: certificate.cert,
  });
  const shared = {
    region: configuration.region,
    endpoint: `https://127.0.0.1:${(server.address() as AddressInfo).port}`,
    forcePathStyle: true,
    credentials: { accessKeyId: "REUSEFIXTURE", secretAccessKey: "synthetic-test-only" },
    maxAttempts: 1,
  };
  const writer = new S3Client({
    ...shared,
    requestHandler: {
      httpsAgent: agent,
      connectionTimeout: 1000,
      requestTimeout: 80,
      socketTimeout: 80,
      throwOnRequestTimeout: true,
    },
  });
  const reader = new S3Client({
    ...shared,
    requestHandler: {
      httpsAgent: agent,
      connectionTimeout: 1000,
      requestTimeout: 1000,
      socketTimeout: 1000,
      throwOnRequestTimeout: true,
    },
  });
  try {
    await writer.send(
      new PutObjectCommand({
        Bucket: configuration.bucket,
        Key: "timer",
        Body: Buffer.from("initial"),
      })
    );
    await delay(20);
    const response = await reader.send(
      new GetObjectCommand({ Bucket: configuration.bucket, Key: "timer" })
    );
    await response.Body?.transformToByteArray();
    assert.equal(response.$metadata.httpStatusCode, 200);
    assert.equal(requestSockets.length, 2);
    assert.equal(requestSockets[0], requestSockets[1]);
  } finally {
    writer.destroy();
    reader.destroy();
    agent.destroy();
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve()))
    );
  }
});
