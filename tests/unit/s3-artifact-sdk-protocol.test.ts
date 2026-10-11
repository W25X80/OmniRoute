import { ArtifactStorageError } from "../../src/domain/persistence/artifactStore.ts";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import test from "node:test";
import { S3Client } from "@aws-sdk/client-s3";
import {
  createS3ArtifactStore,
  type S3ArtifactTransport,
} from "../../src/lib/artifacts/s3ArtifactStore.ts";

const configuration = {
  bucket: "omniroute-private-artifacts",
  region: "eu-central-1",
  prefix: "omniroute/protocol/artifacts/",
  expectedBucketOwner: "123456789012",
};

type FixtureRequest = {
  method: string;
  protocol: string;
  hostname: string;
  path: string;
  headers: Record<string, string>;
  body?: unknown;
};

/** Exercises actual SDK REST/XML serialization with no socket or AWS credential lookup. */
function protocolFixture(losePutAcknowledgment = false) {
  let lost = false;
  const objects = new Map<string, Uint8Array>();
  const requests: FixtureRequest[] = [];
  let closed = false;
  const client = new S3Client({
    region: configuration.region,
    credentials: { accessKeyId: "ARTIFACTTESTKEY", secretAccessKey: "artifact-fixture-only-key" },
    maxAttempts: 3,
    ignoreConfiguredEndpointUrls: true,
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
    requestHandler: {
      async handle(request: FixtureRequest) {
        requests.push(request);
        assert.equal(request.protocol, "https:");
        assert.equal(
          request.headers["x-amz-expected-bucket-owner"],
          configuration.expectedBucketOwner
        );
        if (request.method === "PUT") {
          assert.equal(request.headers["if-none-match"], "*");
          assert.equal(request.headers["x-amz-server-side-encryption"], "AES256");
          assert.ok(request.body instanceof Uint8Array);
          const contents = Buffer.from(request.body);
          const expected = createHash("sha256").update(contents).digest("base64");
          assert.equal(request.headers["x-amz-checksum-sha256"], expected);
          assert.equal(request.headers["content-length"], String(contents.byteLength));
          if (objects.has(request.path)) {
            return {
              response: {
                statusCode: 412,
                headers: {},
                body: Readable.from(["<Error><Code>PreconditionFailed</Code></Error>"]),
              },
            };
          }
          objects.set(request.path, contents);
          if (losePutAcknowledgment && !lost) {
            lost = true;
            throw Object.assign(new Error("acknowledgment lost"), { name: "TimeoutError" });
          }
          return {
            response: {
              statusCode: 200,
              headers: { etag: '"fixture-etag"' },
              body: Readable.from([]),
            },
          };
        }
        if (request.method === "GET") {
          const contents = objects.get(request.path);
          if (!contents) {
            return {
              response: {
                statusCode: 404,
                headers: { "content-type": "application/xml" },
                body: Readable.from([
                  "<Error><Code>NoSuchKey</Code><Message>Missing</Message></Error>",
                ]),
              },
            };
          }
          return {
            response: {
              statusCode: 200,
              headers: {
                "content-length": String(contents.byteLength),
                "x-amz-checksum-sha256": createHash("sha256").update(contents).digest("base64"),
              },
              body: Readable.from([contents]),
            },
          };
        }
        assert.equal(request.method, "DELETE");
        objects.delete(request.path);
        return { response: { statusCode: 204, headers: {}, body: Readable.from([]) } };
      },
      destroy() {
        closed = true;
      },
    },
  });
  const transport: S3ArtifactTransport = {
    send(command, options) {
      return client.send(command, options) as ReturnType<S3ArtifactTransport["send"]>;
    },
    destroy() {
      client.destroy();
    },
  };
  return { transport, objects, requests, isClosed: () => closed };
}

test("SDK wire fixture preserves owner, conditional writes, checksums and private object identity", async () => {
  const fixture = protocolFixture();
  const store = createS3ArtifactStore(configuration, { loadClient: async () => fixture.transport });
  const bytes = Buffer.from('{"provider":"fixture","request":1}');
  const first = await store.put(bytes);
  const second = await store.put(bytes);
  assert.notEqual(first.reference, second.reference);
  assert.equal(fixture.objects.size, 2);
  assert.deepEqual(await store.get(first.reference), bytes);
  assert.equal(await store.delete(first.reference), true);
  assert.equal(await store.get(first.reference), null);
  assert.deepEqual(await store.get(second.reference), bytes);
  await store.close();
  assert.equal(fixture.isClosed(), true);
  assert.equal(
    fixture.requests.every((request) => request.hostname.endsWith("s3.eu-central-1.amazonaws.com")),
    true
  );
});

test("SDK retry after lost upload acknowledgment retains key and refuses overwrite", async () => {
  const fixture = protocolFixture(true);
  const store = createS3ArtifactStore(configuration, { loadClient: async () => fixture.transport });
  await assert.rejects(
    store.put(Buffer.from('{"request":"retry-fixture"}')),
    (error) => error instanceof ArtifactStorageError && error.kind === "put_unknown"
  );
  const puts = fixture.requests.filter((request) => request.method === "PUT");
  assert.equal(puts.length, 2);
  assert.equal(puts[0].path, puts[1].path);
  assert.equal(
    puts.every((request) => request.headers["if-none-match"] === "*"),
    true
  );
  assert.equal(fixture.objects.size, 1);
  assert.equal(
    fixture.requests.some((request) => request.method === "DELETE"),
    false
  );
  await store.close();
});

test("SDK ignores endpoint URL environment overrides in the private artifact profile", async () => {
  const originalService = process.env.AWS_ENDPOINT_URL_S3;
  const originalGlobal = process.env.AWS_ENDPOINT_URL;
  process.env.AWS_ENDPOINT_URL_S3 = "http://127.0.0.1:1/untrusted";
  process.env.AWS_ENDPOINT_URL = "http://127.0.0.1:1/untrusted";
  try {
    const fixture = protocolFixture();
    const store = createS3ArtifactStore(configuration, {
      loadClient: async () => fixture.transport,
    });
    await store.put(Buffer.from("private artifact"));
    assert.equal(fixture.requests[0].protocol, "https:");
    assert.equal(fixture.requests[0].hostname.endsWith("s3.eu-central-1.amazonaws.com"), true);
    await store.close();
  } finally {
    if (originalService === undefined) delete process.env.AWS_ENDPOINT_URL_S3;
    else process.env.AWS_ENDPOINT_URL_S3 = originalService;
    if (originalGlobal === undefined) delete process.env.AWS_ENDPOINT_URL;
    else process.env.AWS_ENDPOINT_URL = originalGlobal;
  }
});
