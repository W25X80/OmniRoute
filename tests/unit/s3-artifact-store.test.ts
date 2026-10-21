import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { ArtifactStorageError } from "../../src/domain/persistence/artifactStore.ts";
import {
  createS3ArtifactStore,
  artifactStorageDiagnostic,
  MAX_ARTIFACT_BYTES,
  type S3ArtifactTransport,
} from "../../src/lib/artifacts/s3ArtifactStore.ts";
import { parseS3ArtifactConfiguration } from "../../src/lib/artifacts/s3ArtifactConfiguration.ts";

const configuration = {
  bucket: "omniroute-private-artifacts",
  region: "eu-central-1",
  prefix: "omniroute/test/artifacts/",
  expectedBucketOwner: "123456789012",
};
const bytes = Buffer.from('{"request":"bounded artifact"}');
const checksum = createHash("sha256").update(bytes).digest("hex");
const reference = `s3/01234567-89ab-4cde-8fab-0123456789ab_${checksum}.json`;

function storageError(kind: ArtifactStorageError["kind"]) {
  return (error: unknown) => error instanceof ArtifactStorageError && error.kind === kind;
}

function fakeTransport(
  send: S3ArtifactTransport["send"]
): S3ArtifactTransport & { destroyed: boolean } {
  return {
    async send(command, options) {
      const output = await send(command, options);
      return {
        ...output,
        $metadata: {
          httpStatusCode: command instanceof DeleteObjectCommand ? 204 : 200,
          ...output.$metadata,
        },
      };
    },
    destroyed: false,
    destroy() {
      this.destroyed = true;
    },
  };
}

function objectBody(contents: Uint8Array = bytes) {
  const body = Readable.from([contents]);
  return { Body: body, ContentLength: contents.byteLength, $metadata: { httpStatusCode: 200 } };
}

test("configuration rejects endpoint/credential injection and ambiguous ownership", () => {
  for (const patch of [
    { endpoint: "http://untrusted.invalid" },
    { credentials: { accessKeyId: "secret" } },
    { expectedBucketOwner: "" },
    { prefix: "../other/" },
    { prefix: "omniroute/test" },
    { bucket: "directory--x-s3" },
    { bucket: "foreign-bucket.s3.amazonaws.com" },
  ])
    assert.throws(
      () => parseS3ArtifactConfiguration({ ...configuration, ...patch }),
      storageError("configuration")
    );
});

test("unused close loads no SDK client and rejects later writes", async () => {
  let calls = 0;
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () => {
      calls++;
      return fakeTransport(async () => ({ $metadata: {} }));
    },
  });
  await store.close();
  assert.equal(calls, 0);
  await assert.rejects(store.put(bytes), storageError("closed"));
});

test("independent writes of identical bytes have immutable different references", async () => {
  const inputs: PutObjectCommand["input"][] = [];
  const transport = fakeTransport(async (command) => {
    assert.ok(command instanceof PutObjectCommand);
    inputs.push(command.input);
    return { $metadata: { httpStatusCode: 200 } };
  });
  const store = createS3ArtifactStore(configuration, { loadClient: async () => transport });
  const first = await store.put(bytes);
  const second = await store.put(bytes);
  assert.notEqual(first.reference, second.reference);
  assert.equal(first.checksum, checksum);
  assert.equal(second.checksum, checksum);
  for (const input of inputs) {
    assert.equal(input.IfNoneMatch, "*");
    assert.equal(input.ExpectedBucketOwner, configuration.expectedBucketOwner);
    assert.equal(input.ServerSideEncryption, "AES256");
    assert.equal(input.ContentLength, bytes.byteLength);
    assert.equal(input.ChecksumSHA256, Buffer.from(checksum, "hex").toString("base64"));
    assert.equal(input.Key?.startsWith(configuration.prefix + "s3/"), true);
  }
  await store.close();
  assert.equal(transport.destroyed, true);
});

test("put snapshots caller bytes before the first asynchronous boundary", async () => {
  let input: PutObjectCommand["input"] | undefined;
  const transport = fakeTransport(async (command) => {
    assert.ok(command instanceof PutObjectCommand);
    input = command.input;
    return { $metadata: { httpStatusCode: 200 } };
  });
  const store = createS3ArtifactStore(configuration, { loadClient: async () => transport });
  const mutable = Buffer.from(bytes);
  const pending = store.put(mutable);
  mutable.fill(0);
  const stored = await pending;
  assert.equal(stored.checksum, checksum);
  assert.deepEqual(input?.Body, bytes);
  await store.close();
});

test("upload failure exposes uncertainty without invoking object deletion", async () => {
  const commands: string[] = [];
  const transport = fakeTransport(async (command) => {
    commands.push(command.constructor.name);
    throw new Error("network credentials=secret path=/internal");
  });
  const store = createS3ArtifactStore(configuration, { loadClient: async () => transport });
  await assert.rejects(store.put(bytes), (error) => {
    assert.ok(error instanceof ArtifactStorageError);
    assert.equal(error.kind, "put_unknown");
    assert.equal(error.message, "Artifact upload outcome is unknown");
    assert.equal(error.reference?.startsWith("s3/"), true);
    return true;
  });
  assert.deepEqual(commands, ["PutObjectCommand"]);
  await store.close();
});

test("ninth concurrent operation rejects before transport, close drains admitted work", async () => {
  const resolvers: Array<() => void> = [];
  let sent = 0;
  const transport = fakeTransport(async () => {
    sent++;
    await new Promise<void>((resolve) => resolvers.push(resolve));
    return { $metadata: { httpStatusCode: 204 } };
  });
  const store = createS3ArtifactStore(configuration, { loadClient: async () => transport });
  const pending = Array.from({ length: 8 }, () => store.delete(reference));
  await assert.rejects(store.delete(reference), storageError("overloaded"));
  for (let i = 0; i < 20 && sent < 8; i++) await delay(1);
  assert.equal(sent, 8);
  let closed = false;
  const closing = store.close().then(() => {
    closed = true;
  });
  await assert.rejects(store.get(reference), storageError("closed"));
  await delay(1);
  assert.equal(closed, false);
  for (const resolve of resolvers) resolve();
  await Promise.all(pending);
  await closing;
  assert.equal(transport.destroyed, true);
});

test("deadline aborts requests and keeps opaque unresolved work within admission cap", async () => {
  const resolvers: Array<() => void> = [];
  let aborted = 0;
  const transport = fakeTransport(async (_, { abortSignal }) => {
    abortSignal.addEventListener("abort", () => aborted++, { once: true });
    await new Promise<void>((resolve) => resolvers.push(resolve));
    return { $metadata: {} };
  });
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () => transport,
    deadlineMs: 30,
  });
  const pending = Array.from({ length: 8 }, () => store.delete(reference));
  const settled = await Promise.allSettled(pending);
  assert.equal(
    settled.every(
      (result) => result.status === "rejected" && storageError("deadline")(result.reason)
    ),
    true
  );
  assert.equal(aborted, 8);
  await assert.rejects(store.delete(reference), storageError("overloaded"));
  for (const resolve of resolvers) resolve();
  await store.close();
});

test("close refuses to certify drainage of a transport that ignores abort", async () => {
  let release!: () => void;
  const transport = fakeTransport(async () => {
    await new Promise<void>((resolve) => {
      release = resolve;
    });
    return { $metadata: {} };
  });
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () => transport,
    deadlineMs: 20,
  });
  await assert.rejects(store.delete(reference), storageError("deadline"));
  await assert.rejects(store.close(), storageError("deadline"));
  assert.equal(transport.destroyed, true);
  release();
});

test("valid get verifies body length and reference checksum", async () => {
  const transport = fakeTransport(async (command) => {
    assert.ok(command instanceof GetObjectCommand);
    assert.equal(command.input.ExpectedBucketOwner, configuration.expectedBucketOwner);
    assert.equal(command.input.ChecksumMode, "ENABLED");
    return objectBody();
  });
  const store = createS3ArtifactStore(configuration, { loadClient: async () => transport });
  assert.deepEqual(await store.get(reference), bytes);
  await store.close();
});

test("corrupt and oversized body failures close response streams", async () => {
  for (const response of [
    { ...objectBody(Buffer.from("corrupt")) },
    { ...objectBody(), ContentLength: 2 },
    { ...objectBody(), ContentLength: MAX_ARTIFACT_BYTES + 1 },
    { ...objectBody(), ContentLength: undefined },
    { ...objectBody(), ChecksumSHA256: "invalid" },
  ]) {
    const transport = fakeTransport(async () => response);
    const store = createS3ArtifactStore(configuration, { loadClient: async () => transport });
    await assert.rejects(store.get(reference), storageError("integrity"));
    assert.equal(response.Body.destroyed, true);
    await store.close();
  }
});

test("string chunks are rejected and the body is destroyed", async () => {
  const body = Readable.from(["not binary"]);
  const transport = fakeTransport(async () => ({ Body: body, ContentLength: 10, $metadata: {} }));
  const store = createS3ArtifactStore(configuration, { loadClient: async () => transport });
  await assert.rejects(store.get(reference), storageError("integrity"));
  assert.equal(body.destroyed, true);
  await store.close();
});

test("a late GET response after deadline is discarded", async () => {
  let release!: () => void;
  const response = objectBody();
  const transport = fakeTransport(async () => {
    await new Promise<void>((resolve) => {
      release = resolve;
    });
    return response;
  });
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () => transport,
    deadlineMs: 20,
  });
  await assert.rejects(store.get(reference), storageError("deadline"));
  release();
  await store.close();
  assert.equal(response.Body.destroyed, true);
});

test("only positive object absence is null or false; authorization/bucket failures propagate", async () => {
  const missing = Object.assign(new Error("missing"), {
    name: "NoSuchKey",
    $metadata: { httpStatusCode: 404 },
  });
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () =>
      fakeTransport(async () => {
        throw missing;
      }),
  });
  assert.equal(await store.get(reference), null);
  assert.equal(await store.delete(reference), false);
  await store.close();
  for (const name of ["AccessDenied", "NoSuchBucket", "NotFound"]) {
    const error = Object.assign(new Error("internal detail"), {
      name,
      $metadata: { httpStatusCode: name === "AccessDenied" ? 403 : 404 },
    });
    const other = createS3ArtifactStore(configuration, {
      loadClient: async () =>
        fakeTransport(async () => {
          throw error;
        }),
    });
    await assert.rejects(other.get(reference), storageError("unavailable"));
    await assert.rejects(other.delete(reference), storageError("unavailable"));
    await other.close();
  }
});

test("invalid references and excessive writes make no transport request", async () => {
  let requests = 0;
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () =>
      fakeTransport(async () => {
        requests++;
        return { $metadata: {} };
      }),
  });
  for (const bad of [
    "../secrets.json",
    reference + "?versionId=secret",
    "s3/" + "a".repeat(300),
    "s3://foreign/key",
  ]) {
    await assert.rejects(store.get(bad), storageError("invalid_reference"));
    await assert.rejects(store.delete(bad), storageError("invalid_reference"));
  }
  await assert.rejects(store.put(Buffer.alloc(MAX_ARTIFACT_BYTES + 1)), storageError("too_large"));
  assert.equal(requests, 0);
  await store.close();
});

test("KMS encryption is scoped to explicit configured key", async () => {
  const kmsKeyId = "arn:aws:kms:eu-central-1:123456789012:key/01234567-89ab-4cde-8fab-0123456789ab";
  const transport = fakeTransport(async (command) => {
    assert.ok(command instanceof PutObjectCommand);
    assert.equal(command.input.ServerSideEncryption, "aws:kms");
    assert.equal(command.input.SSEKMSKeyId, kmsKeyId);
    assert.equal(command.input.BucketKeyEnabled, true);
    return { $metadata: {} };
  });
  const store = createS3ArtifactStore(
    { ...configuration, kmsKeyId },
    { loadClient: async () => transport }
  );
  await store.put(bytes);
  await store.close();
});

test("web stream deadline cancels the body and releases its reader", async () => {
  let canceled = false;
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
    },
    cancel() {
      canceled = true;
    },
  });
  const transport = fakeTransport(async () => ({
    Body: body,
    ContentLength: bytes.byteLength,
    $metadata: {},
  }));
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () => transport,
    deadlineMs: 20,
  });
  await assert.rejects(store.get(reference), storageError("deadline"));
  await store.close();
  assert.equal(canceled, true);
  assert.equal(body.locked, false);
});

test("late upload acknowledgment leaves an uncertain immutable owned identity", async () => {
  let release!: () => void;
  const commands: string[] = [];
  const transport = fakeTransport(async (command) => {
    commands.push(command.constructor.name);
    await new Promise<void>((resolve) => {
      release = resolve;
    });
    return { $metadata: { httpStatusCode: 200 } };
  });
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () => transport,
    deadlineMs: 20,
  });
  await assert.rejects(store.put(bytes), (error) => {
    assert.ok(error instanceof ArtifactStorageError);
    assert.equal(error.kind, "put_unknown");
    assert.equal(error.reference?.endsWith(`_${checksum}.json`), true);
    return true;
  });
  release();
  await store.close();
  assert.deepEqual(commands, ["PutObjectCommand"]);
});

test("a resolved response without positive acknowledgment cannot publish or acknowledge deletion", async () => {
  const response = { ...objectBody(), $metadata: { httpStatusCode: 403 } };
  const transport = fakeTransport(async () => response);
  const store = createS3ArtifactStore(configuration, { loadClient: async () => transport });
  await assert.rejects(store.put(bytes), storageError("put_unknown"));
  await assert.rejects(store.delete(reference), storageError("unavailable"));
  await assert.rejects(store.get(reference), storageError("unavailable"));
  assert.equal(response.Body.destroyed, true);
  await store.close();
});

test("durable preparation cannot consume the independent transport budget", async () => {
  let sends = 0;
  const store = createS3ArtifactStore(configuration, {
    deadlineMs: 100,
    loadClient: async () =>
      fakeTransport(async () => {
        sends++;
        await delay(60);
        return { $metadata: { httpStatusCode: 200 } };
      }),
  });
  await store.warmup!();
  try {
    const stored = await store.put(bytes, {
      async onPrepared() {
        await delay(60);
      },
    });
    assert.equal(stored.checksum, checksum);
    assert.equal(sends, 1);
  } finally {
    await store.close();
  }
});

test("expired preparation cannot dispatch before a delayed timer callback runs", async () => {
  let sends = 0;
  const store = createS3ArtifactStore(configuration, {
    deadlineMs: 20,
    loadClient: async () =>
      fakeTransport(async () => {
        sends++;
        return { $metadata: { httpStatusCode: 200 } };
      }),
  });
  await store.warmup!();
  try {
    await assert.rejects(
      store.put(bytes, {
        async onPrepared() {
          const until = performance.now() + 30;
          while (performance.now() < until) {
            /* Hold the timer task behind this microtask. */
          }
        },
      }),
      (error) => {
        assert.ok(error instanceof ArtifactStorageError);
        assert.equal(error.kind, "deadline");
        assert.equal(error.putStarted, false);
        return true;
      }
    );
    assert.equal(sends, 0);
  } finally {
    await store.close();
  }
});

test("SDK timer provenance is a fixed safe enum without retaining raw messages", () => {
  for (const [message, expected] of [
    [
      "@smithy/node-http-handler - the request socket did not establish a connection with the server within the configured timeout of 1000 ms.",
      "connect",
    ],
    [
      "@smithy/node-http-handler - [ERROR] a request has exceeded the configured 4000 ms requestTimeout.",
      "request",
    ],
    [
      "@smithy/node-http-handler - the request socket timed out after 4000 ms of inactivity (configured by client requestHandler).",
      "socket",
    ],
    ["Bearer private credential https://private.invalid/object", "transport"],
  ]) {
    const error = Object.assign(new Error(message), {
      name: "TimeoutError",
      code: "ETIMEDOUT",
      $metadata: { requestId: "safe-123" },
    });
    const diagnostic = artifactStorageDiagnostic(error);
    assert.equal(diagnostic.timeoutSource, expected);
    assert.equal(diagnostic.code, "ETIMEDOUT");
    assert.equal(diagnostic.requestId, "safe-123");
    assert.equal(JSON.stringify(diagnostic).includes(message), false);
  }
});

test("unknown PUT retains one dispatch and exposes only content-free phase and pool gauges", async () => {
  let sends = 0;
  const transport = fakeTransport(async () => {
    sends++;
    throw Object.assign(
      new Error(
        "@smithy/node-http-handler - [ERROR] a request has exceeded the configured 4000 ms requestTimeout. secret=private"
      ),
      {
        name: "TimeoutError",
        code: "ETIMEDOUT",
      }
    );
  });
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () => ({
      ...transport,
      snapshot: () => ({ activeSockets: 3, queuedRequests: 2 }),
    }),
  });
  await assert.rejects(store.put(bytes), (error) => {
    assert.ok(error instanceof ArtifactStorageError);
    assert.equal(error.kind, "put_unknown");
    assert.equal(error.putStarted, true);
    assert.equal(error.diagnostic?.timeoutSource, "request");
    assert.equal(error.diagnostic?.operation, "put");
    assert.equal(error.diagnostic?.phase, "transport");
    assert.equal(error.diagnostic?.sizeBytes, bytes.byteLength);
    assert.equal(error.diagnostic?.activeSockets, 3);
    assert.equal(error.diagnostic?.queuedRequests, 2);
    assert.equal(error.diagnostic?.activeOperations, 1);
    assert.equal(Number.isSafeInteger(error.diagnostic?.elapsedMs), true);
    assert.equal(JSON.stringify(error.diagnostic).includes("private"), false);
    assert.equal(JSON.stringify(error.diagnostic).includes(configuration.bucket), false);
    return true;
  });
  assert.equal(sends, 1);
  await store.close();
});

for (const method of ["get", "delete"] as const) {
  test(`${method} keeps its original single budget and rejects a response after a blocked loop`, async () => {
    const response = objectBody();
    const transport = fakeTransport(async () => {
      const until = performance.now() + 30;
      while (performance.now() < until) {
        /* Delay the operation timer task. */
      }
      return response;
    });
    const store = createS3ArtifactStore(configuration, {
      deadlineMs: 20,
      loadClient: async () => transport,
    });
    await store.warmup!();
    await assert.rejects(store[method](reference), (error) => {
      assert.ok(error instanceof ArtifactStorageError);
      assert.equal(error.kind, "deadline");
      assert.equal(error.diagnostic?.operation, method);
      assert.equal(error.diagnostic?.phase, "transport");
      assert.ok((error.diagnostic?.transportMs ?? 0) >= 30);
      return true;
    });
    if (method === "get") assert.equal(response.Body.destroyed, true);
    await store.close();
  });
}

test(
  "reentrant close cannot succeed before the admitted PUT settles",
  { timeout: 1000 },
  async () => {
    let sends = 0;
    let closeSucceeded = false;
    let closing: Promise<void> | undefined;
    let preparedReference = "";
    const store = createS3ArtifactStore(configuration, {
      deadlineMs: 20,
      loadClient: async () =>
        fakeTransport(async () => {
          sends++;
          return { $metadata: { httpStatusCode: 200 } };
        }),
    });
    const put = store.put(bytes, {
      async onPrepared(prepared) {
        preparedReference = prepared.reference;
        closing = store.close();
        void closing.then(
          () => {
            closeSucceeded = true;
          },
          () => {}
        );
        await closing;
      },
    });
    await assert.rejects(put, (error) => {
      assert.ok(error instanceof ArtifactStorageError);
      assert.equal(error.kind, "deadline");
      assert.equal(error.putStarted, false);
      return true;
    });
    assert.equal(closeSucceeded, false);
    assert.equal(
      store.isPutActive!(preparedReference),
      true,
      "the public deadline does not release raw work"
    );
    await assert.rejects(closing!, storageError("deadline"));
    await delay(0);
    assert.equal(store.isPutActive!(preparedReference), false);
    assert.equal(closeSucceeded, false);
    assert.equal(sends, 0);
    await assert.rejects(store.put(bytes), storageError("closed"));
  }
);

test("reentrant preparation cannot exceed the eight-operation raw admission cap", async () => {
  let preparations = 0;
  let sends = 0;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const nested: Array<Promise<unknown>> = [];
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () =>
      fakeTransport(async () => {
        sends++;
        return { $metadata: { httpStatusCode: 200 } };
      }),
  });
  const onPrepared = async () => {
    preparations++;
    if (preparations <= 8) {
      const child = store.put(bytes, { onPrepared });
      void child.catch(() => {});
      nested.push(child);
    }
    await gate;
  };
  const first = store.put(bytes, { onPrepared });
  const completion = Promise.allSettled([first, ...nested]);
  try {
    assert.equal(preparations, 8);
    await assert.rejects(nested[0], storageError("overloaded"));
    assert.equal(sends, 0);
  } finally {
    release();
    await completion;
    await store.close();
  }
  const results = await completion;
  assert.equal(results.filter((result) => result.status === "fulfilled").length, 8);
  assert.equal(results.filter((result) => result.status === "rejected").length, 1);
  assert.equal(sends, 8);
});
