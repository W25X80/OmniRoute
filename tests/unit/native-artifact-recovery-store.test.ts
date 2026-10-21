import assert from "node:assert/strict";
import test from "node:test";
import { PutObjectCommand } from "@aws-sdk/client-s3";
import { isArtifactStorageErrorKind } from "../../src/domain/persistence/artifactStore.ts";
import { createS3ArtifactStore } from "../../src/lib/artifacts/s3ArtifactStore.ts";
const configuration = {
  bucket: "artifact-recovery-test",
  region: "eu-central-1",
  prefix: "test/",
  expectedBucketOwner: "123456789012",
};

test("cold client timeout is bounded and cannot dispatch after late preparation", async () => {
  let resolve!: (transport: { send(): Promise<never>; destroy(): void }) => void;
  let sends = 0;
  const ready = new Promise<{ send(): Promise<never>; destroy(): void }>((done) => {
    resolve = done;
  });
  const store = createS3ArtifactStore(configuration, { deadlineMs: 20, loadClient: () => ready });
  await assert.rejects(store.warmup!(), (error) => isArtifactStorageErrorKind(error, "deadline"));
  resolve({
    async send() {
      sends++;
      throw new Error("must not send");
    },
    destroy() {},
  });
  await store.close();
  assert.equal(sends, 0);
});

test("preparation failure has explicit not-sent evidence and safe diagnostics; subsequent preparation recovers", async () => {
  let loads = 0,
    sends = 0;
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () => {
      if (++loads === 1)
        throw Object.assign(new Error("secret bearer must never be retained"), {
          name: "CredentialsProviderError",
          $metadata: { httpStatusCode: 503, requestId: "safe-123" },
        });
      return {
        async send(command) {
          assert.ok(command instanceof PutObjectCommand);
          sends++;
          return { $metadata: { httpStatusCode: 200 } };
        },
        destroy() {},
      };
    },
  });
  await assert.rejects(store.put(Buffer.from("safe")), (error) => {
    assert.ok(isArtifactStorageErrorKind(error, "unavailable"));
    assert.equal(error.putStarted, false);
    assert.equal(error.diagnostic?.name, "CredentialsProviderError");
    assert.equal(error.diagnostic?.status, 503);
    assert.equal(error.diagnostic?.requestId, "safe-123");
    assert.equal(error.diagnostic?.operation, "put");
    assert.equal(error.diagnostic?.phase, "client");
    assert.equal(error.diagnostic?.transportMs, 0);
    assert.equal(error.diagnostic?.sizeBytes, 4);
    assert.equal(JSON.stringify(error).includes("bearer"), false);
    return true;
  });
  await store.warmup!();
  await store.put(Buffer.from("safe"));
  assert.equal(loads, 2);
  assert.equal(sends, 1);
  await store.close();
});

test("durable allocation binds bytes, scope and mandatory dispatch acknowledgement", async () => {
  let sends = 0;
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () => ({
      async send() {
        sends++;
        return { $metadata: { httpStatusCode: 200 } };
      },
      destroy() {},
    }),
  });
  const bytes = Buffer.from("durable");
  const prepared = store.prepare!(bytes);
  await assert.rejects(store.put(bytes, { prepared }), (error) =>
    isArtifactStorageErrorKind(error, "configuration")
  );
  await assert.rejects(
    store.put(Buffer.from("changed"), { prepared, async onPrepared() {} }),
    (error) => isArtifactStorageErrorKind(error, "integrity")
  );
  await assert.rejects(
    store.put(bytes, {
      prepared,
      async onPrepared() {
        throw new Error("commit unknown");
      },
    }),
    /commit unknown/
  );
  assert.equal(sends, 0);
  const result = await store.put(bytes, { prepared, async onPrepared() {} });
  assert.equal(result.reference, prepared.reference);
  assert.equal(sends, 1);
  await store.close();
});

test("oversized detail is rejected before durable allocation or client preparation", async () => {
  let loads = 0;
  const store = createS3ArtifactStore(configuration, {
    loadClient: async () => {
      loads++;
      throw new Error("must not load");
    },
  });
  assert.throws(
    () => store.prepare!(Buffer.alloc(8 * 1024 * 1024 + 1)),
    (error) => isArtifactStorageErrorKind(error, "too_large")
  );
  assert.equal(loads, 0);
  await store.close();
});
