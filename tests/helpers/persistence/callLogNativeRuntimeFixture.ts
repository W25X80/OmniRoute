import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import type { NativeArtifactStore } from "../../../src/domain/persistence/artifactStore.ts";
import {
  createS3ArtifactStore as createNativeStore,
  type S3ArtifactTransport,
} from "../../../src/lib/artifacts/s3ArtifactStore.ts";
export {
  readPostgresEnvironment,
  readS3ArtifactConfiguration,
  initializePostgresControlPlane,
  nativeRuntimeFixture,
} from "./runtimeNativeIntegrationFixture.ts";

export function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

export const callLogNativeFixture = {
  puts: 0,
  closes: 0,
  gate: undefined as ReturnType<typeof barrier> | undefined,
  entered: undefined as ReturnType<typeof barrier> | undefined,
  objects: new Map<string, Uint8Array>(),
  arm() {
    this.gate = barrier();
    this.entered = barrier();
    return { entered: this.entered.promise, release: this.gate.resolve };
  },
};

/** Real SQL/runtime/store; only the independent S3 transport uses a deterministic barrier. */
export function createS3ArtifactStore(): NativeArtifactStore {
  const prefix = "gateway/call-logs/";
  const transport: S3ArtifactTransport = {
    async send(command, options) {
      assert.equal(command.input.Bucket, "native-call-log-fixture");
      assert.equal(command.input.ExpectedBucketOwner, "123456789012");
      assert.ok(command.input.Key?.startsWith(prefix + "s3/"));
      const reference = command.input.Key.slice(prefix.length);
      if (command instanceof PutObjectCommand) {
        callLogNativeFixture.puts++;
        const gate = callLogNativeFixture.gate;
        callLogNativeFixture.entered?.resolve();
        await gate?.promise;
        if (options.abortSignal.aborted)
          throw Object.assign(new Error("Fixture transport was aborted"), { name: "AbortError" });
        assert.ok(command.input.Body instanceof Uint8Array);
        const bytes = Buffer.from(command.input.Body);
        assert.equal(command.input.ContentLength, bytes.byteLength);
        assert.equal(
          command.input.ChecksumSHA256,
          createHash("sha256").update(bytes).digest("base64")
        );
        assert.equal(command.input.IfNoneMatch, "*");
        assert.equal(callLogNativeFixture.objects.has(reference), false);
        callLogNativeFixture.objects.set(reference, bytes);
        return { $metadata: { httpStatusCode: 200 } };
      }
      if (command instanceof GetObjectCommand) {
        const bytes = callLogNativeFixture.objects.get(reference);
        if (!bytes)
          throw Object.assign(new Error("Missing fixture object"), {
            name: "NoSuchKey",
            $metadata: { httpStatusCode: 404 },
          });
        return {
          Body: bytes,
          ContentLength: bytes.byteLength,
          ChecksumSHA256: createHash("sha256").update(bytes).digest("base64"),
          $metadata: { httpStatusCode: 200 },
        };
      }
      assert.ok(command instanceof DeleteObjectCommand);
      callLogNativeFixture.objects.delete(reference);
      return { $metadata: { httpStatusCode: 204 } };
    },
    destroy() {},
  };
  const store = createNativeStore(
    {
      bucket: "native-call-log-fixture",
      region: "eu-central-1",
      prefix,
      expectedBucketOwner: "123456789012",
    },
    { loadClient: async () => transport }
  );
  let countedClose = false;
  return {
    ...store,
    async close() {
      await store.close();
      if (!countedClose) {
        countedClose = true;
        callLogNativeFixture.closes++;
      }
    },
  };
}
