import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import {
  createS3ArtifactStore as createNativeStore,
  type S3ArtifactTransport,
} from "../../../src/lib/artifacts/s3ArtifactStore.ts";
import type { NativeArtifactStore } from "../../../src/domain/persistence/artifactStore.ts";
import type {
  PostgresControlPlane,
  PostgresControlPlaneOptions,
} from "../../../src/lib/db/postgres/controlPlane.ts";
import { readPostgresEnvironment as readEnvironment } from "../../../src/lib/db/postgres/environment.ts";
import { testPostgresOptions } from "./postgresTestDatabase.ts";

export const streamingNativeFixture = {
  sqliteAttempts: 0,
  sqliteAttemptStacks: [] as string[],
  artifactsClosed: 0,
  artifactPuts: 0,
  plane: undefined as PostgresControlPlane | undefined,
};

export async function readPostgresEnvironment() {
  const test = await testPostgresOptions();
  return readEnvironment({
    OMNIROUTE_POSTGRES_HOST: test.host,
    OMNIROUTE_POSTGRES_PORT: String(test.port),
    OMNIROUTE_POSTGRES_DATABASE: test.database,
    OMNIROUTE_POSTGRES_USER: test.user,
    OMNIROUTE_POSTGRES_PASSWORD: test.password,
    OMNIROUTE_POSTGRES_TLS: "verify-full",
    OMNIROUTE_POSTGRES_CA_FILE: process.env.OMNIROUTE_TEST_POSTGRES_CA,
  });
}

export function readS3ArtifactConfiguration() {
  return {};
}

export async function initializePostgresControlPlane(options: PostgresControlPlaneOptions) {
  const { initializePostgresControlPlane: initialize } =
    await import("../../../src/lib/db/postgres/controlPlane.ts");
  const plane = await initialize({
    ...options,
    runtime: { async applyRuntimeSettings() {}, async scheduleCatalogRevalidationAfterInit() {} },
  });
  streamingNativeFixture.plane = plane;
  return plane;
}

/** Real store/serialization; the deterministic owned transport excludes live AWS/S3. */
export function createS3ArtifactStore(): NativeArtifactStore {
  const bytesByKey = new Map<string, Uint8Array>();
  const prefix = "gateway/call-logs/";
  const missing = () =>
    Object.assign(new Error("Missing fixture object"), {
      name: "NoSuchKey",
      $metadata: { httpStatusCode: 404 },
    });
  const transport: S3ArtifactTransport = {
    async send(command, options) {
      assert.equal(command.input.Bucket, "native-streaming-fixture");
      assert.equal(command.input.ExpectedBucketOwner, "123456789012");
      assert.ok(command.input.Key?.startsWith(prefix + "s3/"));
      const key = command.input.Key;
      if (options.abortSignal.aborted)
        throw Object.assign(new Error("Fixture transport was aborted"), { name: "AbortError" });
      if (command instanceof PutObjectCommand) {
        assert.ok(command.input.Body instanceof Uint8Array);
        const bytes = Buffer.from(command.input.Body);
        assert.equal(command.input.ContentLength, bytes.byteLength);
        assert.equal(
          command.input.ChecksumSHA256,
          createHash("sha256").update(bytes).digest("base64")
        );
        assert.equal(command.input.IfNoneMatch, "*");
        assert.equal(bytesByKey.has(key), false);
        bytesByKey.set(key, bytes);
        streamingNativeFixture.artifactPuts++;
        return { $metadata: { httpStatusCode: 200 } };
      }
      if (command instanceof GetObjectCommand) {
        const bytes = bytesByKey.get(key);
        if (!bytes) throw missing();
        return {
          Body: bytes,
          ContentLength: bytes.byteLength,
          ChecksumSHA256: createHash("sha256").update(bytes).digest("base64"),
          $metadata: { httpStatusCode: 200 },
        };
      }
      assert.ok(command instanceof DeleteObjectCommand);
      if (!bytesByKey.delete(key)) throw missing();
      return { $metadata: { httpStatusCode: 204 } };
    },
    destroy() {},
  };
  const store = createNativeStore(
    {
      bucket: "native-streaming-fixture",
      region: "eu-central-1",
      prefix,
      expectedBucketOwner: "123456789012",
    },
    { loadClient: async () => transport }
  );
  let closed = false;
  return {
    ...store,
    async close() {
      await store.close();
      if (!closed) {
        closed = true;
        bytesByKey.clear();
        streamingNativeFixture.artifactsClosed++;
      }
    },
  };
}
