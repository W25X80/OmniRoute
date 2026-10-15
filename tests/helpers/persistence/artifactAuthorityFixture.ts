import assert from "node:assert/strict";
import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import {
  createS3ArtifactStore as createNativeStore,
  type S3ArtifactTransport,
} from "../../../src/lib/artifacts/s3ArtifactStore.ts";

export const artifactAuthorityFixture = {
  puts: 0,
  gets: 0,
  deletes: 0,
  creates: 0,
  closes: 0,
  objects: new Map<string, { bytes: Buffer; checksum: string }>(),
  getFailure: undefined as unknown,
  reset() {
    this.puts = 0;
    this.gets = 0;
    this.deletes = 0;
    this.creates = 0;
    this.closes = 0;
    this.objects.clear();
    this.getFailure = undefined;
  },
  effects() {
    return { puts: this.puts, gets: this.gets, deletes: this.deletes };
  },
};

/** The real store and SDK command constructors run; only its external transport is replaced. */
export function createS3ArtifactStore() {
  artifactAuthorityFixture.creates++;
  const transport: S3ArtifactTransport = {
    async send(command) {
      assert.equal(command.input.Bucket, "native-artifact-authority-test");
      assert.equal(command.input.ExpectedBucketOwner, "123456789012");
      assert.ok(command.input.Key?.startsWith("gateway/call-logs/s3/"));
      if (command instanceof PutObjectCommand) {
        artifactAuthorityFixture.puts++;
        assert.ok(command.input.Body instanceof Uint8Array);
        artifactAuthorityFixture.objects.set(command.input.Key!, {
          bytes: Buffer.from(command.input.Body),
          checksum: command.input.ChecksumSHA256!,
        });
        return { $metadata: { httpStatusCode: 200 } };
      }
      if (command instanceof GetObjectCommand) {
        artifactAuthorityFixture.gets++;
        if (artifactAuthorityFixture.getFailure) throw artifactAuthorityFixture.getFailure;
        const stored = artifactAuthorityFixture.objects.get(command.input.Key!);
        if (!stored)
          throw Object.assign(new Error("missing test object"), {
            name: "NoSuchKey",
            $metadata: { httpStatusCode: 404 },
          });
        return {
          Body: stored.bytes,
          ContentLength: stored.bytes.length,
          ChecksumSHA256: stored.checksum,
          $metadata: { httpStatusCode: 200 },
        };
      }
      assert.ok(command instanceof DeleteObjectCommand);
      artifactAuthorityFixture.deletes++;
      artifactAuthorityFixture.objects.delete(command.input.Key!);
      return { $metadata: { httpStatusCode: 204 } };
    },
    destroy() {
      artifactAuthorityFixture.closes++;
    },
  };
  return createNativeStore(
    {
      bucket: "native-artifact-authority-test",
      region: "eu-central-1",
      prefix: "gateway/call-logs/",
      expectedBucketOwner: "123456789012",
    },
    { loadClient: async () => transport }
  );
}
