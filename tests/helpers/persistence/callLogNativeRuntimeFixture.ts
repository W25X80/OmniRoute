import { randomUUID } from "node:crypto";
import type { NativeArtifactStore } from "../../../src/domain/persistence/artifactStore.ts";
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

/** Real SQL/runtime; only the independent S3 transport is replaced by a deterministic barrier. */
export function createS3ArtifactStore(): NativeArtifactStore {
  let closed = false;
  return {
    async put(bytes) {
      callLogNativeFixture.puts++;
      const gate = callLogNativeFixture.gate;
      callLogNativeFixture.entered?.resolve();
      await gate?.promise;
      if (closed) throw new Error("Test artifact store was closed during an accepted write");
      const reference = `native-call-log-fixture/${randomUUID()}`;
      callLogNativeFixture.objects.set(reference, bytes);
      return { reference, sizeBytes: bytes.byteLength, checksum: "fixture-checksum" };
    },
    async get(reference) {
      return callLogNativeFixture.objects.get(reference) ?? null;
    },
    async delete(reference) {
      return callLogNativeFixture.objects.delete(reference);
    },
    async close() {
      if (!closed) {
        closed = true;
        callLogNativeFixture.closes++;
      }
    },
  };
}
