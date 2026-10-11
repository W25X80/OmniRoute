import { createHash, randomUUID } from "node:crypto";
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

/** SQL/stream qualification excludes AWS/S3. Serialization and artifact readers remain production. */
export function createS3ArtifactStore(): NativeArtifactStore {
  const bytesByReference = new Map<string, Uint8Array>();
  let closed = false;
  const assertOpen = () => {
    if (closed) throw new Error("Fixture artifact transport closed");
  };
  return {
    async put(bytes) {
      assertOpen();
      const checksum = createHash("sha256").update(bytes).digest("hex");
      const reference = `fixture/${randomUUID()}-${checksum}.json`;
      bytesByReference.set(reference, Uint8Array.from(bytes));
      streamingNativeFixture.artifactPuts++;
      return { reference, sizeBytes: bytes.byteLength, checksum };
    },
    async get(reference) {
      assertOpen();
      return bytesByReference.get(reference) ?? null;
    },
    async delete(reference) {
      assertOpen();
      return bytesByReference.delete(reference);
    },
    async close() {
      if (!closed) {
        closed = true;
        bytesByReference.clear();
        streamingNativeFixture.artifactsClosed++;
      }
    },
  };
}
