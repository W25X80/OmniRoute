import type { NativeArtifactStore } from "../../../src/domain/persistence/artifactStore.ts";
import { initializePostgresControlPlane as initializeNativePlane } from "../../../src/lib/db/postgres/controlPlane.ts";
import type { PostgresControlPlane } from "../../../src/lib/db/postgres/controlPlane.ts";
import type { PostgresControlPlaneOptions } from "../../../src/lib/db/postgres/controlPlane.ts";
import { testPostgresOptions } from "./postgresTestDatabase.ts";

export const nativeRuntimeFixture = {
  artifactsClosed: 0,
  plane: undefined as PostgresControlPlane | undefined,
};
export async function readPostgresEnvironment() {
  return testPostgresOptions();
}
export function readS3ArtifactConfiguration() {
  return {};
}
export async function initializePostgresControlPlane(options: PostgresControlPlaneOptions) {
  const plane = await initializeNativePlane({
    ...options,
    runtime: {
      async applyRuntimeSettings() {},
      async scheduleCatalogRevalidationAfterInit() {},
    },
  });
  nativeRuntimeFixture.plane = plane;
  return plane;
}

/** This integration proposal exercises real PostgreSQL; the independent S3 transport is excluded. */
export function createS3ArtifactStore(): NativeArtifactStore {
  let closed = false;
  return {
    async put() {
      throw new Error("S3 is outside the native SQL lifetime oracle");
    },
    async get() {
      throw new Error("S3 is outside the native SQL lifetime oracle");
    },
    async delete() {
      throw new Error("S3 is outside the native SQL lifetime oracle");
    },
    async close() {
      if (!closed) {
        closed = true;
        nativeRuntimeFixture.artifactsClosed++;
      }
    },
  };
}
