import type { NativeArtifactStore } from "../../../src/domain/persistence/artifactStore.ts";
import { createS3ArtifactStore as createMemoryStore } from "./streamingNativeIntegrationFixture.ts";
export {
  readPostgresEnvironment,
  readS3ArtifactConfiguration,
  initializePostgresControlPlane,
} from "./streamingNativeIntegrationFixture.ts";

export const completedDetailNativeFixture = { reads: 0 };
/** Actual artifact writer/reader with controlled in-memory carrier; this oracle excludes S3. */
export function createS3ArtifactStore(): NativeArtifactStore {
  const store = createMemoryStore();
  return {
    ...store,
    async get(reference) {
      completedDetailNativeFixture.reads++;
      return store.get(reference);
    },
  };
}
