import { createS3ArtifactStore as createMemoryStore } from "./streamingNativeIntegrationFixture.ts";
export {
  readPostgresEnvironment,
  readS3ArtifactConfiguration,
  initializePostgresControlPlane,
} from "./streamingNativeIntegrationFixture.ts";

let release!: () => void;
let entered!: () => void;
const wait = new Promise<void>((resolve) => {
  release = resolve;
});
const putEntered = new Promise<void>((resolve) => {
  entered = resolve;
});
export const continuationNativeFixture = {
  putEntered,
  releasePut: () => release(),
  putCalls: 0,
  references: new Set<string>(),
};

/** Controlled memory transport stall, not an AWS/S3 availability certificate. */
export function createS3ArtifactStore() {
  const store = createMemoryStore();
  return {
    ...store,
    async put(bytes: Uint8Array) {
      continuationNativeFixture.putCalls++;
      entered();
      await wait;
      const receipt = await store.put(bytes);
      continuationNativeFixture.references.add(receipt.reference);
      return receipt;
    },
  };
}
