import { getSelectedControlPlane } from "../../../src/lib/db/controlPlaneContext.ts";

export const localHealthFixture = {
  reads: 0,
  nodes: [] as Array<{ id: string; prefix: string; baseUrl: string }>,
  readBarrier: undefined as Promise<void> | undefined,
  readFailure: undefined as Error | undefined,
};

export async function getCachedProviderNodes() {
  getSelectedControlPlane()?.assertReady();
  localHealthFixture.reads++;
  if (localHealthFixture.readBarrier) await localHealthFixture.readBarrier;
  getSelectedControlPlane()?.assertReady();
  if (localHealthFixture.readFailure) throw localHealthFixture.readFailure;
  return localHealthFixture.nodes;
}

export function isAutomatedTestProcess() {
  return false;
}
