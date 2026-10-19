import * as providers from "../../src/lib/db/providers.ts";

export * from "../../src/lib/db/providers.ts";

export const concurrencyPorts = {
  afterRead: null as (() => Promise<void>) | null,
  patches: [] as Record<string, unknown>[],
};

export const getProviderConnections: typeof providers.getProviderConnections = async (...args) => {
  const snapshot = await providers.getProviderConnections(...args);
  const interleave = concurrencyPorts.afterRead;
  concurrencyPorts.afterRead = null;
  if (interleave) await interleave();
  return snapshot;
};

export const updateProviderConnection: typeof providers.updateProviderConnection = async (
  id,
  patch
) => {
  concurrencyPorts.patches.push({ ...patch });
  return providers.updateProviderConnection(id, patch);
};
