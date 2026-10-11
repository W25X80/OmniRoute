import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import type { ProviderRecord } from "../../../src/domain/persistence/providerRepositories.ts";
import type { ProviderConformanceFixture } from "../../helpers/persistence/providerRepositoryConformance.ts";
import { registerProviderRepositoryConformance } from "../../helpers/persistence/providerRepositoryConformance.ts";

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-provider-conformance-"));
process.env.DATA_DIR = dataDir;
process.env.STORAGE_ENCRYPTION_KEY = "isolated-provider-conformance-encryption-key";

const core = await import("../../../src/lib/db/core.ts");
const providers = await import("../../../src/lib/db/providers.ts");

const fixture: ProviderConformanceFixture = {
  connections: {
    list: providers.getProviderConnections,
    listRaw: providers.getRawProviderConnections,
    count: async (filter) => providers.getProviderConnectionsCount(filter),
    findById: providers.getProviderConnectionById,
    displayMetadata: async (ids) => providers.getProviderConnectionDisplayMetadata(ids),
    create: providers.createProviderConnection,
    update: providers.updateProviderConnection,
    reorder: providers.reorderProviderConnections,
    deleteById: providers.deleteProviderConnection,
    deleteByIds: providers.deleteProviderConnections,
    deleteByProvider: async (provider) =>
      (await providers.deleteProviderConnectionsByProvider(provider)) ?? 0,
    mergeProviderSpecificData: providers.mergeConnectionProviderSpecificData,
    touchLastUsed: providers.touchConnectionLastUsed,
    touchSyncedModelsAt: providers.touchConnectionSyncedModelsAt,
    resetBackoff: providers.resetConnectionBackoff,
    distinctGroups: providers.getDistinctGroups,
  },
  nodes: {
    list: async (...args) => (await providers.getProviderNodes(...args)) as ProviderRecord[],
    count: async (filter) => providers.getProviderNodesCount(filter),
    findById: providers.getProviderNodeById,
    resolveForConnection: providers.resolveProviderNodeForConnection,
    create: providers.createProviderNode,
    update: providers.updateProviderNode,
    deleteById: providers.deleteProviderNode,
  },
  async reset() {
    core.resetDbInstance();
    fs.rmSync(dataDir, { recursive: true, force: true });
    fs.mkdirSync(dataDir, { recursive: true });
  },
  async updateStoredConnection(id, patch) {
    const allowed = new Set(["priority", "updated_at"]);
    const keys = Object.keys(patch);
    if (keys.some((key) => !allowed.has(key))) throw new Error("Unsupported fixture field");
    core
      .getDbInstance()
      .prepare(
        `UPDATE provider_connections SET ${keys.map((key) => `${key}=?`).join(",")} WHERE id=?`
      )
      .run(...keys.map((key) => patch[key]), id);
    const { invalidateDbCache } = await import("../../../src/lib/db/readCache.ts");
    invalidateDbCache("connections");
  },
};

test.after(() => {
  core.resetDbInstance();
  fs.rmSync(dataDir, { recursive: true, force: true });
});

registerProviderRepositoryConformance("SQLite", async () => fixture);
