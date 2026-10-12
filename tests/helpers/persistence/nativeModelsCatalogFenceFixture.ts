import type { PostgresControlPlaneBinding } from "../../../src/lib/db/controlPlaneContext";
import type { PostgresDatabase, PostgresTransaction } from "../../../src/lib/db/postgres/database";
import { PostgresError } from "../../../src/lib/db/postgres/database";
import { CATALOG_TABLES, type CatalogValueRow } from "../../../src/lib/db/postgres/catalogSnapshot";

export function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

type ProviderCatalog = Awaited<
  ReturnType<typeof import("../../../src/lib/catalog/openrouterCatalog").getOpenRouterCatalog>
>;
export const catalogFixture = {
  fetches: 0,
  discoveryHits: 0,
  aiHordeRefreshes: 0,
  catalog: { data: [] } as unknown as ProviderCatalog,
  gate: undefined as
    { entered: ReturnType<typeof barrier>; release: ReturnType<typeof barrier> } | undefined,
};

export const aiHordeImageCatalog = {
  async ensureFresh() {
    catalogFixture.aiHordeRefreshes++;
  },
};
export async function incrementCcDiscoveryHitCountAsync() {
  catalogFixture.discoveryHits++;
}

export async function getOpenRouterCatalog(): Promise<ProviderCatalog> {
  catalogFixture.fetches++;
  const gate = catalogFixture.gate;
  if (gate) {
    catalogFixture.gate = undefined;
    gate.entered.resolve();
    await gate.release.promise;
  }
  return catalogFixture.catalog;
}

/** SQL transport fixture, not a PostgreSQL engine oracle. Domain owners stay real. */
export function createCatalogFixture() {
  let authority = "catalog-fence-fixture";
  let generation = "1";
  let values: CatalogValueRow[] = Object.entries({
    requireLogin: true,
    requireAuthForModels: true,
    password: "configured-fixture-password-hash",
    setupComplete: true,
    hideAutoCombos: true,
    autoRoutingEnabled: false,
    hideNoThinkVariants: true,
    blockedProviders: [],
  }).map(([key, value]) => ({ namespace: "settings", key, value: JSON.stringify(value) }));
  let capabilities: Record<string, unknown>[] = [];
  let unknownNextCapabilityCommit = false;
  let rejectNextCapabilityCommit = false;
  let replaceAuthorityAfterCommit = false;
  let capabilityWrites = 0;
  const connections = ["openai", "openrouter"].map((provider) => ({
    id: provider + "-fixture",
    provider,
    isActive: true,
    authType: "apikey",
    testStatus: "active",
    providerSpecificData: {},
  }));
  const database = {
    async transaction<T>(body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
      let changed = false;
      let wroteCapabilities = false;
      let localValues = structuredClone(values);
      let localCapabilities = structuredClone(capabilities);
      let localGeneration = generation;
      const tx: PostgresTransaction = {
        async query<Row>(text: string, parameters: unknown[] = []) {
          let rows: unknown[] = [];
          let rowCount = 0;
          if (text.includes("AS tables")) {
            rows = [
              {
                values: structuredClone(localValues),
                tables: Object.fromEntries(
                  CATALOG_TABLES.map((table) => [
                    table,
                    table === "model_capabilities" ? structuredClone(localCapabilities) : [],
                  ])
                ),
                authority,
                generation: localGeneration,
              },
            ];
            rowCount = 1;
          } else if (text.startsWith("SELECT pg_advisory")) {
            rowCount = 1;
          } else if (text.startsWith("INSERT INTO omniroute.key_value")) {
            const [namespace, key, value] = parameters as [string, string, string];
            localValues = localValues.filter(
              (row) => row.namespace !== namespace || row.key !== key
            );
            localValues.push({ namespace, key, value });
            changed = true;
            rowCount = 1;
          } else if (text.startsWith("INSERT INTO omniroute.model_capabilities")) {
            const entries = JSON.parse(String(parameters[0])) as Record<string, unknown>[];
            for (const entry of entries) {
              localCapabilities = localCapabilities.filter(
                (row) => row.provider !== entry.provider || row.model_id !== entry.model_id
              );
              localCapabilities.push(entry);
            }
            capabilityWrites++;
            wroteCapabilities = true;
            changed = true;
            rowCount = entries.length;
          } else if (text.startsWith("UPDATE omniroute.api_key_policy_epoch")) {
            localGeneration = String(BigInt(localGeneration) + 1n);
            rows = [{ authority_id: authority, generation: localGeneration }];
            changed = true;
            rowCount = 1;
          } else if (text.includes("SELECT authority_id,generation")) {
            rows = [{ authority_id: authority, generation: localGeneration }];
            rowCount = 1;
          } else throw new Error("Unexpected catalog fixture SQL: " + text);
          return { rows: rows as Row[], rowCount, command: "SELECT" };
        },
      };
      const result = await body(tx);
      if (wroteCapabilities && rejectNextCapabilityCommit) {
        rejectNextCapabilityCommit = false;
        throw new PostgresError("transaction_failed");
      }
      if (changed) {
        values = localValues;
        capabilities = localCapabilities;
        generation = localGeneration;
      }
      if (wroteCapabilities && replaceAuthorityAfterCommit) {
        replaceAuthorityAfterCommit = false;
        authority = "replacement-catalog-authority";
        generation = "1";
      }
      if (wroteCapabilities && unknownNextCapabilityCommit) {
        unknownNextCapabilityCommit = false;
        throw new PostgresError("commit_unknown");
      }
      return result;
    },
  } as unknown as PostgresDatabase;
  const binding = {
    database,
    databaseIdentity: Object.freeze({}),
    assertReady() {},
    providers: {
      async listRaw() {
        return structuredClone(connections);
      },
    },
    nodes: {
      async list() {
        return [];
      },
    },
    combos: {
      async list() {
        return [];
      },
    },
  } as unknown as PostgresControlPlaneBinding;
  return {
    database,
    binding,
    capabilityWrites: () => capabilityWrites,
    generation: () => generation,
    unknownNextCapabilityCommit() {
      unknownNextCapabilityCommit = true;
    },
    rejectNextCapabilityCommit() {
      rejectNextCapabilityCommit = true;
    },
    replaceAuthorityAfterCapabilityCommit() {
      replaceAuthorityAfterCommit = true;
    },
  };
}
