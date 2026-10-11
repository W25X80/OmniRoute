import type { PostgresControlPlane } from "../../../src/lib/db/postgres/controlPlane.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";
import { withPostgresControlPlane } from "../../../src/lib/db/controlPlaneContext.ts";
import {
  initializePostgresControlPlane as initializeFixturePlane,
  withPostgresCatalogSnapshot as sourceCatalogFixture,
} from "./runtimeOwnedWorkFixture.ts";
export {
  readPostgresEnvironment,
  readS3ArtifactConfiguration,
  createS3ArtifactStore,
  initializeCircuitBreakerPersistence,
  initializeQuotaSnapshotCache,
  flushCircuitBreakerPersistence,
} from "./runtimeOwnedWorkFixture.ts";
export { fixture, barrier } from "./runtimeOwnedWorkFixture.ts";

export const proxySecretDatabaseFixture = {
  ciphertext: null as string | null,
  reads: 0,
  failure: undefined as Error | undefined,
  afterCatalog: undefined as (() => void) | undefined,
  reset() {
    this.ciphertext = null;
    this.reads = 0;
    this.failure = undefined;
    this.afterCatalog = undefined;
  },
};

/** Only the database transport is controlled; the actual repository and runtime select scopes. */
export async function initializePostgresControlPlane(): Promise<PostgresControlPlane> {
  const plane = await initializeFixturePlane();
  const identity = Object.freeze({});
  return {
    get state() {
      return plane.state;
    },
    bootstrapSecrets: plane.bootstrapSecrets,
    run(body) {
      return plane.run(async (binding) => {
        const database = {
          async transaction<T>(operation: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
            return binding.database!.transaction((transaction) =>
              operation({
                async query<Row>(text: string, values?: unknown[]) {
                  if (text === "SELECT * FROM omniroute.proxy_subscriptions WHERE id=$1") {
                    binding.assertReady();
                    proxySecretDatabaseFixture.reads++;
                    if (proxySecretDatabaseFixture.failure)
                      throw proxySecretDatabaseFixture.failure;
                    return {
                      rows: [
                        {
                          id: values?.[0],
                          control_secret_enc: proxySecretDatabaseFixture.ciphertext,
                        },
                      ] as unknown as Row[],
                      rowCount: 1,
                      command: "SELECT",
                    };
                  }
                  return transaction.query<Row>(text, values);
                },
              })
            );
          },
        } as unknown as PostgresDatabase;
        const scoped = { ...binding, database, databaseIdentity: identity };
        return withPostgresControlPlane(scoped, () => body(scoped));
      });
    },
    close: () => plane.close(),
  };
}

export async function withPostgresCatalogSnapshot<T>(
  database: PostgresDatabase,
  body: () => T | Promise<T>
): Promise<T> {
  return sourceCatalogFixture(database, async () => {
    proxySecretDatabaseFixture.afterCatalog?.();
    return body();
  });
}
