import { getSelectedPersistenceIdentity } from "../../../src/lib/db/controlPlaneContext.ts";
import { fixture as runtime } from "./runtimeOwnedWorkFixture.ts";
export * from "../../../src/shared/utils/featureFlags.ts";
export * from "./runtimeOwnedWorkFixture.ts";
import type { PostgresDatabase } from "../../../src/lib/db/postgres/database.ts";
import { withPostgresCatalogSnapshot as fixtureCatalog } from "./runtimeOwnedWorkFixture.ts";

export const recoveryAuthorityFixture = {
  flag: true,
  reads: 0,
  nativeReads: 0,
  catalogWaits: [] as Promise<void>[],
  catalogEntered: undefined as (() => void) | undefined,
};

export async function withPostgresCatalogSnapshot<T>(
  database: PostgresDatabase,
  body: () => T | Promise<T>
): Promise<T> {
  const wait = recoveryAuthorityFixture.catalogWaits.shift();
  if (wait) {
    recoveryAuthorityFixture.catalogEntered?.();
    await wait;
  }
  return fixtureCatalog(database, body);
}
/** Catalog I/O only is controlled; actual factory/runtime/context selection are production. */
export function isFeatureFlagEnabled(key: string): boolean {
  if (key !== "STREAM_RECOVERY_TOOLCALL_ORDER_FIX") return false;
  const identity = getSelectedPersistenceIdentity();
  if (identity) {
    if (runtime.selectedGeneration() === undefined)
      throw new Error("Prepared fixture catalog absent");
    recoveryAuthorityFixture.nativeReads++;
  }
  recoveryAuthorityFixture.reads++;
  return recoveryAuthorityFixture.flag;
}
