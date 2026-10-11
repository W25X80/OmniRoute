import { runPersistenceBackgroundWork } from "../persistenceRuntime";
import { assertSqliteBackendAllowed } from "../controlPlaneContext";
import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import {
  catalogValue,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  putCatalogValue,
  deleteCatalogValue,
  deleteCatalogMatchingKeys,
  afterPostgresCatalogCommit,
} from "../postgres/catalogSnapshot";
/** Canonical comparison and write helpers for connection-scoped synced model catalogs. */

import { backupDbFile } from "../backup";
import { getDbInstance } from "../core";
import { invalidateModelCatalogCache } from "../readCache";
import { getKeyValue } from "./shared";

type ModelNormalizer<T> = (models: unknown) => T[];

// #11016: the Feature 5004 reconciler copies provider-declared windows
// (`inputTokenLimit`, captured at /models discovery) into `auto:discovery`
// overrides — the only source the REQUEST-TIME token-limit chain trusts for a
// freshly synced model that models.dev has not indexed yet. Before this, the
// reconcile ran only at startup + every 24h, so a model synced mid-cycle was
// enforced at the provider's static `defaultContextLength` (128K for
// OpenRouter) for up to a day even though the catalog advertised its real
// window (measured: `openrouter/stealth/ox-alpha` advertised 1,048,576,
// enforced 128,000). Run the reconcile opportunistically right after a synced
// catalog write changes, debounced + fire-and-forget so the sync request is
// never blocked (dynamic import: `contextWindowResolver` reads this module via
// `getAllSyncedAvailableModels`, so a static import would be circular).
let reconcileAfterSyncTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleReconcileAfterSyncWrite(): void {
  if (getSelectedPostgresDatabase()) {
    void runPersistenceBackgroundWork(async () => {
      const reconciler = await import("../../contextWindowResolver");
      await reconciler.runContextWindowReconcile();
    }).catch(() => {
      // Periodic reconciliation retries this optional follow-up.
    });
    return;
  }
  if (reconcileAfterSyncTimer) return; // debounce bursty multi-connection writes
  reconcileAfterSyncTimer = setTimeout(() => {
    reconcileAfterSyncTimer = null;
    void import("../../contextWindowResolver")
      .then((m) => m.runContextWindowReconcile())
      .catch(() => {
        // Swallow — the periodic reconcile still runs; sync must never fail on it.
      });
  }, 0);
  reconcileAfterSyncTimer.unref?.();
}

export function finishSyncedAvailableModelsWrite(): void {
  if (getSelectedPostgresDatabase()) return;
  backupDbFile("pre-write");
  invalidateModelCatalogCache();
}

/**
 * Delete all synced models for every connection belonging to a provider.
 * Returns the number of connection-scoped synced model lists removed.
 */
export async function deleteSyncedAvailableModelsForProvider(providerId: string): Promise<number> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const keyPrefix = `${providerId}:`;
    const result = getSelectedPostgresDatabase()
      ? await deleteCatalogMatchingKeys(
          "syncedAvailableModels",
          (key) => [...key].slice(0, keyPrefix.length).join("") === keyPrefix
        )
      : db!
          .prepare(
            "DELETE FROM key_value WHERE namespace = 'syncedAvailableModels' AND substr(key, 1, ?) = ?"
          )
          .run(keyPrefix.length, keyPrefix);
    const changes = Number(result.changes || 0);
    if (changes > 0) finishSyncedAvailableModelsWrite();
    return changes;
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

export async function persistCanonicalSyncedAvailableModelsAsync<T>(
  key: string,
  normalizedModels: T[],
  normalizeModels: ModelNormalizer<T>
): Promise<boolean> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const existingRow = getSelectedPostgresDatabase()
      ? catalogValue("syncedAvailableModels", key)
      : db!
          .prepare(
            "SELECT value FROM key_value WHERE namespace = 'syncedAvailableModels' AND key = ?"
          )
          .get(key);
    const existingValue = getKeyValue(existingRow).value;
    let existingModels: T[] | null = null;
    if (existingValue !== null) {
      try {
        existingModels = normalizeModels(JSON.parse(existingValue));
      } catch {
        existingModels = null;
      }
    }

    const unchanged =
      (normalizedModels.length > 0 &&
        existingModels !== null &&
        JSON.stringify(existingModels) === JSON.stringify(normalizedModels)) ||
      (normalizedModels.length === 0 && existingValue === null);
    if (unchanged) return false;

    if (normalizedModels.length === 0) {
      getSelectedPostgresDatabase()
        ? await deleteCatalogValue("syncedAvailableModels", key)
        : db!
            .prepare("DELETE FROM key_value WHERE namespace = 'syncedAvailableModels' AND key = ?")
            .run(key);
    } else {
      getSelectedPostgresDatabase()
        ? await putCatalogValue("syncedAvailableModels", key, JSON.stringify(normalizedModels))
        : db!
            .prepare(
              "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('syncedAvailableModels', ?, ?)"
            )
            .run(key, JSON.stringify(normalizedModels));
    }
    finishSyncedAvailableModelsWrite();
    if (getSelectedPostgresDatabase()) afterPostgresCatalogCommit(scheduleReconcileAfterSyncWrite);
    else scheduleReconcileAfterSyncWrite();
    return true;
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

export function persistCanonicalSyncedAvailableModels<T>(
  key: string,
  normalizedModels: T[],
  normalizeModels: ModelNormalizer<T>
): boolean {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  const existingRow = db
    .prepare("SELECT value FROM key_value WHERE namespace = 'syncedAvailableModels' AND key = ?")
    .get(key);
  const existingValue = getKeyValue(existingRow).value;
  let existingModels: T[] | null = null;
  if (existingValue !== null) {
    try {
      existingModels = normalizeModels(JSON.parse(existingValue));
    } catch {
      existingModels = null;
    }
  }

  const unchanged =
    (normalizedModels.length > 0 &&
      existingModels !== null &&
      JSON.stringify(existingModels) === JSON.stringify(normalizedModels)) ||
    (normalizedModels.length === 0 && existingValue === null);
  if (unchanged) return false;

  if (normalizedModels.length === 0) {
    db.prepare("DELETE FROM key_value WHERE namespace = 'syncedAvailableModels' AND key = ?").run(
      key
    );
  } else {
    db.prepare(
      "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('syncedAvailableModels', ?, ?)"
    ).run(key, JSON.stringify(normalizedModels));
  }
  finishSyncedAvailableModelsWrite();
  scheduleReconcileAfterSyncWrite();
  return true;
}
