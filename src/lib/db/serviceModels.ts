import { assertSqliteBackendAllowed } from "./controlPlaneContext";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import {
  catalogValue,
  requirePostgresCatalogSnapshot,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  putCatalogValue,
  deleteCatalogValue,
} from "./postgres/catalogSnapshot";
/** Persists and retrieves the model list synced from embedded services (9router, etc.). */

import { getDbInstance } from "./core";

const NAMESPACE = "serviceModels";

export interface ServiceModel {
  id: string;
  name?: string;
  object?: string;
  owned_by?: string;
  created?: number;
  available?: boolean;
  [key: string]: unknown;
}

export function getServiceModels(tool: string): ServiceModel[] {
  requirePostgresCatalogSnapshot();

  const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
  const row = (
    getSelectedPostgresDatabase()
      ? catalogValue(NAMESPACE, tool)
      : db!
          .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
          .get(NAMESPACE, tool)
  ) as { value: string } | undefined;
  if (!row?.value) return [];
  try {
    const parsed = JSON.parse(row.value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

/**
 * Persist a new model list for a tool, with soft-delete pruning.
 *
 * Models present in the new payload are UPSERTed with `available: true`.
 * Models that were previously stored but are missing from the new payload
 * are marked `available: false` (not deleted — preserves history).
 */
export async function saveServiceModelsAsync(tool: string, models: ServiceModel[]): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();

    // Load existing stored models to compute the diff.
    const existing = getServiceModels(tool);
    const incomingIds = new Set(models.map((m) => m.id));

    // Mark incoming models as available, and pruned ones as unavailable.
    const incomingWithFlag: ServiceModel[] = models.map((m) => ({ ...m, available: true }));
    const pruned: ServiceModel[] = existing
      .filter((m) => !incomingIds.has(m.id))
      .map((m) => ({ ...m, available: false }));

    const merged = [...incomingWithFlag, ...pruned];

    if (merged.length === 0) {
      getSelectedPostgresDatabase()
        ? await deleteCatalogValue(NAMESPACE, tool)
        : db!.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(NAMESPACE, tool);
    } else {
      getSelectedPostgresDatabase()
        ? await putCatalogValue(NAMESPACE, tool, JSON.stringify(merged))
        : db!
            .prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)")
            .run(NAMESPACE, tool, JSON.stringify(merged));
    }
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

/**
 * Mark all stored models for a tool as unavailable.
 * Called when the supervisor transitions to stopped or error state so the
 * model catalog reflects that none of the models are currently reachable.
 */
export async function markAllUnavailableAsync(tool: string): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const existing = getServiceModels(tool);
    if (existing.length === 0) return;
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const updated: ServiceModel[] = existing.map((m) => ({ ...m, available: false }));
    getSelectedPostgresDatabase()
      ? await putCatalogValue(NAMESPACE, tool, JSON.stringify(updated))
      : db!
          .prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)")
          .run(NAMESPACE, tool, JSON.stringify(updated));
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

export function saveServiceModels(tool: string, models: ServiceModel[]): void {
  assertSqliteBackendAllowed();
  const db = getDbInstance();

  // Load existing stored models to compute the diff.
  const existing = getServiceModels(tool);
  const incomingIds = new Set(models.map((m) => m.id));

  // Mark incoming models as available, and pruned ones as unavailable.
  const incomingWithFlag: ServiceModel[] = models.map((m) => ({ ...m, available: true }));
  const pruned: ServiceModel[] = existing
    .filter((m) => !incomingIds.has(m.id))
    .map((m) => ({ ...m, available: false }));

  const merged = [...incomingWithFlag, ...pruned];

  if (merged.length === 0) {
    db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(NAMESPACE, tool);
  } else {
    db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
      NAMESPACE,
      tool,
      JSON.stringify(merged)
    );
  }
}

export function markAllUnavailable(tool: string): void {
  assertSqliteBackendAllowed();
  const existing = getServiceModels(tool);
  if (existing.length === 0) return;
  const db = getDbInstance();
  const updated: ServiceModel[] = existing.map((m) => ({ ...m, available: false }));
  db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
    NAMESPACE,
    tool,
    JSON.stringify(updated)
  );
}
