import { assertSqliteBackendAllowed } from "../controlPlaneContext";
import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import {
  catalogNamespaceRows,
  catalogValue,
  hasPostgresCatalogSnapshot,
  requirePostgresCatalogSnapshot,
  withPostgresCatalogSnapshot,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  putCatalogValue,
  deleteCatalogValue,
} from "../postgres/catalogSnapshot";
/** db/models/aliases.ts — model alias CRUD (modelAliases namespace, providerAliases namespace). */

import { getDbInstance } from "../core";
import { backupDbFile } from "../backup";
import { getKeyValue } from "./shared";
import { finishModelCatalogWriteWithBackup } from "./modelCatalogWriteSignals";

export async function getModelAliases() {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const rows = getSelectedPostgresDatabase()
      ? catalogNamespaceRows("modelAliases")
      : db!.prepare("SELECT key, value FROM key_value WHERE namespace = 'modelAliases'").all();
    const result: Record<string, unknown> = {};
    for (const row of rows) {
      const { key, value } = getKeyValue(row);
      if (!key || value === null) continue;
      result[key] = JSON.parse(value);
    }
    return result;
  };
  if (catalogDatabase && !hasPostgresCatalogSnapshot(catalogDatabase))
    return withPostgresCatalogSnapshot(catalogDatabase, execute);
  return execute();
}

export async function setModelAlias(alias: string, model: unknown) {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    getSelectedPostgresDatabase()
      ? await putCatalogValue("modelAliases", alias, JSON.stringify(model))
      : db!
          .prepare(
            "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('modelAliases', ?, ?)"
          )
          .run(alias, JSON.stringify(model));
    finishModelCatalogWriteWithBackup();
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

export async function deleteModelAlias(alias: string) {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    getSelectedPostgresDatabase()
      ? await deleteCatalogValue("modelAliases", alias)
      : db!
          .prepare("DELETE FROM key_value WHERE namespace = 'modelAliases' AND key = ?")
          .run(alias);
    await unmarkManagedModelAlias(alias);
    finishModelCatalogWriteWithBackup();
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

// ──────── Managed-alias provenance marker (#11836) ────────
// Distinguishes an alias OmniRoute itself generated during a provider model sync from one a
// person typed in the dashboard. Only alias names recorded here are eligible for the
// sync's prune passes — resolveManagedModelAlias() and syncManagedAvailableModelAliases()
// must never delete or "adopt" an alias unless its name is present in this set, so a
// hand-created custom alias that happens to already point at a model's full id is never
// silently claimed (and later pruned) as if OmniRoute had generated it.
const MANAGED_ALIAS_NAMES_KEY = "names";

async function getManagedModelAliasNamesRow(): Promise<string[]> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const row = getSelectedPostgresDatabase()
      ? catalogValue("managedModelAliasNames", MANAGED_ALIAS_NAMES_KEY)
      : db!
          .prepare(
            "SELECT value FROM key_value WHERE namespace = 'managedModelAliasNames' AND key = ?"
          )
          .get(MANAGED_ALIAS_NAMES_KEY);
    const parsed = getKeyValue(row).value;
    if (!parsed) return [];
    try {
      const v = JSON.parse(parsed);
      return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
    } catch {
      return [];
    }
  };
  if (catalogDatabase && !hasPostgresCatalogSnapshot(catalogDatabase))
    return withPostgresCatalogSnapshot(catalogDatabase, execute);
  return execute();
}

/**
 * The set of alias names OmniRoute has generated/adopted itself during a managed
 * provider model sync (OpenRouter + OpenAI/Anthropic-compatible custom providers).
 * Only these are eligible for sync-triggered pruning.
 */
export async function getManagedModelAliasNames(): Promise<Set<string>> {
  return new Set(await getManagedModelAliasNamesRow());
}

async function writeManagedModelAliasNames(names: Set<string>): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    getSelectedPostgresDatabase()
      ? await putCatalogValue(
          "managedModelAliasNames",
          MANAGED_ALIAS_NAMES_KEY,
          JSON.stringify(Array.from(names))
        )
      : db!
          .prepare(
            "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('managedModelAliasNames', ?, ?)"
          )
          .run(MANAGED_ALIAS_NAMES_KEY, JSON.stringify(Array.from(names)));
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

/** Record that `alias` was generated/adopted by OmniRoute's own sync logic. */
export async function markManagedModelAlias(alias: string): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const names = await getManagedModelAliasNames();
    if (names.has(alias)) return;
    names.add(alias);
    await writeManagedModelAliasNames(names);
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

/** Clear the managed marker for `alias` (called whenever an alias row is deleted). */
export async function unmarkManagedModelAlias(alias: string): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const names = await getManagedModelAliasNames();
    if (!names.has(alias)) return;
    names.delete(alias);
    await writeManagedModelAliasNames(names);
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

/**
 * Cascade-delete every model-alias row that resolves to the given provider.
 *
 * Managed/imported aliases are stored as `key = <alias>`, `value = "<providerId>/<model>"`
 * (e.g. `setModelAlias("x-fast", "providerX/fast-model")`). When a custom provider is
 * removed, its connections and node are deleted but these alias rows are left behind,
 * which then block re-importing the same provider ("already exists" / no new models) — see
 * #1409. This removes every alias whose stored value begins with `<providerId>/`, so a
 * fresh import is unblocked.
 *
 * Only string values starting with the exact `"<providerId>/"` prefix match, so unrelated
 * providers and user-facing settings aliases (whose value is the bare alias, not a
 * `<providerId>/<model>` string) are left untouched.
 *
 * @returns the list of alias keys that were removed.
 */
// ──────── Provider-scoped aliases (#9068) ────────
// These survive rediscovery: an alias in this namespace is never touched by
// model sync, and always resolves the same way regardless of upstream ID changes.

export type ProviderAliasMap = Record<string, string>; // alias → upstream model ID

/**
 * Get the provider-scoped alias map for a given provider.
 * Returns `{}` when no aliases have been set.
 */
export function getProviderAliases(providerId: string): ProviderAliasMap {
  requirePostgresCatalogSnapshot();

  const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
  const row = getSelectedPostgresDatabase()
    ? catalogValue("providerAliases", providerId)
    : db!
        .prepare("SELECT value FROM key_value WHERE namespace = 'providerAliases' AND key = ?")
        .get(providerId);
  const parsed = getKeyValue(row).value;
  if (!parsed) return {};
  try {
    const v = JSON.parse(parsed);
    return typeof v === "object" && v !== null ? (v as ProviderAliasMap) : {};
  } catch {
    return {};
  }
}

/**
 * Set a provider-scoped alias.
 */
export async function setProviderAliasAsync(
  providerId: string,
  alias: string,
  upstreamModelId: string
): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const current = getProviderAliases(providerId);
    current[alias] = upstreamModelId;
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    getSelectedPostgresDatabase()
      ? await putCatalogValue("providerAliases", providerId, JSON.stringify(current))
      : db!
          .prepare(
            "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('providerAliases', ?, ?)"
          )
          .run(providerId, JSON.stringify(current));
    if (!getSelectedPostgresDatabase()) backupDbFile("pre-write");
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

/**
 * Remove a provider-scoped alias.
 */
export async function removeProviderAliasAsync(providerId: string, alias: string): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const current = getProviderAliases(providerId);
    if (!(alias in current)) return;
    delete current[alias];
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    if (Object.keys(current).length === 0) {
      getSelectedPostgresDatabase()
        ? await deleteCatalogValue("providerAliases", providerId)
        : db!
            .prepare("DELETE FROM key_value WHERE namespace = 'providerAliases' AND key = ?")
            .run(providerId);
    } else {
      getSelectedPostgresDatabase()
        ? await putCatalogValue("providerAliases", providerId, JSON.stringify(current))
        : db!
            .prepare(
              "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('providerAliases', ?, ?)"
            )
            .run(providerId, JSON.stringify(current));
    }
    backupDbFile("pre-write");
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

export async function deleteModelAliasesForProvider(providerId: string): Promise<string[]> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const prefix = `${providerId}/`;
    const aliases = await getModelAliases();
    const removed: string[] = [];
    for (const [alias, value] of Object.entries(aliases)) {
      if (typeof value !== "string" || !value.startsWith(prefix)) continue;
      await deleteModelAlias(alias);
      removed.push(alias);
    }
    return removed;
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

export function setProviderAlias(providerId: string, alias: string, upstreamModelId: string): void {
  assertSqliteBackendAllowed();
  const current = getProviderAliases(providerId);
  current[alias] = upstreamModelId;
  const db = getDbInstance();
  db.prepare(
    "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('providerAliases', ?, ?)"
  ).run(providerId, JSON.stringify(current));
  backupDbFile("pre-write");
}

export function removeProviderAlias(providerId: string, alias: string): void {
  assertSqliteBackendAllowed();
  const current = getProviderAliases(providerId);
  if (!(alias in current)) return;
  delete current[alias];
  const db = getDbInstance();
  if (Object.keys(current).length === 0) {
    db.prepare("DELETE FROM key_value WHERE namespace = 'providerAliases' AND key = ?").run(
      providerId
    );
  } else {
    db.prepare(
      "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('providerAliases', ?, ?)"
    ).run(providerId, JSON.stringify(current));
  }
  backupDbFile("pre-write");
}
