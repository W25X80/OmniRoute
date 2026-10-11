import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import {
  catalogNamespaceRows,
  catalogValue,
  hasPostgresCatalogSnapshot,
  withPostgresCatalogSnapshot,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  putCatalogValue,
} from "../postgres/catalogSnapshot";
/** db/models/mitmAlias.ts — MITM alias CRUD (mitmAlias namespace). */

import { getDbInstance } from "../core";
import { backupDbFile } from "../backup";
import { getKeyValue } from "./shared";

export async function getMitmAlias(toolName?: string) {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    if (toolName) {
      const row = getSelectedPostgresDatabase()
        ? catalogValue("mitmAlias", toolName)
        : db!
            .prepare("SELECT value FROM key_value WHERE namespace = 'mitmAlias' AND key = ?")
            .get(toolName);
      const value = getKeyValue(row).value;
      return value ? JSON.parse(value) : {};
    }
    const rows = getSelectedPostgresDatabase()
      ? catalogNamespaceRows("mitmAlias")
      : db!.prepare("SELECT key, value FROM key_value WHERE namespace = 'mitmAlias'").all();
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

export async function setMitmAliasAll(toolName: string, mappings: unknown) {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    getSelectedPostgresDatabase()
      ? await putCatalogValue("mitmAlias", toolName, JSON.stringify(mappings || {}))
      : db!
          .prepare(
            "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('mitmAlias', ?, ?)"
          )
          .run(toolName, JSON.stringify(mappings || {}));
    if (!getSelectedPostgresDatabase()) backupDbFile("pre-write");
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}
