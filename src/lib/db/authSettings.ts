import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { decrypt } from "./encryption";
import {
  interpretSettingsRows,
  parseSettingsRevision,
  SETTINGS_REVISION_KEY,
} from "./settings/persistenceSemantics";
import { readAuthGeneration, type AuthSettingsSnapshot } from "./authSettingsSemantics";

/** Live authority, independent of prepared catalog/read-cache snapshots. */
export async function getAuthSettingsSnapshot(): Promise<AuthSettingsSnapshot> {
  const postgres = getSelectedPostgresDatabase();
  const rows = postgres
    ? (
        await postgres.transaction((tx) =>
          tx.query<{ key: string; value: string }>(
            "SELECT key,value FROM omniroute.key_value WHERE namespace='settings'"
          )
        )
      ).rows
    : (getDbInstance()
        .prepare("SELECT key,value FROM key_value WHERE namespace='settings'")
        .all() as Array<{ key: string; value: string }>);
  const settings = interpretSettingsRows(rows, process.env, decrypt);
  return {
    settings,
    revision: parseSettingsRevision(rows.find((row) => row.key === SETTINGS_REVISION_KEY)?.value),
    generation: readAuthGeneration(settings),
  };
}
