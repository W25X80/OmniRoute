import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import { interpretSettingsRows } from "../settings/persistenceSemantics";
import { acquirePostgresLock } from "./locks";

/** Runs only a synchronous in-memory projection while the current policy is authoritative. */
export async function withSelectedContinuationPrivacy<T>(
  apiKeyId: string | null,
  body: () => T,
  options: { requirePipelineLogging?: boolean } = {}
): Promise<T | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) throw new Error("Native continuation privacy requires PostgreSQL");
  if (apiKeyId !== null && (typeof apiKeyId !== "string" || apiKeyId.length === 0))
    throw new Error("Invalid continuation principal");
  return database.transaction(async (transaction) => {
    await acquirePostgresLock(transaction, "keyPolicy", "shared");
    if (apiKeyId !== null) {
      const row = (
        await transaction.query<{ no_log: number }>(
          "SELECT no_log FROM omniroute.api_keys WHERE id=$1",
          [apiKeyId]
        )
      ).rows[0];
      if (!row || row.no_log === 1) return null;
    }
    if (options.requirePipelineLogging) {
      const rows = await transaction.query<{ key: string; value: string }>(
        "SELECT key,value FROM omniroute.key_value WHERE namespace='settings' AND key='call_log_pipeline_enabled'"
      );
      const enabled = interpretSettingsRows(
        rows.rows,
        process.env,
        (value) => value
      ).call_log_pipeline_enabled;
      if (enabled !== true && enabled !== "1" && enabled !== "true") return null;
    }
    // Null principal retains the existing authenticated-by-context/keyless logging path.
    // A provided principal is never treated as anonymous when its row is missing.
    return body();
  });
}
