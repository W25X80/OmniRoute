import { credentialFailureState, credentialVersion } from "@/lib/providers/credentialFailure";
import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import { getDbInstance, rowToCamel } from "../core";
import { decryptConnectionFields } from "../encryption";
import { backupDbFile } from "../backup";
import { invalidateDbCache } from "../readCache";
import { bumpProxyConfigGeneration } from "../settings";
import { advanceApiKeyPolicyEpoch } from "../postgres/apiKeyPolicyState";
import { afterProviderRuntimeWrite, writeProviderRuntime } from "../postgres/providerRuntime";

function matchesVersion(row: Record<string, unknown> | undefined, expected: string): boolean {
  return !!row && credentialVersion(decryptConnectionFields(rowToCamel(row))) === expected;
}

/** Compare and expire atomically: a failed old credential cannot disable a freshly rotated one. */
export async function expireProviderCredentialsIfUnchanged(
  id: string,
  expectedVersion: string,
  errorCode: unknown
): Promise<boolean> {
  if (!id || !/^[a-f0-9]{64}$/.test(expectedVersion)) return false;
  const failure = credentialFailureState(errorCode, new Date().toISOString());
  const database = getSelectedPostgresDatabase();
  let applied: boolean;
  if (database) {
    applied = await writeProviderRuntime(database, async (transaction) => {
      const { rows } = await transaction.query<Record<string, unknown>>(
        "SELECT access_token,refresh_token FROM omniroute.provider_connections WHERE id=$1 FOR UPDATE",
        [id]
      );
      if (!matchesVersion(rows[0], expectedVersion)) return false;
      await transaction.query(
        `UPDATE omniroute.provider_connections SET is_active=0,test_status='expired',
         last_error=$1,last_error_type=$2,last_error_source='oauth',last_error_at=$3,
         error_code=$4,updated_at=$3 WHERE id=$5`,
        [failure.lastError, failure.lastErrorType, failure.lastErrorAt, failure.errorCode, id]
      );
      await advanceApiKeyPolicyEpoch(transaction);
      return true;
    });
  } else {
    const db = getDbInstance();
    applied = db.transaction(() => {
      const row = db
        .prepare("SELECT access_token,refresh_token FROM provider_connections WHERE id=?")
        .get(id) as Record<string, unknown> | undefined;
      if (!matchesVersion(row, expectedVersion)) return false;
      const result = db
        .prepare(
          `UPDATE provider_connections SET is_active=0,test_status='expired',
           last_error=?,last_error_type=?,last_error_source='oauth',last_error_at=?,
           error_code=?,updated_at=? WHERE id=?`
        )
        .run(
          failure.lastError,
          failure.lastErrorType,
          failure.lastErrorAt,
          failure.errorCode,
          failure.lastErrorAt,
          id
        );
      return result.changes > 0;
    })();
    if (applied) backupDbFile("pre-write");
  }
  if (applied) {
    await afterProviderRuntimeWrite("oauth_terminal_failure", async () => {
      invalidateDbCache("connections", id);
      await bumpProxyConfigGeneration();
    });
  }
  return applied;
}
