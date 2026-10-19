import { readAuthGeneration, AUTH_GENERATION_SETTING } from "./authSettingsSemantics";
import type { SyncTokenRecord } from "./syncTokens";
import { apiKeyLifecycleAllows } from "./apiKeys/persistenceSemantics";
import { isSyntheticApiKeyId } from "@/shared/constants/apiKeyIdentities";

/** SQLite historical datetime text has UTC semantics, never process-local time. */
export function authTimestamp(value: unknown): number {
  if (typeof value !== "string" || !value.trim()) return NaN;
  const utc = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?$/.test(value)
    ? value.replace(" ", "T") + "Z"
    : value;
  return Date.parse(utc);
}

export function syncTokenIsLive(
  record: SyncTokenRecord,
  generation: string | null,
  linkedKey: Record<string, unknown> | null,
  now = Date.now()
): boolean {
  if (
    !generation ||
    record.authGeneration !== generation ||
    !["session", "cli", "internal"].includes(record.issuedBy ?? "") ||
    record.revokedAt ||
    !Number.isFinite(authTimestamp(record.createdAt)) ||
    !Number.isFinite(authTimestamp(record.expiresAt)) ||
    authTimestamp(record.expiresAt) <= now
  )
    return false;
  if (!record.syncApiKeyId || isSyntheticApiKeyId(record.syncApiKeyId)) return true;
  if (!linkedKey || !apiKeyLifecycleAllows({ ...linkedKey, expires_at: null }, now)) return false;
  return (
    !linkedKey.expires_at ||
    (Number.isFinite(authTimestamp(linkedKey.expires_at)) &&
      authTimestamp(linkedKey.expires_at) > now)
  );
}

/** One statement observes token, generation and linked key in the same MVCC snapshot. */
export const SYNC_TOKEN_AUTH_SELECT = `SELECT t.*,g.value AS current_generation,
  k.id AS linked_id,k.is_active AS linked_active,k.is_banned AS linked_banned,
  k.revoked_at AS linked_revoked,k.expires_at AS linked_expiry
  FROM sync_tokens t LEFT JOIN key_value g ON g.namespace='settings' AND g.key='managementAuthGeneration'
  LEFT JOIN api_keys k ON k.id=t.sync_api_key_id WHERE t.token_hash=`;

export function syncRowAuthorization(row: Record<string, unknown>) {
  let generation: string | null = null;
  try {
    const parsed = JSON.parse(String(row.current_generation));
    generation = readAuthGeneration({ [AUTH_GENERATION_SETTING]: parsed });
  } catch {
    /* fail closed */
  }
  return {
    generation,
    linkedKey: row.linked_id
      ? {
          is_active: row.linked_active,
          is_banned: row.linked_banned,
          revoked_at: row.linked_revoked,
          expires_at: row.linked_expiry,
        }
      : null,
  };
}
