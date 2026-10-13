import { pickCodexConnectionForUser } from "@/lib/oauth/utils/codexConnectionSelection";
import { isCommonChatGptWebRetiredProviderId } from "@/shared/constants/chatgptWebRetirement";
import { isMicrosoftDesignerWebRetiredProviderId } from "@/shared/constants/designerWebRetirement";
import { isRuntimeRetiredProviderId } from "@/shared/constants/providerRetirement";
import { cleanNulls, rowToCamel } from "../caseMapping";
import { decryptConnectionFields, encryptConnectionFields, looksEncrypted } from "../encryption";
import {
  sanitizeQuotaWindowThresholds,
  sanitizeRateLimitOverrides,
  toRecord,
  toStringOrNull,
  withNullableMaxConcurrent,
  withNullableQuotaWindowThresholds,
  withNullableRateLimitOverrides,
  type JsonRecord,
} from "../providers/columns";
import {
  isLocalProviderId,
  normalizeBaseUrlForDedup,
  normalizeConnectionProviderSpecificData,
} from "../providers/persistenceSemantics";
import {
  isMatchingOauthIdentity,
  isWebCookieProviderId,
  parseProviderSpecificData,
  webSessionCredentialKey,
} from "../webSessionDedup";
import type { PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";

export const PROVIDER_CONNECTION_ORDER =
  "priority ASC NULLS FIRST, updated_at DESC NULLS LAST, insertion_order ASC";

export function connectionRequiresPolicyReadback(provider: unknown): boolean {
  return (
    isMicrosoftDesignerWebRetiredProviderId(provider) ||
    isRuntimeRetiredProviderId(provider) ||
    isCommonChatGptWebRetiredProviderId(provider)
  );
}

// Deliberately matches the existing public projection allowlist, including its exclusions.
export const PROVIDER_CONNECTION_PROJECTION_COLUMNS = new Set(
  `id provider auth_type name email priority is_active access_token refresh_token expires_at
   token_expires_at scope project_id test_status error_code last_error last_error_at
   last_error_type last_error_source backoff_level rate_limited_until health_check_interval
   last_health_check_at last_tested api_key id_token provider_specific_data expires_in
   display_name global_priority default_model token_type consecutive_use_count
   rate_limit_protection last_used_at group max_concurrent proxy_enabled per_key_proxy_enabled
   quota_window_thresholds_json rate_limit_overrides_json created_at updated_at synced_models_at`
    .split(/\s+/)
    .filter(Boolean)
);

export function lockProviders(transaction: PostgresTransaction): Promise<unknown> {
  return acquirePostgresLock(transaction, "providers");
}

export function normalizeConnectionResult(record: JsonRecord): JsonRecord {
  return withNullableRateLimitOverrides(
    withNullableQuotaWindowThresholds(
      withNullableMaxConcurrent(cleanNulls(record), record),
      record
    ),
    record
  );
}

export function parseConnectionRow(row: JsonRecord): JsonRecord {
  const { insertion_order: _insertionOrder, ...source } = row;
  return normalizeConnectionResult(toRecord(rowToCamel(source)));
}

export function sanitizeConnectionMaps(record: JsonRecord): void {
  for (const [field, sanitize] of [
    ["quotaWindowThresholds", sanitizeQuotaWindowThresholds],
    ["rateLimitOverrides", sanitizeRateLimitOverrides],
  ] as const) {
    if (!(field in record)) continue;
    const result = sanitize(record[field]);
    if (result.rejected.length > 0) {
      throw new Error(
        `Refusing to persist ${field} with rejected keys: ${result.rejected.join(", ")}`
      );
    }
    record[field] = result.sanitized;
  }
}

const CREDENTIAL_FIELDS = ["apiKey", "accessToken", "refreshToken", "idToken"] as const;

/** Encrypt only supplied credentials; unrelated writes retain the original ciphertext bytes. */
export function connectionPersistence(
  merged: JsonRecord,
  patch: JsonRecord,
  existing?: JsonRecord
): JsonRecord {
  const persistence = { ...merged };
  for (const field of CREDENTIAL_FIELDS) {
    const value = merged[field];
    if ((!existing || Object.hasOwn(patch, field)) && value != null && typeof value !== "string") {
      throw new Error("PostgreSQL provider credentials must be strings or null");
    }
  }
  if (existing) {
    for (const field of CREDENTIAL_FIELDS) {
      if (!Object.hasOwn(patch, field)) persistence[field] = existing[field];
    }
  }
  const encrypted = encryptConnectionFields(persistence);
  for (const field of CREDENTIAL_FIELDS) {
    const supplied = !existing || Object.hasOwn(patch, field);
    const value = merged[field];
    if (
      supplied &&
      typeof value === "string" &&
      value.length > 0 &&
      !looksEncrypted(encrypted[field])
    ) {
      throw new Error("PostgreSQL provider credential encryption failed");
    }
  }
  return encrypted;
}

export function mergeConnectionPatch(
  existing: JsonRecord,
  data: JsonRecord,
  now: string,
  clearChildCooldowns: boolean
): JsonRecord {
  const merged: JsonRecord = { ...existing, ...data, updatedAt: now };
  merged.providerSpecificData = normalizeConnectionProviderSpecificData(
    toStringOrNull(merged.provider),
    merged.providerSpecificData,
    merged,
    existing.providerSpecificData
  );
  if (
    clearChildCooldowns &&
    Object.hasOwn(data, "rateLimitedUntil") &&
    (data.rateLimitedUntil == null || data.rateLimitedUntil === "")
  ) {
    const psd = { ...toRecord(merged.providerSpecificData) };
    delete psd.codexScopeRateLimitedUntil;
    delete psd.codexScopeRateLimitSource;
    merged.providerSpecificData = psd;
  }
  if (clearChildCooldowns) sanitizeConnectionMaps(merged);
  return merged;
}

function codexData(row: JsonRecord): JsonRecord {
  // SQLite's json_extract rejects malformed JSON in the Codex identity query.
  const raw = row.provider_specific_data;
  return toRecord(typeof raw === "string" ? JSON.parse(raw) : raw);
}

function findCodexIdentity(
  rows: JsonRecord[],
  incoming: JsonRecord,
  psd: JsonRecord
): { existing: JsonRecord | null; promoted: boolean } {
  const workspaceId = toStringOrNull(psd.workspaceId);
  const userId = toStringOrNull(psd.chatgptUserId);
  const oauthRows = rows.filter((row) => row.auth_type === "oauth");
  if (userId) {
    const sameWorkspace = oauthRows.filter((row) => {
      const workspace = codexData(row).workspaceId;
      return workspaceId ? workspace === workspaceId : workspace == null || workspace === "";
    });
    const strong = sameWorkspace.find((row) => codexData(row).chatgptUserId === userId);
    if (strong) return { existing: strong, promoted: false };
    if (workspaceId) {
      const candidates = [...sameWorkspace].sort((a, b) =>
        String(a.created_at) < String(b.created_at)
          ? -1
          : String(a.created_at) > String(b.created_at)
            ? 1
            : 0
      );
      const existing = pickCodexConnectionForUser(
        candidates,
        userId,
        toStringOrNull(incoming.email)
      );
      return { existing, promoted: existing !== null };
    }
  } else if (workspaceId && incoming.email) {
    return {
      existing:
        oauthRows.find(
          (row) => codexData(row).workspaceId === workspaceId && row.email === incoming.email
        ) ?? null,
      promoted: false,
    };
  }
  return { existing: null, promoted: false };
}

/** Input rows are in source insertion order, matching SQLite's first-row dedup behavior. */
export function findExistingConnection(
  rows: JsonRecord[],
  data: JsonRecord,
  normalizedProviderSpecificData: JsonRecord
): { existing: JsonRecord | null; promoted: boolean } {
  const psd = toRecord(data.providerSpecificData);
  if (data.authType === "oauth" && data.provider === "codex") {
    return findCodexIdentity(rows, data, psd);
  }
  let existing: JsonRecord | undefined;
  if (data.authType === "oauth" && data.email) {
    existing = rows.find(
      (row) =>
        row.auth_type === "oauth" &&
        row.email === data.email &&
        isMatchingOauthIdentity(
          row,
          toStringOrNull(psd.username),
          toStringOrNull(psd.profileArn),
          toStringOrNull(psd.organizationUUID)
        )
    );
  } else if (data.authType === "apikey") {
    const apiRows = rows.filter((row) => row.auth_type === "apikey");
    if (data.name && !isWebCookieProviderId(data.provider)) {
      existing = apiRows.find((row) => row.name === data.name);
    }
    const apiKey = typeof data.apiKey === "string" ? data.apiKey.trim() : "";
    if (!existing && apiKey) {
      existing = apiRows.find((row) => {
        const decrypted = decryptConnectionFields(toRecord(rowToCamel(row)));
        if (toStringOrNull(decrypted.apiKey)?.trim() !== apiKey) return false;
        return (
          !isLocalProviderId(data.provider) ||
          normalizeBaseUrlForDedup(
            parseProviderSpecificData(row.provider_specific_data)?.baseUrl
          ) === normalizeBaseUrlForDedup(psd.baseUrl)
        );
      });
    }
  } else if (data.authType === "cookie") {
    const cookieRows = rows.filter((row) => row.auth_type === "cookie");
    const credential = webSessionCredentialKey(normalizedProviderSpecificData);
    existing = credential
      ? cookieRows.find(
          (row) =>
            webSessionCredentialKey(parseProviderSpecificData(row.provider_specific_data)) ===
            credential
        )
      : data.name
        ? cookieRows.find(
            (row) =>
              row.name === data.name &&
              !webSessionCredentialKey(parseProviderSpecificData(row.provider_specific_data))
          )
        : undefined;
  }
  return { existing: existing ?? null, promoted: false };
}

export function pageSql(
  limit: number | undefined,
  offset: number | undefined,
  values: unknown[]
): string {
  if (limit === undefined) return "";
  if (!Number.isInteger(limit) || !Number.isInteger(offset ?? 0)) {
    throw new Error("Provider pagination requires integer limit and offset");
  }
  values.push(limit < 0 ? null : limit, Math.max(0, offset ?? 0));
  return ` LIMIT $${values.length - 1} OFFSET $${values.length}`;
}
