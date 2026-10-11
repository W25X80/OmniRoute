/** Shared connection construction and storage mapping. No database access. */

import { normalizeProviderSpecificData } from "@/lib/providers/requestDefaults";
import { withDerivedCookieExpiry } from "@/shared/utils/webCookieExpiry";
import { LOCAL_PROVIDERS, WEB_COOKIE_PROVIDERS } from "@/shared/constants/providers";
import { ensureCodexFingerprintSeed } from "@omniroute/open-sse/config/codexIdentity.ts";
import {
  normalizeBooleanColumn,
  sanitizeQuotaWindowThresholds,
  sanitizeRateLimitOverrides,
  serializeJsonField,
  type JsonRecord,
} from "./columns";

/**
 * normalizeProviderSpecificData + the Codex fingerprint-seed invariant: Codex
 * OAuth connections whose convergence mode derives account-scoped identities
 * (device/session/full — the default session included) carry a persisted
 * random seed (`codexFingerprintSeed`) as the derivation source. Created here
 * at the persistence choke point so every write path (manual create, OAuth
 * persist, edit, import) is covered; the seed is never regenerated once valid,
 * so identities stay put across saves. Pre-seed connections rotate from the
 * legacy connection-id derivation exactly once on their next write — the
 * OmniRoute analog of sub2api's migration-225 backfill (v0.1.178, #5696).
 */
export function normalizeConnectionProviderSpecificData(
  provider: string | null,
  providerSpecificData: unknown,
  credentials: { accessToken?: unknown; refreshToken?: unknown },
  existingProviderSpecificData?: unknown
) {
  const normalized = normalizeProviderSpecificData(provider, providerSpecificData);
  const withExpiry = withDerivedCookieExpiryForProvider(provider, normalized, credentials);
  if (provider !== "codex") return withExpiry;
  return ensureCodexFingerprintSeed(
    withExpiry,
    credentials,
    (existingProviderSpecificData as Record<string, unknown> | null) ?? null
  );
}

function withDerivedCookieExpiryForProvider(
  provider: string | null,
  providerSpecificData: unknown,
  credentials: { accessToken?: unknown; refreshToken?: unknown } | unknown
): Record<string, unknown> {
  const key = String(provider || "").toLowerCase();
  if (!(WEB_COOKIE_PROVIDERS as Record<string, unknown>)[key]) {
    // Both branches must satisfy the Codex seed signature below; the
    // passthrough keeps whatever shape normalization already returned.
    return (providerSpecificData ?? {}) as Record<string, unknown>;
  }
  const source = credentials as Record<string, unknown> | null;
  const credential =
    source && typeof source === "object"
      ? (typeof source.apiKey === "string" && source.apiKey) ||
        (typeof source.cookie === "string" && source.cookie) ||
        null
      : null;
  return withDerivedCookieExpiry(providerSpecificData, credential);
}
export function isLocalProviderId(providerId: unknown): boolean {
  return (
    typeof providerId === "string" &&
    Object.prototype.hasOwnProperty.call(LOCAL_PROVIDERS, providerId)
  );
}

/** Trim + strip a trailing slash so cosmetic differences don't defeat the match. */
export function normalizeBaseUrlForDedup(value: unknown): string {
  return typeof value === "string" ? value.trim().replace(/\/+$/, "") : "";
}

export function buildNewProviderConnection(
  data: JsonRecord,
  normalizedProviderSpecificData: JsonRecord | undefined,
  now: string,
  id: string,
  maxPriority: number
): JsonRecord {
  // Generate name: prefer explicit name, then email, then a stable short-ID label.
  // Avoid sequential "Account N" — it reassigns when accounts are deleted/reordered.
  let connectionName = data.name || null;
  if (!connectionName && (data.authType === "oauth" || data.authType === "access_token")) {
    if (data.email) {
      connectionName = data.email as string;
    } else if (data.displayName) {
      connectionName = data.displayName as string;
    }
    // Otherwise leave null — UI will fall back to getAccountDisplayName() → "Account #<id>"
  }

  // Auto-increment priority
  let connectionPriority = data.priority;
  if (!connectionPriority) {
    connectionPriority = maxPriority + 1;
  }

  const connection: Record<string, unknown> = {
    id,
    provider: data.provider,
    authType: data.authType || "oauth",
    name: connectionName,
    priority: connectionPriority,
    isActive: data.isActive !== undefined ? data.isActive : true,
    createdAt: now,
    updatedAt: now,
    proxyEnabled: normalizeBooleanColumn(data.proxyEnabled, true),
    perKeyProxyEnabled: normalizeBooleanColumn(data.perKeyProxyEnabled, false),
    quotaVisible: normalizeBooleanColumn(data.quotaVisible, true),
  };

  // Optional fields
  const optionalFields = [
    "displayName",
    "email",
    "globalPriority",
    "defaultModel",
    "accessToken",
    "refreshToken",
    "expiresAt",
    // #5326's payload sets this and _insertConnectionRow binds it, but it was
    // missing from this allowlist — so every created row stored NULL however good
    // the payload was. The update path already carries it (`data.tokenExpiresAt`).
    "tokenExpiresAt",
    "tokenType",
    "scope",
    "idToken",
    "projectId",
    "apiKey",
    "testStatus",
    "lastTested",
    "lastError",
    "lastErrorAt",
    "lastErrorType",
    "lastErrorSource",
    "rateLimitedUntil",
    "expiresIn",
    "errorCode",
    "consecutiveUseCount",
    "rateLimitProtection",
    "group",
    "maxConcurrent",
    "proxyEnabled",
    "perKeyProxyEnabled",
    "quotaVisible",
    "quotaWindowThresholds",
    "rateLimitOverrides",
    "healthCheckInterval",
  ];
  for (const field of optionalFields) {
    if (data[field] !== undefined && data[field] !== null) {
      connection[field] = data[field];
    }
  }
  if (normalizedProviderSpecificData && Object.keys(normalizedProviderSpecificData).length > 0) {
    connection.providerSpecificData = normalizedProviderSpecificData;
  }
  // Sanitize the window-thresholds map up front so the in-memory `connection`
  // matches the row we're about to insert. The serialize path runs the same
  // sanitizer on the way to SQLite. Assigning null (when sanitize collapses
  // to no-overrides) keeps the field present on the returned object so the
  // UI can tell "field was read, no overrides" apart from "field absent."
  if ("quotaWindowThresholds" in connection) {
    const result = sanitizeQuotaWindowThresholds(connection.quotaWindowThresholds);
    if (result.rejected.length > 0) {
      throw new Error(
        `Refusing to persist quotaWindowThresholds with rejected keys: ${result.rejected.join(", ")}`
      );
    }
    connection.quotaWindowThresholds = result.sanitized;
  }

  // Same sanitization for rateLimitOverrides — keep in-memory representation
  // in sync with what gets persisted. Reject (don't silently drop) invalid
  // keys/values so a direct DB writer can't lose operator intent.
  if ("rateLimitOverrides" in connection) {
    const result = sanitizeRateLimitOverrides(connection.rateLimitOverrides);
    if (result.rejected.length > 0) {
      throw new Error(
        `Refusing to persist rateLimitOverrides with rejected keys: ${result.rejected.join(", ")}`
      );
    }
    connection.rateLimitOverrides = result.sanitized;
  }

  return connection;
}
export function buildInsertConnectionRowParams(conn: JsonRecord) {
  return {
    id: conn.id,
    provider: conn.provider,
    authType: conn.authType || null,
    name: conn.name || null,
    email: conn.email || null,
    priority: conn.priority || 0,
    isActive: conn.isActive === false ? 0 : 1,
    accessToken: conn.accessToken || null,
    refreshToken: conn.refreshToken || null,
    expiresAt: conn.expiresAt || null,
    tokenExpiresAt: conn.tokenExpiresAt || null,
    scope: conn.scope || null,
    projectId: conn.projectId || null,
    testStatus: conn.testStatus || null,
    errorCode: conn.errorCode || null,
    lastError: conn.lastError || null,
    lastErrorAt: conn.lastErrorAt || null,
    lastErrorType: conn.lastErrorType || null,
    lastErrorSource: conn.lastErrorSource || null,
    backoffLevel: conn.backoffLevel || 0,
    rateLimitedUntil: conn.rateLimitedUntil || null,
    healthCheckInterval: conn.healthCheckInterval ?? null,
    lastHealthCheckAt: conn.lastHealthCheckAt || null,
    lastTested: conn.lastTested || null,
    apiKey: conn.apiKey || null,
    idToken: conn.idToken || null,
    providerSpecificData: conn.providerSpecificData
      ? JSON.stringify(conn.providerSpecificData)
      : null,
    expiresIn: conn.expiresIn || null,
    displayName: conn.displayName || null,
    globalPriority: conn.globalPriority || null,
    defaultModel: conn.defaultModel || null,
    tokenType: conn.tokenType || null,
    consecutiveUseCount: conn.consecutiveUseCount || 0,
    rateLimitProtection:
      conn.rateLimitProtection === true || conn.rateLimitProtection === 1 ? 1 : 0,
    lastUsedAt: conn.lastUsedAt || null,
    group: conn.group || null,
    maxConcurrent: conn.maxConcurrent ?? null,
    proxyEnabled: normalizeBooleanColumn(conn.proxyEnabled, true) ? 1 : 0,
    perKeyProxyEnabled: normalizeBooleanColumn(conn.perKeyProxyEnabled, false) ? 1 : 0,
    quotaVisible: normalizeBooleanColumn(conn.quotaVisible, true) ? 1 : 0,
    quotaWindowThresholdsJson: serializeJsonField(conn.quotaWindowThresholds),
    rateLimitOverridesJson: serializeJsonField(conn.rateLimitOverrides),
    createdAt: conn.createdAt,
    updatedAt: conn.updatedAt,
  };
}

export function buildUpdateConnectionRowParams(id: string, data: JsonRecord, now: unknown) {
  return {
    id,
    provider: data.provider,
    authType: data.authType || null,
    name: data.name || null,
    email: data.email || null,
    priority: data.priority || 0,
    isActive: data.isActive === false ? 0 : 1,
    accessToken: data.accessToken || null,
    refreshToken: data.refreshToken || null,
    expiresAt: data.expiresAt || null,
    tokenExpiresAt: data.tokenExpiresAt || null,
    scope: data.scope || null,
    projectId: data.projectId || null,
    testStatus: data.testStatus || null,
    errorCode: data.errorCode || null,
    lastError: data.lastError || null,
    lastErrorAt: data.lastErrorAt || null,
    lastErrorType: data.lastErrorType || null,
    lastErrorSource: data.lastErrorSource || null,
    backoffLevel: data.backoffLevel || 0,
    rateLimitedUntil: data.rateLimitedUntil || null,
    healthCheckInterval: data.healthCheckInterval ?? null,
    lastHealthCheckAt: data.lastHealthCheckAt || null,
    lastTested: data.lastTested || null,
    apiKey: data.apiKey || null,
    idToken: data.idToken || null,
    providerSpecificData: data.providerSpecificData
      ? JSON.stringify(data.providerSpecificData)
      : null,
    expiresIn: data.expiresIn || null,
    displayName: data.displayName || null,
    globalPriority: data.globalPriority || null,
    defaultModel: data.defaultModel || null,
    tokenType: data.tokenType || null,
    consecutiveUseCount: data.consecutiveUseCount || 0,
    rateLimitProtection:
      data.rateLimitProtection === true || data.rateLimitProtection === 1 ? 1 : 0,
    lastUsedAt: data.lastUsedAt || null,
    group: data.group || null,
    maxConcurrent: data.maxConcurrent ?? null,
    quotaWindowThresholdsJson: serializeJsonField(data.quotaWindowThresholds),
    proxyEnabled: normalizeBooleanColumn(data.proxyEnabled, true) ? 1 : 0,
    perKeyProxyEnabled: normalizeBooleanColumn(data.perKeyProxyEnabled, false) ? 1 : 0,
    quotaVisible: normalizeBooleanColumn(data.quotaVisible, true) ? 1 : 0,
    rateLimitOverridesJson: serializeJsonField(data.rateLimitOverrides),
    lastPingAt: data.lastPingAt || null,
    lastPingedResetKey: data.lastPingedResetKey || null,
    updatedAt: now,
  };
}
