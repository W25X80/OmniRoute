/**
 * db/providers.js — Provider connections and nodes CRUD.
 */

import { v4 as uuidv4 } from "uuid";
import { getSelectedControlPlane } from "./controlPlaneContext";

import { isCommonChatGptWebRetiredProviderId } from "@/shared/constants/chatgptWebRetirement";
import { getDbInstance, rowToCamel, cleanNulls } from "./core";
import { backupDbFile } from "./backup";
import { encryptConnectionFields, decryptConnectionFields } from "./encryption";
import { createLazyRowProxy } from "./providers/lazyConnectionView";
import { invalidateDbCache, getCachedRawProviderConnections } from "./readCache";
import { invalidateConnectionUpdate, type UpdateOpts } from "./readCache";
import { reorderConnections } from "./providers/deletion";
import { bumpProxyConfigGeneration, getSettings } from "./settings";
import {
  webSessionCredentialKey,
  parseProviderSpecificData,
  isMatchingOauthIdentity,
  isWebCookieProviderId,
} from "./webSessionDedup";
import { pickCodexConnectionForUser } from "@/lib/oauth/utils/codexConnectionSelection";
import { isMicrosoftDesignerWebRetiredProviderId } from "@/shared/constants/designerWebRetirement";
import { reconcileCodexUsageHistory } from "./providers/usageIdentityReconciliation";
import { isRuntimeRetiredProviderId } from "@/shared/constants/providerRetirement";
import { applyCodexChildCooldownClearOnUpdate } from "./providers/codexAccountState";
import {
  normalizeConnectionProviderSpecificData,
  isLocalProviderId,
  normalizeBaseUrlForDedup,
  buildNewProviderConnection,
  buildInsertConnectionRowParams,
  buildUpdateConnectionRowParams as _buildUpdateConnectionRowParams,
} from "./providers/persistenceSemantics";

import {
  withNullableMaxConcurrent,
  withNullableQuotaWindowThresholds,
  withNullableRateLimitOverrides,
  sanitizeRateLimitOverrides,
  toRecord,
  sanitizeQuotaWindowThresholds,
  toStringOrNull,
  toNumberOrZero,
} from "./providers/columns";

type JsonRecord = Record<string, unknown>;

const CONNECTION_CREDENTIAL_FIELDS = ["apiKey", "accessToken", "refreshToken", "idToken"] as const;

export { ManagementPasswordAsCredentialError } from "./providers/credentialGuard";
import { assertApiKeyIsNotManagementPassword as assertCredential } from "./providers/credentialGuard";

async function assertApiKeyIsNotManagementPassword(apiKey: unknown): Promise<void> {
  await assertCredential(apiKey, getSettings);
}

interface StatementLike<TRow = unknown> {
  all: (...params: unknown[]) => TRow[];
  get: (...params: unknown[]) => TRow | undefined;
  run: (...params: unknown[]) => { changes?: number };
}

interface DbLike {
  prepare: <TRow = unknown>(sql: string) => StatementLike<TRow>;
  transaction: <T>(fn: () => T) => () => T;
}

// Real column set for provider_connections (must match the CREATE TABLE in
// core.ts's SCHEMA_SQL). getProviderConnections()'s optional `columns`
// projection is interpolated directly into the SELECT clause, so every
// requested name must be validated against this allowlist before use —
// there is no current caller that passes untrusted input, but the
// projection API itself must never accept an arbitrary string.
export const PROVIDER_CONNECTIONS_COLUMNS = new Set([
  "id",
  "provider",
  "auth_type",
  "name",
  "email",
  "priority",
  "is_active",
  "access_token",
  "refresh_token",
  "expires_at",
  "token_expires_at",
  "scope",
  "project_id",
  "test_status",
  "error_code",
  "last_error",
  "last_error_at",
  "last_error_type",
  "last_error_source",
  "backoff_level",
  "rate_limited_until",
  "health_check_interval",
  "last_health_check_at",
  "last_tested",
  "api_key",
  "id_token",
  "provider_specific_data",
  "expires_in",
  "display_name",
  "global_priority",
  "default_model",
  "token_type",
  "consecutive_use_count",
  "rate_limit_protection",
  "last_used_at",
  "group",
  "max_concurrent",
  "proxy_enabled",
  "per_key_proxy_enabled",
  "quota_window_thresholds_json",
  "rate_limit_overrides_json",
  "created_at",
  "updated_at",
  "synced_models_at",
]);

// ──────────────── Provider Connections ────────────────

/**
 * Returns provider connections as lazy-decrypting proxies: encrypted
 * credential fields (apiKey, accessToken, refreshToken, idToken) are only
 * decrypted on first property access, not eagerly for every row. Column-
 * projected reads (`columns` passed) bypass the raw-row cache — the cache
 * key doesn't account for projection, so a projected read could otherwise
 * poison the cache for a subsequent full-row read of the same filter.
 *
 * When `limit`/`offset` are provided, the cache is also bypassed since
 * the cache key doesn't account for pagination.
 */
export async function getProviderConnections(
  filter: JsonRecord = {},
  limit?: number,
  offset?: number,
  columns?: string[]
) {
  const selected = getSelectedControlPlane();
  if (selected) return selected.providers.list(filter, limit, offset, columns);
  const useCache = !columns?.length && limit === undefined && offset === undefined;
  const raw = useCache
    ? await getCachedRawProviderConnections(filter)
    : await getRawProviderConnections(filter, limit, offset, columns);
  return raw.map(createLazyRowProxy);
}

/**
 * Same as getProviderConnections but WITHOUT decryptConnectionFields.
 * Returns raw rows with encrypted credential fields intact — callers
 * that only need metadata (id, priority, backoffLevel, etc.) avoid
 * the O(n) AES-GCM decrypt cost on every cache fill.
 *
 * Used by the lazy-decryption path in auth selection (auth.ts) where
 * 10k+ connections are filtered in JS but only 1 needs its apiKey
 * decrypted.
 *
 * @param limit — Optional SQL LIMIT clause to cap rows returned
 * @param offset — Optional SQL OFFSET for pagination
 */
export async function getRawProviderConnections(
  filter: JsonRecord = {},
  limit?: number,
  offset?: number,
  columns?: string[]
) {
  const selected = getSelectedControlPlane();
  if (selected) return selected.providers.listRaw(filter, limit, offset, columns);
  const db = getDbInstance() as unknown as DbLike;
  let selectCols = "*";
  if (columns?.length) {
    const invalidColumns = columns.filter((col) => !PROVIDER_CONNECTIONS_COLUMNS.has(col));
    if (invalidColumns.length > 0) {
      throw new Error(
        `getProviderConnections: invalid column(s) requested: ${invalidColumns.join(", ")}`
      );
    }
    // "group" is a reserved SQL keyword — the schema declares it quoted, so
    // it must be re-quoted here too or the generated SELECT is a syntax error.
    selectCols = columns.map((col) => (col === "group" ? `"group"` : col)).join(", ");
  }
  let sql = `SELECT ${selectCols} FROM provider_connections`;
  const conditions: string[] = [];
  const params: Record<string, unknown> = {};

  if (filter.provider) {
    conditions.push("provider = @provider");
    params.provider = filter.provider;
  }
  if (filter.isActive !== undefined) {
    conditions.push("is_active = @isActive");
    params.isActive = filter.isActive ? 1 : 0;
  }
  if (filter.authType) {
    conditions.push("auth_type = @authType");
    params.authType = filter.authType;
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }
  sql += " ORDER BY priority ASC, updated_at DESC";
  if (limit !== undefined) {
    sql += " LIMIT @limit OFFSET @offset";
    params.limit = limit;
    params.offset = offset ?? 0;
  }

  const rows = db.prepare(sql).all(params);
  return rows.map((r) => {
    const camelRow = rowToCamel(r);
    return withNullableRateLimitOverrides(
      withNullableQuotaWindowThresholds(
        withNullableMaxConcurrent(cleanNulls(camelRow), camelRow),
        camelRow
      ),
      camelRow
    );
  });
}

export async function getProviderConnectionsCountAsync(filter: JsonRecord = {}): Promise<number> {
  const selected = getSelectedControlPlane();
  return selected ? selected.providers.count(filter) : getProviderConnectionsCount(filter);
}

export function getProviderConnectionsCount(filter: JsonRecord = {}): number {
  const db = getDbInstance() as unknown as DbLike;
  let sql = "SELECT count(*) as cnt FROM provider_connections";
  const conditions: string[] = [];
  const params: Record<string, unknown> = {};

  if (filter.provider) {
    conditions.push("provider = @provider");
    params.provider = filter.provider;
  }
  if (filter.isActive !== undefined) {
    conditions.push("is_active = @isActive");
    params.isActive = filter.isActive ? 1 : 0;
  }
  if (filter.authType) {
    conditions.push("auth_type = @authType");
    params.authType = filter.authType;
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  const row = db.prepare(sql).get(params) as { cnt: number };
  return row.cnt;
}

export async function getProviderConnectionById(id: string) {
  const selected = getSelectedControlPlane();
  if (selected) return selected.providers.findById(id);
  const db = getDbInstance() as unknown as DbLike;
  const row = db.prepare("SELECT * FROM provider_connections WHERE id = ?").get(id);
  if (!row) return null;

  const camelRow = rowToCamel(row);
  return decryptConnectionFields(
    withNullableRateLimitOverrides(
      withNullableQuotaWindowThresholds(
        withNullableMaxConcurrent(cleanNulls(camelRow), camelRow),
        camelRow
      ),
      camelRow
    )
  );
}

export interface ProviderConnectionDisplayMetadata {
  id: string;
  name: string | null;
  displayName: string | null;
  email: string | null;
}

/**
 * Reads only the non-credential fields needed by account display-name resolvers.
 *
 * This avoids decrypting provider credentials when a dashboard only needs labels.
 */
export function getProviderConnectionDisplayMetadata(
  connectionIds: readonly string[]
): ProviderConnectionDisplayMetadata[] {
  const ids = [...new Set(connectionIds.filter((id) => id.length > 0))];
  if (ids.length === 0) return [];

  const db = getDbInstance() as unknown as DbLike;
  const rows = db
    .prepare(
      `SELECT id, name, display_name, email FROM provider_connections
       WHERE id IN (${ids.map(() => "?").join(", ")})`
    )
    .all(...ids);

  return rows.map((row) => {
    const view = rowToCamel(row) as JsonRecord;
    return {
      id: toStringOrNull(view.id) || "",
      name: toStringOrNull(view.name),
      displayName: toStringOrNull(view.displayName),
      email: toStringOrNull(view.email),
    };
  });
}

export async function getProviderConnectionDisplayMetadataAsync(
  connectionIds: readonly string[]
): Promise<ProviderConnectionDisplayMetadata[]> {
  const selected = getSelectedControlPlane();
  return selected
    ? selected.providers.displayMetadata(connectionIds)
    : getProviderConnectionDisplayMetadata(connectionIds);
}

// #3368 PR6 — dedup web-session cookie/token credentials on connection create.
// Re-importing the same session (e.g. via bulk web-session import) under a
// different or blank name must update the existing connection instead of
// inserting a duplicate, mirroring the apikey dedup (#3023). Extracted from
// createProviderConnection to keep that function below the complexity baseline.
// provider_specific_data is plaintext JSON, so the value is compared directly
// without decryption.
/**
 * #12173 — the API-key-value dedup (#3023) matches purely on `provider +
 * apiKey`, which is correct for hosted providers where the key alone is the
 * account identity. Local/self-hosted providers (LM Studio, Ollama, vLLM,
 * llama.cpp, ...) commonly ship an optional/cosmetic API key, so users
 * legitimately reuse the same placeholder value (e.g. "lm-studio") across two
 * physically distinct servers that are actually distinguished by base URL.
 * Gate the extra baseUrl check to this provider set only — hosted-provider
 * dedup must stay untouched.
 */
/**
 * Find the cookie connection an incoming import should overwrite.
 *
 * #15159 / B-01 — the credential is the identity; `name` is not. This used to try
 * the name FIRST ("name-based upsert for parity with the apikey path"), so
 * re-importing under an existing name returned that row and the caller's
 * `merged = { ...decryptedExisting, ...data }` wrote the new cookie over it.
 * Because `name` is a user-editable display label, two different accounts
 * legitimately share one — and the second import silently destroyed the first
 * account's stored session, unrecoverably.
 *
 * This is the same hazard the OAuth branch of this file already fixed
 * (:532-540): "two different IdPs ... can share the same email address; matching
 * on email alone would silently overwrite the other account's connection on the
 * second login." The cookie branch was never given the same treatment.
 *
 * The name lookup is now only a fallback for rows with no derivable credential
 * key — there the name is genuinely all there is to match on. Every other import
 * matches on its credential, and one with a different credential gets its own row.
 *
 * Trade-off, stated plainly: a *rotated* cookie under a stable name now creates a
 * second row instead of updating in place. That is the intended direction of the
 * error — a caller can delete a stale row, but cannot recover a session that was
 * already overwritten. #3368's actual goal (same cookie under a different name
 * dedupes) is unaffected: that was always the credential loop's job.
 */
function findExistingCookieConnection(
  db: DbLike,
  provider: unknown,
  name: unknown,
  normalizedProviderSpecificData: unknown
): JsonRecord | null {
  // 1) Credential-value dedup — the authoritative identity for a web session.
  const newCredKey = webSessionCredentialKey(normalizedProviderSpecificData);
  if (newCredKey) {
    const cookieRows = db
      .prepare("SELECT * FROM provider_connections WHERE provider = ? AND auth_type = 'cookie'")
      .all(provider) as JsonRecord[];
    for (const row of cookieRows) {
      const psd = parseProviderSpecificData(row.provider_specific_data);
      if (psd && webSessionCredentialKey(psd) === newCredKey) return row;
    }
  }

  // 2) Name fallback, only when this import carries no credential to compare
  //    against AND the candidate row itself has none. Matching a credential-less
  //    import onto a row that *does* hold a credential would clobber it with
  //    nothing — the same data loss, reached from the other direction.
  if (name && !newCredKey) {
    const byName =
      (db
        .prepare(
          "SELECT * FROM provider_connections WHERE provider = ? AND auth_type = 'cookie' AND name = ?"
        )
        .get(provider, name) as JsonRecord | undefined) || null;
    if (
      byName &&
      !webSessionCredentialKey(parseProviderSpecificData(byName.provider_specific_data))
    ) {
      return byName;
    }
  }

  return null;
}

export async function createProviderConnection(data: JsonRecord) {
  const selected = getSelectedControlPlane();
  if (selected) return selected.providers.create(data);
  await assertApiKeyIsNotManagementPassword(data.apiKey);
  const db = getDbInstance() as unknown as DbLike;
  const now = new Date().toISOString();
  const normalizedProviderSpecificData = normalizeConnectionProviderSpecificData(
    toStringOrNull(data.provider),
    data.providerSpecificData,
    data
  );

  let existing: JsonRecord | null = null;
  let promotedCodexIdentity = false;

  const providerSpecificData = toRecord(data.providerSpecificData);
  const workspaceId = toStringOrNull(providerSpecificData.workspaceId);
  const chatgptUserId = toStringOrNull(providerSpecificData.chatgptUserId);

  if (data.authType === "oauth" && data.provider === "codex" && chatgptUserId) {
    const strongSql = workspaceId
      ? "SELECT * FROM provider_connections WHERE provider = ? AND auth_type = 'oauth' AND json_extract(provider_specific_data, '$.workspaceId') = ? AND json_extract(provider_specific_data, '$.chatgptUserId') = ?"
      : "SELECT * FROM provider_connections WHERE provider = ? AND auth_type = 'oauth' AND (json_extract(provider_specific_data, '$.workspaceId') IS NULL OR json_extract(provider_specific_data, '$.workspaceId') = '') AND json_extract(provider_specific_data, '$.chatgptUserId') = ?";
    existing =
      ((workspaceId
        ? db.prepare(strongSql).get(data.provider, workspaceId, chatgptUserId)
        : db.prepare(strongSql).get(data.provider, chatgptUserId)) as JsonRecord | undefined) ||
      null;

    if (!existing && workspaceId) {
      const workspaceMatches = db
        .prepare(
          `SELECT * FROM provider_connections
           WHERE provider = ? AND auth_type = 'oauth'
             AND json_extract(provider_specific_data, '$.workspaceId') = ?
           ORDER BY created_at`
        )
        .all(data.provider, workspaceId) as JsonRecord[];
      existing = pickCodexConnectionForUser(
        workspaceMatches,
        chatgptUserId,
        toStringOrNull(data.email)
      );
      promotedCodexIdentity = existing !== null;
    }
  } else if (data.authType === "oauth" && data.email) {
    if (data.provider === "codex") {
      if (workspaceId) {
        existing =
          (db
            .prepare(
              `SELECT * FROM provider_connections
               WHERE provider = ? AND auth_type = 'oauth'
                 AND json_extract(provider_specific_data, '$.workspaceId') = ?
                 AND email = ?
               LIMIT 1`
            )
            .get(data.provider, workspaceId, data.email) as JsonRecord | undefined) || null;
      }
    } else {
      // For other providers (or Codex without workspaceId), match on email —
      // disambiguated by providerSpecificData.username and/or
      // providerSpecificData.profileArn when present on both sides. Two
      // different IdPs (or two distinct Kiro/AWS profiles authenticated via
      // the same email-carrying IdP) can share the same email address;
      // matching on email alone would silently overwrite the other
      // account's connection on the second login. Only fall back to the
      // bare email-only match when neither side carries a username/profileArn
      // (legacy rows created before this disambiguation existed).
      const incomingUsername = toStringOrNull(providerSpecificData.username);
      const incomingProfileArn = toStringOrNull(providerSpecificData.profileArn);
      // Claude: one identity reaches its personal workspace and every Team
      // organization with the same email and the same accountUUID, so
      // organizationUUID is what separates the accounts.
      const incomingOrganizationUuid = toStringOrNull(providerSpecificData.organizationUUID);
      const emailMatches = db
        .prepare(
          "SELECT * FROM provider_connections WHERE provider = ? AND auth_type = 'oauth' AND email = ?"
        )
        .all(data.provider, data.email) as JsonRecord[];
      existing =
        emailMatches.find((row) =>
          isMatchingOauthIdentity(
            row,
            incomingUsername,
            incomingProfileArn,
            incomingOrganizationUuid
          )
        ) || null;
    }
  } else if (data.authType === "apikey") {
    if (data.name && !isWebCookieProviderId(data.provider)) {
      existing =
        (db
          .prepare(
            "SELECT * FROM provider_connections WHERE provider = ? AND auth_type = 'apikey' AND name = ?"
          )
          .get(data.provider, data.name) as JsonRecord | undefined) || null;
    }
    // #3023 — dedup by API key value: re-adding the same key (under a different
    // or blank name) must update the existing connection, not insert a duplicate
    // row. Stored keys use non-deterministic AES-GCM, so ciphertext can't be
    // compared directly — decrypt each apikey row for this provider and match the
    // plaintext (trimmed) instead.
    const newApiKey = typeof data.apiKey === "string" ? data.apiKey.trim() : "";
    if (!existing && newApiKey) {
      const isLocal = isLocalProviderId(data.provider);
      const newBaseUrl = normalizeBaseUrlForDedup(providerSpecificData.baseUrl);
      const apiKeyRows = db
        .prepare("SELECT * FROM provider_connections WHERE provider = ? AND auth_type = 'apikey'")
        .all(data.provider) as JsonRecord[];
      for (const row of apiKeyRows) {
        const decrypted = decryptConnectionFields(toRecord(rowToCamel(row)));
        if (toStringOrNull(decrypted.apiKey)?.trim() !== newApiKey) continue;
        // #12173 — for local/self-hosted providers, a differing base URL means
        // this is a different physical server, not the same account; fall
        // through to inserting a new connection even though the apiKey matches.
        if (isLocal) {
          const existingBaseUrl = normalizeBaseUrlForDedup(
            parseProviderSpecificData(row.provider_specific_data)?.baseUrl
          );
          if (existingBaseUrl !== newBaseUrl) continue;
        }
        existing = row;
        break;
      }
    }
  } else if (data.authType === "cookie") {
    existing = findExistingCookieConnection(
      db,
      data.provider,
      data.name,
      normalizedProviderSpecificData
    );
  } else if (data.authType === "access_token") {
    // #1290 — bare access-token imports (e.g. a raw ChatGPT website access
    // token with no refresh token) are intentionally never deduped: every
    // import creates a new connection. Unlike oauth (workspace+email) or
    // apikey (key-value) imports, a bare access token has no refresh token
    // and no stable long-lived identity to safely dedup against — matching
    // on email alone here would risk silently overwriting an existing full
    // oauth connection for the same account.
  }

  if (existing) {
    const existingId = toStringOrNull(existing.id);
    if (!existingId) return null;
    const rawExisting = toRecord(rowToCamel(existing));
    const decryptedExisting = decryptConnectionFields({ ...rawExisting });
    const merged: JsonRecord = { ...decryptedExisting, ...data, updatedAt: now };
    merged.providerSpecificData = normalizeConnectionProviderSpecificData(
      toStringOrNull(merged.provider),
      merged.providerSpecificData,
      merged,
      decryptedExisting.providerSpecificData
    );
    const persistence: JsonRecord = { ...merged };
    for (const field of CONNECTION_CREDENTIAL_FIELDS) {
      if (!Object.hasOwn(data, field)) {
        persistence[field] = rawExisting[field];
      }
    }
    db.transaction(() => {
      if (promotedCodexIdentity) {
        reconcileCodexUsageHistory(db, {
          connectionId: existingId,
          existing,
          merged,
          matchedExistingCodexByWorkspace: true,
        });
      }
      _updateConnectionRow(db, existingId, encryptConnectionFields(persistence));
    })();
    backupDbFile("pre-write");
    invalidateDbCache("connections");
    const returnedConnection = withNullableRateLimitOverrides(
      withNullableQuotaWindowThresholds(
        withNullableMaxConcurrent(cleanNulls(merged), merged),
        merged
      ),
      merged
    );

    if (
      isMicrosoftDesignerWebRetiredProviderId(merged.provider) ||
      isRuntimeRetiredProviderId(merged.provider) ||
      isCommonChatGptWebRetiredProviderId(merged.provider)
    ) {
      invalidateDbCache("connections");
      return (await getProviderConnectionById(existingId)) ?? returnedConnection;
    }

    return returnedConnection;
  }

  const max = !data.priority
    ? (db
        .prepare("SELECT MAX(priority) as maxP FROM provider_connections WHERE provider = ?")
        .get(data.provider) as JsonRecord | undefined)
    : undefined;
  const connection = buildNewProviderConnection(
    data,
    normalizedProviderSpecificData,
    now,
    uuidv4(),
    toNumberOrZero(toRecord(max).maxP)
  );

  _insertConnectionRow(db, encryptConnectionFields({ ...connection }));
  const providerId = toStringOrNull(data.provider);
  if (providerId) {
    reorderConnections(db, providerId);
  }
  backupDbFile("pre-write");
  invalidateDbCache("connections"); // Bust connections read cache

  const returnedConnection = withNullableRateLimitOverrides(
    withNullableQuotaWindowThresholds(
      withNullableMaxConcurrent(cleanNulls(connection), connection),
      connection
    ),
    connection
  );

  if (
    isMicrosoftDesignerWebRetiredProviderId(data.provider) ||
    isRuntimeRetiredProviderId(providerId) ||
    isCommonChatGptWebRetiredProviderId(providerId)
  ) {
    return (await getProviderConnectionById(String(connection.id))) ?? returnedConnection;
  }

  return returnedConnection;
}

function _insertConnectionRow(db: DbLike, conn: JsonRecord) {
  db.prepare(
    `
    INSERT INTO provider_connections (
      id, provider, auth_type, name, email, priority, is_active,
      access_token, refresh_token, expires_at, token_expires_at,
      scope, project_id, test_status, error_code, last_error,
      last_error_at, last_error_type, last_error_source, backoff_level,
      rate_limited_until, health_check_interval, last_health_check_at,
      last_tested, api_key, id_token, provider_specific_data,
      expires_in, display_name, global_priority, default_model,
      token_type, consecutive_use_count, rate_limit_protection, last_used_at, "group", max_concurrent,
      proxy_enabled, per_key_proxy_enabled, quota_visible, quota_window_thresholds_json, rate_limit_overrides_json,
      created_at, updated_at
    ) VALUES (
      @id, @provider, @authType, @name, @email, @priority, @isActive,
      @accessToken, @refreshToken, @expiresAt, @tokenExpiresAt,
      @scope, @projectId, @testStatus, @errorCode, @lastError,
      @lastErrorAt, @lastErrorType, @lastErrorSource, @backoffLevel,
      @rateLimitedUntil, @healthCheckInterval, @lastHealthCheckAt,
      @lastTested, @apiKey, @idToken, @providerSpecificData,
      @expiresIn, @displayName, @globalPriority, @defaultModel,
      @tokenType, @consecutiveUseCount, @rateLimitProtection, @lastUsedAt, @group, @maxConcurrent,
      @proxyEnabled, @perKeyProxyEnabled, @quotaVisible, @quotaWindowThresholdsJson, @rateLimitOverridesJson,
      @createdAt, @updatedAt
    )
  `
  ).run(buildInsertConnectionRowParams(conn));
}

// Assembles the `.run()` params for _updateConnectionRow's UPDATE statement.
// Split out purely to keep _updateConnectionRow under the max-lines-per-function
// gate — same field mapping/normalization as before, just relocated.

function _updateConnectionRow(db: DbLike, id: string, data: JsonRecord) {
  const now = data.updatedAt || new Date().toISOString();
  db.prepare(
    `
    UPDATE provider_connections SET
      provider = @provider, auth_type = @authType, name = @name, email = @email,
      priority = @priority, is_active = @isActive, access_token = @accessToken,
      refresh_token = @refreshToken, expires_at = @expiresAt, token_expires_at = @tokenExpiresAt,
      scope = @scope, project_id = @projectId, test_status = @testStatus, error_code = @errorCode,
      last_error = @lastError, last_error_at = @lastErrorAt, last_error_type = @lastErrorType,
      last_error_source = @lastErrorSource, backoff_level = @backoffLevel,
      rate_limited_until = @rateLimitedUntil, health_check_interval = @healthCheckInterval,
      last_health_check_at = @lastHealthCheckAt, last_tested = @lastTested, api_key = @apiKey,
      id_token = @idToken, provider_specific_data = @providerSpecificData,
      expires_in = @expiresIn, display_name = @displayName, global_priority = @globalPriority,
      default_model = @defaultModel, token_type = @tokenType,
      consecutive_use_count = @consecutiveUseCount,
      rate_limit_protection = @rateLimitProtection,
      last_used_at = @lastUsedAt,
      "group" = @group,
      max_concurrent = @maxConcurrent,
      quota_window_thresholds_json = @quotaWindowThresholdsJson,
      proxy_enabled = @proxyEnabled,
      per_key_proxy_enabled = @perKeyProxyEnabled,
      quota_visible = @quotaVisible,
      rate_limit_overrides_json = @rateLimitOverridesJson,
      last_ping_at = @lastPingAt,
      last_pinged_reset_key = @lastPingedResetKey,
      updated_at = @updatedAt
    WHERE id = @id
  `
  ).run(_buildUpdateConnectionRowParams(id, data, now));
}

export async function updateProviderConnection(id: string, data: JsonRecord, opts?: UpdateOpts) {
  const selected = getSelectedControlPlane();
  if (selected) return selected.providers.update(id, data, opts);
  const db = getDbInstance() as unknown as DbLike;
  const existing = db.prepare("SELECT * FROM provider_connections WHERE id = ?").get(id);
  if (!existing) return null;

  // The incoming value only. A connection that already holds the password has
  // to stay editable, or an operator cannot repair the one this guard exists
  // to prevent -- and re-checking the merged value would spend a bcrypt round
  // on every unrelated field edit.
  await assertApiKeyIsNotManagementPassword(data.apiKey);

  const existingCamel = toRecord(rowToCamel(existing));
  const merged: JsonRecord = {
    ...existingCamel,
    ...data,
    updatedAt: new Date().toISOString(),
  };
  merged.providerSpecificData = applyCodexChildCooldownClearOnUpdate(
    data,
    normalizeConnectionProviderSpecificData(
      toStringOrNull(merged.provider),
      merged.providerSpecificData,
      merged,
      existingCamel.providerSpecificData
    )
  );
  // Mirror the sanitization the create path applies — keep the returned
  // object in lockstep with what we persist.
  if ("quotaWindowThresholds" in merged) {
    const result = sanitizeQuotaWindowThresholds(merged.quotaWindowThresholds);
    if (result.rejected.length > 0) {
      throw new Error(
        `Refusing to persist quotaWindowThresholds with rejected keys: ${result.rejected.join(", ")}`
      );
    }
    // For updates we always carry the key forward (even as null) so the read
    // path surfaces the cleared state to callers that merged it.
    merged.quotaWindowThresholds = result.sanitized;
  }
  if ("rateLimitOverrides" in merged) {
    const result = sanitizeRateLimitOverrides(merged.rateLimitOverrides);
    if (result.rejected.length > 0) {
      throw new Error(
        `Refusing to persist rateLimitOverrides with rejected keys: ${result.rejected.join(", ")}`
      );
    }
    merged.rateLimitOverrides = result.sanitized;
  }
  const existingRecord = toRecord(existing);

  db.transaction(() => {
    reconcileCodexUsageHistory(db, {
      connectionId: id,
      existing: existingRecord,
      merged,
    });
    _updateConnectionRow(db, id, encryptConnectionFields({ ...merged }));
  })();
  backupDbFile("pre-write");
  invalidateConnectionUpdate(id, data, opts);
  bumpProxyConfigGeneration();

  // Zero is the internal move-to-top sentinel. Explicit positive priorities
  // are operator-selected values, not ranks to compact after every edit.
  if (data.priority === 0) {
    const existingRecord = toRecord(existing);
    const providerId =
      typeof existingRecord.provider === "string"
        ? existingRecord.provider
        : String(existingRecord.provider || "");
    reorderConnections(db, providerId);
    return getProviderConnectionById(id);
  }

  const returnedConnection = withNullableRateLimitOverrides(
    withNullableQuotaWindowThresholds(
      withNullableMaxConcurrent(cleanNulls(merged), merged),
      merged
    ),
    merged
  );

  if (
    isMicrosoftDesignerWebRetiredProviderId(merged.provider) ||
    isRuntimeRetiredProviderId(merged.provider) ||
    isCommonChatGptWebRetiredProviderId(merged.provider)
  ) {
    return (await getProviderConnectionById(id)) ?? returnedConnection;
  }

  return returnedConnection;
}

export { mergeConnectionProviderSpecificData } from "./providers/providerSpecificDataMerge";

export {
  updateCodexScopedQuotaState,
  updateCodexScopeCooldown,
  applyCodexChildCooldownClearOnUpdate,
  stripCodexChildCooldownFields,
  stripCodexChildCooldownsFromConnection,
  stripCodexChildCooldownsFromConnectionAsync,
  hasCodexScopeCooldown,
  hasCodexScopeCooldownAsync,
  liftCodexScopeCooldownOnHeadroom,
  liftCodexScopeCooldownOnHeadroomAsync,
} from "./providers/codexAccountState";

/**
 * Lightweight stat bump — updates lastUsedAt and consecutiveUseCount without
 * SELECT, re-encrypt, cache invalidation, or file backup.
 * Safe for the hot getProviderCredentials path where only usage stats change.
 * Fixes the cache-thrashing bug where every credential selection invalidated
 * the 5s TTL cache and paid 3000-row decryption cost on the next request.
 */
export async function touchConnectionLastUsed(
  id: string,
  consecutiveUseCount: number
): Promise<void> {
  const selected = getSelectedControlPlane();
  if (selected) return selected.providers.touchLastUsed(id, consecutiveUseCount);
  if (!id) return;
  const db = getDbInstance() as unknown as DbLike;
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE provider_connections SET
      last_used_at = @lastUsedAt,
      consecutive_use_count = @consecutiveUseCount,
      updated_at = @updatedAt
    WHERE id = @id`
  ).run({
    lastUsedAt: now,
    consecutiveUseCount,
    updatedAt: now,
    id,
  });
}

/**
 * #12849: stamp when a connection's synced model catalog was last written.
 * getActiveSyncedCatalog reads this to stop treating a synced catalog as
 * authoritative forever — a connection synced once and never refreshed
 * silently pinned routing to that point-in-time snapshot with no staleness
 * check. Lightweight targeted UPDATE, mirrors touchConnectionLastUsed.
 */
export async function touchConnectionSyncedModelsAt(id: string): Promise<void> {
  const selected = getSelectedControlPlane();
  if (selected) return selected.providers.touchSyncedModelsAt(id);
  if (!id) return;
  const db = getDbInstance() as unknown as DbLike;
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE provider_connections SET
      synced_models_at = @syncedModelsAt,
      updated_at = @updatedAt
    WHERE id = @id`
  ).run({
    syncedModelsAt: now,
    updatedAt: now,
    id,
  });
}

/**
 * Lightweight backoff reset — runs a targeted UPDATE without SELECT or re-encrypt.
 * Follows the `clearConnectionErrorIfUnchanged` pattern but without the CAS check,
 * since the caller already verified the connection is eligible for reset.
 * Resets all backoff/error columns so the connection re-enters the selection pool.
 * Does invalidateDbCache + bumpProxyConfigGeneration since backoff affects priority.
 * #13389: `skipModelCatalog` — the catalog builder never reads backoff/error
 * state, so this must not bust the expensive-to-rebuild `/v1/models` cache.
 */
export async function resetConnectionBackoff(id: string): Promise<void> {
  const selected = getSelectedControlPlane();
  if (selected) return selected.providers.resetBackoff(id);
  if (!id) return;
  const db = getDbInstance() as unknown as DbLike;
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE provider_connections SET
      backoff_level = 0,
      test_status = 'active',
      last_error = NULL,
      last_error_at = NULL,
      last_error_type = NULL,
      last_error_source = NULL,
      error_code = NULL,
      updated_at = @updatedAt
    WHERE id = @id`
  ).run({
    updatedAt: now,
    id,
  });
  invalidateDbCache("connections", id, { skipModelCatalog: true });
  bumpProxyConfigGeneration();
}

export async function cleanupProviderConnections() {
  return 0;
}

export async function getDistinctGroups(): Promise<string[]> {
  const selected = getSelectedControlPlane();
  if (selected) return selected.providers.distinctGroups();
  const db = getDbInstance() as unknown as DbLike;
  const rows = db
    .prepare(
      'SELECT DISTINCT "group" FROM provider_connections WHERE "group" IS NOT NULL ORDER BY "group"'
    )
    .all() as Array<{ group?: string }>;
  return rows.map((r) => String(r.group ?? "")).filter(Boolean);
}

export {
  autoMigrateLegacyEncryptedConnections,
  autoMigrateLegacyEncryptedConnectionsAsync,
  getGheCopilotHosts,
  getGheCopilotHostsAsync,
} from "./providers/migrations";
export {
  deleteProviderConnection,
  deleteProviderConnections,
  deleteProviderConnectionsByProvider,
  reorderProviderConnections,
} from "./providers/deletion";

// ──────────────── Re-exports from leaf modules ────────────────

export {
  getProviderNodes,
  getProviderNodesCount,
  getProviderNodesCountAsync,
  getProviderNodeById,
  resolveProviderNodeForConnection,
  createProviderNode,
  updateProviderNode,
  deleteProviderNode,
} from "./providers/nodes";
export {
  setConnectionRateLimitUntil,
  setConnectionRateLimitUntilAsync,
  markConnectionRateLimitedUntil,
  markConnectionRateLimitedUntilAsync,
  clearConnectionRateLimit,
  clearConnectionRateLimitAsync,
  getEffectiveQuotaUsage,
  clearStaleCrashCooldowns,
  clearStaleCrashCooldownsAsync,
  formatResetCountdown,
  isConnectionRateLimited,
  isConnectionRateLimitedAsync,
  getRateLimitedConnections,
  getRateLimitedConnectionsAsync,
  clearConnectionErrorIfUnchanged,
} from "./providers/rateLimit";
