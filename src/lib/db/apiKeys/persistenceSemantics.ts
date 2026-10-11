import { rowToCamel } from "../caseMapping";
import { parseApiKeyCodexServiceMode } from "@/shared/constants/codexServiceMode";
import type { ApiKeyMetadata, ApiKeyRecord } from "@/domain/persistence/apiKeyRepository";
import { parseApiKeyUsageLimitFields } from "../apiKeyUsageLimitFields";
import {
  parseAllowedModels,
  parseAllowedCombos,
  parseNoLog,
  parseAutoResolve,
  parseDisableNonPublicModels,
  parseAllowUsageCommand,
  parseIsActive,
  parseAccessSchedule,
  parseRateLimits,
  parseAllowedConnections,
  parseAllowedQuotas,
  parseStringList,
  parseNullableTimestamp,
  parseIsBanned,
  parseStreamDefaultMode,
  parseCacheDefaultMode,
  parseChaosModeEnabled,
  parseCompressionEnabled,
  parseAllowAutoCombos,
  parseCatalogScope,
  parseModelAccessMode,
} from "./rowParsers";

export class ApiKeyPolicyInvariantError extends Error {
  readonly code = "LEASE_KEY_POLICY_INVALID";
}

export function assertExclusiveLeaseKeyPolicy(
  scopes: readonly string[],
  allowedConnections: readonly string[]
): void {
  if (scopes.includes("lease:exclusive") && allowedConnections.length === 0) {
    throw new ApiKeyPolicyInvariantError("lease:exclusive requires explicit allowedConnections");
  }
}

/** Pure hydration mirrors the SQLite facade's metadata representation. */
export function hydrateApiKeyMetadata(row: Record<string, unknown>): ApiKeyMetadata {
  const record = row;
  const metadataId = typeof record.id === "string" ? record.id : "";
  const metadataName = typeof record.name === "string" ? record.name : "";
  const machineIdRaw = record.machine_id ?? record.machineId;
  const metadataMachineId = typeof machineIdRaw === "string" ? machineIdRaw : null;

  const rawMaxRPD = record.max_requests_per_day ?? record.maxRequestsPerDay;
  const rawMaxRPM = record.max_requests_per_minute ?? record.maxRequestsPerMinute;
  const rawThrottleDelayMs = record.throttle_delay_ms ?? record.throttleDelayMs;

  const rawMaxSessions = record.max_sessions ?? record.maxSessions;

  const rawAllowedModels = record.allowed_models ?? record.allowedModels;
  const metadata: ApiKeyMetadata = {
    id: metadataId,
    name: metadataName,
    machineId: metadataMachineId,
    modelAccessMode: parseModelAccessMode(
      record.model_access_mode ?? record.modelAccessMode,
      rawAllowedModels
    ),
    allowedModels: parseAllowedModels(rawAllowedModels),
    blockedModels: parseAllowedModels(record.blocked_models ?? record.blockedModels),
    allowedCombos: parseAllowedCombos(record.allowed_combos ?? record.allowedCombos),
    allowedConnections: parseAllowedConnections(
      record.allowed_connections ?? record.allowedConnections
    ),
    allowedQuotas: parseAllowedQuotas(record.allowed_quotas ?? record.allowedQuotas),
    noLog: parseNoLog(record.no_log ?? record.noLog),
    autoResolve: parseAutoResolve(record.auto_resolve ?? record.autoResolve),
    isActive: parseIsActive(record.is_active ?? record.isActive),
    accessSchedule: parseAccessSchedule(record.access_schedule ?? record.accessSchedule),
    rateLimits: parseRateLimits(record.rate_limits ?? record.rateLimits),
    maxRequestsPerDay: typeof rawMaxRPD === "number" && rawMaxRPD > 0 ? rawMaxRPD : null,
    maxRequestsPerMinute: typeof rawMaxRPM === "number" && rawMaxRPM > 0 ? rawMaxRPM : null,
    throttleDelayMs:
      typeof rawThrottleDelayMs === "number" && rawThrottleDelayMs > 0 ? rawThrottleDelayMs : null,
    // T08: max concurrent sessions; 0 = unlimited (default & backward-compatible)
    maxSessions: typeof rawMaxSessions === "number" && rawMaxSessions > 0 ? rawMaxSessions : 0,
    revokedAt: parseNullableTimestamp(record.revoked_at ?? record.revokedAt),
    expiresAt: parseNullableTimestamp(record.expires_at ?? record.expiresAt),
    ipAllowlist: parseStringList(record.ip_allowlist ?? record.ipAllowlist),
    scopes: parseStringList(record.scopes),
    isBanned: parseIsBanned(record.is_banned ?? record.isBanned),
    keyHash: (record.key_hash ?? record.keyHash) as string | null,
    proxyId:
      typeof record.proxy_id === "string" && record.proxy_id.trim() !== "" ? record.proxy_id : null,
    allowedEndpoints: parseStringList(record.allowed_endpoints ?? record.allowedEndpoints),
    streamDefaultMode: parseStreamDefaultMode(
      record.stream_default_mode ?? record.streamDefaultMode
    ),
    codexServiceMode: parseApiKeyCodexServiceMode(
      record.codex_service_mode ?? record.codexServiceMode
    ),
    cacheDefaultMode: parseCacheDefaultMode(record.cache_default_mode ?? record.cacheDefaultMode),
    disableNonPublicModels: parseDisableNonPublicModels(
      record.disable_non_public_models ?? record.disableNonPublicModels
    ),
    allowUsageCommand: parseAllowUsageCommand(
      record.allow_usage_command ?? record.allowUsageCommand
    ),
    chaosModeEnabled: parseChaosModeEnabled(record.chaos_mode_enabled ?? record.chaosModeEnabled),
    compressionEnabled: parseCompressionEnabled(
      record.compression_enabled ?? record.compressionEnabled
    ),
    allowAutoCombos: parseAllowAutoCombos(record.allow_auto_combos ?? record.allowAutoCombos),
    catalogScope: parseCatalogScope(record.catalog_scope ?? record.catalogScope),
    ...parseApiKeyUsageLimitFields(record),
  };

  return metadata;
}

/** Administrative views retain source nullable scalars and raw fields not parsed by SQLite. */
export function hydrateApiKeyRecord(row: Record<string, unknown>): ApiKeyRecord {
  const camelRow = rowToCamel(row) as ApiKeyRecord;
  camelRow.modelAccessMode = parseModelAccessMode(camelRow.modelAccessMode, camelRow.allowedModels);
  camelRow.allowedModels = parseAllowedModels(camelRow.allowedModels);
  camelRow.blockedModels = parseAllowedModels(camelRow.blockedModels);
  camelRow.allowedCombos = parseAllowedCombos(camelRow.allowedCombos);
  camelRow.allowedConnections = parseAllowedConnections(camelRow.allowedConnections);
  camelRow.allowedQuotas = parseAllowedQuotas(camelRow.allowedQuotas);
  camelRow.noLog = parseNoLog(camelRow.noLog);
  camelRow.autoResolve = parseAutoResolve(camelRow.autoResolve);
  camelRow.isActive = parseIsActive(camelRow.isActive);
  camelRow.accessSchedule = parseAccessSchedule(camelRow.accessSchedule);
  camelRow.rateLimits = parseRateLimits(camelRow.rateLimits);
  camelRow.isBanned = parseIsBanned(camelRow.isBanned);
  camelRow.scopes = parseStringList(camelRow.scopes);
  camelRow.allowedEndpoints = parseStringList(camelRow.allowedEndpoints);
  camelRow.streamDefaultMode = parseStreamDefaultMode(camelRow.streamDefaultMode);
  camelRow.codexServiceMode = parseApiKeyCodexServiceMode(camelRow.codexServiceMode);
  camelRow.cacheDefaultMode = parseCacheDefaultMode(camelRow.cacheDefaultMode);
  camelRow.disableNonPublicModels = parseDisableNonPublicModels(camelRow.disableNonPublicModels);
  camelRow.allowUsageCommand = parseAllowUsageCommand(camelRow.allowUsageCommand);
  camelRow.chaosModeEnabled = parseChaosModeEnabled(camelRow.chaosModeEnabled);
  camelRow.compressionEnabled = parseCompressionEnabled(camelRow.compressionEnabled);
  camelRow.allowAutoCombos = parseAllowAutoCombos(camelRow.allowAutoCombos);
  camelRow.catalogScope = parseCatalogScope(camelRow.catalogScope);
  Object.assign(camelRow, parseApiKeyUsageLimitFields(camelRow));
  // Native insertion_order is an internal pagination tie-breaker, never public metadata.
  delete camelRow.insertionOrder;
  return camelRow;
}

export function apiKeyLifecycleAllows(row: Record<string, unknown>, now: number): boolean {
  if (parseIsBanned(row.is_banned) || !parseIsActive(row.is_active)) return false;
  if (typeof row.revoked_at === "string" && row.revoked_at.trim() !== "") return false;
  if (typeof row.expires_at === "string" && row.expires_at.trim() !== "") {
    const expiry = Date.parse(row.expires_at);
    if (Number.isFinite(expiry) && expiry <= now) return false;
  }
  return true;
}
