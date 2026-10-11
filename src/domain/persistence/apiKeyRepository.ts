import type { ApiKeyCodexServiceMode } from "@/shared/constants/codexServiceMode";
import type { ModelAccessMode } from "@/lib/db/apiKeys/modelAccessMode";
import type { ApiKeyPermissionsUpdate } from "@/lib/db/apiKeys/permissionsUpdate";
import type { AccessSchedule, RateLimitRule } from "@/lib/db/apiKeys/types";

export type { ApiKeyPermissionsUpdate };

export interface CreateApiKeyOptions {
  modelAccessMode?: ModelAccessMode;
  allowedModels?: string[];
  allowedCombos?: string[];
  allowedConnections?: string[];
  expiresAt?: string | null;
}

export interface ApiKeyMetadata {
  id: string;
  name: string;
  machineId: string | null;
  modelAccessMode: ModelAccessMode;
  allowedModels: string[];
  blockedModels: string[];
  allowedCombos: string[];
  allowedConnections: string[];
  allowedQuotas: string[];
  noLog: boolean;
  autoResolve: boolean;
  isActive: boolean;
  accessSchedule: AccessSchedule | null;
  maxRequestsPerDay: number | null;
  maxRequestsPerMinute: number | null;
  throttleDelayMs: number | null;
  rateLimits: RateLimitRule[] | null;
  maxSessions: number;
  revokedAt: string | null;
  expiresAt: string | null;
  ipAllowlist: string[];
  scopes: string[];
  isBanned: boolean;
  keyHash: string | null;
  proxyId: string | null;
  allowedEndpoints: string[];
  streamDefaultMode: "legacy" | "json";
  codexServiceMode: ApiKeyCodexServiceMode;
  cacheDefaultMode: "legacy" | "bypass";
  disableNonPublicModels: boolean;
  allowUsageCommand: boolean;
  usageLimitEnabled: boolean;
  dailyUsageLimitUsd: number | null;
  weeklyUsageLimitUsd: number | null;
  chaosModeEnabled: boolean;
  compressionEnabled: boolean;
  allowAutoCombos: boolean;
  catalogScope: "all" | "combos" | "models";
}

export interface ApiKeyRecord extends Record<string, unknown> {
  id: string;
  name: string;
  key: string;
  machineId: string | null;
  createdAt: string;
  lastUsedAt: string | null;
  keyPrefix: string | null;
  modelAccessMode: ModelAccessMode;
  allowedModels: string[];
  blockedModels: string[];
  allowedCombos: string[];
  allowedConnections: string[];
  allowedQuotas: string[];
  noLog: boolean;
  autoResolve: boolean;
  isActive: boolean;
  accessSchedule: AccessSchedule | null;
  throttleDelayMs?: number | null;
  rateLimits: RateLimitRule[] | null;
  scopes: string[];
  proxyId?: string | null;
  isBanned?: boolean;
  expiresAt?: string | null;
  allowedEndpoints: string[];
  streamDefaultMode: "legacy" | "json";
  codexServiceMode: ApiKeyCodexServiceMode;
  cacheDefaultMode: "legacy" | "bypass";
  disableNonPublicModels?: boolean;
  allowUsageCommand?: boolean;
  usageLimitEnabled?: boolean;
  dailyUsageLimitUsd?: number | null;
  weeklyUsageLimitUsd?: number | null;
  chaosModeEnabled?: boolean;
  compressionEnabled: boolean;
  allowAutoCombos: boolean;
  catalogScope: "all" | "combos" | "models";
}

/** Domain operations only: no SQL, database handles, or SQLite file lifecycle. */
export interface ApiKeyRepository {
  getApiKeys(limit?: number, offset?: number): Promise<ApiKeyRecord[]>;
  getApiKeysCount(): Promise<number>;
  getApiKeyById(id: string): Promise<ApiKeyRecord | null>;
  createApiKey(
    name: string,
    machineId: string,
    scopes?: string[],
    options?: CreateApiKeyOptions
  ): Promise<ApiKeyRecord>;
  regenerateApiKey(id: string): Promise<{ id: string; key: string } | null>;
  updateApiKeyPermissions(id: string, update: string[] | ApiKeyPermissionsUpdate): Promise<boolean>;
  deleteApiKey(id: string): Promise<boolean>;
  revokeApiKey(id: string): Promise<boolean>;
  setApiKeyExpiry(id: string, expiresAt: string | null): Promise<boolean>;
  validateApiKey(key: string | null | undefined): Promise<boolean>;
  getApiKeyMetadata(key: string | null | undefined): Promise<ApiKeyMetadata | null>;
  isModelAllowedForKey(
    key: string | null | undefined,
    modelId: string | null | undefined,
    resolvedModelId?: string | null
  ): Promise<boolean>;
  isModelBlockedByPatterns(
    blockedModels: string[] | null | undefined,
    modelId: string
  ): Promise<boolean>;
  pickApiKeyForInternalUse(
    purpose?: "combo-health-check" | "cloud-sync-verify" | "internal-probe"
  ): Promise<string | null>;
  getExclusiveLeaseConnectionIds(): Promise<Set<string>>;
  clearApiKeyCaches(): void;
}
