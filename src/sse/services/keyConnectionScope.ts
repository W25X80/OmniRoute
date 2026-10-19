import { errorResponse } from "@omniroute/open-sse/utils/error.ts";
import {
  connectionScopeFromResolvedIds,
  type ConnectionScope,
} from "@omniroute/open-sse/services/connectionScope.ts";
import { resolveKeyConnectionScope as resolveScope } from "@/lib/api/keyConnectionScope";
import { isModelAllowedForKey } from "@/lib/db/apiKeys";
import {
  comboTargetPassesKeyModelPolicy,
  type ComboTargetKeyPolicyInfo,
} from "@/sse/handlers/chat/comboTargetKeyPolicy";
import { hasApiKeyModelRestrictions } from "@/shared/utils/resolvedModelAccess";
export * from "@omniroute/open-sse/services/comboKeyScope.ts";

export interface KeyScopeApiKeyInfo extends ComboTargetKeyPolicyInfo {
  id?: string;
  allowedConnections?: unknown;
  allowedQuotas?: string[];
}
export async function resolveKeyConnectionScope(
  key: KeyScopeApiKeyInfo | null | undefined
): Promise<{ connectionScope: ConnectionScope; rejection: Response | null }> {
  try {
    const connectionScope = await resolveScope(key);
    return {
      connectionScope,
      rejection:
        connectionScope.kind === "deny"
          ? errorResponse(503, "No eligible connections matched the requested routing constraints")
          : null,
    };
  } catch {
    return {
      connectionScope: connectionScopeFromResolvedIds([]),
      rejection: errorResponse(503, "API key connection scope unavailable"),
    };
  }
}
export function createComboTargetKeyGate(
  policy:
    { apiKey?: string | null; apiKeyInfo?: ComboTargetKeyPolicyInfo | null } | null | undefined,
  requestedModelStr: string
): ((model: string) => Promise<boolean>) | undefined {
  const key = policy?.apiKey,
    info = policy?.apiKeyInfo;
  if (!key || !info || !hasApiKeyModelRestrictions(info)) return undefined;
  return async (targetModelStr) => {
    try {
      return await comboTargetPassesKeyModelPolicy({
        apiKey: key,
        apiKeyInfo: info,
        requestedModelStr,
        targetModelStr,
        isModelAllowedForKey,
      });
    } catch {
      return false;
    }
  };
}

/** Stable safe partition for scoped result caches; unrestricted callers keep shared entries. */
export function keyScopeCacheIdentity(
  info: KeyScopeApiKeyInfo | null | undefined,
  scope: ConnectionScope,
  authorizedModel: string
): string | undefined {
  if (!info) return undefined;
  const list = (value: unknown) =>
    Array.isArray(value) ? value.filter((id): id is string => typeof id === "string").sort() : [];
  const allowedModels = list(info.allowedModels);
  const blockedModels = list(info.blockedModels);
  const modelPolicy = [
    info.modelAccessMode ?? null,
    allowedModels,
    blockedModels,
    info.disableNonPublicModels === true,
  ];
  const scoped =
    scope.kind !== "unrestricted" ||
    list(info.allowedQuotas).length > 0 ||
    hasApiKeyModelRestrictions(info);
  if (!scoped) return undefined;
  return JSON.stringify([
    typeof info.id === "string" ? info.id : "unresolved",
    authorizedModel,
    scope.kind === "connections" ? [...scope.ids].sort() : scope.kind,
    list(info.allowedQuotas),
    modelPolicy,
  ]);
}
