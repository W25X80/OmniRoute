import {
  connectionScopeFromConfiguredIds,
  connectionScopeFromResolvedIds,
  intersectConnectionScopes,
  type ConnectionScope,
} from "@omniroute/open-sse/services/connectionScope.ts";
import { resolveQuotaKeyScope } from "@/lib/quota/quotaKey";

interface KeyConnectionPolicy {
  allowedConnections?: unknown;
  allowedQuotas?: readonly string[];
}

/** Resolve once per selection; every key, target and active quota constraint narrows access. */
export async function resolveKeyConnectionScope(
  key: KeyConnectionPolicy | null | undefined,
  configuredTargetIds?: unknown
): Promise<ConnectionScope> {
  let scope = intersectConnectionScopes(
    connectionScopeFromConfiguredIds(key?.allowedConnections),
    connectionScopeFromConfiguredIds(configuredTargetIds)
  );
  if (key?.allowedQuotas?.length) {
    const quota = await resolveQuotaKeyScope([...key.allowedQuotas]);
    scope = intersectConnectionScopes(scope, connectionScopeFromResolvedIds(quota.connectionIds));
  }
  return scope;
}
