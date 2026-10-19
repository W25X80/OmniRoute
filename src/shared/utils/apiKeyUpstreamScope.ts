/**
 * Upstream scope of the caller's API key for side paths that call a provider on
 * the key's behalf outside the chat dispatcher (e.g. `/v1/messages/count_tokens`).
 *
 * Chat enforces the key's `allowedConnections` and model allow/deny lists before
 * any upstream call. A side path that selects credentials on its own must apply
 * the same scope, or a restricted key reaches connections and models it was
 * never granted. This resolver is authorization-only: it never consumes budget,
 * token-limit or rate-limit state.
 *
 * @module shared/utils/apiKeyUpstreamScope
 */

import { getApiKeyMetadata, isModelAllowedForKey } from "@/lib/db/apiKeys";
import { resolveKeyConnectionScope } from "@/lib/api/keyConnectionScope";
import {
  UNRESTRICTED_CONNECTION_SCOPE,
  type ConnectionScope,
} from "@omniroute/open-sse/services/connectionScope.ts";
import { checkApiKeyAuthorization, resolvePolicyApiKey } from "./apiKeyPolicy";
import { checkResolvedModelPermission } from "./resolvedModelAccess";

export type ApiKeyUpstreamScope =
  /** The key may not reach any upstream for this target. */
  | { permitted: false }
  /** Resolved typed scope, including an unrestricted provider pool. */
  | { permitted: true; connectionScope: ConnectionScope };

const DENIED: ApiKeyUpstreamScope = { permitted: false };
const UNSCOPED: ApiKeyUpstreamScope = {
  permitted: true,
  connectionScope: UNRESTRICTED_CONNECTION_SCOPE,
};

/**
 * Resolve which connections the caller's key may use for `requestedModel`
 * (resolved to `target`). A keyless request keeps the unscoped behavior.
 * An unknown presented key or an unreadable policy cannot authorize an upstream;
 * the caller keeps its local estimate instead.
 */
export async function resolveApiKeyUpstreamScope(
  request: Request,
  requestedModel: string,
  target: { provider: string; model: string }
): Promise<ApiKeyUpstreamScope> {
  const apiKey = await resolvePolicyApiKey(request);
  if (!apiKey) return UNSCOPED;

  let apiKeyInfo: Awaited<ReturnType<typeof getApiKeyMetadata>>;
  try {
    apiKeyInfo = await getApiKeyMetadata(apiKey);
  } catch {
    return DENIED;
  }
  if (!apiKeyInfo) return DENIED;

  if (await checkApiKeyAuthorization(request, apiKey, apiKeyInfo, requestedModel)) return DENIED;

  const resolvedPermission = await checkResolvedModelPermission(
    {
      hasApiKeyMetadata: true,
      apiKey,
      requestedModel,
      resolvedModel: `${target.provider}/${target.model}`,
    },
    isModelAllowedForKey
  );
  if (resolvedPermission !== "allowed") return DENIED;

  // Quota-exclusive keys are bound to their pool's connections and
  // quotaShared-* models; a side path has no pool-aware selection, so it
  // never borrows an upstream for them.
  if (Array.isArray(apiKeyInfo.allowedQuotas) && apiKeyInfo.allowedQuotas.length > 0) {
    return DENIED;
  }

  try {
    const connectionScope = await resolveKeyConnectionScope(apiKeyInfo);
    return connectionScope.kind === "deny" ? DENIED : { permitted: true, connectionScope };
  } catch {
    return DENIED;
  }
}
