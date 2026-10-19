import { enforceApiKeyPolicy, type ApiKeyPolicyResult } from "@/shared/utils/apiKeyPolicy";
import {
  resolveKeyConnectionScope,
  credentialDiagnosticResponse,
} from "@/sse/services/keyConnectionScope";
import {
  connectionScopeFromResolvedIds,
  type ConnectionScope,
} from "@omniroute/open-sse/services/connectionScope.ts";

export interface ScopedApiKeyPolicy {
  policy: ApiKeyPolicyResult;
  connectionScope: ConnectionScope;
  rejection: Response | null;
}
export async function enforceApiKeyPolicyWithScope(
  request: Request,
  model: string | null,
  options?: Parameters<typeof enforceApiKeyPolicy>[2]
): Promise<ScopedApiKeyPolicy> {
  const policy = await enforceApiKeyPolicy(request, model, options);
  if (policy.rejection)
    return {
      policy,
      connectionScope: connectionScopeFromResolvedIds([]),
      rejection: policy.rejection,
    };
  return { policy, ...(await resolveKeyConnectionScope(policy.apiKeyInfo)) };
}
export const unusableCredentialsResponse = credentialDiagnosticResponse;
