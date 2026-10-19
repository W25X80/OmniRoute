import {
  isCredentialDiagnosticSentinel,
  credentialDiagnosticResponse,
} from "@omniroute/open-sse/services/comboKeyScope.ts";
import { errorResponse } from "@omniroute/open-sse/utils/error.ts";
import { HTTP_STATUS } from "@omniroute/open-sse/config/constants.ts";
import { getRegistryEntry } from "@omniroute/open-sse/config/providerRegistry.ts";
import { clearRecoveredProviderState } from "@/sse/services/auth";
import { getProviderCredentialsWithQuotaPreflight } from "@/sse/services/scopedCredentials";
import { handleEmbedding } from "@omniroute/open-sse/handlers/embeddings.ts";
import * as log from "@/sse/utils/logger";
import { enforceApiKeyPolicy } from "@/shared/utils/apiKeyPolicy";
import {
  isKeyScopeBlockedCredentials,
  keyScopeBlockedResponse,
  resolveKeyConnectionScope,
} from "@/sse/services/keyConnectionScope";
import { v1EmbeddingsSchema } from "@/shared/validation/schemas";
import { isValidationFailure, validateBody } from "@/shared/validation/helpers";

/**
 * Handle CORS preflight
 */
export async function OPTIONS() {
  return new Response(null, {
    headers: {
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "*",
    },
  });
}

/**
 * POST /v1/providers/{provider}/embeddings
 */
export async function POST(request, { params }) {
  const { provider: rawProvider } = await params;

  const providerEntry = getRegistryEntry(rawProvider);

  if (!providerEntry) {
    return errorResponse(HTTP_STATUS.BAD_REQUEST, `Unknown provider: ${rawProvider}`);
  }

  const providerAlias = providerEntry.alias || providerEntry.id;

  let rawBody;
  try {
    rawBody = await request.json();
  } catch {
    return errorResponse(HTTP_STATUS.BAD_REQUEST, "Invalid JSON body");
  }
  const validation = validateBody(v1EmbeddingsSchema, rawBody);
  if (isValidationFailure(validation)) {
    return errorResponse(HTTP_STATUS.BAD_REQUEST, validation.error.message);
  }
  const body = validation.data;

  // Add provider prefix if missing
  if (body.model && !body.model.includes("/")) {
    body.model = `${providerAlias}/${body.model}`;
  }

  // Enforce API key policies (model restrictions + budget limits)
  const policy = await enforceApiKeyPolicy(request, body.model);
  if (policy.rejection) return policy.rejection;
  // GHSA-3w2r: credential selection stays inside the key's connection scope,
  // the same scope chat applies.
  const scope = await resolveKeyConnectionScope(policy.apiKeyInfo);
  if (scope.rejection) return scope.rejection;

  // Validate provider match
  if (body.model) {
    const prefix = body.model.split("/")[0];
    if (prefix !== providerAlias && prefix !== rawProvider && prefix !== providerEntry.id) {
      return errorResponse(
        HTTP_STATUS.BAD_REQUEST,
        `Model "${body.model}" does not belong to provider "${rawProvider}"`
      );
    }
  }

  const credentials = await getProviderCredentialsWithQuotaPreflight(
    providerEntry.id,
    null,
    scope.connectionScope
  );
  if (isCredentialDiagnosticSentinel(credentials))
    return credentialDiagnosticResponse("provider", credentials)!;
  if (!credentials) {
    return errorResponse(HTTP_STATUS.BAD_REQUEST, `No credentials for provider: ${rawProvider}`);
  }
  if (isKeyScopeBlockedCredentials(credentials)) {
    return keyScopeBlockedResponse(rawProvider, credentials);
  }

  const result = await handleEmbedding({
    body,
    credentials,
    log,
    // #10347 — thread the selected connection id so a hard upstream failure cools
    // the account instead of re-hitting it on every request.
    connectionId: (credentials as { connectionId?: string } | null)?.connectionId ?? null,
  });

  if (result.success) {
    await clearRecoveredProviderState(credentials);
    return new Response(JSON.stringify(result.data), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }

  return new Response(JSON.stringify({ error: result.error }), {
    status: result.status || 500,
    headers: { "Content-Type": "application/json" },
  });
}
