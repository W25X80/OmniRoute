import type { AudioProvider } from "@omniroute/open-sse/config/audioRegistry.ts";
import {
  isLocalCredentialSelectionFailure,
  markLocalCredentialSelectionFailure,
} from "@omniroute/open-sse/services/localCredentialSelectionFailure.ts";
import {
  credentialDiagnosticResponse,
  keyAdmitsKeylessProviders,
  noActiveCredentialsResponse,
} from "@omniroute/open-sse/services/comboKeyScope.ts";
import {
  connectionScopePermits,
  UNRESTRICTED_CONNECTION_SCOPE,
  type ConnectionScope,
} from "@omniroute/open-sse/services/connectionScope.ts";
import type { CredentialDiagnosticSentinel } from "@omniroute/open-sse/services/credentialSentinel.ts";
import { errorResponse } from "@omniroute/open-sse/utils/error.ts";
import { stripTrailingSlashes } from "@omniroute/open-sse/utils/urlSanitize.ts";
import { isLoopbackNodeHost } from "@/shared/network/loopbackNodeHost";
import { parseOutboundUrl } from "@/shared/network/outboundUrlGuard";
import { getProviderCredentialsWithQuotaPreflight } from "./scopedCredentials";

type AudioOperation = "speech" | "transcriptions" | "translations";
type SelectedCredentials = Exclude<
  NonNullable<Awaited<ReturnType<typeof getProviderCredentialsWithQuotaPreflight>>>,
  CredentialDiagnosticSentinel
>;

/** Project trusted local selection failures only at the exported HTTP boundary. */
export function withAudioSelectionErrorBoundary<TArgs extends unknown[]>(
  handler: (...args: TArgs) => Response | Promise<Response>
): (...args: TArgs) => Promise<Response> {
  return async (...args: TArgs): Promise<Response> => {
    try {
      return await handler(...args);
    } catch (error) {
      if (isLocalCredentialSelectionFailure(error)) {
        return errorResponse(503, "Audio credentials unavailable");
      }
      throw error;
    }
  };
}

/** Resolve only declared local URL shapes; every result is request-owned. */
export function resolveAudioConnectionConfig(
  provider: AudioProvider,
  operation: AudioOperation,
  credentials: { connectionId?: string; providerSpecificData?: unknown }
): AudioProvider | null {
  if (provider.authType !== "none" || provider.format === "gtts") return null;
  const dynamic = Boolean(provider.credentialProviderId);
  const qwen = provider.id === "qwen" && provider.format === "openai";
  const tts = provider.format === "coqui" || provider.format === "tortoise";
  if (!dynamic && !qwen && !tts) return null;
  const data = credentials.providerSpecificData;
  const record = data && typeof data === "object" ? (data as Record<string, unknown>) : {};
  if (!("baseUrl" in record)) return { ...provider };
  if (typeof record.baseUrl !== "string" || !record.baseUrl.trim()) return null;

  try {
    const url = parseOutboundUrl(record.baseUrl.trim());
    // A local node's cached no-auth classification cannot authorize a remote override.
    if (!isLoopbackNodeHost(url.href)) return null;
    const pathname = stripTrailingSlashes(url.pathname);
    url.pathname = pathname;
    if (dynamic) {
      // Compatible-node storage declares an API root, never a complete audio endpoint.
      if (/\/audio\/(?:speech|transcriptions|translations)$/.test(pathname)) return null;
      url.pathname = `${pathname}/audio/${operation}`;
    } else if (qwen) {
      if (!pathname.endsWith(`/audio/${operation}`)) return null;
    } else if (tts) {
      if (operation !== "speech" || !pathname.endsWith("/api/tts")) return null;
    } else {
      return null;
    }
    url.hash = "";
    return { ...provider, baseUrl: url.href };
  } catch {
    return null;
  }
}

/** Keep a concrete pin and its actual selected destination separate from keyless defaults. */
export async function resolveKeylessAudioTarget({
  provider,
  operation,
  requestedModel,
  connectionScope = UNRESTRICTED_CONNECTION_SCOPE,
  forcedConnectionId,
}: {
  provider: AudioProvider;
  operation: AudioOperation;
  requestedModel: string | null;
  connectionScope?: ConnectionScope | null;
  forcedConnectionId?: string | null;
}): Promise<{
  credentials: SelectedCredentials | null;
  resolvedProvider: AudioProvider;
  rejection: Response | null;
}> {
  const scope = connectionScope ?? UNRESTRICTED_CONNECTION_SCOPE;
  const force = forcedConnectionId?.trim() || null;
  const refused = (rejection: Response) => ({
    credentials: null,
    resolvedProvider: provider,
    rejection,
  });
  if (scope.kind === "deny") return refused(noActiveCredentialsResponse(provider.id));
  if (
    (force === "noauth" && connectionScopePermits(scope, "noauth")) ||
    (!force && keyAdmitsKeylessProviders(scope))
  ) {
    return { credentials: null, resolvedProvider: { ...provider }, rejection: null };
  }
  if (force === "noauth") return refused(noActiveCredentialsResponse(provider.id));

  let selected: Awaited<ReturnType<typeof getProviderCredentialsWithQuotaPreflight>>;
  try {
    selected = await getProviderCredentialsWithQuotaPreflight(
      provider.credentialProviderId || provider.id,
      null,
      scope,
      requestedModel,
      { ...(force ? { forcedConnectionId: force } : {}) }
    );
  } catch (error) {
    throw markLocalCredentialSelectionFailure(error);
  }
  const diagnostic = credentialDiagnosticResponse(provider.id, selected);
  if (diagnostic) return refused(diagnostic);
  const credentials = selected as SelectedCredentials | null;
  if (
    !credentials ||
    credentials.connectionId === "noauth" ||
    !connectionScopePermits(scope, credentials.connectionId)
  ) {
    return refused(noActiveCredentialsResponse(provider.id));
  }
  const resolvedProvider = resolveAudioConnectionConfig(provider, operation, credentials);
  if (!resolvedProvider) {
    return refused(errorResponse(400, "Selected audio connection has no supported local endpoint"));
  }
  return { credentials, resolvedProvider, rejection: null };
}
