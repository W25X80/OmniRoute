import { errorResponse, unavailableResponse } from "../utils/error.ts";
import { HTTP_STATUS } from "../config/constants.ts";
import {
  connectionScopeFromConfiguredIds,
  connectionScopePermits,
  connectionScopeFromResolvedIds,
  intersectConnectionScopes,
  UNRESTRICTED_CONNECTION_SCOPE,
  type ConnectionScope,
} from "./connectionScope.ts";
import { isCredentialDiagnosticSentinel } from "./credentialSentinel.ts";

export { isCredentialDiagnosticSentinel };
export interface ComboKeyScope {
  connectionScope?: ConnectionScope;
  isTargetAllowed?: (model: string) => Promise<boolean>;
}
export function isKeyScopeBlockedCredentials(value: unknown): boolean {
  return Boolean(
    value &&
    typeof value === "object" &&
    (value as { blockedByKeyPolicy?: unknown }).blockedByKeyPolicy
  );
}
export function keyScopeBlockedResponse(provider: string, _credentials?: unknown): Response {
  return errorResponse(
    403,
    `[${provider}] No connection is allowed by this API key's connection or quota scope`
  );
}
/** Keep rate diagnostics for existing fallback/429 classification; none is dispatch authority. */
export function withoutKeyScopeBlocked<T>(
  value: T
): Exclude<
  T,
  Exclude<import("./credentialSentinel.ts").CredentialDiagnosticSentinel, { allRateLimited: true }>
> | null {
  const rateLimited = Boolean(
    value && typeof value === "object" && (value as { allRateLimited?: unknown }).allRateLimited
  );
  return (isCredentialDiagnosticSentinel(value) && !rateLimited ? null : value) as Exclude<
    T,
    Exclude<
      import("./credentialSentinel.ts").CredentialDiagnosticSentinel,
      { allRateLimited: true }
    >
  > | null;
}
/** Every diagnostic is a refusal, including expired and lease-mismatch records. */
export function credentialDiagnosticResponse(provider: string, value: unknown): Response | null {
  if (!isCredentialDiagnosticSentinel(value)) return null;
  const record = value as Record<string, unknown>;
  if (record.leaseConnectionMismatch)
    return errorResponse(409, "Managed lease connection mismatch", {
      code: "LEASE_CONNECTION_MISMATCH",
    });
  if (record.blockedByKeyPolicy) return keyScopeBlockedResponse(provider);
  if (record.allRateLimited)
    return unavailableResponse(
      429,
      `[${provider}] All accounts rate limited`,
      record.retryAfter as string | number | Date | undefined,
      record.retryAfterHuman as string | undefined
    );
  const quota = record.expiredStatus === "credits_exhausted";
  return errorResponse(
    quota ? 402 : 401,
    `[${provider}] All ${record.expiredCount || 1} connection(s) ${quota ? "credits exhausted" : "authentication expired"} — please reconnect in the dashboard`
  );
}
export function keyAdmitsKeylessProviders(
  scope: ConnectionScope | null = UNRESTRICTED_CONNECTION_SCOPE
): boolean {
  scope ??= UNRESTRICTED_CONNECTION_SCOPE;
  return (
    scope.kind === "unrestricted" || (scope.kind === "connections" && scope.ids.includes("noauth"))
  );
}
export function noActiveCredentialsResponse(provider: string): Response {
  return errorResponse(HTTP_STATUS.NOT_FOUND, `No active credentials for provider: ${provider}`);
}
export async function keylessProviderRejection(
  provider: string,
  scope: ConnectionScope | null | undefined,
  lookup: () => Promise<unknown>
): Promise<Response | null> {
  if (keyAdmitsKeylessProviders(scope)) return null;
  const credentials = await lookup();
  const diagnostic = credentialDiagnosticResponse(provider, credentials);
  if (diagnostic) return diagnostic;
  const id =
    credentials && typeof credentials === "object"
      ? (credentials as { connectionId?: unknown }).connectionId
      : undefined;
  return typeof id === "string" &&
    connectionScopePermits(scope ?? UNRESTRICTED_CONNECTION_SCOPE, id)
    ? null
    : noActiveCredentialsResponse(provider);
}
export async function filterComboTargetsForKey<T extends { modelStr?: string | null }>(
  targets: T[],
  gate: ComboKeyScope["isTargetAllowed"]
): Promise<T[]> {
  if (!gate) return targets;
  const admitted: T[] = [];
  for (const target of targets)
    if (target.modelStr && (await gate(target.modelStr))) admitted.push(target);
  return admitted;
}
export function comboTargetsNotAllowedResponse(name: string): Response {
  return errorResponse(403, `No target of combo "${name}" is allowed for this API key`);
}
export async function scopeComboTargetsForKey<T extends { modelStr?: string | null }>(
  targets: T[],
  scope: ComboKeyScope | undefined,
  name: string
): Promise<{ targets: T[]; connectionScope: ConnectionScope; rejection: Response | null }> {
  const admitted = await filterComboTargetsForKey(targets, scope?.isTargetAllowed);
  return {
    targets: admitted,
    connectionScope: scope?.connectionScope ?? UNRESTRICTED_CONNECTION_SCOPE,
    rejection:
      targets.length > 0 && admitted.length === 0 ? comboTargetsNotAllowedResponse(name) : null,
  };
}
/** Same raw step semantics as implicitPinAllowlist: a nonempty explicit list wins. */
export function targetConnectionScope(
  scope: ConnectionScope | null,
  target: { connectionId?: string | null; allowedConnectionIds?: string[] | null }
): ConnectionScope {
  const pin = target.connectionId?.trim();
  const configured = target.allowedConnectionIds?.length
    ? target.allowedConnectionIds
    : pin
      ? [pin]
      : null;
  return intersectConnectionScopes(
    scope ?? UNRESTRICTED_CONNECTION_SCOPE,
    connectionScopeFromConfiguredIds(configured)
  );
}
/** Route-serving IDs are computed: an explicitly empty set is always deny. */
export function narrowConnectionScope(
  scope: ConnectionScope | null,
  routeIds: readonly string[] | null | undefined
): ConnectionScope {
  return routeIds == null
    ? (scope ?? UNRESTRICTED_CONNECTION_SCOPE)
    : intersectConnectionScopes(
        scope ?? UNRESTRICTED_CONNECTION_SCOPE,
        connectionScopeFromResolvedIds(routeIds)
      );
}
