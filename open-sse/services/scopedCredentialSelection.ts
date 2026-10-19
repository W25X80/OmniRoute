import {
  connectionScopeIds,
  connectionScopePermits,
  UNRESTRICTED_CONNECTION_SCOPE,
  type ConnectionScope,
} from "./connectionScope.ts";
import {
  isCredentialDiagnosticSentinel,
  type CredentialDiagnosticSentinel,
} from "./credentialSentinel.ts";

export type ScopedCredentialResult<T> = T extends
  { connectionId: string } | CredentialDiagnosticSentinel
  ? T
  : never;

/** Convert to the legacy selector only after deny has been handled, then verify its actual identity. */
export function scopedCredentialSelector<T, Rest extends unknown[]>(
  select: (
    provider: string,
    exclude: string | null,
    allowed: string[] | null,
    ...rest: Rest
  ) => Promise<T>
) {
  return async (
    provider: string,
    exclude: string | null = null,
    scope: ConnectionScope | null = UNRESTRICTED_CONNECTION_SCOPE,
    ...rest: Rest
  ): Promise<ScopedCredentialResult<T> | null> => {
    scope ??= UNRESTRICTED_CONNECTION_SCOPE;
    if (scope.kind === "deny") return null;
    const value = await select(provider, exclude, connectionScopeIds(scope), ...rest);
    if (!value || isCredentialDiagnosticSentinel(value))
      return value as ScopedCredentialResult<T> | null;
    const id = (value as { connectionId?: unknown }).connectionId;
    return typeof id === "string" && connectionScopePermits(scope, id)
      ? (value as ScopedCredentialResult<T>)
      : null;
  };
}
