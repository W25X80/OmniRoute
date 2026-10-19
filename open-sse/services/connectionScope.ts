/** Raw empty configuration permits all; an empty resolved intersection permits none. */
export type ConnectionScope =
  | { readonly kind: "unrestricted" }
  | { readonly kind: "deny" }
  | { readonly kind: "connections"; readonly ids: readonly [string, ...string[]] };

export const UNRESTRICTED_CONNECTION_SCOPE: ConnectionScope = Object.freeze({
  kind: "unrestricted",
});
const DENIED_CONNECTION_SCOPE: ConnectionScope = Object.freeze({ kind: "deny" });
export const CONNECTION_SCOPE_DENIED_CODE = "connection_scope_denied";

export function connectionScopeFromResolvedIds(ids: readonly unknown[]): ConnectionScope {
  const unique = [
    ...new Set(ids.filter((id): id is string => typeof id === "string" && id.trim().length > 0)),
  ];
  const [first, ...rest] = unique;
  return first === undefined
    ? DENIED_CONNECTION_SCOPE
    : { kind: "connections", ids: [first, ...rest] };
}

/** Use only at the persisted configuration boundary, never on a computed set. */
export function connectionScopeFromConfiguredIds(ids: unknown): ConnectionScope {
  return !Array.isArray(ids) || ids.length === 0
    ? UNRESTRICTED_CONNECTION_SCOPE
    : connectionScopeFromResolvedIds(ids);
}

export function intersectConnectionScopes(a: ConnectionScope, b: ConnectionScope): ConnectionScope {
  if (a.kind === "deny" || b.kind === "deny") return DENIED_CONNECTION_SCOPE;
  if (a.kind === "unrestricted") return b;
  if (b.kind === "unrestricted") return a;
  const allowed = new Set(b.ids);
  return connectionScopeFromResolvedIds(a.ids.filter((id) => allowed.has(id)));
}

/** A deny value must be handled before crossing a legacy selector's array boundary. */
export function connectionScopeIds(
  scope: Exclude<ConnectionScope, { kind: "deny" }>
): string[] | null {
  return scope.kind === "unrestricted" ? null : [...scope.ids];
}

export function connectionScopePermits(scope: ConnectionScope, id: string | undefined): boolean {
  return (
    scope.kind === "unrestricted" ||
    (scope.kind === "connections" && typeof id === "string" && scope.ids.includes(id))
  );
}
