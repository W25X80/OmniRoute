import { isPostgresErrorKind } from "./postgres/database";
import { getSelectedPersistenceIdentity } from "./controlPlaneContext";
const sqliteAuthority = {};
/** Stable selected-store authority for runtime caches; no database handle escapes. */
export function getAgentRuntimeAuthority(): object {
  return getSelectedPersistenceIdentity() ?? sqliteAuthority;
}

export function requiresDurableAgentPersistence(): boolean {
  return getSelectedPersistenceIdentity() !== undefined;
}

/** Durable failures cannot be reclassified as a handler failure and written again. */
export function isNativeAgentPersistenceError(error: unknown): boolean {
  return (
    [
      "unavailable",
      "overloaded",
      "closed",
      "query_failed",
      "transaction_failed",
      "commit_unknown",
      "invalid_scope",
      "transaction_timeout",
    ] as const
  ).some((kind) => isPostgresErrorKind(error, kind));
}
