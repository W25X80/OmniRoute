const HOOK_NAME_CONFLICT = Symbol.for("omniroute.middleware.hook-name-conflict");

/** A confirmed duplicate name, never a generic storage failure or uncertain COMMIT. */
export class MiddlewareHookNameConflictError extends Error {
  constructor() {
    super("Hook already exists");
    this.name = "MiddlewareHookNameConflictError";
    Object.defineProperty(this, HOOK_NAME_CONFLICT, { value: true });
  }
}

export function isMiddlewareHookNameConflict(
  error: unknown
): error is MiddlewareHookNameConflictError {
  return (
    error !== null &&
    typeof error === "object" &&
    (error as { [HOOK_NAME_CONFLICT]?: unknown })[HOOK_NAME_CONFLICT] === true
  );
}
