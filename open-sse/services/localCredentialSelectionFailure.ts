/** Trusted server-local identity only; provider payloads cannot mark a failure. */
const failureKey = Symbol.for("omniroute.local-credential-selection-failures.v1");
const failureState = globalThis as typeof globalThis & {
  [failureKey]?: WeakSet<object>;
};
const failures = (failureState[failureKey] ??= new WeakSet<object>());

function isObject(value: unknown): value is object {
  return value !== null && (typeof value === "object" || typeof value === "function");
}

/** Keep native error identity; never inspect a thrown value's message or coercion. */
export function markLocalCredentialSelectionFailure(value: unknown): object {
  const failure = isObject(value) ? value : new Error("Local credential selection failed");
  failures.add(failure);
  return failure;
}

export function isLocalCredentialSelectionFailure(value: unknown): boolean {
  return isObject(value) && failures.has(value);
}
