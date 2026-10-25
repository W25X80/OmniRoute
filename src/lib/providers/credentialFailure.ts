import { createHash } from "node:crypto";

/** A request's credential identity, never the credentials themselves, crosses the state-write port. */
export function credentialVersion(credentials: {
  accessToken?: unknown;
  refreshToken?: unknown;
}): string {
  return createHash("sha256")
    .update(
      JSON.stringify([
        typeof credentials.accessToken === "string" ? credentials.accessToken : null,
        typeof credentials.refreshToken === "string" ? credentials.refreshToken : null,
      ])
    )
    .digest("hex");
}

const REFRESH_FAILURE_CODES = new Set([
  "refresh_token_invalidated",
  "refresh_token_reused",
  "invalid_grant",
  "invalid_token",
  "token_expired",
  "unauthorized",
]);

/** Provider text is deliberately excluded from persisted authentication diagnostics. */
export function credentialFailureState(code: unknown, now: string) {
  return {
    testStatus: "expired",
    isActive: false,
    lastError: "OAuth credentials were rejected. Re-authenticate this account.",
    lastErrorType: "unrecoverable_refresh_error",
    lastErrorSource: "oauth",
    lastErrorAt: now,
    errorCode:
      typeof code === "string" && REFRESH_FAILURE_CODES.has(code)
        ? code
        : "unrecoverable_refresh_error",
  };
}
