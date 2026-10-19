/** Credential diagnostics describe selection failure and never carry provider authority. */
export type CredentialDiagnosticSentinel =
  | { allRateLimited: true; retryAfter?: string | number | Date; retryAfterHuman?: string }
  | { allExpired: true; expiredStatus?: string; expiredCount?: number }
  | { blockedByKeyPolicy: true }
  | { leaseConnectionMismatch: true };

export function isCredentialDiagnosticSentinel(
  value: unknown
): value is CredentialDiagnosticSentinel {
  if (!value || typeof value !== "object") return false;
  const record = value as Record<string, unknown>;
  return Boolean(
    record.allRateLimited ||
    record.allExpired ||
    record.blockedByKeyPolicy ||
    record.leaseConnectionMismatch
  );
}
