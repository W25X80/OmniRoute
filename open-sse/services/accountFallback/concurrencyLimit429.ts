import { RateLimitReason } from "../../config/constants.ts";
import type { RetryHintProvenance } from "../accountFallback.ts";
import { looksLikeConcurrencyLimit } from "../quotaResetParsing.ts";
import * as rotation from "../rotationConfig.ts";

export const CONCURRENCY_LIMITED_REASON = "concurrency_limited";

/** Read only the operator's fixed duration, without evaluating account rotation gates/counters. */
export function resolveConcurrencyLimitCooldownOverride(account?: unknown): number | null {
  return rotation.overrideFor(RateLimitReason.RATE_LIMIT_EXCEEDED, account)?.cooldownMs ?? null;
}

/** Called only after specific quota/terminal/provider rules have had priority. */
export function concurrencyLimit429(
  status: number,
  errorText: unknown,
  retryHint: { retryAfterMs: number; provenance: RetryHintProvenance } | null,
  fixedCooldownMs?: number | null
) {
  if (status !== 429 || !looksLikeConcurrencyLimit(errorText)) return null;
  const enabledHint =
    retryHint && Number.isFinite(retryHint.retryAfterMs) && retryHint.retryAfterMs > 0
      ? retryHint
      : null;
  const fixedCooldown =
    typeof fixedCooldownMs === "number" && Number.isFinite(fixedCooldownMs) && fixedCooldownMs > 0
      ? fixedCooldownMs
      : 1_000;
  const cooldownMs = enabledHint?.retryAfterMs ?? fixedCooldown;
  return {
    shouldFallback: true,
    cooldownMs,
    baseCooldownMs: cooldownMs,
    usedUpstreamRetryHint: enabledHint !== null,
    ...(enabledHint ? { retryHintSource: enabledHint.provenance } : {}),
    reason: CONCURRENCY_LIMITED_REASON,
    skipProviderBreaker: true,
  };
}
