/**
 * Per-credential cooldown for Anthropic's Claude OAuth usage endpoint.
 * Map keys are SHA-256 digests, and the bounded overflow gate refuses new
 * polls rather than evicting a live Retry-After decision.
 */

import { createHash } from "node:crypto";

export const OAUTH_USAGE_429_COOLDOWN_MS = 180_000;
export const OAUTH_USAGE_429_MAX_COOLDOWN_MS = 30 * 60_000;
const MAX_ACTIVE_COOLDOWNS = 512;
const MAX_DATE_MS = 8.64e15;

const oauthCooldown = new Map<string, number>();
let overflowCooldownUntil = 0;

export function hashClaudeCredential(accessToken: string): string {
  return createHash("sha256").update(accessToken).digest("hex");
}

function pruneExpired(now: number): void {
  for (const [key, until] of oauthCooldown) {
    if (until <= now) oauthCooldown.delete(key);
  }
  if (overflowCooldownUntil <= now) overflowCooldownUntil = 0;
}

export type ClaudeOauthUsageGate = "cooldown" | "capacity" | null;

/** Check cooldown before memo admission and refuse untracked polls at capacity. */
export function getClaudeOauthUsageGate(
  accessToken: string | undefined,
  now: number = Date.now()
): ClaudeOauthUsageGate {
  if (!accessToken) return null;
  pruneExpired(now);
  const key = hashClaudeCredential(accessToken);
  const until = oauthCooldown.get(key);
  if ((until !== undefined && until > now) || overflowCooldownUntil > now) return "cooldown";
  return oauthCooldown.size >= MAX_ACTIVE_COOLDOWNS && until === undefined ? "capacity" : null;
}

export function shouldDeferClaudeOauthUsagePoll(
  accessToken: string | undefined,
  now: number = Date.now()
): boolean {
  return getClaudeOauthUsageGate(accessToken, now) !== null;
}

export function isClaudeOauthUsageCoolingDown(
  accessToken: string | undefined,
  now: number = Date.now()
): boolean {
  return getClaudeOauthUsageGate(accessToken, now) === "cooldown";
}

/** Record a 429 without shortening an existing Retry-After. */
export function markClaudeOauthUsage429(
  accessToken: string | undefined,
  now: number = Date.now(),
  cooldownMs: number = OAUTH_USAGE_429_COOLDOWN_MS
): void {
  if (!accessToken) return;
  pruneExpired(now);
  const key = hashClaudeCredential(accessToken);
  const duration = Number.isFinite(cooldownMs)
    ? Math.max(0, Math.min(OAUTH_USAGE_429_MAX_COOLDOWN_MS, cooldownMs))
    : OAUTH_USAGE_429_COOLDOWN_MS;
  const until = now + duration;
  const previous = oauthCooldown.get(key);
  if (previous !== undefined) {
    oauthCooldown.set(key, Math.max(previous, until));
  } else if (oauthCooldown.size < MAX_ACTIVE_COOLDOWNS) {
    oauthCooldown.set(key, until);
  } else {
    // A request already in flight can return 429 after capacity filled. The
    // bounded global gate preserves that retry hint without evicting live keys.
    overflowCooldownUntil = Math.max(overflowCooldownUntil, until);
  }
}

function parseRetryAfterMs(value: string | null, now: number): number | null {
  const text = (value ?? "").trim();
  if (!text) return null;
  const seconds = Number(text);
  if (Number.isFinite(seconds)) return seconds >= 0 ? seconds * 1000 : null;
  const at = Date.parse(text);
  return Number.isFinite(at) && at <= MAX_DATE_MS ? Math.max(0, at - now) : null;
}

function parseResetHeaderMs(value: string | null, now: number): number | null {
  const text = (value ?? "").trim();
  if (!text) return null;
  const numeric = Number(text);
  const at = Number.isFinite(numeric)
    ? numeric < 1e12
      ? numeric * 1000
      : numeric
    : Date.parse(text);
  return Number.isFinite(at) && at > now && at <= MAX_DATE_MS ? at - now : null;
}

/** Parse delta-seconds/date Retry-After, then the furthest Anthropic reset hint. */
export function computeClaudeUsageRetryMs(
  headers: Headers | null | undefined,
  now: number = Date.now()
): number {
  let parsed = headers ? parseRetryAfterMs(headers.get("retry-after"), now) : null;
  if (parsed === null && headers) {
    headers.forEach((value, name) => {
      if (!/^anthropic-ratelimit-.+-reset$/i.test(name)) return;
      const candidate = parseResetHeaderMs(value, now);
      if (candidate !== null && (parsed === null || candidate > parsed)) parsed = candidate;
    });
  }
  return Math.min(
    OAUTH_USAGE_429_MAX_COOLDOWN_MS,
    Math.max(OAUTH_USAGE_429_COOLDOWN_MS, parsed ?? OAUTH_USAGE_429_COOLDOWN_MS)
  );
}

/** Test-only: clear entries and the bounded overflow gate. */
export function _resetClaudeOauthUsageCooldown(): void {
  oauthCooldown.clear();
  overflowCooldownUntil = 0;
}
