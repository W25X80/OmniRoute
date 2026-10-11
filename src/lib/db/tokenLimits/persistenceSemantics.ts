/** Shared token-limit errors and window projection; no database imports. */
import { getBudgetWindow } from "../../../domain/budgetWindow";
import type { BudgetResetInterval } from "../../../domain/budgetWindow";
import type { TokenLimitScopeType, TokenLimit, TokenWindowState } from "../tokenLimits";

// ──────────────── CRUD ────────────────

/** An id-targeted update that would take another limit's (scope, reset interval) slot. */
export class TokenLimitConflictError extends Error {
  constructor(scopeType: TokenLimitScopeType, resetInterval: BudgetResetInterval) {
    super(`This key already has a ${resetInterval} ${scopeType} token limit for that scope.`);
    this.name = "TokenLimitConflictError";
  }
}

/** An id-targeted update for a token limit that does not exist. */
export class TokenLimitNotFoundError extends Error {
  constructor() {
    super("Token limit not found.");
    this.name = "TokenLimitNotFoundError";
  }
}

// ──────────────── Counters (concurrency-safe under WAL) ────────────────

/**
 * Pure boundary calculator. Resolves the active window for a limit at `now`
 * and reports whether the window has rolled since the limit's last-known window.
 * No DB writes. Window math reused from costRules.getBudgetWindow.
 */
export function resetWindowIfElapsed(limit: TokenLimit, now = Date.now()): TokenWindowState {
  const window = getBudgetWindow(limit.resetInterval, limit.resetTime, now);
  const windowStart = String(window.periodStartAt);
  return {
    windowStart,
    didReset: false, // caller compares against the stored counter row to decide rollover
    periodStartAt: window.periodStartAt,
    nextResetAt: window.nextResetAt,
  };
}
