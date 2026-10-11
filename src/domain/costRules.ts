import { getBudgetWindow, normalizeResetTime } from "./budgetWindow";
import type { BudgetResetInterval } from "./budgetWindow";
export { getBudgetWindow } from "./budgetWindow";
export type { BudgetResetInterval } from "./budgetWindow";
/**
 * Cost Rules — Domain Layer (T-19)
 *
 * Business rules for cost management: budget thresholds,
 * scheduled reset windows, and cost summaries per API key.
 *
 * State is persisted in SQLite via domainState.ts.
 *
 * @module domain/costRules
 */

import {
  deleteAllCostData,
  deleteBudget as dbDeleteBudget,
  deleteCostEntries,
  loadAllBudgets,
  loadBudget,
  loadCostEntries,
  loadCostEntriesInRange,
  loadCostTotal,
  saveBudget,
  saveBudgetResetLog,
} from "../lib/db/domainState";
import {
  discardSpendBatchEntries,
  resetSpendBatchWriterForTests,
  spendBatchWriter,
} from "@/lib/spend/batchWriter";
import { recordLedgerFromCost, type CostLedgerRecorderInput } from "@/lib/usage/costLedgerRecorder";
import { getSelectedPostgresDatabase } from "@/lib/db/controlPlaneContext";
import * as postgresBudget from "@/lib/db/postgres/budgetRepository";
import { runPersistenceBackgroundWork } from "@/lib/db/persistenceRuntime";

export interface BudgetConfig {
  dailyLimitUsd?: number;
  weeklyLimitUsd?: number;
  monthlyLimitUsd?: number;
  warningThreshold?: number;
  resetInterval?: BudgetResetInterval;
  resetTime?: string;
  budgetResetAt?: number | null;
  lastBudgetResetAt?: number | null;
  warningEmittedAt?: number | null;
  warningPeriodStart?: number | null;
}

export interface NormalizedBudgetConfig {
  dailyLimitUsd: number;
  weeklyLimitUsd: number;
  monthlyLimitUsd: number;
  warningThreshold: number;
  resetInterval: BudgetResetInterval;
  resetTime: string;
  budgetResetAt: number | null;
  lastBudgetResetAt: number | null;
  warningEmittedAt: number | null;
  warningPeriodStart: number | null;
}

interface CostEntry {
  cost: number;
  timestamp: number;
}

interface SyncBudgetScheduleOptions {
  logReset?: boolean;
  persist?: boolean;
}

export interface BudgetSummary {
  dailyTotal: number;
  monthlyTotal: number;
  totalEntries: number;
  budget: NormalizedBudgetConfig | null;
  totalCostToday: number;
  totalCostMonth: number;
  totalCostPeriod: number;
  activeLimitUsd: number;
  resetInterval: BudgetResetInterval | null;
  resetTime: string | null;
  budgetResetAt: number | null;
  lastBudgetResetAt: number | null;
  periodStartAt: number | null;
  nextResetAt: number | null;
  dailyLimitUsd: number;
  weeklyLimitUsd: number;
  monthlyLimitUsd: number;
  warningThreshold: number | null;
}

const VALID_RESET_INTERVALS = new Set<BudgetResetInterval>(["daily", "weekly", "monthly"]);

/** @type {Map<string, NormalizedBudgetConfig>} In-memory cache for budgets */
const budgets = new Map<string, NormalizedBudgetConfig>();

function toNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  return fallback;
}

function toCostEntries(value: unknown): CostEntry[] {
  if (!Array.isArray(value)) return [];
  const entries: CostEntry[] = [];
  for (const item of value) {
    if (!item || typeof item !== "object" || Array.isArray(item)) continue;
    const record = item as Record<string, unknown>;
    const cost = toNumber(record.cost, Number.NaN);
    const timestamp = toNumber(record.timestamp, Number.NaN);
    if (!Number.isFinite(cost) || !Number.isFinite(timestamp)) continue;
    entries.push({ cost, timestamp });
  }
  return entries;
}

function sumEntries(entries: CostEntry[]): number {
  return entries.reduce((sum, entry) => sum + entry.cost, 0);
}

function normalizeResetInterval(value: unknown): BudgetResetInterval {
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase() as BudgetResetInterval;
    if (VALID_RESET_INTERVALS.has(normalized)) {
      return normalized;
    }
  }
  return "daily";
}

function normalizeTimestamp(value: unknown): number | null {
  const numeric = toNumber(value, Number.NaN);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
}

export function normalizeBudgetConfig(config: BudgetConfig): NormalizedBudgetConfig {
  return {
    dailyLimitUsd: Math.max(0, toNumber(config.dailyLimitUsd)),
    weeklyLimitUsd: Math.max(0, toNumber(config.weeklyLimitUsd)),
    monthlyLimitUsd: Math.max(0, toNumber(config.monthlyLimitUsd)),
    warningThreshold: Math.min(Math.max(toNumber(config.warningThreshold, 0.8), 0), 1),
    resetInterval: normalizeResetInterval(config.resetInterval),
    resetTime: normalizeResetTime(config.resetTime),
    budgetResetAt: normalizeTimestamp(config.budgetResetAt),
    lastBudgetResetAt: normalizeTimestamp(config.lastBudgetResetAt),
    warningEmittedAt: normalizeTimestamp(config.warningEmittedAt),
    warningPeriodStart: normalizeTimestamp(config.warningPeriodStart),
  };
}

export function getActiveBudgetLimit(budget: NormalizedBudgetConfig): number {
  if (budget.resetInterval === "monthly") {
    return budget.monthlyLimitUsd > 0 ? budget.monthlyLimitUsd : budget.dailyLimitUsd;
  }
  if (budget.resetInterval === "weekly") {
    return budget.weeklyLimitUsd > 0 ? budget.weeklyLimitUsd : budget.dailyLimitUsd;
  }
  return budget.dailyLimitUsd;
}

function getBudgetWindowTotal(apiKeyId: string, periodStartAt: number): number {
  try {
    return (
      loadCostTotal(apiKeyId, periodStartAt) +
      spendBatchWriter.getPendingCostTotal(apiKeyId, periodStartAt)
    );
  } catch {
    return 0;
  }
}

function getBudgetWindowRangeTotal(
  apiKeyId: string,
  periodStartAt: number,
  periodEndAt: number
): number {
  try {
    return (
      sumEntries(toCostEntries(loadCostEntriesInRange(apiKeyId, periodStartAt, periodEndAt))) +
      spendBatchWriter.getPendingCostTotal(apiKeyId, periodStartAt, periodEndAt)
    );
  } catch {
    return 0;
  }
}

export function emitBudgetWarning(
  apiKeyId: string,
  budget: NormalizedBudgetConfig,
  projectedTotal: number,
  activeLimitUsd: number,
  nextResetAt: number
) {
  const percentage =
    activeLimitUsd > 0 ? ((projectedTotal / activeLimitUsd) * 100).toFixed(1) : "0.0";
  console.warn(
    `[BudgetWarning] ${apiKeyId} reached ${percentage}% of ${budget.resetInterval} budget ($${projectedTotal.toFixed(4)} / $${activeLimitUsd.toFixed(2)}) — next reset ${new Date(nextResetAt).toISOString()}`
  );
}

function syncBudgetSchedule(
  apiKeyId: string,
  config: BudgetConfig,
  now = Date.now(),
  options: SyncBudgetScheduleOptions = {}
): NormalizedBudgetConfig {
  const normalized = normalizeBudgetConfig(config);
  const window = getBudgetWindow(normalized.resetInterval, normalized.resetTime, now);
  const resetRolled =
    normalized.lastBudgetResetAt !== null && window.periodStartAt > normalized.lastBudgetResetAt;

  if (resetRolled && options.logReset !== false) {
    const previousSpend = getBudgetWindowRangeTotal(
      apiKeyId,
      normalized.lastBudgetResetAt as number,
      window.periodStartAt
    );
    try {
      saveBudgetResetLog({
        apiKeyId,
        resetInterval: normalized.resetInterval,
        previousSpend,
        resetAt: window.periodStartAt,
        nextResetAt: window.nextResetAt,
        periodStart: window.periodStartAt,
        periodEnd: window.nextResetAt,
      });
    } catch {
      // Non-fatal: budget logic still proceeds even if logging fails.
    }
  }

  const updated: NormalizedBudgetConfig = {
    ...normalized,
    budgetResetAt: window.nextResetAt,
    lastBudgetResetAt: window.periodStartAt,
    warningEmittedAt: resetRolled ? null : normalized.warningEmittedAt,
    warningPeriodStart: resetRolled ? null : normalized.warningPeriodStart,
  };

  const changed =
    normalized.budgetResetAt !== updated.budgetResetAt ||
    normalized.lastBudgetResetAt !== updated.lastBudgetResetAt ||
    normalized.warningEmittedAt !== updated.warningEmittedAt ||
    normalized.warningPeriodStart !== updated.warningPeriodStart ||
    normalized.dailyLimitUsd !== updated.dailyLimitUsd ||
    normalized.weeklyLimitUsd !== updated.weeklyLimitUsd ||
    normalized.monthlyLimitUsd !== updated.monthlyLimitUsd ||
    normalized.warningThreshold !== updated.warningThreshold ||
    normalized.resetInterval !== updated.resetInterval ||
    normalized.resetTime !== updated.resetTime;

  if (changed && options.persist !== false) {
    try {
      saveBudget(apiKeyId, updated);
    } catch {
      // Non-critical: in-memory cache still works.
    }
  }

  budgets.set(apiKeyId, updated);
  return updated;
}

/**
 * Set budget for an API key.
 *
 * @param {string} apiKeyId
 * @param {BudgetConfig} config
 */
export function setBudget(apiKeyId: string, config: BudgetConfig) {
  return syncBudgetSchedule(apiKeyId, config, Date.now(), {
    logReset: false,
    persist: true,
  });
}

/**
 * Get budget config for an API key.
 *
 * @param {string} apiKeyId
 * @returns {NormalizedBudgetConfig | null}
 */
export function getBudget(apiKeyId: string): NormalizedBudgetConfig | null {
  const cached = budgets.get(apiKeyId);
  if (cached) {
    return syncBudgetSchedule(apiKeyId, cached);
  }

  try {
    const fromDb = loadBudget(apiKeyId) as BudgetConfig | null;
    if (fromDb) {
      return syncBudgetSchedule(apiKeyId, fromDb);
    }
  } catch {
    // DB may not be ready.
  }

  return null;
}

/**
 * Delete budget config and recorded spend for an API key.
 *
 * @param {string} apiKeyId
 */
export function deleteBudget(apiKeyId: string) {
  budgets.delete(apiKeyId);
  discardSpendBatchEntries(apiKeyId);
  try {
    dbDeleteBudget(apiKeyId);
    deleteCostEntries(apiKeyId);
  } catch {
    // Non-critical.
  }
}

function costLedgerInput(
  apiKeyId: string,
  cost: number,
  details: RecordCostDetails
): CostLedgerRecorderInput {
  return {
    apiKeyId,
    provider: details.provider,
    model: details.model,
    tokens: details.tokens,
    amountUsd: cost,
    serviceTier: details.serviceTier,
    success: details.success,
    timestamp: details.timestamp,
    requestId: details.requestId,
  };
}

/**
 * Record a cost for an API key.
 *
 * When `details` is provided, the same call is appended to the per-request cost
 * ledger (`request_cost_ledger`) with the full breakdown so the amount is
 * traceable to provider/model/tokens — used by the chat paths. Bare calls
 * (web-search cost, etc.) write an amount-only ledger row with unknown
 * provider/model.
 *
 * @param {string} apiKeyId
 * @param {number} cost - Cost in USD
 * @param {RecordCostDetails} [details] - Optional per-request breakdown.
 */
export function recordCost(apiKeyId: string, cost: number, details?: RecordCostDetails): void {
  try {
    spendBatchWriter.increment(apiKeyId, cost, Date.now());
    if (details) {
      // Fire-and-forget — never block the response on ledger I/O.
      void runPersistenceBackgroundWork(() =>
        recordLedgerFromCost(costLedgerInput(apiKeyId, cost, details))
      ).catch((error) => console.error("[CostLedger] Failed to persist accounting", error));
    }
  } catch {
    // Non-critical.
  }
}

/**
 * Optional per-request breakdown passed to {@link recordCost} for ledger
 * traceability. `tokens` is the raw provider/normalized usage object — the
 * same shape `saveRequestUsage` accepts.
 */
export interface RecordCostDetails {
  provider?: string | null;
  model?: string | null;
  tokens?: unknown;
  serviceTier?: string | null;
  success?: boolean;
  timestamp?: string;
  requestId?: string | null;
  /** Internal native admission clock captured at terminal completion; never sent to providers. */
  completedAt?: number;
}

/**
 * Build the provider/model/tokens/serviceTier/requestId breakdown shared by
 * both {@link recordChatCallCost} call sites in chatCore.ts (built once,
 * kept out of that file to stay under its frozen file-size ratchet).
 */
export function buildCostCtx(
  provider: string | null | undefined,
  model: string | null | undefined,
  tokens: unknown,
  serviceTier: string | null | undefined,
  requestId: string | null | undefined
): RecordCostDetails {
  return { provider, model, tokens, serviceTier, requestId };
}

/**
 * Record a chat-path cost when there is a real amount to log — the shared
 * `apiKeyInfo?.id && estimatedCost > 0` guard used at both chatCore.ts
 * `recordCost` call sites. `context` (see {@link buildCostCtx}) is
 * built once by the caller; only `success` varies per call site.
 */
export function recordChatCallCost(
  apiKeyInfo: { id?: string | null } | null | undefined,
  estimatedCost: number,
  context: RecordCostDetails,
  success: boolean
): void {
  if (!apiKeyInfo?.id || estimatedCost <= 0) return;
  recordCost(apiKeyInfo.id, estimatedCost, { ...context, success });
}

/**
 * Sync all budgets against the current clock so overdue resets get persisted.
 */
export function syncAllBudgetSchedules(now = Date.now()) {
  let processed = 0;
  let resetCount = 0;

  try {
    const allBudgets = loadAllBudgets();
    for (const [apiKeyId, budget] of Object.entries(allBudgets)) {
      processed += 1;
      const synced = syncBudgetSchedule(apiKeyId, budget, now, {
        logReset: true,
        persist: true,
      });
      if (budget.lastBudgetResetAt !== synced.lastBudgetResetAt) {
        resetCount += 1;
      }
    }
  } catch {
    // Non-critical.
  }

  return { processed, resetCount };
}

/**
 * Check if an API key has remaining budget.
 *
 * @param {string} apiKeyId
 * @param {number} [additionalCost=0] - Projected cost to check
 * @returns {{ allowed: boolean, reason?: string, dailyUsed: number, dailyLimit: number, warningReached: boolean, remaining: number, periodUsed: number, activeLimitUsd: number, resetInterval: BudgetResetInterval | null, resetTime: string | null, budgetResetAt: number | null, lastBudgetResetAt: number | null, periodStartAt: number | null }}
 */
export function checkBudget(apiKeyId: string, additionalCost = 0) {
  const budget = getBudget(apiKeyId);
  if (!budget) {
    return {
      allowed: true,
      dailyUsed: 0,
      dailyLimit: 0,
      warningReached: false,
      remaining: 0,
      periodUsed: 0,
      activeLimitUsd: 0,
      resetInterval: null,
      resetTime: null,
      budgetResetAt: null,
      lastBudgetResetAt: null,
      periodStartAt: null,
    };
  }

  const window = getBudgetWindow(budget.resetInterval, budget.resetTime);
  const periodUsed = getBudgetWindowTotal(apiKeyId, window.periodStartAt);
  const projectedTotal = periodUsed + additionalCost;
  const activeLimitUsd = getActiveBudgetLimit(budget);
  const warningReached =
    activeLimitUsd > 0 && projectedTotal >= activeLimitUsd * budget.warningThreshold;
  const remaining = Math.max(activeLimitUsd - projectedTotal, 0);

  if (warningReached && budget.warningPeriodStart !== window.periodStartAt) {
    const updatedBudget = {
      ...budget,
      warningEmittedAt: Date.now(),
      warningPeriodStart: window.periodStartAt,
    };
    budgets.set(apiKeyId, updatedBudget);
    try {
      saveBudget(apiKeyId, updatedBudget);
      emitBudgetWarning(
        apiKeyId,
        updatedBudget,
        projectedTotal,
        activeLimitUsd,
        window.nextResetAt
      );
    } catch {
      // Non-critical.
    }
  }

  if (activeLimitUsd > 0 && projectedTotal > activeLimitUsd) {
    return {
      allowed: false,
      reason: `${budget.resetInterval[0].toUpperCase()}${budget.resetInterval.slice(1)} budget exceeded: $${projectedTotal.toFixed(4)} / $${activeLimitUsd.toFixed(2)}`,
      dailyUsed: periodUsed,
      dailyLimit: activeLimitUsd,
      warningReached: true,
      remaining,
      periodUsed,
      activeLimitUsd,
      resetInterval: budget.resetInterval,
      resetTime: budget.resetTime,
      budgetResetAt: window.nextResetAt,
      lastBudgetResetAt: window.periodStartAt,
      periodStartAt: window.periodStartAt,
    };
  }

  return {
    allowed: true,
    dailyUsed: periodUsed,
    dailyLimit: activeLimitUsd,
    warningReached,
    remaining,
    periodUsed,
    activeLimitUsd,
    resetInterval: budget.resetInterval,
    resetTime: budget.resetTime,
    budgetResetAt: window.nextResetAt,
    lastBudgetResetAt: window.periodStartAt,
    periodStartAt: window.periodStartAt,
  };
}

/**
 * Get daily total cost for an API key.
 *
 * @param {string} apiKeyId
 * @returns {number} Total cost today in USD
 */
export function getDailyTotal(apiKeyId: string): number {
  const todayStart = new Date();
  todayStart.setHours(0, 0, 0, 0);

  try {
    return (
      sumEntries(toCostEntries(loadCostEntries(apiKeyId, todayStart.getTime()))) +
      spendBatchWriter.getPendingCostTotal(apiKeyId, todayStart.getTime())
    );
  } catch {
    return 0;
  }
}

/**
 * Get cost summary for an API key.
 *
 * @param {string} apiKeyId
 * @returns {BudgetSummary}
 */
export function getCostSummary(apiKeyId: string): BudgetSummary {
  const now = new Date();
  const todayStart = new Date(now);
  todayStart.setHours(0, 0, 0, 0);
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);

  const budget = getBudget(apiKeyId);
  const window = budget ? getBudgetWindow(budget.resetInterval, budget.resetTime) : null;

  try {
    const dailyEntries = [
      ...toCostEntries(loadCostEntries(apiKeyId, todayStart.getTime())),
      ...spendBatchWriter.getBufferedEntries(apiKeyId, todayStart.getTime()),
    ];
    const monthlyEntries = [
      ...toCostEntries(loadCostEntries(apiKeyId, monthStart.getTime())),
      ...spendBatchWriter.getBufferedEntries(apiKeyId, monthStart.getTime()),
    ];
    const periodEntries =
      window !== null
        ? [
            ...toCostEntries(loadCostEntries(apiKeyId, window.periodStartAt)),
            ...spendBatchWriter.getBufferedEntries(apiKeyId, window.periodStartAt),
          ]
        : [];

    const dailyTotal = sumEntries(dailyEntries);
    const monthlyTotal = sumEntries(monthlyEntries);
    const periodTotal = sumEntries(periodEntries);
    const activeLimitUsd = budget ? getActiveBudgetLimit(budget) : 0;

    return {
      dailyTotal,
      monthlyTotal,
      totalEntries: monthlyEntries.length,
      budget,
      totalCostToday: dailyTotal,
      totalCostMonth: monthlyTotal,
      totalCostPeriod: periodTotal,
      activeLimitUsd,
      resetInterval: budget?.resetInterval ?? null,
      resetTime: budget?.resetTime ?? null,
      budgetResetAt: window?.nextResetAt ?? budget?.budgetResetAt ?? null,
      lastBudgetResetAt: window?.periodStartAt ?? budget?.lastBudgetResetAt ?? null,
      periodStartAt: window?.periodStartAt ?? null,
      nextResetAt: window?.nextResetAt ?? null,
      dailyLimitUsd: budget?.dailyLimitUsd ?? 0,
      weeklyLimitUsd: budget?.weeklyLimitUsd ?? 0,
      monthlyLimitUsd: budget?.monthlyLimitUsd ?? 0,
      warningThreshold: budget?.warningThreshold ?? null,
    };
  } catch {
    return {
      dailyTotal: 0,
      monthlyTotal: 0,
      totalEntries: 0,
      budget,
      totalCostToday: 0,
      totalCostMonth: 0,
      totalCostPeriod: 0,
      activeLimitUsd: budget ? getActiveBudgetLimit(budget) : 0,
      resetInterval: budget?.resetInterval ?? null,
      resetTime: budget?.resetTime ?? null,
      budgetResetAt: budget?.budgetResetAt ?? null,
      lastBudgetResetAt: budget?.lastBudgetResetAt ?? null,
      periodStartAt: budget?.lastBudgetResetAt ?? null,
      nextResetAt: budget?.budgetResetAt ?? null,
      dailyLimitUsd: budget?.dailyLimitUsd ?? 0,
      weeklyLimitUsd: budget?.weeklyLimitUsd ?? 0,
      monthlyLimitUsd: budget?.monthlyLimitUsd ?? 0,
      warningThreshold: budget?.warningThreshold ?? null,
    };
  }
}

/**
 * Clear all cost data (for testing).
 */
export function resetCostData() {
  budgets.clear();
  resetSpendBatchWriterForTests();
  try {
    deleteAllCostData();
  } catch {
    // Non-critical.
  }
}
/** Native ledger attempts finish in the caller's admitted scope; spend remains buffered. */
export async function recordCostAsync(
  id: string,
  cost: number,
  details?: RecordCostDetails
): Promise<void> {
  if (!getSelectedPostgresDatabase()) {
    recordCost(id, cost, details);
    return;
  }
  const completedAt = details?.completedAt;
  if (
    completedAt !== undefined &&
    (!Number.isSafeInteger(completedAt) || completedAt < 0 || completedAt > 8_640_000_000_000_000)
  )
    throw new RangeError("Invalid native cost completion time");
  try {
    spendBatchWriter.increment(id, cost, completedAt === undefined ? Date.now() : completedAt);
    if (details) await recordLedgerFromCost(costLedgerInput(id, cost, details));
  } catch {
    // Ledger/persistence failures remain non-critical, as in the synchronous source path.
  }
}

export async function recordChatCallCostAsync(
  ...args: Parameters<typeof recordChatCallCost>
): Promise<void> {
  const [apiKeyInfo, estimatedCost, context, success] = args;
  if (!apiKeyInfo?.id || estimatedCost <= 0) return;
  await recordCostAsync(apiKeyInfo.id, estimatedCost, { ...context, success });
}

export async function setBudgetAsync(...args: Parameters<typeof setBudget>) {
  const database = getSelectedPostgresDatabase();
  return database ? postgresBudget.setBudgetPostgres(database, ...args) : setBudget(...args);
}

export async function getBudgetAsync(...args: Parameters<typeof getBudget>) {
  const database = getSelectedPostgresDatabase();
  return database ? postgresBudget.getBudgetPostgres(database, ...args) : getBudget(...args);
}

export async function deleteBudgetAsync(...args: Parameters<typeof deleteBudget>) {
  const database = getSelectedPostgresDatabase();
  return database ? postgresBudget.deleteBudgetPostgres(database, ...args) : deleteBudget(...args);
}

export async function checkBudgetAsync(...args: Parameters<typeof checkBudget>) {
  const database = getSelectedPostgresDatabase();
  return database ? postgresBudget.checkBudgetPostgres(database, ...args) : checkBudget(...args);
}

export async function getDailyTotalAsync(...args: Parameters<typeof getDailyTotal>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? postgresBudget.getDailyTotalPostgres(database, ...args)
    : getDailyTotal(...args);
}

export async function getCostSummaryAsync(...args: Parameters<typeof getCostSummary>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? postgresBudget.getCostSummaryPostgres(database, ...args)
    : getCostSummary(...args);
}

export async function syncAllBudgetSchedulesAsync(
  ...args: Parameters<typeof syncAllBudgetSchedules>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? postgresBudget.syncAllBudgetSchedulesPostgres(database, ...args)
    : syncAllBudgetSchedules(...args);
}
