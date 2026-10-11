import {
  getBudgetWindow,
  normalizeBudgetConfig,
  getActiveBudgetLimit,
  emitBudgetWarning,
  type BudgetConfig,
  type NormalizedBudgetConfig,
  type BudgetSummary,
  type checkBudget,
} from "@/domain/costRules";
import { spendBatchWriter, discardSpendBatchEntries } from "@/lib/spend/batchWriter";
import {
  PostgresDomainStateRepository,
  budgetFromRow,
  writeBudget,
  appendResetLog,
} from "./domainStateRepository";
import { readAcceptedSpendSnapshot, type SpendWindow } from "./spendReceipts";
import type { PostgresDatabase, PostgresTransaction } from "./database";

interface SpendReadScope {
  confirmed: Set<string>;
}

async function withBudgetTransaction<T>(
  database: PostgresDatabase,
  body: (tx: PostgresTransaction, scope: SpendReadScope) => Promise<T>
): Promise<T> {
  const completed = await new PostgresDomainStateRepository(database).withDomainLock(async (tx) => {
    const scope: SpendReadScope = { confirmed: new Set() };
    const value = await body(tx, scope);
    return { value, confirmed: [...scope.confirmed] };
  });
  if (completed.confirmed.length) spendBatchWriter.resolveCommittedReceipts(completed.confirmed);
  return completed.value;
}

async function readWindows(
  tx: PostgresTransaction,
  id: string,
  windows: SpendWindow[],
  scope: SpendReadScope
) {
  // The domain lock excludes durable cost writers while this mirror is captured.
  const pending = spendBatchWriter.getBufferedEntries(
    id,
    Math.min(...windows.map((w) => w.since)),
    Math.max(...windows.map((w) => w.until))
  );
  const snapshot = await readAcceptedSpendSnapshot(tx, id, windows, pending);
  for (const receipt of snapshot.confirmedReceiptIds) scope.confirmed.add(receipt);
  return snapshot.totals;
}
async function pgBudget(
  tx: PostgresTransaction,
  id: string,
  now: number,
  scope: SpendReadScope,
  config?: BudgetConfig,
  logReset = true
): Promise<NormalizedBudgetConfig | null> {
  const row =
    config === undefined
      ? (await tx.query("SELECT * FROM omniroute.domain_budgets WHERE api_key_id=$1", [id])).rows[0]
      : null;
  if (config === undefined && !row) return null;
  const normalized = normalizeBudgetConfig(config ?? budgetFromRow(row!));
  const window = getBudgetWindow(normalized.resetInterval, normalized.resetTime, now);
  const rolled =
    normalized.lastBudgetResetAt !== null && window.periodStartAt > normalized.lastBudgetResetAt;
  if (rolled && logReset) {
    const spend = (
      await readWindows(
        tx,
        id,
        [{ since: normalized.lastBudgetResetAt!, until: window.periodStartAt }],
        scope
      )
    )[0].total;
    await appendResetLog(tx, {
      apiKeyId: id,
      resetInterval: normalized.resetInterval,
      previousSpend: spend,
      resetAt: window.periodStartAt,
      nextResetAt: window.nextResetAt,
      periodStart: window.periodStartAt,
      periodEnd: window.nextResetAt,
    });
  }
  const updated = {
    ...normalized,
    budgetResetAt: window.nextResetAt,
    lastBudgetResetAt: window.periodStartAt,
    warningEmittedAt: rolled ? null : normalized.warningEmittedAt,
    warningPeriodStart: rolled ? null : normalized.warningPeriodStart,
  };
  if (config !== undefined || JSON.stringify(updated) !== JSON.stringify(normalized))
    await writeBudget(tx, id, updated);
  return updated;
}

export async function setBudgetPostgres(
  database: PostgresDatabase,
  id: string,
  config: BudgetConfig
): Promise<NormalizedBudgetConfig> {
  return withBudgetTransaction(
    database,
    async (tx, scope) => (await pgBudget(tx, id, Date.now(), scope, config, false))!
  );
}

export async function getBudgetPostgres(
  database: PostgresDatabase,
  id: string
): Promise<NormalizedBudgetConfig | null> {
  return withBudgetTransaction(database, (tx, scope) => pgBudget(tx, id, Date.now(), scope));
}

export async function deleteBudgetPostgres(database: PostgresDatabase, id: string): Promise<void> {
  await new PostgresDomainStateRepository(database).withDomainLock(async (tx) => {
    await tx.query("DELETE FROM omniroute.domain_budgets WHERE api_key_id=$1", [id]);
    await tx.query("DELETE FROM omniroute.domain_budget_reset_logs WHERE api_key_id=$1", [id]);
    await tx.query("DELETE FROM omniroute.domain_cost_history WHERE api_key_id=$1", [id]);
  });
  discardSpendBatchEntries(id);
}

export async function checkBudgetPostgres(
  database: PostgresDatabase,
  id: string,
  additionalCost = 0
): Promise<ReturnType<typeof checkBudget>> {
  let warningAfterCommit:
    { budget: NormalizedBudgetConfig; projected: number; limit: number; next: number } | undefined;
  const result = await withBudgetTransaction(database, async (tx, scope) => {
    const budget = await pgBudget(tx, id, Date.now(), scope);
    if (!budget)
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
    const window = getBudgetWindow(budget.resetInterval, budget.resetTime);
    const used = (
      await readWindows(
        tx,
        id,
        [{ since: window.periodStartAt, until: Number.MAX_SAFE_INTEGER }],
        scope
      )
    )[0].total;
    const projected = used + additionalCost,
      limit = getActiveBudgetLimit(budget);
    const warning = limit > 0 && projected >= limit * budget.warningThreshold;
    if (warning && budget.warningPeriodStart !== window.periodStartAt) {
      await writeBudget(tx, id, {
        ...budget,
        warningEmittedAt: Date.now(),
        warningPeriodStart: window.periodStartAt,
      });
      warningAfterCommit = { budget, projected, limit, next: window.nextResetAt };
    }
    const denied = limit > 0 && projected > limit;
    const verdict = {
      allowed: !denied,
      dailyUsed: used,
      dailyLimit: limit,
      warningReached: denied || warning,
      remaining: Math.max(limit - projected, 0),
      periodUsed: used,
      activeLimitUsd: limit,
      resetInterval: budget.resetInterval,
      resetTime: budget.resetTime,
      budgetResetAt: window.nextResetAt,
      lastBudgetResetAt: window.periodStartAt,
      periodStartAt: window.periodStartAt,
    };
    return denied
      ? {
          ...verdict,
          reason: `${budget.resetInterval[0].toUpperCase()}${budget.resetInterval.slice(1)} budget exceeded: $${projected.toFixed(4)} / $${limit.toFixed(2)}`,
        }
      : verdict;
  });
  if (warningAfterCommit)
    emitBudgetWarning(
      id,
      warningAfterCommit.budget,
      warningAfterCommit.projected,
      warningAfterCommit.limit,
      warningAfterCommit.next
    );
  return result;
}

export async function getDailyTotalPostgres(
  database: PostgresDatabase,
  id: string
): Promise<number> {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return withBudgetTransaction(
    database,
    async (tx, scope) =>
      (
        await readWindows(
          tx,
          id,
          [{ since: today.getTime(), until: Number.MAX_SAFE_INTEGER }],
          scope
        )
      )[0].total
  );
}

export async function getCostSummaryPostgres(
  database: PostgresDatabase,
  id: string
): Promise<BudgetSummary> {
  return withBudgetTransaction(database, async (tx, scope) => {
    const now = new Date(),
      today = new Date(now);
    today.setHours(0, 0, 0, 0);
    const month = new Date(now.getFullYear(), now.getMonth(), 1);
    const budget = await pgBudget(tx, id, now.getTime(), scope);
    const window = budget
      ? getBudgetWindow(budget.resetInterval, budget.resetTime, now.getTime())
      : null;
    const windows = [
      { since: today.getTime(), until: Number.MAX_SAFE_INTEGER },
      { since: month.getTime(), until: Number.MAX_SAFE_INTEGER },
    ];
    if (window) windows.push({ since: window.periodStartAt, until: Number.MAX_SAFE_INTEGER });
    const totals = await readWindows(tx, id, windows, scope);
    const daily = totals[0].total,
      monthly = totals[1];
    return {
      dailyTotal: daily,
      monthlyTotal: monthly.total,
      totalEntries: monthly.count,
      budget,
      totalCostToday: daily,
      totalCostMonth: monthly.total,
      totalCostPeriod: window ? totals[2].total : 0,
      activeLimitUsd: budget ? getActiveBudgetLimit(budget) : 0,
      resetInterval: budget?.resetInterval ?? null,
      resetTime: budget?.resetTime ?? null,
      budgetResetAt: window?.nextResetAt ?? null,
      lastBudgetResetAt: window?.periodStartAt ?? null,
      periodStartAt: window?.periodStartAt ?? null,
      nextResetAt: window?.nextResetAt ?? null,
      dailyLimitUsd: budget?.dailyLimitUsd ?? 0,
      weeklyLimitUsd: budget?.weeklyLimitUsd ?? 0,
      monthlyLimitUsd: budget?.monthlyLimitUsd ?? 0,
      warningThreshold: budget?.warningThreshold ?? null,
    };
  });
}

export async function syncAllBudgetSchedulesPostgres(database: PostgresDatabase, now = Date.now()) {
  return withBudgetTransaction(database, async (tx, scope) => {
    const rows = (await tx.query("SELECT * FROM omniroute.domain_budgets ORDER BY api_key_id"))
      .rows;
    let resetCount = 0;
    for (const row of rows) {
      const synced = await pgBudget(tx, String(row.api_key_id), now, scope);
      if (Number(row.last_budget_reset_at) !== synced!.lastBudgetResetAt) resetCount++;
    }
    return { processed: rows.length, resetCount };
  });
}
