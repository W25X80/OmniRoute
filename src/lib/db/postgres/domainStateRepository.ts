import { captureSpendEntries, assertNewSpendReceipts, appendSpendReceipts } from "./spendReceipts";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import type * as Source from "../domainState";
import { acquirePostgresLock } from "./locks";

type Budget = NonNullable<ReturnType<typeof Source.loadBudget>>;
type Lockout = NonNullable<ReturnType<typeof Source.loadLockoutState>>;
type Breaker = NonNullable<ReturnType<typeof Source.loadCircuitBreakerState>>;
type Chain = NonNullable<ReturnType<typeof Source.loadFallbackChain>>;
type ResetLog = Parameters<typeof Source.saveBudgetResetLog>[0];
type CostEntry = { apiKeyId: string; cost: number; timestamp: number; readonly receiptId?: string };
type Row = Record<string, unknown>;

const numeric = (value: unknown, fallback = 0): number => {
  const number = Number(value);
  return value !== null && Number.isFinite(number) ? number : fallback;
};

export function budgetFromRow(row: Row): Budget {
  return {
    dailyLimitUsd: numeric(row.daily_limit_usd),
    weeklyLimitUsd: numeric(row.weekly_limit_usd),
    monthlyLimitUsd: numeric(row.monthly_limit_usd),
    warningThreshold: numeric(row.warning_threshold, 0.8),
    resetInterval: row.reset_interval as Budget["resetInterval"],
    resetTime: String(row.reset_time || "00:00"),
    budgetResetAt: numeric(row.budget_reset_at) || null,
    lastBudgetResetAt: numeric(row.last_budget_reset_at) || null,
    warningEmittedAt: numeric(row.warning_emitted_at) || null,
    warningPeriodStart: numeric(row.warning_period_start) || null,
  };
}

export async function writeBudget(tx: PostgresTransaction, id: string, config: Partial<Budget>) {
  await tx.query(
    `INSERT INTO omniroute.domain_budgets
      (api_key_id,daily_limit_usd,weekly_limit_usd,monthly_limit_usd,warning_threshold,
       reset_interval,reset_time,budget_reset_at,last_budget_reset_at,warning_emitted_at,warning_period_start)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
     ON CONFLICT(api_key_id) DO UPDATE SET daily_limit_usd=excluded.daily_limit_usd,
       weekly_limit_usd=excluded.weekly_limit_usd,monthly_limit_usd=excluded.monthly_limit_usd,
       warning_threshold=excluded.warning_threshold,reset_interval=excluded.reset_interval,
       reset_time=excluded.reset_time,budget_reset_at=excluded.budget_reset_at,
       last_budget_reset_at=excluded.last_budget_reset_at,warning_emitted_at=excluded.warning_emitted_at,
       warning_period_start=excluded.warning_period_start`,
    [
      id,
      numeric(config.dailyLimitUsd),
      numeric(config.weeklyLimitUsd),
      numeric(config.monthlyLimitUsd),
      numeric(config.warningThreshold, 0.8),
      config.resetInterval || "daily",
      config.resetTime || "00:00",
      config.budgetResetAt ?? null,
      config.lastBudgetResetAt ?? null,
      config.warningEmittedAt ?? null,
      config.warningPeriodStart ?? null,
    ]
  );
}

export async function appendResetLog(tx: PostgresTransaction, entry: ResetLog) {
  await tx.query(
    `INSERT INTO omniroute.domain_budget_reset_logs
      (api_key_id,reset_interval,previous_spend,reset_at,next_reset_at,period_start,period_end)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [
      entry.apiKeyId,
      entry.resetInterval,
      entry.previousSpend,
      entry.resetAt,
      entry.nextResetAt,
      entry.periodStart,
      entry.periodEnd,
    ]
  );
}

/** Accounting and breaker snapshots have independent fences; callbacks contain SQL only. */
export class PostgresDomainStateRepository {
  constructor(readonly database: PostgresDatabase) {}

  async withDomainLock<T>(body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
    return this.database.transaction(async (tx) => {
      await acquirePostgresLock(tx, "accounting");
      return body(tx);
    });
  }

  async saveFallbackChain(model: string, chain: Chain) {
    await this.withDomainLock(async (tx) => {
      await tx.query(
        `INSERT INTO omniroute.domain_fallback_chains(model,chain) VALUES($1,$2)
        ON CONFLICT(model) DO UPDATE SET chain=excluded.chain`,
        [model, JSON.stringify(chain)]
      );
    });
  }
  async loadFallbackChain(model: string): Promise<Chain | null> {
    return this.database.transaction(async (tx) => {
      const row = (
        await tx.query<{ chain: string }>(
          "SELECT chain FROM omniroute.domain_fallback_chains WHERE model=$1",
          [model]
        )
      ).rows[0];
      return row ? JSON.parse(row.chain) : null;
    });
  }
  async loadAllFallbackChains(): Promise<Record<string, Chain>> {
    return this.database.transaction(async (tx) => {
      const rows = (
        await tx.query<{ model: string; chain: string }>(
          "SELECT model,chain FROM omniroute.domain_fallback_chains"
        )
      ).rows;
      return Object.fromEntries(rows.map((row) => [row.model, JSON.parse(row.chain)]));
    });
  }
  async deleteFallbackChain(model: string): Promise<boolean> {
    return this.withDomainLock(
      async (tx) =>
        (await tx.query("DELETE FROM omniroute.domain_fallback_chains WHERE model=$1", [model]))
          .rowCount! > 0
    );
  }
  async deleteAllFallbackChains() {
    await this.withDomainLock(async (tx) => {
      await tx.query("DELETE FROM omniroute.domain_fallback_chains");
    });
  }
  async saveBudget(id: string, config: Partial<Budget>) {
    await this.withDomainLock((tx) => writeBudget(tx, id, config));
  }
  async loadBudget(id: string): Promise<Budget | null> {
    return this.database.transaction(async (tx) => {
      const row = (
        await tx.query("SELECT * FROM omniroute.domain_budgets WHERE api_key_id=$1", [id])
      ).rows[0];
      return row ? budgetFromRow(row) : null;
    });
  }
  async loadAllBudgets(): Promise<Record<string, Budget>> {
    return this.database.transaction(async (tx) => {
      const rows = (await tx.query("SELECT * FROM omniroute.domain_budgets ORDER BY api_key_id"))
        .rows;
      return Object.fromEntries(rows.map((row) => [String(row.api_key_id), budgetFromRow(row)]));
    });
  }
  async saveBudgetResetLog(entry: ResetLog) {
    await this.withDomainLock((tx) => appendResetLog(tx, entry));
  }
  async loadBudgetResetLogs(id: string, limit = 10) {
    return this.database.transaction(async (tx) => {
      const rows = (
        await tx.query(
          `SELECT * FROM omniroute.domain_budget_reset_logs
        WHERE api_key_id=$1 ORDER BY reset_at DESC,id DESC LIMIT $2`,
          [id, Math.max(1, Math.floor(limit))]
        )
      ).rows;
      return rows.map((r) => ({
        id: numeric(r.id),
        apiKeyId: String(r.api_key_id),
        resetInterval: r.reset_interval as ResetLog["resetInterval"],
        previousSpend: numeric(r.previous_spend),
        resetAt: numeric(r.reset_at),
        nextResetAt: numeric(r.next_reset_at),
        periodStart: numeric(r.period_start),
        periodEnd: numeric(r.period_end),
      }));
    });
  }
  async deleteBudget(id: string) {
    await this.withDomainLock(async (tx) => {
      await tx.query("DELETE FROM omniroute.domain_budgets WHERE api_key_id=$1", [id]);
      await tx.query("DELETE FROM omniroute.domain_budget_reset_logs WHERE api_key_id=$1", [id]);
    });
  }
  async saveCostEntry(id: string, cost: number, timestamp = Date.now()) {
    await this.batchSaveCostEntries([{ apiKeyId: id, cost, timestamp }]);
  }
  async batchSaveCostEntries(entries: CostEntry[]) {
    if (!entries.length) return;
    const accepted = captureSpendEntries(entries);
    await this.withDomainLock(async (tx) => {
      await assertNewSpendReceipts(tx, accepted);
      await appendSpendReceipts(tx, accepted);
    });
  }
  async loadCostTotal(id: string, since: number): Promise<number> {
    return this.database.transaction(async (tx) =>
      numeric(
        (
          await tx.query(
            "SELECT COALESCE(SUM(cost),0) AS total FROM omniroute.domain_cost_history WHERE api_key_id=$1 AND timestamp >= $2",
            [id, since]
          )
        ).rows[0]?.total
      )
    );
  }
  async loadCostEntries(id: string, since: number) {
    return this.loadCostEntriesInRange(id, since, Number.MAX_SAFE_INTEGER);
  }
  async loadCostEntriesInRange(id: string, since: number, until: number) {
    return this.database.transaction(async (tx) =>
      (
        await tx.query(
          `SELECT cost,timestamp FROM omniroute.domain_cost_history
       WHERE api_key_id=$1 AND timestamp >= $2 AND timestamp < $3 ORDER BY timestamp,id`,
          [id, since, until]
        )
      ).rows.map((r) => ({
        cost: numeric(r.cost),
        timestamp: numeric(r.timestamp),
      }))
    );
  }
  async cleanOldCostEntries(before: number): Promise<number> {
    return this.withDomainLock(
      async (tx) =>
        (await tx.query("DELETE FROM omniroute.domain_cost_history WHERE timestamp < $1", [before]))
          .rowCount || 0
    );
  }
  async deleteCostEntries(id: string) {
    await this.withDomainLock(async (tx) => {
      await tx.query("DELETE FROM omniroute.domain_cost_history WHERE api_key_id=$1", [id]);
    });
  }
  async deleteAllCostData() {
    await this.withDomainLock(async (tx) => {
      await tx.query("DELETE FROM omniroute.domain_cost_history");
      await tx.query("DELETE FROM omniroute.domain_budgets");
      await tx.query("DELETE FROM omniroute.domain_budget_reset_logs");
    });
  }
  async saveLockoutState(identifier: string, state: Lockout) {
    await this.withDomainLock(async (tx) => {
      await tx.query(
        `INSERT INTO omniroute.domain_lockout_state(identifier,attempts,locked_until) VALUES($1,$2,$3)
       ON CONFLICT(identifier) DO UPDATE SET attempts=excluded.attempts,locked_until=excluded.locked_until`,
        [identifier, JSON.stringify(state.attempts), state.lockedUntil]
      );
    });
  }
  async loadLockoutState(identifier: string): Promise<Lockout | null> {
    return this.database.transaction(async (tx) => {
      const row = (
        await tx.query("SELECT * FROM omniroute.domain_lockout_state WHERE identifier=$1", [
          identifier,
        ])
      ).rows[0];
      return row
        ? {
            attempts: JSON.parse(String(row.attempts)),
            lockedUntil: numeric(row.locked_until) || null,
          }
        : null;
    });
  }
  async deleteLockoutState(identifier: string) {
    await this.withDomainLock(async (tx) => {
      await tx.query("DELETE FROM omniroute.domain_lockout_state WHERE identifier=$1", [
        identifier,
      ]);
    });
  }
  async loadAllLockedIdentifiers() {
    return this.database.transaction(async (tx) =>
      (
        await tx.query(
          "SELECT identifier,locked_until FROM omniroute.domain_lockout_state WHERE locked_until > $1",
          [Date.now()]
        )
      ).rows.map((r) => ({
        identifier: String(r.identifier),
        lockedUntil: numeric(r.locked_until),
      }))
    );
  }
  async saveCircuitBreakerState(name: string, state: Breaker) {
    await this.database.transaction(async (tx) => {
      await acquirePostgresLock(tx, "circuitBreakers");
      await tx.query(
        `INSERT INTO omniroute.domain_circuit_breakers(name,state,failure_count,last_failure_time,options)
       VALUES($1,$2,$3,$4,$5) ON CONFLICT(name) DO UPDATE SET state=excluded.state,
       failure_count=excluded.failure_count,last_failure_time=excluded.last_failure_time,options=excluded.options`,
        [
          name,
          state.state,
          state.failureCount,
          state.lastFailureTime,
          state.options ? JSON.stringify(state.options) : null,
        ]
      );
    });
  }
  async loadCircuitBreakerState(name: string): Promise<Breaker | null> {
    return this.database.transaction(async (tx) => {
      const row = (
        await tx.query("SELECT * FROM omniroute.domain_circuit_breakers WHERE name=$1", [name])
      ).rows[0];
      return row
        ? {
            state: String(row.state),
            failureCount: numeric(row.failure_count),
            lastFailureTime: numeric(row.last_failure_time) || null,
            options: row.options ? JSON.parse(String(row.options)) : null,
          }
        : null;
    });
  }
  async loadAllCircuitBreakerStates() {
    return this.database.transaction(async (tx) =>
      (
        await tx.query(
          "SELECT name,state,failure_count,last_failure_time FROM omniroute.domain_circuit_breakers"
        )
      ).rows.map((r) => ({
        name: String(r.name),
        state: String(r.state),
        failureCount: numeric(r.failure_count),
        lastFailureTime: numeric(r.last_failure_time) || null,
      }))
    );
  }
  async loadAllCircuitBreakerSnapshots(): Promise<Array<Breaker & { name: string }>> {
    return this.database.transaction(async (tx) =>
      (await tx.query("SELECT * FROM omniroute.domain_circuit_breakers")).rows.map((row) => ({
        name: String(row.name),
        state: String(row.state),
        failureCount: numeric(row.failure_count),
        lastFailureTime: numeric(row.last_failure_time) || null,
        options: row.options ? JSON.parse(String(row.options)) : null,
      }))
    );
  }

  async deleteCircuitBreakerState(name: string) {
    await this.database.transaction(async (tx) => {
      await acquirePostgresLock(tx, "circuitBreakers");
      await tx.query("DELETE FROM omniroute.domain_circuit_breakers WHERE name=$1", [name]);
    });
  }
  async deleteAllCircuitBreakerStates() {
    await this.database.transaction(async (tx) => {
      await acquirePostgresLock(tx, "circuitBreakers");
      await tx.query("DELETE FROM omniroute.domain_circuit_breakers");
    });
  }
}

interface LockoutVerdict {
  locked: boolean;
  remainingMs?: number;
  attempts?: number;
}
export async function updatePostgresLockout(
  database: PostgresDatabase,
  id: string,
  config: {
    maxAttempts: number;
    lockoutDurationMs: number;
    attemptWindowMs: number;
  },
  failed: boolean
): Promise<LockoutVerdict> {
  return new PostgresDomainStateRepository(database).withDomainLock(async (tx) => {
    const row = (
      await tx.query(
        "SELECT attempts,locked_until FROM omniroute.domain_lockout_state WHERE identifier=$1",
        [id]
      )
    ).rows[0];
    const now = Date.now();
    let attempts: number[] = row ? JSON.parse(String(row.attempts)) : [];
    let lockedUntil = row?.locked_until === null ? null : Number(row?.locked_until) || null;
    if (!failed && lockedUntil && now < lockedUntil)
      return {
        locked: true,
        remainingMs: lockedUntil - now,
        attempts: attempts.length,
      };
    if (!failed && lockedUntil) {
      lockedUntil = null;
      attempts = [];
    }
    attempts = attempts.filter((t) => t > now - config.attemptWindowMs);
    if (failed) {
      attempts.push(now);
      if (attempts.length >= config.maxAttempts) lockedUntil = now + config.lockoutDurationMs;
    }
    if (!row && !failed) return { locked: false, attempts: 0 };
    await tx.query(
      `INSERT INTO omniroute.domain_lockout_state(identifier,attempts,locked_until) VALUES($1,$2,$3)
      ON CONFLICT(identifier) DO UPDATE SET attempts=excluded.attempts,locked_until=excluded.locked_until`,
      [id, JSON.stringify(attempts), lockedUntil]
    );
    return lockedUntil && now < lockedUntil
      ? {
          locked: true,
          remainingMs: lockedUntil - now,
          attempts: attempts.length,
        }
      : failed
        ? { locked: false }
        : { locked: false, attempts: attempts.length };
  });
}
