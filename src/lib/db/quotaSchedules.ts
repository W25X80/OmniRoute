/**
 * db/quotaSchedules.ts — CRUD for the quota_schedules table.
 *
 * Time-aware quota windows for a quota pool: when OmniRoute may use the pool's
 * accounts, how much upstream quota it must leave behind (`reserve_percent`),
 * and how much it may consume itself inside the window (`budget_*`).
 * See migration 189 and src/lib/quota/schedules.ts for the semantics.
 *
 * All SQL goes through prepared statements — never raw string interpolation
 * (Hard Rule #5). Import getDbInstance from ./core (Hard Rule #5).
 *
 * The QuotaSchedule shape is imported type-only from the quota layer, so this
 * module stays the single mapping point between rows and the engine's type
 * without creating a runtime import cycle.
 */

import type { QuotaSchedule } from "@/lib/quota/schedules";
import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { PostgresQuotaRepository } from "./postgres/quotaRepository";
import {
  serializeQuotaDays as serializeDays,
  serializeQuotaReserves as serializeReserves,
  rowToQuotaSchedule as rowToSchedule,
  type QuotaScheduleRow as ScheduleRow,
} from "./quotaSemantics";

export { parseDays, parseReserves } from "./quotaSemantics";

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

interface StatementLike<TRow = unknown> {
  all: (...params: unknown[]) => TRow[];
  get: (...params: unknown[]) => TRow | undefined;
  run: (...params: unknown[]) => { changes: number };
}

interface DbLike {
  prepare: <TRow = unknown>(sql: string) => StatementLike<TRow>;
  transaction: <T>(fn: () => T) => () => T;
}

function getDb(): DbLike {
  return getDbInstance() as unknown as DbLike;
}

function makeId(): string {
  return `sched-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** A schedule as submitted by a caller: the id and poolId are assigned here. */
export type ScheduleInput = Omit<QuotaSchedule, "id" | "poolId"> & { id?: string };

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * All schedules for a pool, ordered priority DESC then id ASC — the same order
 * `resolveActiveSchedule` applies, so the UI list reads as the engine resolves.
 * Returns [] when the table is missing (migration not yet applied): schedules
 * are additive, and their absence must never block traffic.
 */
export function listSchedules(poolId: string): QuotaSchedule[] {
  try {
    const rows = getDb()
      .prepare<ScheduleRow>(
        `SELECT id, pool_id, label, days, start_minute, end_minute, timezone, mode,
                reserves, budget_value, budget_unit, budget_window, priority, enabled
         FROM quota_schedules
         WHERE pool_id = ?
         ORDER BY priority DESC, id ASC`
      )
      .all(poolId);
    return rows.map(rowToSchedule);
  } catch {
    // Table absent (migration not run) or DB unavailable — fail-open.
    return [];
  }
}

/** Only the enabled rows — what the hot path needs. */
export function listEnabledSchedules(poolId: string): QuotaSchedule[] {
  return listSchedules(poolId).filter((s) => s.enabled);
}

/**
 * Replace a pool's entire schedule set in one transaction.
 *
 * Whole-set replacement (rather than per-row PATCH) keeps the editor honest: the
 * resolution rules are order- and overlap-sensitive, so the UI always reasons
 * about the complete set it is about to save.
 *
 * @returns the persisted schedules, in resolution order.
 */
export function replaceSchedules(poolId: string, schedules: ScheduleInput[]): QuotaSchedule[] {
  const db = getDb();

  const del = db.prepare(`DELETE FROM quota_schedules WHERE pool_id = ?`);
  const ins = db.prepare(
    `INSERT INTO quota_schedules
       (id, pool_id, label, days, start_minute, end_minute, timezone, mode,
        reserves, budget_value, budget_unit, budget_window, priority, enabled)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );

  db.transaction(() => {
    del.run(poolId);
    for (const s of schedules) {
      const hasBudget = s.budgetValue != null && s.budgetUnit != null;
      ins.run(
        s.id ?? makeId(),
        poolId,
        s.label ?? null,
        serializeDays(s.days),
        s.startMinute,
        s.endMinute,
        s.timezone ?? null,
        s.mode,
        serializeReserves(s.reserves),
        hasBudget ? s.budgetValue : null,
        hasBudget ? s.budgetUnit : null,
        s.budgetWindow,
        s.priority,
        s.enabled ? 1 : 0
      );
    }
  })();

  return listSchedules(poolId);
}

/**
 * Drop every schedule of a pool. Called when the pool itself is deleted — the
 * table has no FK cascade, for the same reason quota_allocation_model_caps has
 * none (avoiding a fragile 3-table cascade chain in SQLite).
 */
export function deleteSchedulesForPool(poolId: string): number {
  try {
    return getDb().prepare(`DELETE FROM quota_schedules WHERE pool_id = ?`).run(poolId).changes;
  } catch {
    return 0;
  }
}

function selectedQuotaRepository(): PostgresQuotaRepository | undefined {
  const database = getSelectedPostgresDatabase();
  return database ? new PostgresQuotaRepository(database) : undefined;
}

export async function listSchedulesAsync(poolId: string): Promise<QuotaSchedule[]> {
  try {
    const repository = selectedQuotaRepository();
    return repository ? await repository.listSchedules(poolId) : listSchedules(poolId);
  } catch {
    // Preserve the source schedule gate's fail-open policy, without switching backends.
    return [];
  }
}

export async function listEnabledSchedulesAsync(poolId: string): Promise<QuotaSchedule[]> {
  return (await listSchedulesAsync(poolId)).filter((schedule) => schedule.enabled);
}

export async function replaceSchedulesAsync(
  poolId: string,
  schedules: ScheduleInput[]
): Promise<QuotaSchedule[]> {
  const repository = selectedQuotaRepository();
  return repository
    ? repository.replaceSchedules(poolId, schedules)
    : replaceSchedules(poolId, schedules);
}

export async function deleteSchedulesForPoolAsync(poolId: string): Promise<number> {
  try {
    const repository = selectedQuotaRepository();
    return repository
      ? await repository.deleteSchedulesForPool(poolId)
      : deleteSchedulesForPool(poolId);
  } catch {
    return 0;
  }
}
