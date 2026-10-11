/**
 * db/providerPlans.ts — CRUD for provider_plans table.
 *
 * Stores per-connection quota dimension plans (manual overrides or auto-
 * detected). dimensions_json is a JSON-serialized QuotaDimension[] array.
 * getPlan() and listPlans() parse it back to objects on read.
 *
 * All SQL is via prepared statements (Hard Rule #5).
 * Part of: Group B — Quota Sharing Engine (plan 22, frente F2).
 */

import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { PostgresQuotaRepository } from "./postgres/quotaRepository";
import { rowToQuotaPlan as rowToPlan, type QuotaPlanRow as PlanRow } from "./quotaSemantics";

// ---------------------------------------------------------------------------
// Local type shapes (aligned with src/lib/quota/dimensions.ts — merged by F7)
// ---------------------------------------------------------------------------

type QuotaUnit = "percent" | "requests" | "tokens" | "usd";
type QuotaWindow = "5h" | "hourly" | "daily" | "weekly" | "monthly";

export interface QuotaDimension {
  unit: QuotaUnit;
  window: QuotaWindow;
  limit: number;
}

export interface ProviderPlan {
  connectionId: string | null;
  provider: string;
  dimensions: QuotaDimension[];
  source: "auto" | "manual";
}

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
}

function getDb(): DbLike {
  return getDbInstance() as unknown as DbLike;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Get the plan for a specific provider connection, or null if not found.
 * Parses dimensions_json into a typed QuotaDimension array.
 */
export function getPlan(connectionId: string): ProviderPlan | null {
  const row = getDb()
    .prepare<PlanRow>(
      `SELECT connection_id, provider, dimensions_json, source, updated_at
       FROM provider_plans WHERE connection_id = ?`
    )
    .get(connectionId);
  if (!row) return null;
  return rowToPlan(row);
}

/**
 * List all provider plans stored in the DB.
 */
export function listPlans(): ProviderPlan[] {
  const rows = getDb()
    .prepare<PlanRow>(
      `SELECT connection_id, provider, dimensions_json, source, updated_at
       FROM provider_plans ORDER BY provider ASC`
    )
    .all();
  return rows.map(rowToPlan);
}

/**
 * Upsert a provider plan. If a row for connectionId already exists it is
 * replaced (ON CONFLICT DO UPDATE). Serializes dimensions to JSON.
 *
 * @param connectionId Unique provider connection identifier.
 * @param provider     Provider name (e.g. "codex", "kimi").
 * @param dimensions   Array of QuotaDimension objects.
 * @param source       "auto" = detected at runtime; "manual" = operator config.
 */
export function upsertPlan(
  connectionId: string,
  provider: string,
  dimensions: QuotaDimension[],
  source: "auto" | "manual"
): void {
  const now = new Date().toISOString();
  const dimensionsJson = JSON.stringify(dimensions);

  getDb()
    .prepare(
      `INSERT INTO provider_plans (connection_id, provider, dimensions_json, source, updated_at)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(connection_id)
       DO UPDATE SET
         provider = excluded.provider,
         dimensions_json = excluded.dimensions_json,
         source = excluded.source,
         updated_at = excluded.updated_at`
    )
    .run(connectionId, provider, dimensionsJson, source, now);
}

/**
 * Delete the plan for a connection (clears override, falls back to auto/catalog).
 * Returns true if a row was deleted, false if not found.
 */
export function deletePlan(connectionId: string): boolean {
  const result = getDb()
    .prepare("DELETE FROM provider_plans WHERE connection_id = ?")
    .run(connectionId);
  return result.changes > 0;
}

function selectedQuotaRepository(): PostgresQuotaRepository | undefined {
  const database = getSelectedPostgresDatabase();
  return database ? new PostgresQuotaRepository(database) : undefined;
}

export async function getPlanAsync(connectionId: string): Promise<ProviderPlan | null> {
  const repository = selectedQuotaRepository();
  return repository ? repository.getPlan(connectionId) : getPlan(connectionId);
}

export async function listPlansAsync(): Promise<ProviderPlan[]> {
  const repository = selectedQuotaRepository();
  return repository ? repository.listPlans() : listPlans();
}

export async function upsertPlanAsync(
  connectionId: string,
  provider: string,
  dimensions: QuotaDimension[],
  source: "auto" | "manual"
): Promise<void> {
  const repository = selectedQuotaRepository();
  if (repository) await repository.upsertPlan(connectionId, provider, dimensions, source);
  else upsertPlan(connectionId, provider, dimensions, source);
}

export async function deletePlanAsync(connectionId: string): Promise<boolean> {
  const repository = selectedQuotaRepository();
  return repository ? repository.deletePlan(connectionId) : deletePlan(connectionId);
}
