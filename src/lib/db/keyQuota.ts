/**
 * db/keyQuota.ts — Per-API-key tpm/rpm/monthly-amount quota (RIC-741 / M3 D5).
 *
 * KISS counter + threshold quota: a single config row on `api_key_quota_limits`
 * (tpm / rpm / monthly USD) plus 2-bucket sliding-window counters on
 * `api_key_quota_counters` for tpm & rpm. The monthly amount is not counted
 * here — it is read from the cost ledger's current-calendar-month SUM
 * (`aggregateLedgerThisMonth`), so one source of truth for money.
 *
 * A NULL limit means "unlimited" (0 is normalized to unlimited on read).
 *
 * @module db/keyQuota
 */

import { getDbInstance } from "./core";
import { aggregateLedgerThisMonth } from "./costLedger";
import { toNumber } from "@/shared/utils/numeric";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import type { PostgresDatabase, PostgresTransaction } from "./postgres/database";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface KeyQuotaLimits {
  apiKeyId: string;
  /** Tokens per minute; null/0 = unlimited. */
  tpmLimit: number | null;
  /** Requests per minute; null/0 = unlimited. */
  rpmLimit: number | null;
  /** Calendar-month USD spend cap; null/0 = unlimited. */
  monthlyAmountUsd: number | null;
}

export interface KeyQuotaCounters {
  /** Effective tokens used in the current minute window. */
  tpmUsed: number;
  /** Effective requests used in the current minute window. */
  rpmUsed: number;
  /** Calendar-month USD spend (from the cost ledger). */
  monthlyAmountUsd: number;
}

export interface KeyQuotaStatus {
  enabled: boolean;
  limits: KeyQuotaLimits;
  counters: KeyQuotaCounters;
  tpmExceeded: boolean;
  rpmExceeded: boolean;
  monthlyExceeded: boolean;
  /** ISO timestamp the current minute window ends. */
  windowResetAtIso: string;
}

export type KeyQuotaDimension = "tpm" | "rpm";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** 2-bucket sliding window over a 60s fixed window. */
export const KEY_QUOTA_WINDOW_MS = 60_000;

/** dimension_key fragment for the counters table. */
const DIMENSION_TPM = "key-quota:tpm";
const DIMENSION_RPM = "key-quota:rpm";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type JsonRecord = Record<string, unknown>;

function asRecord(value: unknown): JsonRecord {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : {};
}

/** NULL or <=0 reads as unlimited → null. */
function toLimitOrNull(value: unknown): number | null {
  const numeric = toNumber(value);
  return numeric > 0 ? numeric : null;
}

/** Convert a limit back to a null-safe string for the DB (NULL stores unlimited). */
function limitToDb(value: number | null | undefined): number | null {
  if (value === undefined) return null;
  return value !== null && value > 0 ? value : null;
}

function toIsoWindowStart(nowMs: number): string {
  const windowStart = Math.floor(nowMs / KEY_QUOTA_WINDOW_MS) * KEY_QUOTA_WINDOW_MS;
  return new Date(windowStart).toISOString();
}

// ---------------------------------------------------------------------------
// Config CRUD
// ---------------------------------------------------------------------------

function rowToLimits(row: unknown): KeyQuotaLimits {
  const r = asRecord(row);
  return {
    apiKeyId: typeof r.api_key_id === "string" ? r.api_key_id : "",
    tpmLimit: toLimitOrNull(r.tpm_limit),
    rpmLimit: toLimitOrNull(r.rpm_limit),
    monthlyAmountUsd: toLimitOrNull(r.monthly_amount_usd),
  };
}

/**
 * Load quota limits for a key. Returns all-null limits when no row exists.
 */
export function getKeyQuotaLimits(apiKeyId: string): KeyQuotaLimits {
  if (!apiKeyId) return { apiKeyId, tpmLimit: null, rpmLimit: null, monthlyAmountUsd: null };
  const db = getDbInstance();
  const row = db
    .prepare(
      `SELECT api_key_id, tpm_limit, rpm_limit, monthly_amount_usd
       FROM api_key_quota_limits WHERE api_key_id = ?`
    )
    .get(apiKeyId);
  return row
    ? rowToLimits(row)
    : { apiKeyId, tpmLimit: null, rpmLimit: null, monthlyAmountUsd: null };
}

export interface UpsertKeyQuotaLimitsInput {
  tpmLimit?: number | null;
  rpmLimit?: number | null;
  monthlyAmountUsd?: number | null;
}

/**
 * Upsert quota limits for a key. Omitted fields keep their existing value.
 * Returns the resulting limits.
 */
export function upsertKeyQuotaLimits(
  apiKeyId: string,
  input: UpsertKeyQuotaLimitsInput
): KeyQuotaLimits {
  if (!apiKeyId) return { apiKeyId, tpmLimit: null, rpmLimit: null, monthlyAmountUsd: null };

  const db = getDbInstance();
  const existing = getKeyQuotaLimits(apiKeyId);
  const tpm = limitToDb(input.tpmLimit !== undefined ? input.tpmLimit : existing.tpmLimit);
  const rpm = limitToDb(input.rpmLimit !== undefined ? input.rpmLimit : existing.rpmLimit);
  const monthly = limitToDb(
    input.monthlyAmountUsd !== undefined ? input.monthlyAmountUsd : existing.monthlyAmountUsd
  );

  db.prepare(
    `INSERT INTO api_key_quota_limits (api_key_id, tpm_limit, rpm_limit, monthly_amount_usd, updated_at)
     VALUES (?, ?, ?, ?, datetime('now'))
     ON CONFLICT(api_key_id) DO UPDATE SET
       tpm_limit = excluded.tpm_limit,
       rpm_limit = excluded.rpm_limit,
       monthly_amount_usd = excluded.monthly_amount_usd,
       updated_at = excluded.updated_at`
  ).run(apiKeyId, tpm, rpm, monthly);

  return { apiKeyId, tpmLimit: tpm, rpmLimit: rpm, monthlyAmountUsd: monthly };
}

export function clearKeyQuotaLimits(apiKeyId: string): void {
  if (!apiKeyId) return;
  const db = getDbInstance();
  db.prepare("DELETE FROM api_key_quota_limits WHERE api_key_id = ?").run(apiKeyId);
  db.prepare("DELETE FROM api_key_quota_counters WHERE api_key_id = ?").run(apiKeyId);
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

function bucketIndex(nowMs: number): number {
  return Math.floor(nowMs / KEY_QUOTA_WINDOW_MS);
}

function getBucketCount(apiKeyId: string, dimensionKey: string, bucket: number): number {
  const db = getDbInstance();
  const row = db
    .prepare(
      `SELECT consumed FROM api_key_quota_counters
       WHERE api_key_id = ? AND dimension_key = ? AND bucket_index = ?`
    )
    .get(apiKeyId, dimensionKey, bucket) as { consumed?: number } | undefined;
  return toNumber(row?.consumed);
}

/**
 * Effective count for a 2-bucket sliding window:
 *   effective = prev × (1 − elapsed/window) + curr
 */
function effectiveWindowCount(apiKeyId: string, dimensionKey: string, nowMs: number): number {
  const current = bucketIndex(nowMs);
  const prev = current - 1;
  const curr = getBucketCount(apiKeyId, dimensionKey, current);
  const prevCount = getBucketCount(apiKeyId, dimensionKey, prev);
  const elapsed = nowMs - current * KEY_QUOTA_WINDOW_MS;
  const weight = Math.max(0, Math.min(1, elapsed / KEY_QUOTA_WINDOW_MS));
  return prevCount * (1 - weight) + curr;
}

/**
 * Atomically add `delta` to the current window bucket (UPSERT).
 */
export function incrementKeyQuotaCounter(
  apiKeyId: string,
  dimension: KeyQuotaDimension,
  delta: number,
  nowMs = Date.now()
): void {
  if (!apiKeyId || delta <= 0) return;
  const dimensionKey = dimension === "tpm" ? DIMENSION_TPM : DIMENSION_RPM;
  const bucket = bucketIndex(nowMs);
  const db = getDbInstance();
  db.prepare(
    `INSERT INTO api_key_quota_counters (api_key_id, dimension_key, bucket_index, consumed, updated_at)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(api_key_id, dimension_key, bucket_index) DO UPDATE SET
       consumed = consumed + excluded.consumed,
       updated_at = excluded.updated_at`
  ).run(apiKeyId, dimensionKey, bucket, delta, nowMs);
}

/**
 * Increment both tpm (by billable tokens) and rpm (by 1) for a completed call.
 */
export function recordKeyQuotaUsage(
  apiKeyId: string,
  billableTokens: number,
  nowMs = Date.now()
): void {
  if (!apiKeyId) return;
  if (billableTokens > 0) incrementKeyQuotaCounter(apiKeyId, "tpm", billableTokens, nowMs);
  incrementKeyQuotaCounter(apiKeyId, "rpm", 1, nowMs);
}

// ---------------------------------------------------------------------------
// Status / enforcement read
// ---------------------------------------------------------------------------

/**
 * Effective usage in the current windows plus the month ledger total.
 */
export function getKeyQuotaCounters(
  apiKeyId: string,
  deps: { now?: () => number; monthTotalUsd?: number } = {}
): KeyQuotaCounters {
  const nowMs = (deps.now ?? Date.now)();
  return {
    tpmUsed: effectiveWindowCount(apiKeyId, DIMENSION_TPM, nowMs),
    rpmUsed: effectiveWindowCount(apiKeyId, DIMENSION_RPM, nowMs),
    monthlyAmountUsd:
      deps.monthTotalUsd !== undefined ? deps.monthTotalUsd : getLedgerMonthTotal(apiKeyId, nowMs),
  };
}

/**
 * Resolve the current calendar-month ledger total for a key, with a lazy
 * dynamic import to avoid a hard module cycle cost on cold starts.
 */
function getLedgerMonthTotal(apiKeyId: string, nowMs: number): number {
  try {
    return aggregateLedgerThisMonth(apiKeyId, new Date(nowMs).toISOString()).amountUsd;
  } catch {
    return 0;
  }
}

/**
 * Full enforcement-ready status for a key: limits, current usage, and per
 * dimension exceeded flags.
 */
export function getKeyQuotaStatus(
  apiKeyId: string,
  deps: { now?: () => number; monthTotalUsd?: number } = {}
): KeyQuotaStatus {
  const limits = getKeyQuotaLimits(apiKeyId);
  const counters = getKeyQuotaCounters(apiKeyId, deps);
  const enabled =
    limits.tpmLimit !== null || limits.rpmLimit !== null || limits.monthlyAmountUsd !== null;

  const nowMs = (deps.now ?? Date.now)();

  return {
    enabled,
    limits,
    counters,
    tpmExceeded: enabled && limits.tpmLimit !== null && counters.tpmUsed >= limits.tpmLimit,
    rpmExceeded: enabled && limits.rpmLimit !== null && counters.rpmUsed >= limits.rpmLimit,
    monthlyExceeded:
      enabled &&
      limits.monthlyAmountUsd !== null &&
      counters.monthlyAmountUsd >= limits.monthlyAmountUsd,
    windowResetAtIso: toIsoWindowStart(nowMs + KEY_QUOTA_WINDOW_MS),
  };
}

export async function getKeyQuotaLimitsAsync(apiKeyId: string): Promise<KeyQuotaLimits> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getKeyQuotaLimits(apiKeyId);
  const empty = {
    apiKeyId,
    tpmLimit: null,
    rpmLimit: null,
    monthlyAmountUsd: null,
  };
  if (!apiKeyId) return empty;
  return database.transaction(async (tx) => {
    const row = (
      await tx.query("SELECT * FROM omniroute.api_key_quota_limits WHERE api_key_id=$1", [apiKeyId])
    ).rows[0];
    return row ? rowToLimits(row) : empty;
  });
}

export async function upsertKeyQuotaLimitsAsync(
  apiKeyId: string,
  input: UpsertKeyQuotaLimitsInput
): Promise<KeyQuotaLimits> {
  const database = getSelectedPostgresDatabase();
  if (!database) return upsertKeyQuotaLimits(apiKeyId, input);
  const empty = {
    apiKeyId,
    tpmLimit: null,
    rpmLimit: null,
    monthlyAmountUsd: null,
  };
  if (!apiKeyId) return empty;
  return database.transaction(async (tx) => {
    await tx.query("SELECT pg_advisory_xact_lock(18765,9)");
    const row = (
      await tx.query("SELECT * FROM omniroute.api_key_quota_limits WHERE api_key_id=$1", [apiKeyId])
    ).rows[0];
    const existing = row ? rowToLimits(row) : empty;
    const tpm = limitToDb(input.tpmLimit !== undefined ? input.tpmLimit : existing.tpmLimit);
    const rpm = limitToDb(input.rpmLimit !== undefined ? input.rpmLimit : existing.rpmLimit);
    const monthly = limitToDb(
      input.monthlyAmountUsd !== undefined ? input.monthlyAmountUsd : existing.monthlyAmountUsd
    );
    await tx.query(
      `INSERT INTO omniroute.api_key_quota_limits(api_key_id,tpm_limit,rpm_limit,monthly_amount_usd,updated_at)
      VALUES($1,$2,$3,$4,$5) ON CONFLICT(api_key_id) DO UPDATE SET tpm_limit=excluded.tpm_limit,
      rpm_limit=excluded.rpm_limit,monthly_amount_usd=excluded.monthly_amount_usd,updated_at=excluded.updated_at`,
      [apiKeyId, tpm, rpm, monthly, new Date().toISOString()]
    );
    return {
      apiKeyId,
      tpmLimit: tpm,
      rpmLimit: rpm,
      monthlyAmountUsd: monthly,
    };
  });
}

export async function clearKeyQuotaLimitsAsync(id: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return clearKeyQuotaLimits(id);
  if (!id) return;
  await database.transaction(async (tx) => {
    await tx.query("SELECT pg_advisory_xact_lock(18765,9)");
    await tx.query("DELETE FROM omniroute.api_key_quota_limits WHERE api_key_id=$1", [id]);
    await tx.query("DELETE FROM omniroute.api_key_quota_counters WHERE api_key_id=$1", [id]);
  });
}

async function addKeyQuota(
  tx: PostgresTransaction,
  id: string,
  dimension: KeyQuotaDimension,
  delta: number,
  now: number
): Promise<void> {
  if (!id || !Number.isFinite(delta) || delta <= 0) return;
  await tx.query(
    `INSERT INTO omniroute.api_key_quota_counters(api_key_id,dimension_key,bucket_index,consumed,updated_at)
    VALUES($1,$2,$3,$4,$5) ON CONFLICT(api_key_id,dimension_key,bucket_index) DO UPDATE SET
    consumed=api_key_quota_counters.consumed+excluded.consumed,updated_at=excluded.updated_at`,
    [id, dimension === "tpm" ? DIMENSION_TPM : DIMENSION_RPM, bucketIndex(now), delta, now]
  );
}

export async function incrementKeyQuotaCounterAsync(
  id: string,
  dimension: KeyQuotaDimension,
  delta: number,
  now = Date.now()
): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return incrementKeyQuotaCounter(id, dimension, delta, now);
  await database.transaction((tx) => addKeyQuota(tx, id, dimension, delta, now));
}

export async function recordKeyQuotaUsageAsync(
  id: string,
  tokens: number,
  now = Date.now()
): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return recordKeyQuotaUsage(id, tokens, now);
  if (!id) return;
  await database.transaction(async (tx) => {
    await addKeyQuota(tx, id, "tpm", tokens, now);
    await addKeyQuota(tx, id, "rpm", 1, now);
  });
}

async function readNativeKeyQuotaState(
  database: PostgresDatabase,
  id: string,
  deps: { now?: () => number; monthTotalUsd?: number } = {}
) {
  const now = (deps.now ?? Date.now)(),
    current = bucketIndex(now),
    date = new Date(now);
  const month = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1)).toISOString();
  const row = await database.transaction(async (tx) => {
    await tx.query("SELECT pg_advisory_xact_lock(18765,9)");
    return (
      await tx.query(
        `SELECT q.tpm_limit,q.rpm_limit,q.monthly_amount_usd,
      COALESCE((SELECT SUM(consumed) FROM omniroute.api_key_quota_counters
        WHERE api_key_id=$1 AND dimension_key='key-quota:tpm' AND bucket_index=$2),0) AS tpm_current,
      COALESCE((SELECT SUM(consumed) FROM omniroute.api_key_quota_counters
        WHERE api_key_id=$1 AND dimension_key='key-quota:tpm' AND bucket_index=$3),0) AS tpm_previous,
      COALESCE((SELECT SUM(consumed) FROM omniroute.api_key_quota_counters
        WHERE api_key_id=$1 AND dimension_key='key-quota:rpm' AND bucket_index=$2),0) AS rpm_current,
      COALESCE((SELECT SUM(consumed) FROM omniroute.api_key_quota_counters
        WHERE api_key_id=$1 AND dimension_key='key-quota:rpm' AND bucket_index=$3),0) AS rpm_previous,
      ${deps.monthTotalUsd === undefined ? "COALESCE((SELECT SUM(amount_usd) FROM omniroute.request_cost_ledger WHERE api_key_id=$1 AND timestamp >= $4 AND success=1),0)" : "$4::double precision"} AS ledger_month
      FROM (SELECT $1::text AS api_key_id) key_scope LEFT JOIN omniroute.api_key_quota_limits q
      ON q.api_key_id=key_scope.api_key_id`,
        [id, current, current - 1, deps.monthTotalUsd ?? month]
      )
    ).rows[0];
  });
  const weight = Math.max(
    0,
    Math.min(1, (now - current * KEY_QUOTA_WINDOW_MS) / KEY_QUOTA_WINDOW_MS)
  );
  const limits: KeyQuotaLimits = {
    apiKeyId: id,
    tpmLimit: toLimitOrNull(row.tpm_limit),
    rpmLimit: toLimitOrNull(row.rpm_limit),
    monthlyAmountUsd: toLimitOrNull(row.monthly_amount_usd),
  };
  const counters: KeyQuotaCounters = {
    tpmUsed: toNumber(row.tpm_current) + toNumber(row.tpm_previous) * (1 - weight),
    rpmUsed: toNumber(row.rpm_current) + toNumber(row.rpm_previous) * (1 - weight),
    monthlyAmountUsd: toNumber(row.ledger_month),
  };
  return { limits, counters, now };
}

export async function getKeyQuotaCountersAsync(
  id: string,
  deps: { now?: () => number; monthTotalUsd?: number } = {}
): Promise<KeyQuotaCounters> {
  const database = getSelectedPostgresDatabase();
  return database
    ? (await readNativeKeyQuotaState(database, id, deps)).counters
    : getKeyQuotaCounters(id, deps);
}

export async function getKeyQuotaStatusAsync(
  id: string,
  deps: { now?: () => number; monthTotalUsd?: number } = {}
): Promise<KeyQuotaStatus> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getKeyQuotaStatus(id, deps);
  const { limits, counters, now } = await readNativeKeyQuotaState(database, id, deps);
  const enabled =
    limits.tpmLimit !== null || limits.rpmLimit !== null || limits.monthlyAmountUsd !== null;
  return {
    enabled,
    limits,
    counters,
    tpmExceeded: enabled && limits.tpmLimit !== null && counters.tpmUsed >= limits.tpmLimit,
    rpmExceeded: enabled && limits.rpmLimit !== null && counters.rpmUsed >= limits.rpmLimit,
    monthlyExceeded:
      enabled &&
      limits.monthlyAmountUsd !== null &&
      counters.monthlyAmountUsd >= limits.monthlyAmountUsd,
    windowResetAtIso: toIsoWindowStart(now + KEY_QUOTA_WINDOW_MS),
  };
}
