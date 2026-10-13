import type { PostgresTransaction } from "./database";
import {
  decrypt,
  encrypt,
  looksEncrypted,
  requireExplicitStorageEncryptionKey,
} from "../encryption";
import { acquirePostgresServiceLock, type PostgresServiceLockOwner } from "./locks";

export function safeInteger(value: unknown): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number)) throw new Error("Stored integer exceeds the supported range");
  return number;
}

export async function first<Row = Record<string, unknown>>(
  transaction: PostgresTransaction,
  text: string,
  values: unknown[] = []
): Promise<Row | undefined> {
  const row = (await transaction.query<Row>(text, values)).rows[0];
  return row === undefined ? undefined : normalizeIntegers(row);
}

export async function rows<Row = Record<string, unknown>>(
  transaction: PostgresTransaction,
  text: string,
  values: unknown[] = []
): Promise<Row[]> {
  return (await transaction.query<Row>(text, values)).rows.map(normalizeIntegers);
}

export async function affected(
  transaction: PostgresTransaction,
  text: string,
  values: unknown[] = []
): Promise<number> {
  return (await transaction.query(text, values)).rowCount ?? 0;
}

/** Serialize read-modify-write operations within each service domain. */
export async function lockServices(
  transaction: PostgresTransaction,
  owner: PostgresServiceLockOwner = "files-batches"
): Promise<void> {
  await acquirePostgresServiceLock(transaction, owner);
}

export function placeholders(count: number, offset = 0): string {
  return Array.from({ length: count }, (_, index) => `$${index + offset + 1}`).join(", ");
}

export function protectSecret(value: string | null | undefined): string | null | undefined {
  if (value == null || value === "") return value;
  if (typeof value !== "string") throw new Error("Invalid credential value");
  requireExplicitStorageEncryptionKey();
  const encrypted = encrypt(value);
  if (
    typeof encrypted !== "string" ||
    !looksEncrypted(encrypted) ||
    typeof decrypt(encrypted, { quiet: true }) !== "string"
  )
    throw new Error("Credential encryption failed");
  return encrypted;
}

const INTEGER_COLUMNS = new Set([
  "bytes",
  "expires_at",
  "deleted_at",
  "in_progress_at",
  "finalizing_at",
  "completed_at",
  "failed_at",
  "expired_at",
  "cancelling_at",
  "cancelled_at",
  "request_counts_total",
  "request_counts_completed",
  "request_counts_failed",
  "output_expires_after_seconds",
  "line_number",
  "insertion_order",
  "enabled",
  "failure_count",
  "last_status",
  "http_status",
  "latency_ms",
  "pass_rate",
  "total",
  "passed",
  "failed",
  "avg_latency_ms",
  "sort_order",
  "batch_size",
  "include_bodies",
  "max_body_bytes",
  "max_rows_per_run",
  "cursor_row_id",
  "exported_total",
  "is_active",
  "daily_budget",
  "hourly_budget",
  "daily_used",
  "hourly_used",
  "max_active_keys",
  "daily_issue_limit",
  "hourly_issue_limit",
  "daily_issued",
  "hourly_issued",
  "pid",
  "port",
  "auto_update",
  "auto_start",
  "auto_restart_adopted",
  "provider_expose",
  "request_count",
  "seq",
  "feasibility",
  "opt_in",
  "tombstoned",
  "interval_ms",
  "records_affected",
  "duration_ms",
]);
function normalizeIntegers<Row>(row: Row): Row {
  if (!row || typeof row !== "object") return row;
  const record = { ...row } as Record<string, unknown>;
  for (const key of Object.keys(record)) {
    if (
      INTEGER_COLUMNS.has(key) &&
      typeof record[key] === "string" &&
      /^-?\d+$/.test(record[key] as string)
    )
      record[key] = safeInteger(record[key]);
  }
  return record as Row;
}
