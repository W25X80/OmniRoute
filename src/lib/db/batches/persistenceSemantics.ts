/** Shared batch row projection and sweep bounds; no database imports. */
import { rowToCamel } from "../caseMapping";
import type { BatchRecord, BatchItemCheckpoint } from "../batches";

export function parseBatchRow(row: any): BatchRecord {
  const camel = rowToCamel(row) as any;
  if (camel.metadata && typeof camel.metadata === "string") {
    try {
      camel.metadata = JSON.parse(camel.metadata);
    } catch {
      camel.metadata = null;
    }
  }
  if (camel.errors && typeof camel.errors === "string") {
    try {
      camel.errors = JSON.parse(camel.errors);
    } catch {
      camel.errors = null;
    }
  }
  if (camel.usage && typeof camel.usage === "string") {
    try {
      camel.usage = JSON.parse(camel.usage);
    } catch {
      camel.usage = null;
    }
  }
  // Normalize numeric date fields to ensure they are valid numbers
  const coerceNum = (v: any): number | null => {
    if (typeof v === "number" && Number.isFinite(v)) return v;
    if (v == null) return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };

  camel.createdAt = coerceNum(camel.createdAt) ?? 0;
  camel.inProgressAt = coerceNum(camel.inProgressAt);
  camel.expiresAt = coerceNum(camel.expiresAt);
  camel.finalizingAt = coerceNum(camel.finalizingAt);
  camel.completedAt = coerceNum(camel.completedAt);
  camel.failedAt = coerceNum(camel.failedAt);
  camel.expiredAt = coerceNum(camel.expiredAt);
  camel.cancellingAt = coerceNum(camel.cancellingAt);
  camel.cancelledAt = coerceNum(camel.cancelledAt);
  return camel as BatchRecord;
}

function parseJsonColumn(value: unknown): any | null {
  if (value == null) return null;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

export function parseBatchItemCheckpoint(row: any): BatchItemCheckpoint {
  return {
    batchId: row.batch_id,
    lineNumber: Number(row.line_number),
    customId: row.custom_id ?? null,
    status: row.status,
    result: parseJsonColumn(row.result_json),
    error: parseJsonColumn(row.error_json),
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at),
  };
}

/** Both sweep modes commit in chunks of this many batches (SEC-D, LEDGER-4). */
export const INSTANCE_SWEEP_CHUNK = 200;

/**
 * Upper bound on the number of `INSTANCE_SWEEP_CHUNK`-sized chunks a single
 * `deleteCompletedBatches` call may run (#13680). `sweepLoop` is a synchronous
 * `for (;;)` over `better-sqlite3` — with no cap, one request could hold the
 * Node.js event loop for as long as it takes to sweep every completed batch on
 * the instance. 25 × 200 = 5000 batches/request is a judgment call, not a hard
 * constraint; any caller with more to sweep gets `hasMore: true` back and
 * resumes by calling again — resumption falls out naturally from rowid
 * ordering plus delete-as-you-go (already-swept rows are gone, so the next
 * SELECT picks up the next-lowest surviving rowid on its own; no cursor field
 * needed).
 */
export const MAX_CHUNKS_PER_REQUEST = 25;
