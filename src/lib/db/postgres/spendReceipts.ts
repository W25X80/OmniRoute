import { randomUUID } from "node:crypto";
import type { BufferedCostEntry } from "@/lib/spend/batchWriter";
import type { PostgresTransaction } from "./database";

/** Retain receipt identity when history retention deletes the corresponding event row. */
export const POSTGRES_SPEND_RECEIPT_SCHEMA_STATEMENTS: readonly string[] = [
  `CREATE TABLE omniroute.domain_cost_receipts (
    receipt_id uuid PRIMARY KEY, api_key_id text COLLATE "C" NOT NULL,
    cost double precision NOT NULL, timestamp bigint NOT NULL,
    history_id bigint NOT NULL UNIQUE
  )`,
  "CREATE INDEX idx_dcr_key_timestamp ON omniroute.domain_cost_receipts(api_key_id,timestamp)",
];

const SPEND_RECEIPT_ERROR = Symbol.for("omniroute.postgres.spendReceiptError");

export class PostgresSpendReceiptError extends Error {
  constructor() {
    super("Spend receipt has already been committed or has conflicting event data");
    this.name = "PostgresSpendReceiptError";
    Object.defineProperty(this, SPEND_RECEIPT_ERROR, { value: true });
  }
}

export function isPostgresSpendReceiptError(error: unknown): error is PostgresSpendReceiptError {
  return Boolean(
    error &&
    typeof error === "object" &&
    (error as { [SPEND_RECEIPT_ERROR]?: unknown })[SPEND_RECEIPT_ERROR] === true
  );
}

function receiptId(value: string): string {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) {
    throw new TypeError("Invalid spend receipt identity");
  }
  return value.toLowerCase();
}

/** Capture immutable event data before the first asynchronous boundary. */
export function captureSpendEntries(
  entries: readonly BufferedCostEntry[]
): readonly BufferedCostEntry[] {
  return entries.map((entry) => {
    if (!entry.apiKeyId || !Number.isFinite(entry.cost) || !Number.isSafeInteger(entry.timestamp)) {
      throw new TypeError("Invalid spend event");
    }
    return Object.freeze({
      apiKeyId: entry.apiKeyId,
      cost: entry.cost,
      timestamp: entry.timestamp,
      receiptId: receiptId(entry.receiptId ?? randomUUID()),
    });
  });
}

/** A repeated submission is rejected, not replayed or silently converted into success. */
export async function assertNewSpendReceipts(
  tx: PostgresTransaction,
  entries: readonly BufferedCostEntry[]
): Promise<void> {
  const ids = entries.map((entry) => receiptId(entry.receiptId!));
  if (new Set(ids).size !== ids.length) throw new PostgresSpendReceiptError();
  if (
    (
      await tx.query(
        "SELECT receipt_id FROM omniroute.domain_cost_receipts WHERE receipt_id=ANY($1::uuid[]) LIMIT 1",
        [ids]
      )
    ).rows.length
  )
    throw new PostgresSpendReceiptError();
}

/** Allocate one history identity per captured event; never correlate by result order. */
export async function appendSpendReceipts(
  tx: PostgresTransaction,
  entries: readonly BufferedCostEntry[]
): Promise<void> {
  await tx.query(
    `WITH captured AS MATERIALIZED (
      SELECT events.*,nextval(pg_get_serial_sequence('omniroute.domain_cost_history','id')::regclass) AS history_id
      FROM jsonb_to_recordset($1::jsonb)
      AS events(receipt_id uuid,api_key_id text,cost double precision,timestamp bigint)
    ), written AS (
      INSERT INTO omniroute.domain_cost_history(id,api_key_id,cost,timestamp)
      SELECT history_id,api_key_id,cost,timestamp FROM captured RETURNING id
    )
    INSERT INTO omniroute.domain_cost_receipts(receipt_id,api_key_id,cost,timestamp,history_id)
    SELECT c.receipt_id,c.api_key_id,c.cost,c.timestamp,c.history_id
    FROM captured c JOIN written w ON w.id=c.history_id`,
    [
      JSON.stringify(
        entries.map((entry) => ({
          receipt_id: entry.receiptId,
          api_key_id: entry.apiKeyId,
          cost: entry.cost,
          timestamp: entry.timestamp,
        }))
      ),
    ]
  );
}

export interface SpendWindow {
  since: number;
  until: number;
}
export interface AcceptedSpendSnapshot {
  totals: Array<{ total: number; count: number }>;
  confirmedReceiptIds: string[];
}

/**
 * Called under domain lock 8. Capture pending events before the query. Its single
 * statement snapshots history, receipts and all requested windows; aggregates stay
 * on the database rather than copying an entire month into the request event loop.
 */
export async function readAcceptedSpendSnapshot(
  tx: PostgresTransaction,
  id: string,
  windows: readonly SpendWindow[],
  pending: readonly BufferedCostEntry[]
): Promise<AcceptedSpendSnapshot> {
  if (
    !windows.length ||
    windows.length > 32 ||
    windows.some(
      (w) => !Number.isSafeInteger(w.since) || !Number.isSafeInteger(w.until) || w.until < w.since
    )
  )
    throw new TypeError("Invalid spend windows");
  const mirror = pending.map((entry) => {
    if (
      entry.apiKeyId !== id ||
      !entry.receiptId ||
      !Number.isFinite(entry.cost) ||
      !Number.isSafeInteger(entry.timestamp)
    )
      throw new TypeError("Missing or invalid pending spend identity");
    return {
      receipt_id: receiptId(entry.receiptId),
      api_key_id: entry.apiKeyId,
      cost: entry.cost,
      timestamp: entry.timestamp,
    };
  });
  if (new Set(mirror.map((entry) => entry.receipt_id)).size !== mirror.length)
    throw new PostgresSpendReceiptError();
  const rows = (
    await tx.query<{
      kind: "total" | "confirmed" | "conflict";
      window_no: number | null;
      total: number | null;
      count: string | null;
      receipt_id: string | null;
    }>(
      `WITH pending AS MATERIALIZED (
      SELECT * FROM jsonb_to_recordset($4::jsonb)
      AS events(receipt_id uuid,api_key_id text,cost double precision,timestamp bigint)
    ), window_bounds AS MATERIALIZED (
      SELECT * FROM jsonb_to_recordset($5::jsonb) AS ranges(window_no integer,start_at bigint,end_at bigint)
    ), confirmed AS MATERIALIZED (
      SELECT p.receipt_id FROM pending p JOIN omniroute.domain_cost_receipts r USING(receipt_id)
      WHERE r.api_key_id=p.api_key_id AND r.cost=p.cost AND r.timestamp=p.timestamp
    ), accepted AS MATERIALIZED (
      SELECT cost,timestamp FROM omniroute.domain_cost_history
      WHERE api_key_id=$1 AND timestamp >= $2 AND timestamp < $3
      UNION ALL
      SELECT p.cost,p.timestamp FROM pending p WHERE p.api_key_id=$1 AND p.timestamp >= $2 AND p.timestamp < $3
        AND NOT EXISTS(SELECT 1 FROM confirmed c WHERE c.receipt_id=p.receipt_id)
    )
    SELECT 'total' AS kind,w.window_no,COALESCE(SUM(a.cost),0) AS total,COUNT(a.timestamp) AS count,NULL::uuid AS receipt_id
    FROM window_bounds w LEFT JOIN accepted a ON a.timestamp >= w.start_at AND a.timestamp < w.end_at GROUP BY w.window_no
    UNION ALL
    SELECT 'confirmed',NULL::integer,NULL::double precision,NULL::bigint,receipt_id FROM confirmed
    UNION ALL
    SELECT 'conflict',NULL::integer,NULL::double precision,NULL::bigint,p.receipt_id
    FROM pending p JOIN omniroute.domain_cost_receipts r USING(receipt_id)
    WHERE r.api_key_id IS DISTINCT FROM p.api_key_id OR r.cost IS DISTINCT FROM p.cost OR r.timestamp IS DISTINCT FROM p.timestamp`,
      [
        id,
        Math.min(...windows.map((w) => w.since)),
        Math.max(...windows.map((w) => w.until)),
        JSON.stringify(mirror),
        JSON.stringify(
          windows.map((w, index) => ({ window_no: index, start_at: w.since, end_at: w.until }))
        ),
      ]
    )
  ).rows;
  if (rows.some((row) => row.kind === "conflict")) throw new PostgresSpendReceiptError();
  const totals = rows
    .filter((row) => row.kind === "total")
    .sort((a, b) => a.window_no! - b.window_no!)
    .map((row) => ({ total: Number(row.total), count: Number(row.count) }));
  if (
    totals.length !== windows.length ||
    totals.some((row) => !Number.isFinite(row.total) || !Number.isSafeInteger(row.count))
  )
    throw new RangeError("Spend aggregate exceeds the supported numeric range");
  return {
    totals,
    confirmedReceiptIds: rows
      .filter((row) => row.kind === "confirmed")
      .map((row) => row.receipt_id!),
  };
}
