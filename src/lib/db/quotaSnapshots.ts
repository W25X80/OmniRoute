import { getDbInstance, rowToCamel } from "./core";
import type { QuotaSnapshotRow, ProviderUtilizationPoint } from "@/shared/types/utilization";
import {
  hasCodexScopeCooldown,
  liftCodexScopeCooldownOnHeadroom,
} from "./providers/codexAccountState";
import { isCodexSparkQuotaKey } from "@omniroute/open-sse/config/codexQuotaScopes";
import { getSelectedPostgresDatabase, getSelectedPersistenceIdentity } from "./controlPlaneContext";

type JsonRecord = Record<string, unknown>;

interface StatementLike<TRow = unknown> {
  all: (...params: unknown[]) => TRow[];
  get: (...params: unknown[]) => TRow | undefined;
  run: (...params: unknown[]) => { changes: number };
}

interface DbLike {
  prepare: <TRow = unknown>(sql: string) => StatementLike<TRow>;
}

let lastCleanupAt = 0;

export function saveQuotaSnapshot(snapshot: Omit<QuotaSnapshotRow, "id" | "created_at">): void {
  const db = getDbInstance() as unknown as DbLike;
  const now = new Date().toISOString();

  try {
    db.prepare(
      `INSERT INTO quota_snapshots
       (provider, connection_id, window_key, remaining_percentage, is_exhausted,
        next_reset_at, window_duration_ms, raw_data, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      snapshot.provider,
      snapshot.connection_id,
      snapshot.window_key,
      snapshot.remaining_percentage,
      snapshot.is_exhausted,
      snapshot.next_reset_at,
      snapshot.window_duration_ms,
      snapshot.raw_data,
      now
    );
  } catch (err: any) {
    if (err?.message?.includes("no such table")) {
      console.warn(
        "[QuotaSnapshots] Skipping save: quota_snapshots table not found. Awaiting migration."
      );
      return;
    }
    throw err;
  }

  // #12860: When a new snapshot demonstrates headroom on a Codex connection,
  // lift any fallback-sourced scope cooldown parked by quota preflight.
  maybeLiftCodexCooldownOnHeadroom(snapshot);
}

/**
 * `rowToCamel` leaves snake_case keys intact on some driver paths, so accept
 * either casing rather than trusting one shape.
 */
type SnapshotShape = Partial<QuotaSnapshotRow> & {
  windowKey?: string;
  remainingPercentage?: number;
  isExhausted?: number;
};

function snapshotHasHeadroom(s: SnapshotShape): boolean {
  const pct = s.remainingPercentage ?? s.remaining_percentage ?? 0;
  const exhausted = s.isExhausted ?? s.is_exhausted ?? 0;
  return pct > 0 && exhausted !== 1;
}

function maybeLiftCodexCooldownOnHeadroom(
  snapshot: Omit<QuotaSnapshotRow, "id" | "created_at">
): void {
  if (
    snapshot.provider?.toLowerCase() !== "codex" ||
    typeof snapshot.connection_id !== "string" ||
    snapshot.connection_id.length === 0 ||
    (snapshot.remaining_percentage ?? 0) <= 0 ||
    snapshot.is_exhausted === 1
  ) {
    return;
  }

  try {
    const scope = isCodexSparkQuotaKey(snapshot.window_key) ? "spark" : "codex";
    // The scope-wide read is only worth paying for when a cooldown is
    // actually parked on this connection+scope; the common case is clean.
    if (!hasCodexScopeCooldown(snapshot.connection_id, scope)) return;

    const scopeWindows = getLatestQuotaSnapshotsForConnection(snapshot.connection_id).filter(
      (s: SnapshotShape) => {
        const key = s.windowKey ?? s.window_key;
        return scope === "spark" ? isCodexSparkQuotaKey(key) : !isCodexSparkQuotaKey(key);
      }
    );
    // Every window in the scope must be healthy: one exhausted window still
    // justifies the cooldown even when a sibling reports full headroom.
    if (scopeWindows.length > 0 && scopeWindows.every(snapshotHasHeadroom)) {
      liftCodexScopeCooldownOnHeadroom(snapshot.connection_id, scope);
    }
  } catch (err) {
    console.debug("[QuotaSnapshots] Headroom evaluation skipped:", err);
  }
}

export function getQuotaSnapshots(opts: {
  provider?: string;
  connectionId?: string;
  since: string;
  until?: string;
}): QuotaSnapshotRow[] {
  const db = getDbInstance() as unknown as DbLike;
  const conditions: string[] = ["created_at >= ?"];
  const params: unknown[] = [opts.since];

  if (opts.provider) {
    conditions.push("provider = ?");
    params.push(opts.provider);
  }

  if (opts.connectionId) {
    conditions.push("connection_id = ?");
    params.push(opts.connectionId);
  }

  if (opts.until) {
    conditions.push("created_at <= ?");
    params.push(opts.until);
  }

  try {
    const sql = `SELECT * FROM quota_snapshots WHERE ${conditions.join(" AND ")} ORDER BY created_at ASC`;
    const rows = db.prepare(sql).all(...params);
    return rows.map((r) => rowToCamel(r) as unknown as QuotaSnapshotRow);
  } catch (err: any) {
    if (err?.message?.includes("no such table")) {
      return [];
    }
    throw err;
  }
}

/**
 * Returns the single latest snapshot row for each distinct `window_key`
 * ever observed for this connection.
 *
 * Deliberately NOT a "most recent N rows across all windows" query: a
 * connection with many quota windows where only a subset actively churn
 * (frequent writes as they drain) and the rest stay idle/healthy (a single
 * old row each, thanks to the #4438 no-op-write dedup) would otherwise have
 * its recent-rows slice flooded entirely by the hot windows, silently
 * evicting the idle windows from rehydration (#8431). Scoping "latest" PER
 * window_key via a window function keeps every window visible regardless of
 * how skewed the write frequency is across windows.
 */
export function getLatestQuotaSnapshotsForConnection(connectionId: string): QuotaSnapshotRow[] {
  const db = getDbInstance() as unknown as DbLike;

  try {
    const rows = db
      .prepare(
        `SELECT * FROM (
           SELECT *, ROW_NUMBER() OVER (
             PARTITION BY window_key ORDER BY created_at DESC, id DESC
           ) AS rn
           FROM quota_snapshots
           WHERE connection_id = ?
         )
         WHERE rn = 1`
      )
      .all(connectionId);

    return rows.map((row) => rowToCamel(row) as unknown as QuotaSnapshotRow);
  } catch (err: any) {
    if (err?.message?.includes("no such table")) {
      return [];
    }
    throw err;
  }
}

export function getAggregatedSnapshots(opts: {
  provider?: string;
  since: string;
  until?: string;
  bucketMinutes: number;
  aggregateBy?: "provider" | "connection";
}): ProviderUtilizationPoint[] {
  const db = getDbInstance() as unknown as DbLike;
  const conditions: string[] = ["created_at >= ?"];
  const params: unknown[] = [opts.since];

  if (opts.provider) {
    conditions.push("provider = ?");
    params.push(opts.provider);
  }

  if (opts.until) {
    conditions.push("created_at <= ?");
    params.push(opts.until);
  }

  const bucketSeconds = Number(opts.bucketMinutes) * 60;
  if (!Number.isFinite(bucketSeconds) || bucketSeconds <= 0) {
    throw new Error("Invalid bucket size");
  }

  const groupFields =
    opts.aggregateBy === "connection"
      ? "bucket, provider, connection_id, window_key"
      : "bucket, provider, window_key";
  const selectKey =
    opts.aggregateBy === "connection" ? "provider || ':' || connection_id as provider" : "provider";

  try {
    const sql = `
      SELECT
        datetime((strftime('%s', created_at) / ${bucketSeconds}) * ${bucketSeconds}, 'unixepoch') as bucket,
        ${selectKey},
        AVG(remaining_percentage) as remainingPct,
        MAX(is_exhausted) as isExhausted,
        window_key
      FROM quota_snapshots
      WHERE ${conditions.join(" AND ")}
      GROUP BY ${groupFields}
      ORDER BY bucket ASC
    `;

    const rows = db.prepare(sql).all(...params) as Array<{
      bucket: string;
      provider: string;
      remainingPct: number | null;
      isExhausted: number;
      windowKey: string;
    }>;

    return rows.map((r) => ({
      timestamp: r.bucket,
      provider: r.provider,
      remainingPct: r.remainingPct ?? 0,
      isExhausted: r.isExhausted === 1,
      windowKey: r.windowKey,
    }));
  } catch (err: any) {
    if (err?.message?.includes("no such table")) {
      return [];
    }
    throw err;
  }
}

export function cleanupOldSnapshots(retentionDays = 90): number {
  const now = Date.now();
  const cleanupThresholdMs = 6 * 60 * 60 * 1000;

  if (now - lastCleanupAt < cleanupThresholdMs) {
    return 0;
  }

  const db = getDbInstance() as unknown as DbLike;
  const cutoffDate = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000).toISOString();

  try {
    const result = db.prepare("DELETE FROM quota_snapshots WHERE created_at < ?").run(cutoffDate);
    lastCleanupAt = now;
    return result.changes;
  } catch (err: any) {
    if (err?.message?.includes("no such table")) {
      return 0;
    }
    throw err;
  }
}

function nativeSnapshot(row: Record<string, unknown>): QuotaSnapshotRow {
  // Match the public facade's historical rowToCamel output, including bigint conversion.
  const values = {
    ...row,
    id: Number(row.id),
    is_exhausted: Number(row.is_exhausted),
    remaining_percentage:
      row.remaining_percentage === null ? null : Number(row.remaining_percentage),
    window_duration_ms: row.window_duration_ms === null ? null : Number(row.window_duration_ms),
  };
  return rowToCamel(values) as unknown as QuotaSnapshotRow;
}

export async function saveQuotaSnapshotAsync(
  snapshot: Omit<QuotaSnapshotRow, "id" | "created_at">
): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return saveQuotaSnapshot(snapshot);
  await database.transaction(async (tx) => {
    await tx.query(
      `INSERT INTO omniroute.quota_snapshots
    (provider,connection_id,window_key,remaining_percentage,is_exhausted,next_reset_at,window_duration_ms,raw_data,created_at)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [
        snapshot.provider,
        snapshot.connection_id,
        snapshot.window_key,
        snapshot.remaining_percentage,
        snapshot.is_exhausted,
        snapshot.next_reset_at,
        snapshot.window_duration_ms,
        snapshot.raw_data,
        new Date().toISOString(),
      ]
    );
  });
  await maybeLiftCodexCooldownOnHeadroomAsync(snapshot);
}

async function maybeLiftCodexCooldownOnHeadroomAsync(
  snapshot: Omit<QuotaSnapshotRow, "id" | "created_at">
): Promise<void> {
  if (
    snapshot.provider?.toLowerCase() !== "codex" ||
    !snapshot.connection_id ||
    (snapshot.remaining_percentage ?? 0) <= 0 ||
    snapshot.is_exhausted === 1
  )
    return;
  try {
    const scope = isCodexSparkQuotaKey(snapshot.window_key) ? "spark" : "codex";
    const codex = await import("./providers/codexAccountState");
    if (!(await codex.hasCodexScopeCooldownAsync(snapshot.connection_id, scope))) return;
    const windows = (
      await getLatestQuotaSnapshotsForConnectionAsync(snapshot.connection_id)
    ).filter((s: SnapshotShape) => {
      const key = s.windowKey ?? s.window_key;
      return scope === "spark" ? isCodexSparkQuotaKey(key) : !isCodexSparkQuotaKey(key);
    });
    if (windows.length && windows.every(snapshotHasHeadroom))
      await codex.liftCodexScopeCooldownOnHeadroomAsync(snapshot.connection_id, scope);
  } catch (error) {
    console.debug("[QuotaSnapshots] Headroom evaluation skipped:", error);
  }
}

export async function getQuotaSnapshotsAsync(
  opts: Parameters<typeof getQuotaSnapshots>[0]
): Promise<QuotaSnapshotRow[]> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getQuotaSnapshots(opts);
  const params: unknown[] = [opts.since],
    where = ["created_at >= $1"];
  if (opts.provider) {
    params.push(opts.provider);
    where.push(`provider=$${params.length}`);
  }
  if (opts.connectionId) {
    params.push(opts.connectionId);
    where.push(`connection_id=$${params.length}`);
  }
  if (opts.until) {
    params.push(opts.until);
    where.push(`created_at <= $${params.length}`);
  }
  return database.transaction(async (tx) =>
    (
      await tx.query(
        `SELECT * FROM omniroute.quota_snapshots WHERE ${where.join(" AND ")}
    ORDER BY created_at ASC,id ASC`,
        params
      )
    ).rows.map(nativeSnapshot)
  );
}

export async function getLatestQuotaSnapshotsForConnectionAsync(
  id: string
): Promise<QuotaSnapshotRow[]> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getLatestQuotaSnapshotsForConnection(id);
  return database.transaction(async (tx) =>
    (
      await tx.query(
        `SELECT * FROM (
    SELECT *,ROW_NUMBER() OVER(PARTITION BY window_key ORDER BY created_at DESC,id DESC) AS rn
    FROM omniroute.quota_snapshots WHERE connection_id=$1) snapshots WHERE rn=1`,
        [id]
      )
    ).rows.map(nativeSnapshot)
  );
}

export async function getAggregatedSnapshotsAsync(
  opts: Parameters<typeof getAggregatedSnapshots>[0]
): Promise<ProviderUtilizationPoint[]> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getAggregatedSnapshots(opts);
  const seconds = Number(opts.bucketMinutes) * 60;
  if (!Number.isFinite(seconds) || seconds <= 0) throw new Error("Invalid bucket size");
  const params: unknown[] = [seconds, opts.since],
    where = ["created_at >= $2"];
  if (opts.provider) {
    params.push(opts.provider);
    where.push(`provider=$${params.length}`);
  }
  if (opts.until) {
    params.push(opts.until);
    where.push(`created_at <= $${params.length}`);
  }
  const connection = opts.aggregateBy === "connection";
  return database.transaction(async (tx) => {
    const rows = (
      await tx.query(
        `SELECT to_char(to_timestamp(FLOOR(EXTRACT(EPOCH FROM created_at::timestamptz)/$1)*$1)
      AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') AS bucket,
      ${connection ? "provider || ':' || connection_id" : "provider"} AS provider,
      AVG(remaining_percentage) AS remaining_pct,MAX(is_exhausted) AS exhausted,window_key
      FROM omniroute.quota_snapshots WHERE ${where.join(" AND ")}
      GROUP BY bucket,${connection ? "provider,connection_id" : "provider"},window_key ORDER BY bucket ASC`,
        params
      )
    ).rows;
    return rows.map((r) => ({
      timestamp: String(r.bucket),
      provider: String(r.provider),
      remainingPct: Number(r.remaining_pct ?? 0),
      isExhausted: Number(r.exhausted) === 1,
      windowKey: String(r.window_key),
    }));
  });
}

const nativeSnapshotCleanup = new WeakMap<object, { last: number; running?: Promise<number> }>();
export async function cleanupOldSnapshotsAsync(retentionDays = 90): Promise<number> {
  const database = getSelectedPostgresDatabase();
  if (!database) return cleanupOldSnapshots(retentionDays);
  const identity = getSelectedPersistenceIdentity()!;
  let state = nativeSnapshotCleanup.get(identity);
  if (!state) {
    state = { last: 0 };
    nativeSnapshotCleanup.set(identity, state);
  }
  const now = Date.now();
  if (now - state.last < 6 * 60 * 60 * 1000) return 0;
  if (state.running) return state.running;
  const current = state;
  const cutoff = new Date(now - retentionDays * 86_400_000).toISOString();
  current.running = database
    .transaction(
      async (tx) =>
        (await tx.query("DELETE FROM omniroute.quota_snapshots WHERE created_at < $1", [cutoff]))
          .rowCount || 0
    )
    .then((count) => {
      current.last = now;
      return count;
    })
    .finally(() => {
      current.running = undefined;
    });
  return current.running;
}

export async function getAllLatestQuotaSnapshotsAsync(): Promise<QuotaSnapshotRow[]> {
  const database = getSelectedPostgresDatabase();
  if (!database) {
    const rows = getDbInstance()
      .prepare(
        `SELECT * FROM(SELECT *,ROW_NUMBER() OVER(PARTITION BY connection_id,window_key ORDER BY created_at DESC,id DESC) rn FROM quota_snapshots) WHERE rn=1`
      )
      .all();
    return rows.map((row) => rowToCamel(row) as unknown as QuotaSnapshotRow);
  }
  return database.transaction(async (tx) =>
    (
      await tx.query(`SELECT * FROM(SELECT *,ROW_NUMBER() OVER(
    PARTITION BY connection_id,window_key ORDER BY created_at DESC,id DESC) AS rn FROM omniroute.quota_snapshots)
    snapshots WHERE rn=1`)
    ).rows.map(nativeSnapshot)
  );
}
