/**
 * db/proxyLogs.ts — Read queries over the `proxy_logs` table.
 * Extracted from the /api/logs/export route handler.
 *
 * Hard Rule #5: routes must not embed raw SQL — these queries live here so the
 * /api/logs/export route can delegate.
 *
 * NOTE: The SELECT * intentionally returns the historical `public_ip` column,
 * NOT `clientIp`. This differs from GET /api/usage/proxy-logs which exposes
 * the value as `clientIp`. Callers of the export endpoint should read
 * `public_ip`. This inconsistency will be resolved in a future DB migration
 * (#2880).
 *
 * Sliced out of #3500 (proxy_logs cluster, slice 4).
 */

import type { PostgresDatabase } from "./database";
import { queryUsageRows, queryNamedUsageRows } from "./usageQueries";
import { normalizeProxyHostForLog } from "../../proxyLogHost";
import { sanitizeTimingMs } from "@omniroute/open-sse/utils/timingMs.ts";

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/**
 * Returns all proxy_logs rows with timestamp >= `since`, ordered by timestamp
 * descending (most recent first).
 *
 * @param since - ISO-8601 timestamp lower bound, e.g. "2024-01-01T00:00:00.000Z".
 */
export async function exportProxyLogsSince(
  database: PostgresDatabase,
  since: string
): Promise<Record<string, unknown>[]> {
  return (await queryNamedUsageRows(
    database,
    "SELECT * FROM omniroute.proxy_logs WHERE timestamp >= @since ORDER BY timestamp DESC,id DESC",
    { since }
  )) as Record<string, unknown>[];
}

/**
 * Total number of proxy_logs rows with timestamp >= `since` — a cheap
 * aggregate query that never materializes the matching rows themselves.
 * Used by /api/logs/export to report `totalAvailable` without paying the
 * cost of fetching every row just to count them (#13123).
 */
export async function countProxyLogsSince(
  database: PostgresDatabase,
  since: string
): Promise<number> {
  const row = (
    await queryNamedUsageRows(
      database,
      "SELECT COUNT(*) AS count FROM omniroute.proxy_logs WHERE timestamp >= @since",
      { since },
      [
        "n",
        "count",
        "proxy_port",
        "latency_ms",
        "upstream_status",
        "tls_fingerprint",
        "headers_ms",
        "first_chunk_ms",
        "attempt_number",
      ]
    )
  )[0] as { count: number };
  return row.count;
}

const PAGE_SIZE = 500;

/**
 * Streams proxy_logs rows with timestamp >= `since`, up to `limit` rows,
 * ordered by timestamp descending — paginated via SQL LIMIT/OFFSET in fixed
 * batches, never buffering more than `PAGE_SIZE` rows at once (#13123: the
 * previous `exportProxyLogsSince()` + slice-after-fetch approach still
 * materialized every matching row before the row cap was even applied). Note:
 * the `SqliteAdapter` (`./adapters/types.ts`) intentionally exposes only
 * `run`/`get`/`all` — no `.iterate()` cursor — so LIMIT/OFFSET batching is
 * the cursor-equivalent available without widening that shared interface
 * across all 4 driver adapters.
 * This is a live, best-effort export, not a transactionally consistent snapshot:
 * concurrent inserts/deletes can shift OFFSET pages and repeat or omit rows.
 * The preflight COUNT is an estimate; the route reports its emitted count last.
 */
export async function* iterateProxyLogsSince(
  database: PostgresDatabase,
  since: string,
  limit: number
): AsyncGenerator<Record<string, unknown>, void, void> {
  let offset = 0;
  let yielded = 0;
  while (yielded < limit) {
    const pageLimit = Math.min(PAGE_SIZE, limit - yielded);
    const page = (await queryNamedUsageRows(
      database,
      "SELECT * FROM omniroute.proxy_logs WHERE timestamp >= @since ORDER BY timestamp DESC,id DESC LIMIT @pageLimit OFFSET @offset",
      { since, pageLimit, offset }
    )) as Record<string, unknown>[];
    if (page.length === 0) break;
    for (const row of page) {
      yield row;
      yielded++;
    }
    offset += page.length;
    if (page.length < pageLimit) break;
  }
}

// 24h window for "last known egress IP" lookups. This helper answers a
// different question from proxyEgress.ts (#10677): that module reports which
// connections share an egress IP *right now*, derived from their proxy config
// and a live probe (5 min cache), while the lock needs the IP a connection
// actually *left through* on its recent traffic — history, which only
// proxy_logs holds. Hence a local window constant rather than a dependency.
// Exported so callers can build `since` without duplicating the window.
export const EGRESS_IP_LOOKUP_WINDOW_MS = 24 * 60 * 60 * 1000;

/**
 * Last non-null egress IP observed for a connection within the window, or
 * null. Best-effort by design: egress_ip is only populated once the egress IP
 * has been probed (cache TTL 5 min), so a cold cache yields null and the
 * caller must fall back to today's behavior. Synchronous read (#10539 — no
 * in-memory cache to go stale). The table has no index on connection_id
 * (migration 134, YAGNI); the scan is bounded by the window via
 * idx_pl_timestamp and this helper only runs at 429 frequency.
 */
export async function getRecentEgressIpForConnection(
  database: PostgresDatabase,
  connectionId: string,
  since: string
): Promise<{ egressIp: string; at: string } | null> {
  const row = (
    await queryUsageRows(
      database,
      `SELECT egress_ip, timestamp FROM omniroute.proxy_logs
       WHERE connection_id = $1 AND egress_ip IS NOT NULL AND timestamp >= $2
       ORDER BY timestamp DESC LIMIT 1`,
      [connectionId, since],
      [
        "n",
        "count",
        "proxy_port",
        "latency_ms",
        "upstream_status",
        "tls_fingerprint",
        "headers_ms",
        "first_chunk_ms",
        "attempt_number",
      ]
    )
  )[0] as { egress_ip: string; timestamp: string } | undefined;
  if (!row) return null;
  return { egressIp: row.egress_ip, at: row.timestamp };
}

/** Native sibling lookup; the family comes from the same policy allowlist as the auth gate. */
export async function getEgressSiblingConnectionIds(
  database: PostgresDatabase,
  egressIp: string,
  since: string,
  excludedConnectionId: string,
  providers: readonly string[]
): Promise<string[]> {
  const rows = await queryUsageRows<{ connection_id: string }>(
    database,
    `SELECT DISTINCT connection_id FROM omniroute.proxy_logs
     WHERE egress_ip = $1 AND timestamp >= $2 AND connection_id != $3
     AND provider = ANY($4::text[])`,
    [egressIp, since, excludedConnectionId, [...providers]]
  );
  return rows.map((row) => row.connection_id);
}

/**
 * Normalizes a proxy host lookup key: trim, strip one bracket pair, lowercase.
 * Anything that is not a non-empty string normalizes to "" so callers never
 * throw on missing or mistyped input — the reader below maps "" to null.
 */
function normalizeProxyHostKey(host: unknown): string {
  return normalizeProxyHostForLog(host) ?? "";
}

/**
 * Last non-null egress IP observed through a proxy endpoint within the
 * window, keyed by (host, port), or null. Best-effort by design: egress_ip is
 * only populated once the egress IP has been probed (cache TTL 5 min), so a
 * cold cache yields null and the caller must fall back to today's behavior.
 * Synchronous read (no in-memory cache to go stale). The table has no index
 * on (proxy_host, proxy_port) (migration 134, YAGNI); the scan is bounded by
 * the window via idx_pl_timestamp, same as getRecentEgressIpForConnection.
 * The host key is normalized (trim, strip one bracket pair, lowercase);
 * IPv4-mapped IPv6 forms are intentionally not unmapped. The port must be an
 * integer in 1-65535 — anything else returns null instead of throwing.
 */
export async function getRecentEgressIpForProxy(
  database: PostgresDatabase,
  host: string,
  port: number
): Promise<{ egressIp: string; at: string } | null> {
  const h = normalizeProxyHostKey(host);
  if (!h) return null;
  if (!Number.isInteger(port) || port < 1 || port > 65535) return null;

  const since = new Date(Date.now() - EGRESS_IP_LOOKUP_WINDOW_MS).toISOString();
  const row = (
    await queryUsageRows(
      database,
      `SELECT egress_ip, timestamp FROM omniroute.proxy_logs
       WHERE proxy_host = $1 AND proxy_port = $2
         AND egress_ip IS NOT NULL AND timestamp >= $3
       ORDER BY timestamp DESC LIMIT 1`,
      [h, port, since],
      [
        "n",
        "count",
        "proxy_port",
        "latency_ms",
        "upstream_status",
        "tls_fingerprint",
        "headers_ms",
        "first_chunk_ms",
        "attempt_number",
      ]
    )
  )[0] as { egress_ip: string; timestamp: string } | undefined;
  if (!row) return null;
  return { egressIp: row.egress_ip, at: row.timestamp };
}

/**
 * Distinct non-null egress IPs observed through a proxy endpoint since
 * `sinceIso` (up to `limit`). Same host-key normalization and port validation
 * as `getRecentEgressIpForProxy`: anything unusable yields `[]`, never a throw.
 */
export async function getRecentEgressIpsForProxy(
  database: PostgresDatabase,
  host: string,
  port: number,
  sinceIso: string,
  limit = 3
): Promise<string[]> {
  const h = normalizeProxyHostKey(host);
  if (!h) return [];
  if (!Number.isInteger(port) || port < 1 || port > 65535) return [];
  if (typeof sinceIso !== "string" || !sinceIso) return [];
  const capped = Number.isInteger(limit) && limit > 0 ? Math.min(limit, 10) : 3;

  const rows = (await queryUsageRows(
    database,
    `SELECT DISTINCT egress_ip FROM omniroute.proxy_logs
       WHERE proxy_host = $1 AND proxy_port = $2
         AND egress_ip IS NOT NULL AND timestamp >= $3
       LIMIT $4`,
    [h, port, sinceIso, capped],
    [
      "n",
      "count",
      "proxy_port",
      "latency_ms",
      "upstream_status",
      "tls_fingerprint",
      "headers_ms",
      "first_chunk_ms",
      "attempt_number",
    ]
  )) as Array<{ egress_ip: string }>;
  return rows.map((r) => r.egress_ip).filter((ip) => typeof ip === "string" && ip);
}

export type PoolEgressObservationCounts = {
  connections: number;
  distinctExits: number;
  maxConnectionsOnOneExit: number;
};

export type PoolEgressFailureFamily = {
  family: string;
  count: number;
};

export type PoolEgressFailureExit = {
  exit: string;
  failures: number;
  byFamily: PoolEgressFailureFamily[];
};

export type PoolEgressFailureBreakdown = {
  byExit: PoolEgressFailureExit[];
  byFamily: PoolEgressFailureFamily[];
  unattributed: number;
  attributionNote: string;
};

export type PoolEgressObservation = PoolEgressObservationCounts & {
  failures: PoolEgressFailureBreakdown;
};

// Per-family breakdowns only cover requests whose per-family attribution was
// logged: `proxy_logs.correlation_id` is NULL for legacy rows and whenever the
// attribution flag is off (`isRotationAttributionEnabled()` gates the proxy
// side), while `call_logs.correlation_id` is written by `saveCallLog` with
// no gate. Unmatchable rows land in the explicit `unattributed` bucket.
export const POOL_EGRESS_ATTRIBUTION_NOTE =
  "per-family breakdown covers only requests logged with attribution on";

/**
 * Per-exit failure breakdown for a proxy pool's members since `since`: one row
 * per failed proxied request (`status != 'success'`, i.e. `error` and
 * `timeout`), grouped by egress IP and by call-log error family. Families come
 * from `call_logs.error_type` (persisted vocabulary + `unknown`; NULL means
 * "legacy row / not a failure") joined on `correlation_id`; rows with no
 * correlation or no match land in `unattributed`, never in a family.
 *
 * Same member join and same `egress_ip IS NOT NULL AND connection_id IS NOT
 * NULL` filters as the `connections` counters above, so failure totals stay
 * reconcilable with them (no host:port fallback; note `failures` counts rows
 * via `COUNT(*)` while `connections` counts `DISTINCT connection_id`, so the
 * two are comparable but not equatable). A retry can share one
 * correlation_id across several call_logs rows (precedent:
 * agenticConversations.ts keeps the first match), so a correlated scalar
 * sub-query reads `error_type` from the first row by `rowid`, one index seek
 * per failed request on `idx_cl_correlation_id` instead of a pass over the
 * whole call_logs table. The family is computed in a derived table and grouped
 * outside it: grouping on the scalar sub-query's alias in the same SELECT
 * merges distinct families on SQLite. 0 new indexes: the window rides
 * `idx_pl_timestamp`.
 * `scope`/`scopeId` must already be normalized; an empty pool yields empties.
 */
export async function getPoolEgressFailureBreakdown(
  database: PostgresDatabase,
  scope: string,
  scopeId: string | null,
  since: string
): Promise<PoolEgressFailureBreakdown> {
  const rows = (await queryUsageRows(
    database,
    `SELECT exit, family, COUNT(*) AS n
       FROM (
         SELECT l.egress_ip AS exit,
                (SELECT c.error_type FROM omniroute.call_logs c
                 WHERE c.correlation_id = l.correlation_id
                 ORDER BY c.insertion_order LIMIT 1) AS family
         FROM omniroute.proxy_logs l
         JOIN omniroute.proxy_registry r ON l.proxy_host = r.host AND l.proxy_port = r.port
         WHERE r.id IN (SELECT proxy_id FROM omniroute.proxy_assignments WHERE scope = $1 AND scope_id IS NOT DISTINCT FROM $2)
           AND l.timestamp >= $3 AND l.status != 'success'
           AND l.egress_ip IS NOT NULL AND l.connection_id IS NOT NULL
       )
       GROUP BY exit, family
       ORDER BY exit ASC`,
    [scope, scopeId, since],
    [
      "n",
      "count",
      "proxy_port",
      "latency_ms",
      "upstream_status",
      "tls_fingerprint",
      "headers_ms",
      "first_chunk_ms",
      "attempt_number",
    ]
  )) as Array<{ exit: string; family: string | null; n: number }>;
  const byExitMap = new Map<string, Map<string, number>>();
  const byFamilyMap = new Map<string, number>();
  let unattributed = 0;
  for (const row of rows) {
    const family = row.family ?? "unattributed";
    let exitFamilies = byExitMap.get(row.exit);
    if (!exitFamilies) {
      exitFamilies = new Map<string, number>();
      byExitMap.set(row.exit, exitFamilies);
    }
    exitFamilies.set(family, (exitFamilies.get(family) ?? 0) + row.n);
    byFamilyMap.set(family, (byFamilyMap.get(family) ?? 0) + row.n);
    if (family === "unattributed") unattributed += row.n;
  }
  const sortFamilies = (entries: Array<[string, number]>): PoolEgressFailureFamily[] =>
    entries
      .map(([family, count]) => ({ family, count }))
      .sort((a, b) => b.count - a.count || (a.family < b.family ? -1 : 1));
  return {
    byExit: [...byExitMap.entries()].map(([exit, families]) => ({
      exit,
      failures: [...families.values()].reduce((sum, n) => sum + n, 0),
      byFamily: sortFamilies([...families.entries()]),
    })),
    byFamily: sortFamilies([...byFamilyMap.entries()]),
    unattributed,
    attributionNote: POOL_EGRESS_ATTRIBUTION_NOTE,
  };
}

/**
 * Deferred per-attempt upstream timing patch. Rows are inserted with the
 * first-chunk duration unknown on slow streams (NULL); the capture layer
 * patches it once the first useful body byte arrives. Only non-negative
 * integers are written - anything else is rejected (returns false) so
 * partially migrated databases and clock skew never corrupt the row.
 * Returns true when exactly one row was patched.
 */
export async function updateAttemptTiming(
  database: PostgresDatabase,
  id: string,
  patch: { headersMs?: number | null; firstChunkMs?: number | null }
): Promise<boolean> {
  if (typeof id !== "string" || !id) return false;
  const clean = {
    headersMs: sanitizeTimingValue(patch.headersMs),
    firstChunkMs: sanitizeTimingValue(patch.firstChunkMs),
  };
  if (clean.headersMs === undefined && clean.firstChunkMs === undefined) return false;

  const sets: string[] = [];
  const params: Record<string, unknown> = { id };
  if (clean.headersMs !== undefined) {
    sets.push("headers_ms = @headersMs");
    params.headersMs = clean.headersMs;
  }
  if (clean.firstChunkMs !== undefined) {
    sets.push("first_chunk_ms = @firstChunkMs");
    params.firstChunkMs = clean.firstChunkMs;
  }
  const bound = await database.transaction(async (transaction) => {
    const { bindUsageParameters } = await import("./usageQueries");
    const query = bindUsageParameters(
      `UPDATE omniroute.proxy_logs SET ${sets.join(", ")} WHERE id = @id`,
      params
    );
    return transaction.query(query.text, query.values);
  });
  const result = { changes: bound.rowCount };
  return Number(result.changes) === 1;
}

// Undefined = column left untouched; an invalid value is never written.
function sanitizeTimingValue(value: number | null | undefined): number | undefined {
  return value === undefined ? undefined : (sanitizeTimingMs(value) ?? undefined);
}

/**
 * How many distinct observed egress IPs served a proxy pool's members since `since`, how
 * many OmniRoute connections went through them, and the most connections seen behind one
 * egress IP over that window. Members are matched to log rows by host and port, so two
 * registry rows sharing one entry point count together. Only numbers leave this function.
 * `scope` and `scopeId` must already be normalized (normalizeScope and
 * normalizeAssignmentScopeId); an empty pool simply matches no rows.
 */
export async function getPoolEgressObservation(
  database: PostgresDatabase,
  scope: string,
  scopeId: string | null,
  since: string
): Promise<PoolEgressObservation> {
  const perExit = (await queryUsageRows(
    database,
    `SELECT COUNT(DISTINCT l.connection_id) AS n
       FROM omniroute.proxy_logs l
       JOIN omniroute.proxy_registry r ON l.proxy_host = r.host AND l.proxy_port = r.port
       WHERE r.id IN (SELECT proxy_id FROM omniroute.proxy_assignments WHERE scope = $1 AND scope_id IS NOT DISTINCT FROM $2)
         AND l.timestamp >= $3 AND l.egress_ip IS NOT NULL AND l.connection_id IS NOT NULL
       GROUP BY l.egress_ip`,
    [scope, scopeId, since],
    [
      "n",
      "count",
      "proxy_port",
      "latency_ms",
      "upstream_status",
      "tls_fingerprint",
      "headers_ms",
      "first_chunk_ms",
      "attempt_number",
    ]
  )) as Array<{ n: number }>;
  const total = (
    await queryUsageRows(
      database,
      `SELECT COUNT(DISTINCT l.connection_id) AS n
       FROM omniroute.proxy_logs l
       JOIN omniroute.proxy_registry r ON l.proxy_host = r.host AND l.proxy_port = r.port
       WHERE r.id IN (SELECT proxy_id FROM omniroute.proxy_assignments WHERE scope = $1 AND scope_id IS NOT DISTINCT FROM $2)
         AND l.timestamp >= $3 AND l.egress_ip IS NOT NULL AND l.connection_id IS NOT NULL`,
      [scope, scopeId, since],
      [
        "n",
        "count",
        "proxy_port",
        "latency_ms",
        "upstream_status",
        "tls_fingerprint",
        "headers_ms",
        "first_chunk_ms",
        "attempt_number",
      ]
    )
  )[0] as { n: number };
  return {
    connections: total.n,
    distinctExits: perExit.length,
    maxConnectionsOnOneExit: perExit.reduce((max, row) => Math.max(max, row.n), 0),
    failures: await getPoolEgressFailureBreakdown(database, scope, scopeId, since),
  };
}
