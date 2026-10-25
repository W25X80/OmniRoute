import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import { afterProviderRuntimeWrite, writeProviderRuntime } from "../postgres/providerRuntime";
/**
 * db/providers/rateLimit.ts — Rate-limit/quota runtime helpers for provider_connections.
 */

import { getDbInstance } from "../core";
import { invalidateDbCache } from "../readCache";
import { backupDbFile } from "../backup";
import { bumpProxyConfigGeneration } from "../settings";
import {
  stripCodexChildCooldownsFromConnection,
  stripCodexChildCooldownsFromConnectionAsync,
} from "./codexAccountState";

interface StatementLike<TRow = unknown> {
  all: (...params: unknown[]) => TRow[];
  get: (...params: unknown[]) => TRow | undefined;
  run: (...params: unknown[]) => { changes?: number };
}

interface DbLike {
  prepare: <TRow = unknown>(sql: string) => StatementLike<TRow>;
}

// ──────────────── T05: Rate-Limit DB Persistence ──────────────────────────
// Allows rate-limit state to survive token refresh without being accidentally
// cleared. DB column rate_limited_until already exists in schema.
// Ref: sub2api PR #1218 (fix(openai): prevent rescheduling rate-limited accounts)

/**
 * T05: Persist when a connection is rate-limited, directly in DB.
 * This survives token refresh — OAuth flows must NOT override this field.
 *
 * @param connectionId - The provider_connections.id
 * @param until - Epoch ms when the rate limit expires (null to clear)
 */
export function setConnectionRateLimitUntil(connectionId: string, until: number | null): void {
  // Guard: never persist a non-finite or already-expired timestamp. The TEXT
  // column would store "NaN"/"Infinity" and pollute every future read. null
  // is the only clear path (via clearConnectionRateLimit); past/zero
  // timestamps are noops so an expired write cannot overwrite a live row.
  if (until !== null && (!Number.isFinite(until) || until <= Date.now())) return;
  if (until == null) {
    stripCodexChildCooldownsFromConnection(connectionId, { alsoClearTopLevel: true });
    return;
  }
  const db = getDbInstance() as unknown as DbLike;
  db.prepare(
    "UPDATE provider_connections SET rate_limited_until = ?, updated_at = ? WHERE id = ?"
  ).run(until, new Date().toISOString(), connectionId);
  // Routing-only write (rate_limited_until) — the /v1/models builder never reads
  // it, so bust the connections read cache without dropping the catalog cache.
  invalidateDbCache("connections", connectionId, { skipModelCatalog: true });
}

/**
 * Mark a connection as rate-limited until `Date.now() + retryAfterMs`.
 *
 * Best-effort: never throws — a DB failure must not crash the chat path. The T05
 * startup helper `clearStaleCrashCooldowns` will not undo a write made here because
 * the timestamp is always strictly in the future at the moment of write. See Issue #1
 * (per-account 429 cascade not persisting).
 */
export function markConnectionRateLimitedUntil(connectionId: string, retryAfterMs: number): void {
  if (typeof connectionId !== "string" || connectionId.length === 0) return;
  if (!Number.isFinite(retryAfterMs) || retryAfterMs <= 0) return;
  try {
    setConnectionRateLimitUntil(connectionId, Date.now() + retryAfterMs);
  } catch {
    // best-effort
  }
}

/**
 * Clear a connection's persisted 429 cooldown.
 *
 * Best-effort: never throws. Mirrors `resetAccountState`'s in-memory clear so the
 * in-memory AccountState and the DB row agree.
 */
export function clearConnectionRateLimit(connectionId: string): void {
  if (typeof connectionId !== "string" || connectionId.length === 0) return;
  try {
    setConnectionRateLimitUntil(connectionId, null);
  } catch {
    // best-effort
  }
}

/**
 * T05: Check if a connection is currently rate-limited (DB-backed).
 * Use this before account selection to skip transiently rate-limited accounts.
 *
 * @returns true if rate_limited_until is set and in the future
 */
export function isConnectionRateLimited(connectionId: string): boolean {
  const db = getDbInstance() as unknown as DbLike;
  const row = db
    .prepare("SELECT rate_limited_until FROM provider_connections WHERE id = ?")
    .get(connectionId) as { rate_limited_until?: number | null } | undefined;
  if (!row?.rate_limited_until) return false;
  return Date.now() < row.rate_limited_until;
}

/**
 * T05: Get all connections for a provider that are currently rate-limited.
 * Returns an array of { id, rateLimitedUntil } for dashboard display.
 */
export function getRateLimitedConnections(
  provider: string
): Array<{ id: string; rateLimitedUntil: number }> {
  const db = getDbInstance() as unknown as DbLike;
  const now = Date.now();
  const rows = db
    .prepare(
      "SELECT id, rate_limited_until FROM provider_connections WHERE provider = ? AND rate_limited_until > ?"
    )
    .all(provider, now) as Array<{ id: string; rate_limited_until: number }>;
  return rows.map((r) => ({ id: r.id, rateLimitedUntil: r.rate_limited_until }));
}

// ──────────────── T13: Stale Quota Display Fix ─────────────────────────────
// Codex/Claude quotas display stale cumulative usage after the window resets.
// By comparing resetAt timestamp to now(), we can show 0 when window has passed.
// Ref: sub2api PR #1171 (fix: quota display shows stale cumulative usage after reset)

/**
 * T13: Get effective quota usage, zeroing it out if the window has already reset.
 *
 * @param used - Stored usage value (tokens used in the window)
 * @param resetAt - ISO-8601 string or epoch ms when the window resets, or null
 * @returns Effective usage: 0 if window expired, original value otherwise
 */
export function getEffectiveQuotaUsage(
  used: number,
  resetAt: string | number | null | undefined
): number {
  if (!resetAt) return used;
  const resetTime = typeof resetAt === "number" ? resetAt : new Date(resetAt).getTime();
  if (isNaN(resetTime)) return used;
  // Window has passed — display should show 0 (pending next snapshot)
  if (Date.now() >= resetTime) return 0;
  return used;
}

/**
 * Normalize a persisted `rate_limited_until` to epoch ms.
 *
 * The column is written in two shapes: epoch ms by `setConnectionRateLimitUntil`
 * (the chat path) and an ISO-8601 string by `updateProviderConnection` (the
 * dashboard/AUTH path). Returns null when the value is absent or unparseable —
 * callers treat that as "no usable deadline".
 */
function parseCooldownUntilMs(value: string | number | null | undefined): number | null {
  if (value == null || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const raw = String(value).trim();
  if (raw === "") return null;
  if (/^\d+$/.test(raw)) {
    const numeric = Number(raw);
    return Number.isFinite(numeric) ? numeric : null;
  }
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * T05: Startup crash-recovery — clear stale transient connection cooldowns.
 *
 * After an unclean crash (SIGKILL, OOM-kill, large-body burst) the normal
 * error-handler paths that would clear/normalise cooldowns never run.
 * A connection's `rate_limited_until` may have been pushed far into the
 * future by exponential back-off.  On next startup that leaves all affected
 * connections excluded by `getProviderCredentials()`, so every request sits
 * in the Bottleneck queue and times out at `maxWaitMs` (120 s default).
 *
 * Safe invariants:
 *  - Only connections with `rate_limited_until IS NOT NULL` are touched.
 *  - Terminal states (`banned`, `expired`, `credits_exhausted`) are skipped —
 *    those require a deliberate credential change or operator reset.
 *  - Past timestamps are cleared: they are already expired in the lazy
 *    expiry sense, but clearing them resets `backoffLevel` / transient error
 *    fields so the connection gets a clean slate on this fresh process. An
 *    unparseable timestamp is treated the same way — it can never expire
 *    lazily, so leaving it would strand the connection forever.
 *  - FUTURE timestamps are NEVER cleared. Clearing them was the original
 *    behaviour and it wiped legitimate multi-day quota cooldowns on every
 *    container recreate: a GLM weekly cap persisted until 2026-08-29 came
 *    back `active` with `rate_limited_until = NULL`, combo dispatched it
 *    immediately, and the connection re-earned a real upstream 429. A stale
 *    crash-backoff value is bounded by the engine's own cooldown cap, so
 *    honouring it costs at most that window — far less than burning quota
 *    against an upstream that is provably exhausted.
 *
 * Must be called once, early in the startup sequence, before any request
 * is handled.  Returns the number of connections that were cleared.
 */
export function clearStaleCrashCooldowns(): { cleared: number } {
  const db = getDbInstance() as unknown as DbLike;
  const now = new Date().toISOString();
  const nowMs = Date.now();

  // Fetch all connections that have a rate_limited_until set and are NOT in
  // a terminal state.  We do the terminal-status filter in JS to reuse the
  // canonical `TERMINAL_STATUSES` set rather than duplicating the list in SQL.
  const TERMINAL_STATUSES = new Set(["banned", "expired", "credits_exhausted"]);

  const rows = db
    .prepare(
      `SELECT id, test_status, rate_limited_until FROM provider_connections WHERE rate_limited_until IS NOT NULL`
    )
    .all() as Array<{
    id: string;
    test_status: string | null;
    rate_limited_until: string | number | null;
  }>;

  const toReset = rows.filter((r) => {
    const status = (r.test_status || "").trim().toLowerCase();
    if (TERMINAL_STATUSES.has(status)) return false;
    const untilMs = parseCooldownUntilMs(r.rate_limited_until);
    // Unparseable → clear (cannot expire lazily). Future → keep.
    return untilMs === null || untilMs <= nowMs;
  });

  if (toReset.length === 0) return { cleared: 0 };

  const stmt = db.prepare(
    `UPDATE provider_connections SET
       rate_limited_until = NULL,
       test_status        = 'active',
       backoff_level      = 0,
       last_error         = NULL,
       last_error_at      = NULL,
       last_error_type    = NULL,
       last_error_source  = NULL,
       error_code         = NULL,
       updated_at         = ?
     WHERE id = ?`
  );

  for (const row of toReset) {
    stmt.run(now, row.id);
  }

  // Routing/health-only fields (rate_limited_until, test_status, backoff_level,
  // last_error*, error_code) — the /v1/models builder never reads them.
  invalidateDbCache("connections", undefined, { skipModelCatalog: true });

  return { cleared: toReset.length };
}

/**
 * Atomic conditional clear of recoverable error state on a connection row.
 *
 * Returns true when the row was cleared, false when a concurrent writer
 * (markAccountUnavailable, connectionRecovery tick, test, etc.) changed the
 * row between the caller's snapshot read and this UPDATE — in which case the
 * clear is skipped to preserve the freshest error state. Closes the TOCTOU
 * window in the quota-recovery path.
 *
 * CAS token = (test_status, last_error_at, rate_limited_until).
 * Nested Codex child cooldown maps are stripped in the same UPDATE so a
 * concurrent writer cannot re-persist them between two statements.
 */
export async function clearConnectionErrorIfUnchanged(
  id: string,
  expected: {
    testStatus: string | null | undefined;
    lastErrorAt: string | null | undefined;
    rateLimitedUntil: string | null | undefined;
  },
  options: { preserveScopedCooldowns?: boolean } = {}
): Promise<boolean> {
  const database = getSelectedPostgresDatabase();
  if (database) {
    const applied = await writeProviderRuntime(database, async (transaction) => {
      const { rows } = await transaction.query<Record<string, unknown>>(
        `SELECT provider,provider_specific_data FROM omniroute.provider_connections
         WHERE id=$1 AND COALESCE(test_status,'')=$2 AND COALESCE(last_error_at,'')=$3
           AND COALESCE(rate_limited_until,'')=$4 FOR UPDATE`,
        [id, expected.testStatus ?? "", expected.lastErrorAt ?? "", expected.rateLimitedUntil ?? ""]
      );
      if (!rows[0]) return false;
      const row = rows[0];
      let providerData = row.provider_specific_data;
      if (
        !options.preserveScopedCooldowns &&
        row.provider === "codex" &&
        typeof providerData === "string"
      ) {
        try {
          const parsed: unknown = JSON.parse(providerData);
          // json_remove has no matching object paths for scalars/arrays.
          if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
            const data = { ...(parsed as Record<string, unknown>) };
            delete data.codexScopeRateLimitedUntil;
            delete data.codexScopeRateLimitSource;
            providerData = JSON.stringify(data);
          }
        } catch {
          /* Preserve source behavior for invalid JSON. */
        }
      }
      const result = await transaction.query(
        `UPDATE omniroute.provider_connections SET test_status='active',last_error=NULL,
         last_error_at=NULL,last_error_type=NULL,last_error_source=NULL,error_code=NULL,
         rate_limited_until=NULL,backoff_level=0,provider_specific_data=$1,updated_at=$2
         WHERE id=$3 AND COALESCE(test_status,'')=$4 AND COALESCE(last_error_at,'')=$5
           AND COALESCE(rate_limited_until,'')=$6`,
        [
          providerData,
          new Date().toISOString(),
          id,
          expected.testStatus ?? "",
          expected.lastErrorAt ?? "",
          expected.rateLimitedUntil ?? "",
        ]
      );
      return (result.rowCount ?? 0) > 0;
    });
    if (applied)
      await afterProviderRuntimeWrite("conditional_error_clear", async () => {
        invalidateDbCache("connections", id, { skipModelCatalog: true });
        await bumpProxyConfigGeneration();
      });
    return applied;
  }
  const db = getDbInstance() as unknown as DbLike;
  backupDbFile("pre-write");
  const result = db
    .prepare(
      `
    UPDATE provider_connections SET
      test_status = 'active',
      last_error = NULL,
      last_error_at = NULL,
      last_error_type = NULL,
      last_error_source = NULL,
      error_code = NULL,
      rate_limited_until = NULL,
      backoff_level = 0,
      provider_specific_data = CASE
        WHEN ? = 0 AND provider = 'codex' AND json_valid(provider_specific_data)
          THEN json_remove(
            provider_specific_data,
            '$.codexScopeRateLimitedUntil',
            '$.codexScopeRateLimitSource'
          )
        ELSE provider_specific_data
      END,
      updated_at = ?
    WHERE id = ?
      AND IFNULL(test_status, '') = ?
      AND IFNULL(last_error_at, '') = ?
      AND IFNULL(rate_limited_until, '') = ?
    `
    )
    .run(
      options.preserveScopedCooldowns ? 1 : 0,
      new Date().toISOString(),
      id,
      expected.testStatus ?? "",
      expected.lastErrorAt ?? "",
      expected.rateLimitedUntil ?? ""
    );
  const applied = (result.changes ?? 0) > 0;
  if (applied) {
    // Routing/health-only fields + codex-scope cooldown keys — none are read by
    // the /v1/models builder (it only reads providerSpecificData.excludedModels).
    invalidateDbCache("connections", id, { skipModelCatalog: true });
    await bumpProxyConfigGeneration();
  }
  return applied;
}

// T13: Format a reset countdown as a human-readable string ("2h 35m" / "4m 30s").
// The implementation lives in the client-safe formatting utils so client
// components (e.g. CoolingConnectionsPanel) can import it without pulling this
// server-only DB module (better-sqlite3/ioredis) into the browser bundle.
// Re-exported here for existing server-side callers and the db/providers barrel.
export { formatResetCountdown } from "@/shared/utils/formatting";

/** Native async setter; the legacy sync setter remains a SQLite-only compatibility API. */
export async function setConnectionRateLimitUntilAsync(
  connectionId: string,
  until: number | null
): Promise<void> {
  if (until !== null && (!Number.isFinite(until) || until <= Date.now())) return;
  const database = getSelectedPostgresDatabase();
  if (!database) return setConnectionRateLimitUntil(connectionId, until);
  if (until == null) {
    await stripCodexChildCooldownsFromConnectionAsync(connectionId, { alsoClearTopLevel: true });
    return;
  }
  await writeProviderRuntime(database, async (transaction) => {
    await transaction.query(
      "UPDATE omniroute.provider_connections SET rate_limited_until=$1,updated_at=$2 WHERE id=$3",
      [String(until), new Date().toISOString(), connectionId]
    );
  });
  await afterProviderRuntimeWrite("connection_cooldown", () =>
    invalidateDbCache("connections", connectionId, { skipModelCatalog: true })
  );
}

export async function markConnectionRateLimitedUntilAsync(
  connectionId: string,
  retryAfterMs: number
): Promise<void> {
  if (typeof connectionId !== "string" || connectionId.length === 0) return;
  if (!Number.isFinite(retryAfterMs) || retryAfterMs <= 0) return;
  try {
    await setConnectionRateLimitUntilAsync(connectionId, Date.now() + retryAfterMs);
  } catch {
    /* Source best effort: persistence failure cannot crash chat. */
  }
}

export async function clearConnectionRateLimitAsync(connectionId: string): Promise<void> {
  if (typeof connectionId !== "string" || connectionId.length === 0) return;
  try {
    await setConnectionRateLimitUntilAsync(connectionId, null);
  } catch {
    /* Source best effort. */
  }
}

export async function isConnectionRateLimitedAsync(connectionId: string): Promise<boolean> {
  const database = getSelectedPostgresDatabase();
  if (!database) return isConnectionRateLimited(connectionId);
  return database.transaction(async (transaction) => {
    const { rows } = await transaction.query<{ rate_limited_until: string | null }>(
      "SELECT rate_limited_until FROM omniroute.provider_connections WHERE id=$1",
      [connectionId]
    );
    if (!rows[0]?.rate_limited_until) return false;
    // Preserve this legacy helper's numeric-coercion contract; other auth paths parse ISO.
    return Date.now() < Number(rows[0].rate_limited_until);
  });
}

export async function getRateLimitedConnectionsAsync(
  provider: string
): Promise<Array<{ id: string; rateLimitedUntil: number }>> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getRateLimitedConnections(provider);
  return database.transaction(async (transaction) => {
    const { rows } = await transaction.query<{ id: string; rate_limited_until: number }>(
      `SELECT id,rate_limited_until FROM omniroute.provider_connections
       WHERE provider=$1 AND rate_limited_until COLLATE "C" > $2::text COLLATE "C"
       ORDER BY insertion_order`,
      [provider, String(Date.now())]
    );
    // TEXT affinity in SQLite already exposes these values as strings despite the legacy type.
    return rows.map((row) => ({ id: row.id, rateLimitedUntil: row.rate_limited_until }));
  });
}

export async function clearStaleCrashCooldownsAsync(): Promise<{ cleared: number }> {
  const database = getSelectedPostgresDatabase();
  if (!database) return clearStaleCrashCooldowns();
  const result = await writeProviderRuntime(database, async (transaction) => {
    const now = new Date().toISOString(),
      nowMs = Date.now();
    const terminal = new Set(["banned", "expired", "credits_exhausted"]);
    const { rows } = await transaction.query<{
      id: string;
      test_status: string | null;
      rate_limited_until: string | null;
    }>(
      `SELECT id,test_status,rate_limited_until FROM omniroute.provider_connections
       WHERE rate_limited_until IS NOT NULL ORDER BY insertion_order FOR UPDATE`
    );
    let cleared = 0;
    for (const row of rows) {
      if (terminal.has((row.test_status || "").trim().toLowerCase())) continue;
      const until = parseCooldownUntilMs(row.rate_limited_until);
      if (until !== null && until > nowMs) continue;
      const result = await transaction.query(
        `UPDATE omniroute.provider_connections SET rate_limited_until=NULL,test_status='active',
         backoff_level=0,last_error=NULL,last_error_at=NULL,last_error_type=NULL,last_error_source=NULL,
         error_code=NULL,updated_at=$1 WHERE id=$2`,
        [now, row.id]
      );
      cleared += result.rowCount ?? 0;
    }
    return { cleared };
  });
  if (result.cleared > 0)
    await afterProviderRuntimeWrite("startup_cooldowns", () =>
      invalidateDbCache("connections", undefined, { skipModelCatalog: true })
    );
  return result;
}
