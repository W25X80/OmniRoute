import { getSelectedPostgresDatabase, assertSqliteBackendAllowed } from "./controlPlaneContext";
import {
  catalogTableRows,
  compareCatalogText,
  requirePostgresCatalogSnapshot,
  mutatePostgresCatalog,
  writePostgresCatalog,
} from "./postgres/catalogSnapshot";
import { intelligenceExpiryTimestamp } from "./postgres/intelligenceExpiry";
/**
 * modelIntelligence.ts — DB domain module for model task-fitness scores.
 *
 * Persists per-model intelligence from arena ELO, models.dev tier rankings,
 * and user overrides. Resolution chain: user_override → arena_elo → models_dev_tier.
 *
 * @see Migration 097_model_intelligence.sql
 */

import { getDbInstance, rowToCamel } from "./core";

// ──────────────── Types ────────────────

export interface ModelIntelligenceEntry {
  model: string;
  source: string;
  category: string;
  score: number;
  eloRaw: number | null;
  confidence: string | null;
  syncedAt: string;
  expiresAt: string | null;
  votes?: number;
  rank?: number;
}

// ──────────────── Helpers ────────────────

function rowToEntry(row: Record<string, unknown>): ModelIntelligenceEntry {
  const camel = rowToCamel(row) ?? {};
  return {
    model: String(camel.model ?? ""),
    source: String(camel.source ?? ""),
    category: String(camel.category ?? ""),
    score: typeof camel.score === "number" ? camel.score : 0,
    eloRaw: typeof camel.eloRaw === "number" ? camel.eloRaw : null,
    confidence: typeof camel.confidence === "string" ? camel.confidence : null,
    syncedAt: String(camel.syncedAt ?? ""),
    expiresAt: typeof camel.expiresAt === "string" ? camel.expiresAt : null,
  };
}

// ──────────────── CRUD ────────────────

export function getModelIntelligence(
  model: string,
  category: string
): ModelIntelligenceEntry | null {
  if (getSelectedPostgresDatabase()) {
    requirePostgresCatalogSnapshot();
    const sources = ["user_override", "arena_elo", "models_dev_tier"];
    const now = intelligenceExpiryTimestamp("now")!;
    const row = nativeIntelligenceRows()
      .filter(
        (row) =>
          row.model === model &&
          row.category === category &&
          sources.includes(String(row.source)) &&
          live(row, now)
      )
      .sort((a, b) => sources.indexOf(String(a.source)) - sources.indexOf(String(b.source)))[0];
    return row ? rowToEntry(row) : null;
  }

  const db = getDbInstance();
  const row = db
    .prepare(
      `SELECT * FROM model_intelligence
       WHERE model = ? AND category = ?
         AND source IN ('user_override', 'arena_elo', 'models_dev_tier')
         AND (expires_at IS NULL OR datetime(expires_at) > datetime('now'))
       ORDER BY CASE source
         WHEN 'user_override' THEN 1
         WHEN 'arena_elo' THEN 2
         WHEN 'models_dev_tier' THEN 3
       END
       LIMIT 1`
    )
    .get(model, category) as Record<string, unknown> | undefined;

  return row ? rowToEntry(row) : null;
}

export function getModelIntelligenceBySource(
  model: string,
  source: string,
  category: string
): ModelIntelligenceEntry | null {
  if (getSelectedPostgresDatabase()) {
    requirePostgresCatalogSnapshot();
    const now = intelligenceExpiryTimestamp("now")!;
    const row = nativeIntelligenceRows().find(
      (row) =>
        row.model === model && row.source === source && row.category === category && live(row, now)
    );
    return row ? rowToEntry(row) : null;
  }

  const db = getDbInstance();
  const row = db
    .prepare(
      `SELECT * FROM model_intelligence
       WHERE model = ? AND source = ? AND category = ?
         AND (expires_at IS NULL OR datetime(expires_at) > datetime('now'))`
    )
    .get(model, source, category) as Record<string, unknown> | undefined;

  return row ? rowToEntry(row) : null;
}

export function upsertModelIntelligence(entry: Omit<ModelIntelligenceEntry, "syncedAt">): void {
  assertSqliteBackendAllowed();

  const db = getDbInstance();

  db.prepare(
    `INSERT OR REPLACE INTO model_intelligence
       (model, source, category, score, elo_raw, confidence, synced_at, expires_at)
     VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?)`
  ).run(
    entry.model,
    entry.source,
    entry.category,
    entry.score,
    entry.eloRaw ?? null,
    entry.confidence ?? null,
    entry.expiresAt ?? null
  );
}

export function deleteModelIntelligence(model: string, source: string, category: string): boolean {
  assertSqliteBackendAllowed();

  const db = getDbInstance();
  const result = db
    .prepare(
      `DELETE FROM model_intelligence
       WHERE model = ? AND source = ? AND category = ?`
    )
    .run(model, source, category);
  return (result.changes ?? 0) > 0;
}

export function deleteExpiredIntelligence(source?: string): number {
  assertSqliteBackendAllowed();

  const db = getDbInstance();
  const conditions = ["expires_at IS NOT NULL", "datetime(expires_at) < datetime('now')"];
  const params: unknown[] = [];

  if (source) {
    conditions.push("source = ?");
    params.push(source);
  }

  const where = conditions.join(" AND ");
  const result = db.prepare(`DELETE FROM model_intelligence WHERE ${where}`).run(...params);
  return result.changes ?? 0;
}

export function deleteModelIntelligenceBySource(source: string): number {
  assertSqliteBackendAllowed();

  const db = getDbInstance();
  const result = db.prepare(`DELETE FROM model_intelligence WHERE source = ?`).run(source);
  return result.changes ?? 0;
}

export function listModelIntelligence(filters?: {
  source?: string;
  category?: string;
}): ModelIntelligenceEntry[] {
  if (getSelectedPostgresDatabase()) {
    requirePostgresCatalogSnapshot();
    return nativeIntelligenceRows()
      .filter(
        (row) =>
          (!filters?.source || row.source === filters.source) &&
          (!filters?.category || row.category === filters.category)
      )
      .sort(
        (a, b) =>
          compareCatalogText(a.model, b.model) ||
          compareCatalogText(a.source, b.source) ||
          compareCatalogText(a.category, b.category)
      )
      .map(rowToEntry);
  }

  const db = getDbInstance();

  const conditions: string[] = [];
  const params: unknown[] = [];

  if (filters?.source) {
    conditions.push("source = ?");
    params.push(filters.source);
  }
  if (filters?.category) {
    conditions.push("category = ?");
    params.push(filters.category);
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const sql = `SELECT * FROM model_intelligence ${where} ORDER BY model ASC, source ASC, category ASC`;

  const rows = db.prepare(sql).all(...params) as Record<string, unknown>[];
  return rows.map(rowToEntry);
}

export function bulkUpsertModelIntelligence(
  entries: Array<Omit<ModelIntelligenceEntry, "syncedAt">>
): number {
  assertSqliteBackendAllowed();

  if (entries.length === 0) return 0;

  const db = getDbInstance();
  const stmt = db.prepare(
    `INSERT OR REPLACE INTO model_intelligence
       (model, source, category, score, elo_raw, confidence, synced_at, expires_at)
     VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?)`
  );

  const upsertAll = db.transaction(() => {
    let count = 0;
    for (const entry of entries) {
      stmt.run(
        entry.model,
        entry.source,
        entry.category,
        entry.score,
        entry.eloRaw ?? null,
        entry.confidence ?? null,
        entry.expiresAt ?? null
      );
      count++;
    }
    return count;
  });

  return upsertAll();
}

export function getResolvedTaskFitness(model: string, category: string): number | null {
  const entry = getModelIntelligence(model, category);
  return entry ? entry.score : null;
}

/**
 * Latest synced_at for a source, as an ISO string (or null when the source
 * has no rows). Backs the arenaEloSync freshness guard: a non-empty dataset
 * synced within the sync interval does not need re-fetching.
 */
export function getLatestSyncedAt(source: string): string | null {
  if (getSelectedPostgresDatabase()) {
    requirePostgresCatalogSnapshot();
    const values = nativeIntelligenceRows()
      .filter((row) => row.source === source)
      .map((row) => String(row.synced_at))
      .sort(compareCatalogText);
    return values.at(-1) ?? null;
  }

  const db = getDbInstance();
  const row = db
    .prepare(`SELECT MAX(synced_at) as latest FROM model_intelligence WHERE source = ?`)
    .get(source) as { latest: string | null } | undefined;
  return row?.latest ?? null;
}

/**
 * Atomically replace a source's dataset: upsert every refreshed entry and
 * prune rows for that source that are not in the refreshed set — one
 * transaction, so a crash mid-refresh can never leave a half-replaced table.
 *
 * This is the safe replacement for the old delete-expired-then-upsert flow,
 * which lost data when a fetch failed after the delete (remediation 2026-09-12).
 */
export function applyArenaEloRefresh(
  entries: Array<Omit<ModelIntelligenceEntry, "syncedAt">>,
  source = "arena_elo"
): { upserted: number; pruned: number } {
  assertSqliteBackendAllowed();

  const db = getDbInstance();
  const upsertStmt = db.prepare(
    `INSERT OR REPLACE INTO model_intelligence
       (model, source, category, score, elo_raw, confidence, synced_at, expires_at)
     VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?)`
  );

  const refresh = db.transaction(() => {
    let upserted = 0;
    for (const entry of entries) {
      upsertStmt.run(
        entry.model,
        entry.source,
        entry.category,
        entry.score,
        entry.eloRaw ?? null,
        entry.confidence ?? null,
        entry.expiresAt ?? null
      );
      upserted++;
    }
    // Membership prune via a JSON array of "model\0category" pair keys:
    // correct for composite keys (a plain NOT IN over model names is not).
    const refreshedPairs = JSON.stringify(entries.map((e) => `${e.model}\u0000${e.category}`));
    const pruneByPair = db.prepare(
      `DELETE FROM model_intelligence
       WHERE source = ?
         AND model || char(0) || category NOT IN (
           SELECT value FROM json_each(?)
         )`
    );
    const pruneResult = pruneByPair.run(source, refreshedPairs);
    return { upserted, pruned: pruneResult.changes ?? 0 };
  });

  return refresh();
}

/**
 * Write a user_override entry for a model × category combination.
 * Used by taskFitness.ts resolution chain as Layer 1 (highest priority).
 *
 * @param model - Model identifier
 * @param category - Task category
 * @param score - Fitness score [0..1]
 */
export function setUserFitnessOverrideEntry(model: string, category: string, score: number): void {
  assertSqliteBackendAllowed();

  upsertModelIntelligence({
    model: model.toLowerCase(),
    source: "user_override",
    category: category.toLowerCase(),
    score: Math.max(0, Math.min(1, score)),
    eloRaw: null,
    confidence: null,
    expiresAt: null,
  });
}

/**
 * Delete a user_override entry for a model × category combination.
 *
 * @param model - Model identifier
 * @param category - Task category
 * @returns true if an entry was deleted
 */
export function deleteUserFitnessOverrideEntry(model: string, category: string): boolean {
  assertSqliteBackendAllowed();

  return deleteModelIntelligence(model.toLowerCase(), "user_override", category.toLowerCase());
}

function live(row: Record<string, unknown>, now: string): boolean {
  if (row.expires_at === null) return true;
  const expires =
    typeof row.expires_at === "string" ? intelligenceExpiryTimestamp(row.expires_at) : null;
  return expires !== null && compareCatalogText(expires, now) > 0;
}

type IntelligenceWriteEntry = Omit<ModelIntelligenceEntry, "syncedAt">;
async function nativeUpsert(entries: IntelligenceWriteEntry[]): Promise<void> {
  if (!entries.length) return;
  const final = new Map<string, IntelligenceWriteEntry>();
  for (const entry of entries)
    final.set(JSON.stringify([entry.model, entry.source, entry.category]), entry);
  const payload = [...final.values()].map((entry) => ({
    model: entry.model,
    source: entry.source,
    category: entry.category,
    score: Number.isNaN(entry.score)
      ? null
      : Number.isFinite(entry.score)
        ? entry.score
        : String(entry.score),
    elo_raw:
      entry.eloRaw == null || Number.isNaN(entry.eloRaw)
        ? null
        : Number.isFinite(entry.eloRaw)
          ? entry.eloRaw
          : String(entry.eloRaw),
    confidence: entry.confidence ?? null,
    expires_at: entry.expiresAt ?? null,
  }));
  await writePostgresCatalog(
    "INSERT INTO omniroute.model_intelligence(model,source,category,score,elo_raw,confidence,synced_at,expires_at) SELECT model,source,category,score,elo_raw,confidence,to_char(statement_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'),expires_at FROM jsonb_to_recordset($1::jsonb) AS incoming(model text,source text,category text,score double precision,elo_raw double precision,confidence text,expires_at text) ON CONFLICT(model,source,category) DO UPDATE SET score=EXCLUDED.score,elo_raw=EXCLUDED.elo_raw,confidence=EXCLUDED.confidence,synced_at=EXCLUDED.synced_at,expires_at=EXCLUDED.expires_at",
    [JSON.stringify(payload)]
  );
}

export async function upsertModelIntelligenceAsync(entry: IntelligenceWriteEntry): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return upsertModelIntelligence(entry);
  return mutatePostgresCatalog(database, () => nativeUpsert([entry]));
}
export async function deleteModelIntelligenceAsync(
  model: string,
  source: string,
  category: string
): Promise<boolean> {
  const database = getSelectedPostgresDatabase();
  if (!database) return deleteModelIntelligence(model, source, category);
  return mutatePostgresCatalog(
    database,
    async () =>
      ((
        await writePostgresCatalog(
          "DELETE FROM omniroute.model_intelligence WHERE model=$1 AND source=$2 AND category=$3",
          [model, source, category]
        )
      ).rowCount ?? 0) > 0
  );
}
export async function deleteExpiredIntelligenceAsync(source?: string): Promise<number> {
  const database = getSelectedPostgresDatabase();
  if (!database) return deleteExpiredIntelligence(source);
  return mutatePostgresCatalog(database, async () => {
    const now = intelligenceExpiryTimestamp("now")!;
    const obsolete = nativeIntelligenceRows()
      .filter((row) => {
        if (source && row.source !== source) return false;
        const expiry =
          typeof row.expires_at === "string" ? intelligenceExpiryTimestamp(row.expires_at) : null;
        return expiry !== null && compareCatalogText(expiry, now) < 0;
      })
      .map((row) => ({ model: row.model, source: row.source, category: row.category }));
    if (!obsolete.length) return 0;
    return (
      (
        await writePostgresCatalog(
          "DELETE FROM omniroute.model_intelligence AS existing USING jsonb_to_recordset($1::jsonb) AS obsolete(model text,source text,category text) WHERE existing.model=obsolete.model AND existing.source=obsolete.source AND existing.category=obsolete.category",
          [JSON.stringify(obsolete)]
        )
      ).rowCount ?? 0
    );
  });
}
export async function deleteModelIntelligenceBySourceAsync(source: string): Promise<number> {
  const database = getSelectedPostgresDatabase();
  if (!database) return deleteModelIntelligenceBySource(source);
  return mutatePostgresCatalog(
    database,
    async () =>
      (
        await writePostgresCatalog("DELETE FROM omniroute.model_intelligence WHERE source=$1", [
          source,
        ])
      ).rowCount ?? 0
  );
}
export async function bulkUpsertModelIntelligenceAsync(
  entries: IntelligenceWriteEntry[]
): Promise<number> {
  const database = getSelectedPostgresDatabase();
  if (!database) return bulkUpsertModelIntelligence(entries);
  if (!entries.length) return 0;
  return mutatePostgresCatalog(database, async () => {
    await nativeUpsert(entries);
    return entries.length;
  });
}
export async function applyArenaEloRefreshAsync(
  entries: IntelligenceWriteEntry[],
  source = "arena_elo"
): Promise<{ upserted: number; pruned: number }> {
  const database = getSelectedPostgresDatabase();
  if (!database) return applyArenaEloRefresh(entries, source);
  return mutatePostgresCatalog(database, async () => {
    await nativeUpsert(entries);
    const pairs = JSON.stringify(
      entries.map((entry) => ({ model: entry.model, category: entry.category }))
    );
    const result = await writePostgresCatalog(
      "DELETE FROM omniroute.model_intelligence AS existing WHERE source=$1 AND NOT EXISTS(SELECT 1 FROM jsonb_to_recordset($2::jsonb) AS refreshed(model text,category text) WHERE refreshed.model=existing.model AND refreshed.category=existing.category)",
      [source, pairs]
    );
    return { upserted: entries.length, pruned: result.rowCount ?? 0 };
  });
}
export async function setUserFitnessOverrideEntryAsync(
  model: string,
  category: string,
  score: number
): Promise<void> {
  return upsertModelIntelligenceAsync({
    model: model.toLowerCase(),
    source: "user_override",
    category: category.toLowerCase(),
    score: Math.max(0, Math.min(1, score)),
    eloRaw: null,
    confidence: null,
    expiresAt: null,
  });
}
export async function deleteUserFitnessOverrideEntryAsync(
  model: string,
  category: string
): Promise<boolean> {
  return deleteModelIntelligenceAsync(model.toLowerCase(), "user_override", category.toLowerCase());
}

function nativeIntelligenceRows(): Record<string, unknown>[] {
  return catalogTableRows("model_intelligence").map((row) => {
    for (const key of ["score", "elo_raw"]) {
      if (row[key] === "Infinity" || row[key] === "-Infinity") row[key] = Number(row[key]);
    }
    return row;
  });
}
