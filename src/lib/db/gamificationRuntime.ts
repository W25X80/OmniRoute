import { calculateLevel } from "../gamification/xp";
import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { createPostgresGamificationRepository } from "./postgres/gamificationRepository";
import { addXp, hasXpActionToday, getXp, updateLevel, type BadgeDefinition } from "./gamification";

function selected() {
  const database = getSelectedPostgresDatabase();
  return database ? createPostgresGamificationRepository(database) : null;
}
/** Capability check for source cloud/build guards without exposing a native database handle. */
export function isNativeGamificationPersistenceSelected(): boolean {
  return getSelectedPostgresDatabase() !== undefined;
}
export async function awardXpOncePerDayAsync(
  key: string,
  action: string,
  amount: number,
  metadata?: string
): Promise<boolean> {
  const repository = selected();
  if (repository) return repository.awardXpOncePerDay(key, action, amount, metadata);
  if (hasXpActionToday(key, action)) return false;
  addXp(key, action, amount, metadata);
  return true;
}
export async function getBadgeDetailsAsync(id: string) {
  const repository = selected();
  if (repository) return repository.getBadgeDetails(id);
  return getDbInstance()
    .prepare("SELECT name,description,icon,rarity FROM badge_definitions WHERE id=?")
    .get(id) as
    | {
        name: string;
        description: string | null;
        icon: string | null;
        rarity: string;
      }
    | undefined;
}
export async function getActionCountAsync(key: string, action: string): Promise<number> {
  const repository = selected();
  if (repository) return repository.getActionCount(key, action);
  const row = getDbInstance()
    .prepare("SELECT count AS total FROM xp_action_counts WHERE api_key_id=? AND action=?")
    .get(key, action) as { total: number } | undefined;
  return row?.total ?? 0;
}
export async function getUniqueCountAsync(key: string, field: string): Promise<number> {
  const repository = selected();
  if (repository) return repository.getUniqueCount(key, field);
  const row = getDbInstance()
    .prepare(
      "SELECT COUNT(DISTINCT json_extract(metadata,'$.'||?)) AS total FROM xp_audit_log WHERE api_key_id=? AND metadata IS NOT NULL"
    )
    .get(field, key) as { total: number };
  return row?.total ?? 0;
}
export async function getFirstLogDateAsync(key: string): Promise<string | null> {
  const repository = selected();
  if (repository) return repository.getFirstLogDate(key);
  return (
    (
      getDbInstance()
        .prepare("SELECT MIN(created_at) AS first_at FROM xp_audit_log WHERE api_key_id=?")
        .get(key) as { first_at: string | null } | undefined
    )?.first_at ?? null
  );
}
export async function seedBadgesAsync(
  badges: readonly Omit<BadgeDefinition, "createdAt">[]
): Promise<void> {
  const repository = selected();
  if (repository) return repository.seedBadges(badges);
  const db = getDbInstance();
  const insert = db.prepare(
    "INSERT OR IGNORE INTO badge_definitions(id,name,description,icon,category,rarity,criteria,hidden) VALUES(?,?,?,?,?,?,?,?)"
  );
  db.transaction(() => {
    for (const badge of badges)
      insert.run(
        badge.id,
        badge.name,
        badge.description,
        badge.icon,
        badge.category,
        badge.rarity,
        badge.criteria,
        badge.hidden
      );
  })();
}
export async function listInvitesAsync(key: string) {
  const repository = selected();
  if (repository) return repository.listInvites(key);
  const rows = getDbInstance()
    .prepare(
      "SELECT id,code,server_url,max_uses,use_count,expires_at,revoked_at,created_at FROM invite_tokens WHERE created_by=? ORDER BY created_at DESC"
    )
    .all(key) as Array<{
    id: string;
    code: string;
    server_url: string | null;
    max_uses: number;
    use_count: number;
    expires_at: string | null;
    revoked_at: string | null;
    created_at: string;
  }>;
  return rows.map((row) => ({
    id: row.id,
    code: row.code,
    serverUrl: row.server_url,
    maxUses: row.max_uses,
    useCount: row.use_count,
    expiresAt: row.expires_at,
    revokedAt: row.revoked_at,
    createdAt: row.created_at,
  }));
}
export async function getServerEndpointAsync(id: string, connectedOnly = true) {
  const repository = selected();
  if (repository) return repository.getServerEndpoint(id, connectedOnly);
  const row = getDbInstance()
    .prepare(
      `SELECT url,api_key_hash FROM community_servers WHERE id=?${connectedOnly ? " AND status='connected'" : ""}`
    )
    .get(id) as { url: string; api_key_hash: string } | undefined;
  return row ? { url: row.url, apiKeyHash: row.api_key_hash } : undefined;
}
export async function applyRemoteScoresAsync(
  id: string,
  entries: readonly { apiKeyId: string; score: number }[]
): Promise<void> {
  const repository = selected();
  if (repository) return repository.applyRemoteScores(id, entries);
  const db = getDbInstance();
  for (const entry of entries)
    db.prepare(
      "INSERT INTO leaderboard(api_key_id,scope,score,updated_at) VALUES(?,'global',?,datetime('now')) ON CONFLICT(api_key_id,scope) DO UPDATE SET score=excluded.score,updated_at=excluded.updated_at"
    ).run(entry.apiKeyId, entry.score);
  db.prepare(
    "UPDATE community_servers SET last_sync_at=datetime('now'),error_message=NULL WHERE id=?"
  ).run(id);
}
export async function setServerSyncErrorAsync(id: string, message: string): Promise<void> {
  const repository = selected();
  if (repository) return repository.setServerSyncError(id, message);
  getDbInstance()
    .prepare("UPDATE community_servers SET status='error',error_message=? WHERE id=?")
    .run(message, id);
}
export async function getRecentXpAsync(key: string, windowMs: number): Promise<number> {
  const repository = selected();
  if (repository) return repository.getRecentXp(key, windowMs);
  return (
    getDbInstance()
      .prepare(
        "SELECT COALESCE(SUM(xp_earned),0) AS total FROM xp_audit_log WHERE api_key_id=? AND created_at>datetime('now',?)"
      )
      .get(key, `-${Math.ceil(windowMs / 1000)} seconds`) as { total: number }
  ).total;
}
export async function getHighXpKeysAsync(): Promise<
  Array<{ api_key_id: string; hourly_total: number }>
> {
  const repository = selected();
  if (repository) return repository.getHighXpKeys();
  return getDbInstance()
    .prepare(
      "SELECT api_key_id,SUM(xp_earned) AS hourly_total FROM xp_audit_log WHERE created_at>datetime('now','-1 hour') GROUP BY api_key_id HAVING hourly_total>1000"
    )
    .all() as Array<{ api_key_id: string; hourly_total: number }>;
}
export async function getXpDistributionAsync(
  key: string
): Promise<{ total: number; mean: number | null; variance: number | null }> {
  const repository = selected();
  if (repository) return repository.getXpDistribution(key);
  const db = getDbInstance();
  const user = db
    .prepare(
      "SELECT COALESCE(SUM(xp_earned),0) AS total FROM xp_audit_log WHERE api_key_id=? AND created_at>datetime('now','-1 hour')"
    )
    .get(key) as { total: number };
  const stats = db
    .prepare(
      "SELECT AVG(hourly_total) AS mean,CASE WHEN AVG(hourly_total)=0 THEN 1 ELSE AVG(hourly_total*hourly_total)-AVG(hourly_total)*AVG(hourly_total) END AS variance FROM(SELECT api_key_id,SUM(xp_earned) AS hourly_total FROM xp_audit_log WHERE created_at>datetime('now','-1 hour') GROUP BY api_key_id)"
    )
    .get() as { mean: number | null; variance: number | null };
  return { total: user.total, mean: stats.mean, variance: stats.variance };
}
export async function readStreakValueAsync(key: string): Promise<string | null> {
  const repository = selected();
  if (repository) return repository.readStreak(key);
  return (
    (
      getDbInstance()
        .prepare("SELECT value FROM key_value WHERE namespace=? AND key=?")
        .get("gamification:streaks", key) as { value: string } | undefined
    )?.value ?? null
  );
}
export async function listStreakValuesAsync(): Promise<Array<{ value: string }>> {
  const repository = selected();
  if (repository) return repository.listStreaks();
  return getDbInstance()
    .prepare("SELECT value FROM key_value WHERE namespace=?")
    .all("gamification:streaks") as Array<{ value: string }>;
}
export async function advanceStreakValueAsync<T>(
  key: string,
  derive: (raw: string | null) => { value?: string; result: T }
): Promise<T> {
  const repository = selected();
  if (repository) return repository.advanceStreak(key, derive);
  const database = getDbInstance();
  const row = database
    .prepare("SELECT value FROM key_value WHERE namespace=? AND key=?")
    .get("gamification:streaks", key) as { value: string } | undefined;
  // Keep SQLite's original synchronous read/derive/write interval.
  const next = derive(row?.value ?? null);
  if (next.value !== undefined)
    database
      .prepare("INSERT OR REPLACE INTO key_value(namespace,key,value) VALUES(?,?,?)")
      .run("gamification:streaks", key, next.value);
  return next.result;
}

export async function synchronizeLevelAsync(
  key: string
): Promise<{ oldLevel: number; newLevel: number } | null> {
  const repository = selected();
  if (repository) return repository.synchronizeLevel(key);
  const xp = getXp(key);
  if (!xp) return null;
  const newLevel = calculateLevel(xp.totalXp);
  if (newLevel === xp.currentLevel) return null;
  updateLevel(key, newLevel);
  return { oldLevel: xp.currentLevel, newLevel };
}
