/**
 * Leaderboard engine — score management, ranking, and scope rotation.
 *
 * @module lib/gamification/leaderboard
 */

// ─── Types ────────────────────────────────────────────────────────────────────

export type LeaderboardScope = "global" | "weekly" | "monthly" | "tokens_shared" | "contributions";

export interface LeaderboardEntry {
  apiKeyId: string;
  score: number;
}

// ─── Score Management ────────────────────────────────────────────────────────

/**
 * Update score for an API key in a scope. Atomic increment.
 */
export async function updateScore(
  apiKeyId: string,
  scope: LeaderboardScope,
  points: number
): Promise<void> {
  const { updateScoreAsync: dbUpdateScore } = await import("../db/gamification");
  await dbUpdateScore(apiKeyId, scope, points);
}

/**
 * Get rank for an API key in a scope.
 */
export async function getRank(apiKeyId: string, scope: LeaderboardScope): Promise<number> {
  const { getRankAsync: dbGetRank } = await import("../db/gamification");
  return await dbGetRank(apiKeyId, scope);
}

/**
 * Get top N entries for a scope.
 */
export async function getTopN(scope: LeaderboardScope, limit: number = 50, offset: number = 0) {
  const { getTopNAsync: dbGetTopN } = await import("../db/gamification");
  return await dbGetTopN(scope, limit, offset);
}

/**
 * Get neighbors around a user (entries above and below).
 */
export async function getNeighbors(
  apiKeyId: string,
  scope: LeaderboardScope,
  radius: number = 5
): Promise<{ above: LeaderboardEntry[]; below: LeaderboardEntry[] }> {
  const { getLeaderboardNeighborsAsync: getLeaderboardNeighbors } =
    await import("../db/gamification");
  return await getLeaderboardNeighbors(apiKeyId, scope, radius);
}

/**
 * Rotate weekly/monthly scopes. Archive old data, reset current.
 */
export async function rotateScope(scope: "weekly" | "monthly"): Promise<void> {
  const { rotateLeaderboardScopeAsync: rotateLeaderboardScope } =
    await import("../db/gamification");
  await rotateLeaderboardScope(scope);
}
