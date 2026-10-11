import {
  getHighXpKeysAsync,
  getXpDistributionAsync,
  getRecentXpAsync,
} from "../db/gamificationRuntime";
/**
 * Anti-cheat system — server-side validation and anomaly detection.
 *
 * @module lib/gamification/antiCheat
 */

// ─── Types ────────────────────────────────────────────────────────────────────

interface ScoreValidation {
  allowed: boolean;
  reason?: string;
}

interface AnomalyFlag {
  apiKeyId: string;
  xpLastHour: number;
  zScore: number;
}

// ─── Constants ────────────────────────────────────────────────────────────────

const RATE_LIMIT_WINDOW_MS = 60_000; // 1 minute
const MAX_XP_PER_WINDOW = 1000;
const ANOMALY_Z_THRESHOLD = 3;

// ─── Public API ──────────────────────────────────────────────────────────────

/**
 * Validate a score change. Returns true if allowed, false if suspicious.
 */
export async function validateScoreChange(
  apiKeyId: string,
  _action: string,
  amount: number
): Promise<ScoreValidation> {
  // Rate limiting: max XP per window
  const recentXp = await getRecentXp(apiKeyId, RATE_LIMIT_WINDOW_MS);
  if (recentXp + amount > MAX_XP_PER_WINDOW) {
    return {
      allowed: false,
      reason: `Rate limit exceeded: ${recentXp + amount} > ${MAX_XP_PER_WINDOW} XP/min`,
    };
  }

  // Anomaly detection: check if this user's XP velocity is abnormal
  const isAnomaly = await detectAnomaly(apiKeyId);
  if (isAnomaly) {
    return { allowed: false, reason: "Anomalous XP velocity detected" };
  }

  return { allowed: true };
}

/**
 * Get flagged anomalies for admin review.
 */
export async function getAnomalies(): Promise<AnomalyFlag[]> {
  const rows = await getHighXpKeysAsync();
  const results: AnomalyFlag[] = [];
  for (const row of rows) {
    const z = await computeZScore(row.api_key_id);
    results.push({
      apiKeyId: row.api_key_id,
      xpLastHour: row.hourly_total,
      zScore: z ?? 0,
    });
  }
  return results;
}

// ─── Internal Helpers ────────────────────────────────────────────────────────

/**
 * Compute the z-score for a user's hourly XP against the global distribution.
 * Returns null if insufficient data.
 */
async function computeZScore(apiKeyId: string): Promise<number | null> {
  const stats = await getXpDistributionAsync(apiKeyId);
  if (stats.mean === null || stats.variance === null || stats.variance <= 0) return null;
  return (stats.total - stats.mean) / Math.sqrt(stats.variance);
}

/**
 * Get total XP earned in the last N milliseconds.
 */
async function getRecentXp(apiKeyId: string, windowMs: number): Promise<number> {
  return getRecentXpAsync(apiKeyId, windowMs);
}

/**
 * Detect anomalous XP velocity using z-score.
 */
async function detectAnomaly(apiKeyId: string): Promise<boolean> {
  const z = await computeZScore(apiKeyId);
  return z !== null && z > ANOMALY_Z_THRESHOLD;
}
