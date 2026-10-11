import {
  getBadgeDetailsAsync,
  getActionCountAsync,
  awardXpOncePerDayAsync,
  synchronizeLevelAsync,
} from "../db/gamificationRuntime";
/**
 * Gamification event emitter — called from chat pipeline.
 *
 * @module lib/gamification/events
 */

import { logger } from "../../../open-sse/utils/logger.ts";
import { runPersistenceBackgroundWork } from "../db/persistenceRuntime";
import { XP_REWARDS } from "./xp";

const log = logger("GAMIFICATION");

/**
 * Emit a gamification event. All gamification updates happen here.
 * Called from chatCore.ts after successful requests.
 *
 * This function is fire-and-forget — never blocks the request pipeline.
 * All errors are caught and logged, never thrown.
 */
async function applyGamificationEvent(params: {
  apiKeyId: string;
  action:
    | "request"
    | "provider_switch"
    | "model_switch"
    | "combo_create"
    | "combo_use"
    | "token_share"
    | "invite_redeem"
    | "daily_login"
    | "radar_supporter";
  metadata?: Record<string, unknown>;
}): Promise<void> {
  const { apiKeyId, action, metadata } = params;

  if (!apiKeyId) return; // Skip if no API key

  try {
    // A verified Radar supporter is a recognition event, not an XP or
    // leaderboard action. The caller supplies only a one-way key identity.
    if (action === "radar_supporter") {
      await checkAndUnlockBadge(apiKeyId, "radar-supporter", false);
      return;
    }

    // 1. Award XP
    const xpAmount = getXpForAction(action);
    if (xpAmount > 0) {
      // Anti-cheat gate (#2403): the per-key 1000 XP/min rate limit and the z-score anomaly
      // check run before anything is persisted. A rejected award is dropped and logged — the
      // caller is fire-and-forget, so this must never throw.
      const { validateScoreChange } = await import("./antiCheat");
      const verdict = await validateScoreChange(apiKeyId, action, xpAmount);
      if (!verdict.allowed) {
        log.warn("events.award_rejected", {
          apiKeyId,
          action,
          xpAmount,
          reason: verdict.reason,
        });
        return;
      }

      const { addXpAsync: addXp } = await import("../db/gamification");
      await addXp(apiKeyId, action, xpAmount, metadata ? JSON.stringify(metadata) : undefined);

      await syncLevel(apiKeyId);
    }

    // 2. Update streak
    if (action === "request") {
      const { advanceStreak } = await import("./streaks");
      const { currentStreak: streak, extended } = await advanceStreak(apiKeyId);

      // Pay the documented streak_bonus (XP_REWARDS: per consecutive streak day, multiplied
      // by streak length) on the one request per UTC day that extends the streak.
      if (extended) {
        await awardStreakBonus(apiKeyId, streak);
      }

      // Check streak badges
      if (streak >= 365) {
        await checkAndUnlockBadge(apiKeyId, "unstoppable");
      } else if (streak >= 30) {
        await checkAndUnlockBadge(apiKeyId, "monthly-master");
      } else if (streak >= 7) {
        await checkAndUnlockBadge(apiKeyId, "weekly-warrior");
      } else if (streak >= 3) {
        await checkAndUnlockBadge(apiKeyId, "daily-user");
      }
    }

    // 3. Update leaderboard
    const { updateScore } = await import("./leaderboard");
    await updateScore(apiKeyId, "global", xpAmount);

    // Update weekly/monthly
    await updateScore(apiKeyId, "weekly", xpAmount);
    await updateScore(apiKeyId, "monthly", xpAmount);

    // Update specific scopes
    if (action === "token_share") {
      await updateScore(apiKeyId, "tokens_shared", xpAmount);
    }

    // 4. Check action count badges
    await checkActionCountBadges(apiKeyId, action);
  } catch (err) {
    // Never throw — gamification must not break the request pipeline
    log.error("events.error", {
      ...(action === "radar_supporter" ? {} : { apiKeyId }),
      action,
      error: err instanceof Error ? err.message : String(err),
    });
  }
}

/**
 * Recompute the level from total XP and persist it when it changed.
 * Runs after every award so bonus XP (streaks, badges) also counts toward level-ups.
 */
async function syncLevel(apiKeyId: string): Promise<void> {
  const changed = await synchronizeLevelAsync(apiKeyId);
  if (changed) log.info("events.level_up", { apiKeyId, ...changed });
}

/**
 * Award a bonus reward (`streak_bonus`, `badge_unlock`) through the same path as action XP:
 * `xp_audit_log` + `user_levels` via addXp, level sync, and the global/weekly/monthly
 * leaderboard scopes. Idempotency is the caller's responsibility.
 */
async function awardBonusXp(
  apiKeyId: string,
  action: "streak_bonus" | "badge_unlock",
  amount: number,
  metadata: Record<string, unknown>,
  persisted = false
): Promise<void> {
  const { addXpAsync: addXp } = await import("../db/gamification");
  if (!persisted) await addXp(apiKeyId, action, amount, JSON.stringify(metadata));
  await syncLevel(apiKeyId);

  const { updateScore } = await import("./leaderboard");
  await updateScore(apiKeyId, "global", amount);
  await updateScore(apiKeyId, "weekly", amount);
  await updateScore(apiKeyId, "monthly", amount);
  log.info("events.bonus_awarded", { apiKeyId, action, amount, ...metadata });
}

/**
 * Pay `streak_bonus × streak` once per UTC day. The `xp_audit_log` same-day check and the
 * insert run synchronously with no await in between, so two requests racing at the day
 * boundary cannot both pay.
 */
async function awardStreakBonus(apiKeyId: string, streak: number): Promise<void> {
  const amount = XP_REWARDS.streak_bonus * streak;
  const metadata = { streak };
  if (!(await awardXpOncePerDayAsync(apiKeyId, "streak_bonus", amount, JSON.stringify(metadata))))
    return;
  await awardBonusXp(apiKeyId, "streak_bonus", amount, metadata, true);
}

/**
 * Get XP amount for an action.
 */
function getXpForAction(action: string): number {
  const rewards: Record<string, number> = {
    request: 1,
    provider_switch: 5,
    model_switch: 3,
    combo_create: 10,
    combo_use: 2,
    token_share: 1,
    invite_redeem: 50,
    daily_login: 5,
  };
  return rewards[action] || 0;
}

/**
 * Check and unlock a specific badge, paying the documented `badge_unlock` XP once per badge.
 *
 * @param rewardable - `false` for recognition-only unlocks (Radar supporter): the caller
 *   supplies a one-way identity, so the unlock neither earns XP nor logs the identity.
 */
async function checkAndUnlockBadge(
  apiKeyId: string,
  badgeId: string,
  rewardable = true
): Promise<void> {
  const { unlockBadgeAsync: unlockBadge, hasBadgeAsync: hasBadge } =
    await import("../db/gamification");
  // #3472: dedup via user_badges directly. getBadges() INNER-JOINs badge_definitions, which is
  // empty until seeded, so it falsely reported "not earned" and re-emitted the unlock event on
  // every request.
  if (!(await hasBadge(apiKeyId, badgeId))) {
    // unlockBadge is INSERT OR IGNORE on the (api_key_id, badge_id) primary key; only the call
    // that actually inserts the row pays, so concurrent unlocks cannot double-pay.
    const inserted = await unlockBadge(apiKeyId, badgeId);
    if (!inserted) return;
    log.info("events.badge_unlocked", rewardable ? { apiKeyId, badgeId } : { badgeId });
    if (inserted && rewardable) {
      await awardBonusXp(apiKeyId, "badge_unlock", XP_REWARDS.badge_unlock, {
        badgeId,
      });
    }

    // Look up badge details from badge_definitions
    const badgeRow = await getBadgeDetailsAsync(badgeId);

    // Record notification for SSE toast
    const { recordBadgeUnlock } = await import("./notifications");
    recordBadgeUnlock(apiKeyId, {
      badgeId,
      badgeName: badgeRow?.name ?? badgeId,
      badgeDescription: badgeRow?.description ?? "",
      badgeIcon: badgeRow?.icon ?? "award",
      badgeRarity: badgeRow?.rarity ?? "common",
      unlockedAt: new Date().toISOString(),
    });
  }
}

/**
 * Check action count badges after an action.
 */
async function checkActionCountBadges(apiKeyId: string, action: string): Promise<void> {
  const count = await getActionCountAsync(apiKeyId, action);

  // Badge thresholds
  const thresholds: Record<string, Array<{ id: string; threshold: number }>> = {
    request: [
      { id: "first-token", threshold: 1 },
      { id: "token-consumer", threshold: 1000 },
      { id: "token-machine", threshold: 10000 },
      { id: "token-whale", threshold: 100000 },
    ],
    token_share: [
      { id: "generous", threshold: 1000 },
      { id: "philanthropist", threshold: 10000 },
      { id: "token-santa", threshold: 100000 },
      { id: "community-hero", threshold: 1000000 },
    ],
  };

  const badges = thresholds[action];
  if (!badges) return;

  for (const badge of badges) {
    if (count >= badge.threshold) {
      await checkAndUnlockBadge(apiKeyId, badge.id);
    }
  }
}

/** Capture a fresh durable scope at enqueue time; the chat caller may discard this promise. */
export async function emitGamificationEvent(
  params: Parameters<typeof applyGamificationEvent>[0]
): Promise<void> {
  if (!params.apiKeyId) return;
  try {
    await runPersistenceBackgroundWork(() => applyGamificationEvent(params));
  } catch (error) {
    log.error("events.error", {
      ...(params.action === "radar_supporter" ? {} : { apiKeyId: params.apiKeyId }),
      action: params.action,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
