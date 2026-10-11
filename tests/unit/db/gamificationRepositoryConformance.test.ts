import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";
import { registerGamificationConformance } from "../../helpers/persistence/gamificationRepositoryConformance.ts";
const directory = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-game-conformance-"));
process.env.DATA_DIR = directory;
process.env.STORAGE_ENCRYPTION_KEY = "isolated-knowledge-conformance-key";
const core = await import("../../../src/lib/db/core.ts");
const games = await import("../../../src/lib/db/gamification.ts");
const runtime = await import("../../../src/lib/db/gamificationRuntime.ts");
test.after(() => {
  core.resetDbInstance();
  fs.rmSync(directory, { recursive: true, force: true });
});
registerGamificationConformance("SQLite", async () => ({
  games: {
    updateScore: games.updateScoreAsync,
    getRank: games.getRankAsync,
    getTopN: games.getTopNAsync,
    getLeaderboardNeighbors: games.getLeaderboardNeighborsAsync,
    addXp: games.addXpAsync,
    getXp: games.getXpAsync,
    updateLevel: games.updateLevelAsync,
    getAggregateXp: games.getAggregateXpAsync,
    unlockBadge: games.unlockBadgeAsync,
    hasBadge: games.hasBadgeAsync,
    getBadges: games.getBadgesAsync,
    getBadgeDefinitions: games.getBadgeDefinitionsAsync,
    getAllEarnedBadges: games.getAllEarnedBadgesAsync,
    transferTokens: games.transferTokensAsync,
    getBalance: games.getBalanceAsync,
    getHistory: games.getHistoryAsync,
    createInviteToken: games.createInviteTokenAsync,
    getInviteByCode: games.getInviteByCodeAsync,
    redeemInvite: games.redeemInviteAsync,
    revokeInvite: games.revokeInviteAsync,
    connectServer: games.connectServerAsync,
    disconnectServer: games.disconnectServerAsync,
    listServers: games.listServersAsync,
    getConnectedServerByKeyHash: games.getConnectedServerByKeyHashAsync,
    rotateLeaderboardScope: games.rotateLeaderboardScopeAsync,
  },
  async reset() {
    core.resetDbInstance();
    fs.rmSync(directory, { recursive: true, force: true });
    fs.mkdirSync(directory, { recursive: true });
  },
  async seedBadge(badge) {
    await runtime.seedBadgesAsync([badge]);
  },
  async mint(key, amount) {
    core
      .getDbInstance()
      .prepare(
        "INSERT INTO token_ledger(from_api_key_id,to_api_key_id,amount,reason,idempotency_key) VALUES('mint',?,?,'fixture',?)"
      )
      .run(key, amount, `mint:${key}`);
  },
  actionCount: runtime.getActionCountAsync,
  uniqueCount: runtime.getUniqueCountAsync,
}));

test("SQLite async streak owner preserves the synchronous compare/write interval", async () => {
  const results = await Promise.all(
    Array.from({ length: 4 }, () =>
      runtime.advanceStreakValueAsync("compare-write", (raw) =>
        raw ? { result: false } : { value: JSON.stringify({ day: "today" }), result: true }
      )
    )
  );
  assert.equal(results.filter(Boolean).length, 1);
});
