import assert from "node:assert/strict";
import test from "node:test";
import type {
  BadgeDefinition,
  InviteToken,
  LeaderboardRow,
  TokenLedgerEntry,
  UserBadge,
  UserLevelRow,
} from "../../../src/lib/db/gamification.ts";
export interface GamificationConformanceRepository {
  updateScore(key: string, scope: string, points: number): Promise<void>;
  getRank(key: string, scope: string): Promise<number>;
  getTopN(scope: string, limit: number, offset?: number): Promise<LeaderboardRow[]>;
  getLeaderboardNeighbors(
    key: string,
    scope: string,
    radius?: number
  ): Promise<{
    above: Array<{ apiKeyId: string; score: number }>;
    below: Array<{ apiKeyId: string; score: number }>;
  }>;
  addXp(key: string, action: string, amount: number, metadata?: string): Promise<void>;
  getXp(key: string): Promise<UserLevelRow | null>;
  updateLevel(key: string, level: number): Promise<void>;
  getAggregateXp(): Promise<UserLevelRow>;
  unlockBadge(key: string, badge: string): Promise<boolean>;
  hasBadge(key: string, badge: string): Promise<boolean>;
  getBadges(key: string): Promise<UserBadge[]>;
  getBadgeDefinitions(category?: string): Promise<BadgeDefinition[]>;
  getAllEarnedBadges(): Promise<UserBadge[]>;
  transferTokens(
    from: string,
    to: string,
    amount: number,
    reason: string,
    idempotencyKey: string
  ): Promise<{ success: boolean; error?: string }>;
  getBalance(key: string): Promise<number>;
  getHistory(key: string, limit: number): Promise<TokenLedgerEntry[]>;
  createInviteToken(
    id: string,
    code: string,
    hash: string,
    createdBy: string,
    url?: string,
    maxUses?: number
  ): Promise<void>;
  getInviteByCode(code: string): Promise<InviteToken | null>;
  redeemInvite(code: string, usedBy: string): Promise<boolean>;
  revokeInvite(id: string): Promise<void>;
  connectServer(id: string, name: string, url: string, hash: string): Promise<void>;
  disconnectServer(id: string): Promise<void>;
  listServers(): Promise<Array<{ id: string; name: string; url: string; status: string }>>;
  getConnectedServerByKeyHash(hash: string): Promise<{ id: string } | undefined>;
  rotateLeaderboardScope(scope: "weekly" | "monthly"): Promise<void>;
}
export interface GamificationFixture {
  games: GamificationConformanceRepository;
  reset(): Promise<void>;
  seedBadge(badge: Omit<BadgeDefinition, "createdAt">): Promise<void>;
  mint(key: string, amount: number): Promise<void>;
  actionCount(key: string, action: string): Promise<number>;
  uniqueCount(key: string, field: string): Promise<number>;
}
export function registerGamificationConformance(
  name: string,
  create: () => Promise<GamificationFixture>
) {
  async function fresh() {
    const fixture = await create();
    await fixture.reset();
    return fixture;
  }
  test(`${name}: scores, rank ties, pages and scope rotation preserve per-key state`, async () => {
    const { games } = await fresh();
    await games.updateScore("a", "global", 10);
    await games.updateScore("b", "global", 10);
    await games.updateScore("c", "global", 20);
    await games.updateScore("a", "weekly", 5);
    assert.equal(await games.getRank("missing", "global"), 0);
    assert.equal(await games.getRank("a", "global"), 2);
    assert.equal(await games.getRank("b", "global"), 2);
    assert.deepEqual(
      (await games.getTopN("global", 10)).map((row) => row.apiKeyId),
      ["c", "a", "b"]
    );
    assert.equal((await games.getTopN("global", -1)).length, 0);
    assert.equal((await games.getTopN("global", 10, 1))[0].apiKeyId, "a");
    assert.deepEqual(await games.getLeaderboardNeighbors("a", "global", 1), {
      above: [{ apiKeyId: "c", score: 20 }],
      below: [],
    });
    assert.deepEqual(await games.getLeaderboardNeighbors("missing", "global"), {
      above: [],
      below: [],
    });
    await games.rotateLeaderboardScope("weekly");
    assert.equal((await games.getTopN("weekly", 10)).length, 0);
    await games.updateScore("a", "weekly", 7);
    await games.rotateLeaderboardScope("weekly");
    assert.equal((await games.getTopN("weekly", 10))[0].score, 7);
  });
  test(`${name}: XP actions remain durable and aggregates derive levels from summed XP`, async () => {
    const { games, actionCount } = await fresh();
    await games.addXp("a", "request", 5);
    await games.addXp("a", "token_share", 10, JSON.stringify({ amount: 23 }));
    await games.addXp("b", "request", 50);
    assert.equal((await games.getXp("a"))?.totalXp, 15);
    assert.equal(await actionCount("a", "request"), 1);
    assert.equal(await actionCount("a", "token_share"), 23);
    assert.equal(await actionCount("b", "token_share"), 0);
    assert.equal((await games.getAggregateXp()).totalXp, 65);
    await games.updateLevel("a", 3);
    assert.equal((await games.getXp("a"))?.currentLevel, 3);
    assert.equal((await games.getXp("a"))?.totalXp, 15);
    assert.equal(await games.getXp("missing"), null);
  });
  test(`${name}: badges are idempotent before definitions and earned aggregates deduplicate`, async () => {
    const { games, seedBadge } = await fresh();
    assert.equal(await games.unlockBadge("a", "badge"), true);
    assert.equal(await games.unlockBadge("a", "badge"), false);
    assert.equal(await games.hasBadge("a", "badge"), true);
    assert.deepEqual(await games.getBadges("a"), []);
    await seedBadge({
      id: "badge",
      name: "Badge",
      description: null,
      icon: null,
      category: "test",
      rarity: "common",
      criteria: null,
      hidden: 0,
    });
    await games.unlockBadge("b", "badge");
    assert.equal((await games.getBadges("a"))[0].apiKeyId, "a");
    assert.equal((await games.getAllEarnedBadges()).length, 1);
    assert.equal((await games.getAllEarnedBadges())[0].apiKeyId, "*");
    assert.equal((await games.getBadgeDefinitions("missing")).length, 0);
  });
  test(`${name}: badge unique counts preserve scalar JSON value identity and key isolation`, async () => {
    const { games, uniqueCount } = await fresh();
    for (const provider of [1, 1.0, true, "1", 0, false, null, "openai", "openai"])
      await games.addXp("a", "request", 1, JSON.stringify({ provider }));
    await games.addXp("a", "request", 1, JSON.stringify({ other: "missing" }));
    await games.addXp("b", "request", 1, JSON.stringify({ provider: "other-key" }));
    assert.equal(await uniqueCount("a", "provider"), 4);
    assert.equal(await uniqueCount("b", "provider"), 1);
    assert.equal(await uniqueCount("missing", "provider"), 0);
    await games.addXp("complex", "request", 1, '{"provider": {"x":1.0, "text": "two words"}}');
    await games.addXp(
      "complex",
      "request",
      1,
      JSON.stringify({ provider: '{"x":1.0,"text":"two words"}' })
    );
    assert.equal(await uniqueCount("complex", "provider"), 1);
  });
  test(`${name}: ledger balance, global idempotency and per-key history preserve affected state`, async () => {
    const { games, mint } = await fresh();
    await mint("a", 100);
    assert.deepEqual(await games.transferTokens("a", "b", 30, "transfer", "once"), {
      success: true,
    });
    assert.deepEqual(await games.transferTokens("a", "b", 30, "transfer", "once"), {
      success: true,
    });
    assert.equal(await games.getBalance("a"), 70);
    assert.equal(await games.getBalance("b"), 30);
    assert.equal(await games.getBalance("c"), 0);
    assert.deepEqual(await games.getHistory("c", 10), []);
    assert.deepEqual(await games.transferTokens("b", "c", 100, "transfer", "insufficient"), {
      success: false,
      error: "insufficient_balance",
    });
    assert.equal((await games.getHistory("b", 10)).length, 1);
  });
  test(`${name}: invite redemption is bounded and server hashes remain internal`, async () => {
    const { games } = await fresh();
    await games.createInviteToken("id", "code", "hash", "a", "https://example.test", 1);
    assert.equal((await games.getInviteByCode("code"))?.createdBy, "a");
    assert.equal(await games.redeemInvite("code", "b"), true);
    assert.equal(await games.redeemInvite("code", "c"), false);
    await games.createInviteToken("id2", "code2", "hash2", "b");
    await games.revokeInvite("id2");
    assert.equal(await games.redeemInvite("code2", "c"), false);
    assert.equal(await games.getInviteByCode("missing"), null);
    await games.connectServer("server", "name", "https://example.test", "secret-hash");
    assert.equal((await games.getConnectedServerByKeyHash("secret-hash"))?.id, "server");
    const server = (await games.listServers())[0];
    assert.equal(Object.hasOwn(server, "apiKeyHash"), false);
    await games.disconnectServer("server");
    assert.equal(await games.getConnectedServerByKeyHash("secret-hash"), undefined);
    await games.connectServer("server", "new", "https://other.test", "new-hash");
    assert.equal((await games.listServers())[0].status, "connected");
  });
}
