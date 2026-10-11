import assert from "node:assert/strict";
import test from "node:test";
import { createPostgresKnowledgeRepository } from "../../src/lib/db/postgres/knowledgeRepository.ts";
import { createPostgresGamificationRepository } from "../../src/lib/db/postgres/gamificationRepository.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { registerGamificationConformance } from "../helpers/persistence/gamificationRepositoryConformance.ts";

let database: PostgresDatabase;
let games: ReturnType<typeof createPostgresGamificationRepository>;
let knowledge: ReturnType<typeof createPostgresKnowledgeRepository>;
async function reset() {
  await database.transaction(async (tx) => {
    await tx.query("SELECT pg_advisory_xact_lock(18765,16)");
    await tx.query("SELECT pg_advisory_xact_lock(18765,17)");
    await tx.query(
      "TRUNCATE omniroute.leaderboard,omniroute.user_levels,omniroute.badge_definitions,omniroute.user_badges,omniroute.xp_audit_log,omniroute.xp_action_counts,omniroute.token_ledger,omniroute.invite_tokens,omniroute.community_servers RESTART IDENTITY"
    );
    await tx.query("DELETE FROM omniroute.key_value WHERE namespace=ANY($1::text[])", [
      ["notion", "obsidian", "local_corpus", "conductor", "sync", "gamification:streaks"],
    ]);
  });
}
if (!postgresTestsEnabled)
  test("PostgreSQL knowledge requires isolated TLS runner", { skip: true }, () => {});
else {
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-knowledge-conformance-key";
  test.before(async () => {
    database = await createTestPostgresDatabase({
      maxConnections: 8,
      lockTimeoutMs: 2000,
    });
    await migratePostgresSchema(database);
    games = createPostgresGamificationRepository(database);
    knowledge = createPostgresKnowledgeRepository(database);
  });
  test.after(async () => {
    await database?.close();
  });
  registerGamificationConformance("PostgreSQL", async () => ({
    games,
    reset,
    async seedBadge(badge) {
      await games.seedBadges([badge]);
    },
    async mint(key, amount) {
      await database.transaction((tx) =>
        tx.query(
          "INSERT INTO omniroute.token_ledger(from_api_key_id,to_api_key_id,amount,reason,idempotency_key) VALUES('mint',$1,$2,'fixture',$3)",
          [key, amount, `mint:${key}`]
        )
      );
    },
    actionCount: games.getActionCount,
    uniqueCount: games.getUniqueCount,
  }));
  test("PostgreSQL knowledge token replacement rules preserve imported bytes and new credential encryption", async () => {
    await reset();
    await database.transaction((tx) =>
      tx.query(
        "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('notion','integration_token',$1)",
        [JSON.stringify("legacy-token")]
      )
    );
    await knowledge.writeSecret("notion", "integration_token", "ignored", true);
    assert.equal(await knowledge.readSecret("notion", "integration_token"), "legacy-token");
    const raw = await database.transaction((tx) =>
      tx.query<{ value: string }>(
        "SELECT value FROM omniroute.key_value WHERE namespace='notion' AND key='integration_token'"
      )
    );
    assert.equal(raw.rows[0].value, JSON.stringify("legacy-token"));
    await knowledge.remove("notion", "integration_token");
    await knowledge.writeSecret("notion", "integration_token", "new-token", true);
    const encrypted = await database.transaction((tx) =>
      tx.query<{ value: string }>(
        "SELECT value FROM omniroute.key_value WHERE namespace='notion' AND key='integration_token'"
      )
    );
    assert.match(String(JSON.parse(encrypted.rows[0].value)), /^enc:v1:/);
    assert.equal(await knowledge.readSecret("notion", "integration_token"), "new-token");
    await knowledge.write("local_corpus", "root_path", "/approved/corpus");
    await knowledge.write("conductor", "last_event_id", "17");
    assert.equal(await knowledge.read("local_corpus", "root_path"), "/approved/corpus");
    assert.equal(await knowledge.read("conductor", "last_event_id"), "17");
    assert.equal(await knowledge.read("obsidian", "root_path"), null);
  });
  test("PostgreSQL metadata batches fail atomically and preserve coherent secret snapshots", async () => {
    await reset();
    await knowledge.writeMany("obsidian", [
      { key: "api_key", value: "token-one", secret: true },
      { key: "base_url", value: "https://one.test" },
    ]);
    const before = await knowledge.readMany("obsidian", ["api_key", "base_url"]);
    assert.equal(knowledge.decodeSecret(before.api_key), "token-one");
    await database.transaction(async (tx) => {
      await tx.query(
        "CREATE FUNCTION omniroute.reject_knowledge_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.key='base_url' THEN RAISE EXCEPTION 'fixture rejection'; END IF; RETURN NEW; END $$"
      );
      await tx.query(
        "CREATE TRIGGER reject_knowledge_test BEFORE UPDATE ON omniroute.key_value FOR EACH ROW EXECUTE FUNCTION omniroute.reject_knowledge_test()"
      );
    });
    try {
      await assert.rejects(
        knowledge.writeMany("obsidian", [
          { key: "api_key", value: "token-two", secret: true },
          { key: "base_url", value: "https://two.test" },
        ])
      );
      assert.deepEqual(await knowledge.readMany("obsidian", ["api_key", "base_url"]), before);
    } finally {
      await database.transaction(async (tx) => {
        await tx.query("DROP TRIGGER reject_knowledge_test ON omniroute.key_value");
        await tx.query("DROP FUNCTION omniroute.reject_knowledge_test()");
      });
    }
  });
  test("PostgreSQL concurrent awards, ledger spending and bounded invites cannot lose or overdraw state", async () => {
    await reset();
    await Promise.all(Array.from({ length: 6 }, () => games.addXp("a", "request", 5)));
    assert.equal((await games.getXp("a"))?.totalXp, 30);
    assert.equal(await games.getActionCount("a", "request"), 6);
    await database.transaction((tx) =>
      tx.query(
        "INSERT INTO omniroute.token_ledger(from_api_key_id,to_api_key_id,amount,idempotency_key) VALUES('mint','a',100,'fund')"
      )
    );
    const transfers = await Promise.all([
      games.transferTokens("a", "b", 80, "transfer", "one"),
      games.transferTokens("a", "c", 80, "transfer", "two"),
    ]);
    assert.equal(transfers.filter((result) => result.success).length, 1);
    assert.equal(await games.getBalance("a"), 20);
    await games.createInviteToken("id", "code", "hash", "a", undefined, 1);
    assert.equal(
      (
        await Promise.all([games.redeemInvite("code", "b"), games.redeemInvite("code", "c")])
      ).filter(Boolean).length,
      1
    );
  });
  test("PostgreSQL rejected compound XP write leaves no partial audit, count or level state", async () => {
    await reset();
    await database.transaction(async (tx) => {
      await tx.query(
        "CREATE FUNCTION omniroute.reject_xp_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rejection'; END $$"
      );
      await tx.query(
        "CREATE TRIGGER reject_xp_test BEFORE INSERT ON omniroute.xp_action_counts FOR EACH ROW EXECUTE FUNCTION omniroute.reject_xp_test()"
      );
    });
    try {
      await assert.rejects(games.addXp("a", "request", 5));
      assert.equal(await games.getXp("a"), null);
      assert.equal(await games.getActionCount("a", "request"), 0);
      const audit = await database.transaction((tx) =>
        tx.query<{ total: string }>(
          "SELECT COUNT(*) AS total FROM omniroute.xp_audit_log WHERE api_key_id='a'"
        )
      );
      assert.equal(Number(audit.rows[0].total), 0);
    } finally {
      await database.transaction(async (tx) => {
        await tx.query("DROP TRIGGER reject_xp_test ON omniroute.xp_action_counts");
        await tx.query("DROP FUNCTION omniroute.reject_xp_test()");
      });
    }
  });
  test("PostgreSQL daily reward, streak and badge ownership have exactly one winning transition", async () => {
    await reset();
    const awards = await Promise.all(
      Array.from({ length: 4 }, () => games.awardXpOncePerDay("a", "streak_bonus", 10))
    );
    assert.equal(awards.filter(Boolean).length, 1);
    assert.equal((await games.getXp("a"))?.totalXp, 10);
    const badges = await Promise.all(
      Array.from({ length: 4 }, () => games.unlockBadge("a", "badge"))
    );
    assert.equal(badges.filter(Boolean).length, 1);
    const derived = await Promise.all(
      Array.from({ length: 4 }, () =>
        games.advanceStreak("a", (raw) =>
          raw ? { result: false } : { value: JSON.stringify({ day: "today" }), result: true }
        )
      )
    );
    assert.equal(derived.filter(Boolean).length, 1);
    assert.equal(await games.readStreak("a"), JSON.stringify({ day: "today" }));
  });
  test("PostgreSQL Obsidian per-key resolution preserves enabled credential precedence and isolation", async () => {
    await reset();
    const { initializePostgresControlPlane } =
      await import("../../src/lib/db/postgres/controlPlane.ts");
    const { setApiKeyContextSourceAsync } =
      await import("../../src/lib/db/apiKeyContextSources.ts");
    const { getObsidianConfigForApiKeyAsync, saveObsidianConnectionAsync } =
      await import("../../src/lib/db/obsidian.ts");
    const plane = await initializePostgresControlPlane({
      connection: await testPostgresOptions({ maxConnections: 8 }),
      logger: { warn() {} },
      runtime: {
        applyRuntimeSettings() {},
        scheduleCatalogRevalidationAfterInit() {},
      },
    });
    try {
      await plane.run(async (repositories) => {
        const first = await repositories.apiKeys.createApiKey("knowledge-one", "fixture");
        const second = await repositories.apiKeys.createApiKey("knowledge-two", "fixture");
        await saveObsidianConnectionAsync("global", "https://global.test");
        await setApiKeyContextSourceAsync(first.id, "obsidian", {
          token: "one",
          baseUrl: "https://one.test",
          enabled: true,
        });
        await setApiKeyContextSourceAsync(second.id, "obsidian", {
          token: "two",
          baseUrl: "https://two.test",
          enabled: true,
        });
        assert.deepEqual(await getObsidianConfigForApiKeyAsync(first.id), {
          token: "one",
          baseUrl: "https://one.test",
          vaultPath: null,
          source: "api_key",
        });
        assert.deepEqual(await getObsidianConfigForApiKeyAsync(second.id), {
          token: "two",
          baseUrl: "https://two.test",
          vaultPath: null,
          source: "api_key",
        });
        await setApiKeyContextSourceAsync(second.id, "obsidian", { enabled: false });
        assert.equal((await getObsidianConfigForApiKeyAsync(second.id)).token, "global");
        await repositories.apiKeys.deleteApiKey(first.id);
        await repositories.apiKeys.deleteApiKey(second.id);
      });
    } finally {
      await plane.close();
    }
  });
}
