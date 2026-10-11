import assert from "node:assert/strict";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { PostgresDomainStateRepository } from "../../src/lib/db/postgres/domainStateRepository.ts";
import { PostgresTokenLimitRepository } from "../../src/lib/db/postgres/tokenLimitRepository.ts";
import { TokenLimitConflictError, TokenLimitNotFoundError } from "../../src/lib/db/tokenLimits.ts";

test(
  "native PostgreSQL enforcement preserves atomic counters and durable policy state",
  {
    skip: !postgresTestsEnabled,
  },
  async (t) => {
    const database = await createTestPostgresDatabase({ maxConnections: 12 });
    try {
      await migratePostgresSchema(database);
      await database.transaction(async (tx) => {
        await tx.query("TRUNCATE omniroute.api_key_token_limits CASCADE");
        await tx.query(
          "TRUNCATE omniroute.domain_lockout_state,omniroute.domain_fallback_chains,omniroute.domain_circuit_breakers"
        );
        await tx.query("DELETE FROM omniroute.usage_history WHERE api_key_id='token-native-key'");
      });
      const owner = new PostgresTokenLimitRepository(database);
      const domain = new PostgresDomainStateRepository(database);

      await t.test("per-window uniqueness keeps IDs and rejects targeted conflicts", async () => {
        const monthly = await owner.upsert({
          apiKeyId: "token-native-key",
          scopeType: "global",
          tokenLimit: 100,
        });
        const daily = await owner.upsert({
          apiKeyId: "token-native-key",
          scopeType: "global",
          tokenLimit: 10,
          resetInterval: "daily",
        });
        const merged = await owner.upsert({
          apiKeyId: "token-native-key",
          scopeType: "global",
          tokenLimit: 200,
        });
        assert.equal(merged.id, monthly.id);
        assert.equal((await owner.list("token-native-key")).length, 2);
        await assert.rejects(
          owner.upsert({
            id: daily.id,
            apiKeyId: "token-native-key",
            scopeType: "global",
            tokenLimit: 2,
          }),
          TokenLimitConflictError
        );
        await assert.rejects(
          owner.upsert({
            id: "missing",
            apiKeyId: "token-native-key",
            scopeType: "model",
            scopeValue: "model-a",
            tokenLimit: 2,
          }),
          TokenLimitNotFoundError
        );
        await owner.delete(daily.id);
        await owner.delete(monthly.id);
      });

      await t.test(
        "parallel cold reads seed once and include cache tokens only in input",
        async () => {
          const limit = await owner.upsert({
            apiKeyId: "token-native-key",
            scopeType: "global",
            tokenLimit: 1000,
          });
          const now = Date.now();
          await database.transaction(async (tx) => {
            await tx.query(
              `INSERT INTO omniroute.usage_history
          (api_key_id,provider,model,timestamp,tokens_input,tokens_output,tokens_reasoning,tokens_cache_read,tokens_cache_creation)
          VALUES($1,'provider-a','model-a',$2,10,5,3,7,2)`,
              [limit.apiKeyId, new Date(now).toISOString()]
            );
          });
          const reads = await Promise.all(
            Array.from({ length: 6 }, () => owner.currentUsage(limit, now))
          );
          assert.deepEqual(reads, [18, 18, 18, 18, 18, 18]);
          await Promise.all(
            Array.from({ length: 6 }, () =>
              owner.increment(
                limit.id,
                String(
                  new Date(
                    Date.UTC(new Date(now).getUTCFullYear(), new Date(now).getUTCMonth(), 1)
                  ).getTime()
                ),
                4
              )
            )
          );
          assert.equal(await owner.currentUsage(limit, now), 42);
          await owner.delete(limit.id);
        }
      );

      await t.test("existing zero counters do not reseed historical usage", async () => {
        const limit = await owner.upsert({
          apiKeyId: "token-native-key",
          scopeType: "global",
          tokenLimit: 1000,
        });
        const now = Date.now();
        const { resetWindowIfElapsed } = await import("../../src/lib/db/tokenLimits.ts");
        await owner.increment(limit.id, resetWindowIfElapsed(limit, now).windowStart, 0);
        assert.equal(await owner.currentUsage(limit, now), 0);
        await owner.delete(limit.id);
      });

      await t.test(
        "cold seed marks history, then delayed receipt callbacks do not count it twice",
        async () => {
          const limit = await owner.upsert({
            apiKeyId: "token-native-key",
            scopeType: "model",
            scopeValue: "receipt-model",
            tokenLimit: 1000,
          });
          const now = Date.now();
          const ids = await database.transaction(async (tx) => {
            const result = await tx.query<{ id: string }>(
              `INSERT INTO omniroute.usage_history
          (api_key_id,provider,model,timestamp,tokens_input,tokens_output,tokens_reasoning)
          VALUES($1,'provider-a','receipt-model',$2,10,0,0),($1,'provider-a','receipt-model',$2,20,0,0)
          RETURNING id`,
              [limit.apiKeyId, new Date(now).toISOString()]
            );
            return result.rows.map((row) => Number(row.id));
          });
          await Promise.all([
            owner.recordUsage(limit.apiKeyId, "provider-a", "receipt-model", 10, now, ids[0]),
            owner.recordUsage(limit.apiKeyId, "provider-a", "receipt-model", 20, now, ids[1]),
          ]);
          assert.equal(await owner.currentUsage(limit, now), 30);
          await owner.recordUsage(limit.apiKeyId, "provider-a", "receipt-model", 10, now, ids[0]);
          assert.equal(await owner.currentUsage(limit, now), 30);
          await owner.delete(limit.id);
        }
      );

      await t.test(
        "late event accounting uses the event window and preserves its receipt after retention",
        async () => {
          const limit = await owner.upsert({
            apiKeyId: "token-native-key",
            scopeType: "model",
            scopeValue: "late-model",
            tokenLimit: 1000,
            resetInterval: "daily",
          });
          const occurredAt = Date.UTC(2026, 9, 3, 12);
          const id = await database.transaction(async (tx) =>
            Number(
              (
                await tx.query<{ id: string }>(
                  `INSERT INTO omniroute.usage_history
        (api_key_id,provider,model,timestamp,tokens_input,tokens_output,tokens_reasoning)
        VALUES($1,'provider-a','late-model',$2,8,0,0) RETURNING id`,
                  [limit.apiKeyId, new Date(occurredAt).toISOString()]
                )
              ).rows[0].id
            )
          );
          await owner.recordUsage(
            limit.apiKeyId,
            "provider-a",
            "late-model",
            8,
            occurredAt + 2 * 86_400_000,
            id
          );
          assert.equal(await owner.readUsage(limit, occurredAt), 8);
          assert.equal(await owner.readUsage(limit, occurredAt + 2 * 86_400_000), 0);
          await database.transaction(async (tx) => {
            await tx.query("DELETE FROM omniroute.usage_history WHERE id=$1", [id]);
          });
          await owner.recordUsage(limit.apiKeyId, "provider-a", "late-model", 8, Date.now(), id);
          assert.equal(await owner.readUsage(limit, occurredAt), 8);
          await owner.delete(limit.id);
        }
      );

      await t.test("fallback and circuit state survive a repository reload", async () => {
        await domain.saveFallbackChain("policy-model", [
          { provider: "provider-a", priority: 1, enabled: true },
        ]);
        await domain.saveCircuitBreakerState("policy-provider", {
          state: "OPEN",
          failureCount: 5,
          lastFailureTime: 123,
          options: { lastFailureKind: "transient" },
        });
        const reloaded = new PostgresDomainStateRepository(database);
        assert.deepEqual(await reloaded.loadFallbackChain("policy-model"), [
          { provider: "provider-a", priority: 1, enabled: true },
        ]);
        assert.deepEqual(await reloaded.loadCircuitBreakerState("policy-provider"), {
          state: "OPEN",
          failureCount: 5,
          lastFailureTime: 123,
          options: { lastFailureKind: "transient" },
        });
      });
    } finally {
      await database.close();
    }
  }
);
