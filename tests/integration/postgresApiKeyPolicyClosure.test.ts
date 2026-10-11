import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import { withPostgresCatalogSnapshot } from "../../src/lib/db/postgres/catalogSnapshot.ts";
import { PostgresTokenLimitRepository } from "../../src/lib/db/postgres/tokenLimitRepository.ts";
import { PostgresDomainStateRepository } from "../../src/lib/db/postgres/domainStateRepository.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test(
  "native public API-key pre-policy awaits all durable validators without changing scope or error policies",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async (t) => {
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "API_KEY_SECRET",
      "REDIS_URL",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    delete process.env.REDIS_URL;
    const plane = await initializePostgresControlPlane({
      connection: await testPostgresOptions(),
      runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      logger: { warn() {} },
    });
    process.env.API_KEY_SECRET = plane.bootstrapSecrets.apiKeySecret;
    const database = await createTestPostgresDatabase();
    const prefix = `native-pre-policy-${randomUUID()}`;
    const ids: string[] = [];
    try {
      const { enforceApiKeyPolicy } = await import("../../src/shared/utils/apiKeyPolicy.ts");
      const { setBudgetAsync } = await import("../../src/domain/costRules.ts");
      const { upsertKeyQuotaLimitsAsync, incrementKeyQuotaCounterAsync } =
        await import("../../src/lib/db/keyQuota.ts");
      const { resetWindowIfElapsed } = await import("../../src/lib/db/tokenLimits.ts");
      await plane.run((binding) =>
        withPostgresCatalogSnapshot(binding.database!, async () => {
          const tokenOwner = new PostgresTokenLimitRepository(binding.database!);
          const spendOwner = new PostgresDomainStateRepository(binding.database!);
          const createKey = async (label: string) => {
            const key = await binding.apiKeys.createApiKey(
              `${prefix}-${label}`,
              "native-pre-policy-test"
            );
            ids.push(key.id);
            return key;
          };
          const request = (key: string) =>
            new Request("http://localhost/api/v1/chat/completions", {
              method: "POST",
              headers: { Authorization: `Bearer ${key}` },
            });
          const body = (response: Response) =>
            response.json() as Promise<{
              error: { code?: string; message: string; retry_after?: number; reset_at?: string };
            }>;
          const scoped = (key: string, model = "requested/model") =>
            enforceApiKeyPolicy(request(key), model, { meteredBudget: "defer-to-candidate" });
          const exhausted = async (
            id: string,
            scopeType: "global" | "model" | "provider",
            scopeValue?: string
          ) => {
            const limit = await tokenOwner.upsert({
              apiKeyId: id,
              scopeType,
              scopeValue,
              tokenLimit: 10,
              resetInterval: "daily",
              resetTime: "00:00",
            });
            await tokenOwner.increment(limit.id, resetWindowIfElapsed(limit).windowStart, 10);
            return limit;
          };
          const withBlockedTable = async (
            table: "api_key_token_limits" | "api_key_quota_limits" | "domain_budgets",
            run: () => Promise<void>
          ) => {
            const entered = barrier(),
              release = barrier();
            const held = database.transaction(async (tx) => {
              await tx.query(`LOCK TABLE omniroute.${table} IN ACCESS EXCLUSIVE MODE`);
              entered.resolve();
              await release.promise;
            });
            try {
              await entered.promise;
              await run();
            } finally {
              release.resolve();
              await held;
            }
          };

          await t.test(
            "metadata-backed native key without token limits reaches the real public pre-policy allow result",
            async () => {
              const key = await createKey("unlimited");
              const result = await scoped(key.key);
              assert.equal(result.apiKeyInfo?.id, key.id);
              assert.equal(result.rejection, null);
            }
          );
          await t.test(
            "global and exact requested-model token limits retain 429/reset/error fields",
            async () => {
              for (const scope of ["global", "model"] as const) {
                const key = await createKey(`token-${scope}`);
                await exhausted(key.id, scope, scope === "model" ? "requested/model" : undefined);
                const result = await scoped(key.key);
                assert.equal(result.rejection?.status, 429);
                const rejection = await body(result.rejection!);
                assert.equal(rejection.error.code, "token_limit_exceeded");
                assert.match(rejection.error.message, /Token limit exceeded/);
                assert.equal(typeof rejection.error.retry_after, "number");
                assert.equal(typeof rejection.error.reset_at, "string");
                assert.equal(
                  result.rejection!.headers.get("Retry-After"),
                  String(rejection.error.retry_after)
                );
                if (scope === "model")
                  assert.equal((await scoped(key.key, "requested/other")).rejection, null);
              }
            }
          );
          await t.test(
            "provider-scoped token limits remain a resolved-target concern, not requested-model pre-policy",
            async () => {
              const key = await createKey("provider-scope");
              await exhausted(key.id, "provider", "requested");
              assert.equal((await scoped(key.key)).rejection, null);
            }
          );
          await t.test(
            "healthy native per-key quota permits below its threshold and rejects reached quota",
            async () => {
              const key = await createKey("quota");
              await upsertKeyQuotaLimitsAsync(key.id, { rpmLimit: 1 });
              assert.equal((await scoped(key.key)).rejection, null);
              await incrementKeyQuotaCounterAsync(key.id, "rpm", 2);
              const result = await scoped(key.key);
              assert.equal(result.rejection?.status, 429);
              assert.match(
                (await body(result.rejection!)).error.message,
                /Request-per-minute quota exceeded/
              );
            }
          );
          await t.test(
            "non-deferred native budget reads durable spend; CHAT deferral retains candidate-only semantics",
            async () => {
              const key = await createKey("budget");
              await setBudgetAsync(key.id, {
                dailyLimitUsd: 1,
                resetInterval: "daily",
                resetTime: "00:00",
              });
              assert.equal(
                (await enforceApiKeyPolicy(request(key.key), "requested/model")).rejection,
                null
              );
              await spendOwner.saveCostEntry(key.id, 2);
              const result = await enforceApiKeyPolicy(request(key.key), "requested/model");
              assert.equal(result.rejection?.status, 429);
              assert.equal((await body(result.rejection!)).error.code, "budget_exceeded");
              assert.equal((await scoped(key.key)).rejection, null);
            }
          );
          await t.test(
            "native token and non-deferred budget database failures retain policy 503",
            async () => {
              const key = await createKey("fail-closed");
              await withBlockedTable("api_key_token_limits", async () => {
                const result = await scoped(key.key);
                assert.equal(result.rejection?.status, 503);
                assert.equal(
                  (await body(result.rejection!)).error.message,
                  "Token limit policy unavailable"
                );
              });
              await withBlockedTable("domain_budgets", async () => {
                const result = await enforceApiKeyPolicy(request(key.key), "requested/model");
                assert.equal(result.rejection?.status, 503);
                assert.equal(
                  (await body(result.rejection!)).error.message,
                  "Budget policy unavailable"
                );
                assert.equal(
                  (await scoped(key.key)).rejection,
                  null,
                  "deferred CHAT must not query budget owner"
                );
              });
            }
          );
          await t.test(
            "per-key quota infrastructure failure preserves documented B16 fail-open",
            async () => {
              const key = await createKey("quota-fail-open");
              await upsertKeyQuotaLimitsAsync(key.id, { rpmLimit: 1 });
              await incrementKeyQuotaCounterAsync(key.id, "rpm", 2);
              await withBlockedTable("api_key_quota_limits", async () => {
                assert.equal((await scoped(key.key)).rejection, null);
              });
            }
          );
        })
      );
    } finally {
      await plane.close();
      try {
        await database.transaction(async (tx) => {
          await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
          await tx.query("SELECT pg_advisory_xact_lock(18765,8)");
          await tx.query("SELECT pg_advisory_xact_lock(18765,9)");
          for (const table of [
            "api_key_token_limits",
            "api_key_quota_limits",
            "api_key_quota_counters",
            "domain_budgets",
            "domain_budget_reset_logs",
            "domain_cost_history",
            "domain_cost_receipts",
            "request_cost_ledger",
          ])
            await tx.query(`DELETE FROM omniroute.${table} WHERE api_key_id=ANY($1::text[])`, [
              ids,
            ]);
          await tx.query("DELETE FROM omniroute.api_keys WHERE id=ANY($1::text[])", [ids]);
        });
      } finally {
        await database.close();
        for (const [name, value] of previous) {
          if (value === undefined) delete process.env[name];
          else process.env[name] = value;
        }
      }
    }
  }
);
