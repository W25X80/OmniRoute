import assert from "node:assert/strict";
import test from "node:test";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { getAntigravityLocalUsageUnitsAsync } from "../../src/lib/db/antigravityUsage.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import {
  registerAntigravityLocalUsageConformance,
  normalizedRow,
  CONNECTION,
  MODEL,
  RESET,
  type LocalUsageFixture,
} from "../helpers/persistence/antigravityLocalUsageConformance.ts";

const PROJECT = "l61-local-usage-project";

let database: PostgresDatabase;
let binding: PostgresControlPlaneBinding;
let fixture: LocalUsageFixture;
if (!postgresTestsEnabled)
  test("native Antigravity local usage requires isolated TLS", { skip: true }, () => {});
else {
  test.before(async () => {
    database = await createTestPostgresDatabase();
    await migratePostgresSchema(database);
    binding = {
      database,
      databaseIdentity: {},
      assertReady() {},
    } as unknown as PostgresControlPlaneBinding;
    fixture = {
      async seed(inputs) {
        await database.transaction(async (tx) => {
          for (const input of inputs) {
            const row = normalizedRow(input);
            await tx.query(
              "INSERT INTO omniroute.usage_history(provider,model,connection_id,timestamp,tokens_input,tokens_output,tokens_reasoning,tokens_cache_read,tokens_cache_creation,success) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
              [
                row.provider,
                row.model,
                row.connectionId,
                row.timestamp,
                row.input,
                row.output,
                row.reasoning,
                row.cacheRead,
                row.cacheCreation,
                row.success,
              ]
            );
          }
        });
      },
      units: (...args) =>
        withPostgresControlPlane(binding, () => getAntigravityLocalUsageUnitsAsync(...args)),
    };
  });
  test.after(async () => {
    await database?.close();
  });
  async function fresh() {
    await database.transaction((tx) =>
      tx.query("DELETE FROM omniroute.usage_history WHERE connection_id LIKE 'l50-%'")
    );
    return fixture;
  }
  registerAntigravityLocalUsageConformance("PostgreSQL", fresh);
  test("native Antigravity caller applies finite local quota, clamps usage and preserves upstream bypasses", async () => {
    await fresh();
    await fixture.seed([{ input: 1000, output: 1500, reasoning: 500 }]);
    const { getAntigravityUsage } = await withPostgresControlPlane(
      binding,
      () => import("../../open-sse/services/usage/antigravity.ts")
    );
    const originalFetch = globalThis.fetch;
    const oldMode = process.env.ANTIGRAVITY_CREDITS;
    let live = false;
    let fraction = 1;
    let reset: string | null = RESET;
    const requests: Array<{ path: string; method?: string; project: unknown; live: boolean }> = [];
    globalThis.fetch = async (input, options) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
      const path = new URL(url).pathname;
      const body = JSON.parse(String(options?.body || "{}")) as Record<string, unknown>;
      requests.push({ path, method: options?.method, project: body.project, live });
      if (path === "/v1internal:retrieveUserQuotaSummary")
        return new Response("{}", { status: 404 });
      if (path === "/v1internal:retrieveUserQuota")
        return live
          ? Response.json({ buckets: [{ modelId: MODEL, remainingFraction: 1, resetTime: RESET }] })
          : new Response("{}", { status: 404 });
      if (path === "/v1internal:fetchAvailableModels")
        return Response.json({
          models: { [MODEL]: { quotaInfo: { remainingFraction: fraction, resetTime: reset } } },
        });
      if (path === "/v1internal:loadCodeAssist") return Response.json({});
      throw new Error("Unexpected Antigravity fixture endpoint");
    };
    process.env.ANTIGRAVITY_CREDITS = "off";
    try {
      await withPostgresControlPlane(binding, async () => {
        const read = () =>
          getAntigravityUsage(
            "antigravity",
            "l50-native-local-usage-token",
            {},
            PROJECT,
            CONNECTION,
            { forceRefresh: true }
          );
        const first = await read();
        assert.ok(first.quotas);
        assert.equal(first.quotas[MODEL].used, 3);
        assert.equal(first.quotas[MODEL].remainingPercentage, 99.7);
        assert.equal(first.quotas[MODEL].quotaSource, "localUsageHistory");
        await fixture.seed([{ input: 2_000_000 }]);
        const capped = await read();
        assert.ok(capped.quotas);
        assert.equal(capped.quotas[MODEL].used, 1000);
        assert.equal(capped.quotas[MODEL].remainingPercentage, 0);
        const beforeLive = requests.length;
        live = true;
        const upstream = await read();
        assert.ok(upstream.quotas);
        assert.equal(upstream.quotas[MODEL].quotaSource, "retrieveUserQuota");
        assert.equal(upstream.quotas[MODEL].used, 0);
        const liveRequests = requests
          .slice(beforeLive)
          .filter((request) => request.path === "/v1internal:retrieveUserQuota");
        assert.equal(
          liveRequests.length,
          1,
          "a live quota response must be requested, not inferred"
        );
        assert.equal(liveRequests[0].method, "POST");
        assert.equal(liveRequests[0].project, PROJECT);
        assert.equal(liveRequests[0].live, true);
        live = false;
        fraction = 0.5;
        const partial = await read();
        assert.ok(partial.quotas);
        assert.equal(partial.quotas[MODEL].used, 500);
        assert.equal(partial.quotas[MODEL].quotaSource, "fetchAvailableModels");
        fraction = 1;
        reset = null;
        const unlimited = await read();
        assert.ok(unlimited.quotas);
        assert.equal(unlimited.quotas[MODEL].unlimited, true);
        assert.equal(unlimited.quotas[MODEL].used, 0);
        reset = RESET;
        await fixture.seed([{ input: Number.MAX_SAFE_INTEGER }]);
        const failed = await read();
        assert.equal("quotas" in failed, false);
        for (const request of requests) {
          assert.ok(
            [
              "/v1internal:loadCodeAssist",
              "/v1internal:fetchAvailableModels",
              "/v1internal:retrieveUserQuota",
              "/v1internal:retrieveUserQuotaSummary",
            ].includes(request.path)
          );
          assert.equal(request.method, "POST");
          if (request.path !== "/v1internal:loadCodeAssist") assert.equal(request.project, PROJECT);
        }
        assert.match(
          failed.message || "",
          /Antigravity error: Usage value exceeds the supported numeric range/
        );
      });
    } finally {
      globalThis.fetch = originalFetch;
      if (oldMode === undefined) delete process.env.ANTIGRAVITY_CREDITS;
      else process.env.ANTIGRAVITY_CREDITS = oldMode;
    }
  });
  test("native Antigravity local SUM rejects unsupported stored magnitude rather than returning zero", async () => {
    await fresh();
    await database.transaction((tx) =>
      tx.query(
        "INSERT INTO omniroute.usage_history(provider,model,connection_id,timestamp,tokens_input,success) VALUES('antigravity',$1,$2,$3,$4::bigint,1)",
        [MODEL, CONNECTION, "2026-10-11T10:00:00.000Z", "9007199254740993"]
      )
    );
    await assert.rejects(
      fixture.units("antigravity", CONNECTION, MODEL, RESET),
      /supported numeric range/
    );
  });
}
