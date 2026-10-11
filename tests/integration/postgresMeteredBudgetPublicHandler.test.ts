import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import type { PostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";

test(
  "native public chat rejects exhausted metered budget before selecting credentials or cooling a connection",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async () => {
    const names = [
      "DATA_DIR",
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "INITIAL_PASSWORD",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    const directory = mkdtempSync(join(tmpdir(), "omniroute-native-metered-gate-"));
    process.env.DATA_DIR = directory;
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-native-metered-gate-key";
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    delete process.env.INITIAL_PASSWORD;
    const chatUrl = new URL("../../src/sse/handlers/chat.ts", import.meta.url).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/nativeMeteredBudgetGateFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (
          context.parentURL === chatUrl &&
          ["../services/auth", "../services/auth.ts"].includes(specifier)
        )
          return { url: fixtureUrl, shortCircuit: true };
        return nextResolve(specifier, context);
      },
    });
    const originalFetch = globalThis.fetch;
    let fetches = 0;
    globalThis.fetch = async () => {
      fetches++;
      throw new Error("Unexpected outbound fetch in denied budget scenario");
    };
    let plane: PostgresControlPlane | undefined;
    let keyId: string | undefined;
    const connectionIds: string[] = [];
    let meteredGateFixture:
      | typeof import("../helpers/persistence/nativeMeteredBudgetGateFixture.ts").meteredGateFixture
      | undefined;
    try {
      const { initializePostgresControlPlane } =
        await import("../../src/lib/db/postgres/controlPlane.ts");
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      const { withPostgresCatalogSnapshot } =
        await import("../../src/lib/db/postgres/catalogSnapshot.ts");
      await plane.run(async (binding) => {
        const { initializeCircuitBreakerPersistence } =
          await import("../../src/shared/utils/circuitBreaker.ts");
        await initializeCircuitBreakerPersistence(binding.database!);
        const key = await withPostgresCatalogSnapshot(binding.database!, async () => {
          const { createApiKey } = await import("../../src/lib/db/apiKeys.ts");
          return createApiKey(`native-metered-${randomUUID()}`, "native-metered-test");
        });
        const { setBudgetAsync } = await import("../../src/domain/costRules.ts");
        keyId = key.id;
        for (const provider of ["openai", "claude"]) {
          await withPostgresCatalogSnapshot(binding.database!, async () => {
            const { createProviderConnection } = await import("../../src/lib/db/providers.ts");
            const connection = await createProviderConnection({
              provider,
              authType: "apikey",
              name: `native-metered-${randomUUID()}`,
              apiKey: "isolated-placeholder-provider-key",
              isActive: true,
            });
            assert.equal(typeof connection.id, "string");
            connectionIds.push(String(connection.id));
          });
        }
        await setBudgetAsync(key.id, { dailyLimitUsd: 1, resetInterval: "daily" });
        await binding.database!.transaction((tx) =>
          tx.query(
            "INSERT INTO omniroute.domain_cost_history(api_key_id,cost,timestamp) VALUES($1,$2,$3)",
            [key.id, 25, Date.now()]
          )
        );
        const { handleChat } = await withPostgresCatalogSnapshot(binding.database!, async () => {
          ({ meteredGateFixture } =
            await import("../helpers/persistence/nativeMeteredBudgetGateFixture.ts"));
          return import("../../src/sse/handlers/chat.ts");
        });
        const request = (model: string) =>
          new Request("http://localhost/v1/chat/completions", {
            method: "POST",
            headers: { "content-type": "application/json", authorization: `Bearer ${key.key}` },
            body: JSON.stringify({
              model,
              stream: false,
              messages: [{ role: "user", content: "budget gate" }],
            }),
          });
        meteredGateFixture.reset();
        const denied = await withPostgresCatalogSnapshot(binding.database!, () =>
          handleChat(request("openai/gpt-4o-mini"))
        );
        assert.equal(denied.status, 429);
        const body = await denied.json();
        assert.equal(body.error?.code, "BUDGET_EXCEEDED");
        assert.equal(meteredGateFixture.credentialSelections, 0);
        assert.equal(meteredGateFixture.connectionCooldowns, 0);
        assert.equal(fetches, 0);
        assert.ok(!JSON.stringify(body).includes("at /"));
        meteredGateFixture.reset();
        const exempt = await withPostgresCatalogSnapshot(binding.database!, () =>
          handleChat(request("claude/claude-sonnet-4-6"))
        );
        assert.notEqual(exempt.status, 429);
        assert.equal(
          meteredGateFixture.credentialSelections,
          1,
          "flat-rate subscription passes the dollar gate and reaches account selection"
        );
        assert.equal(meteredGateFixture.connectionCooldowns, 0);
        assert.equal(fetches, 0);
      });
    } finally {
      globalThis.fetch = originalFetch;
      if (plane && keyId)
        await plane
          .run(async (binding) => {
            await binding.database!.transaction(async (tx) => {
              await tx.query("DELETE FROM omniroute.domain_cost_history WHERE api_key_id=$1", [
                keyId,
              ]);
              await tx.query("DELETE FROM omniroute.domain_budgets WHERE api_key_id=$1", [keyId]);
              await tx.query("DELETE FROM omniroute.domain_budget_reset_logs WHERE api_key_id=$1", [
                keyId,
              ]);
            });
            await binding.apiKeys.deleteApiKey(keyId!);
            const { deleteProviderConnection } = await import("../../src/lib/db/providers.ts");
            for (const id of connectionIds) await deleteProviderConnection(id);
          })
          .catch(() => {});
      try {
        await plane?.close();
      } finally {
        hooks.deregister();
        meteredGateFixture?.reset();
        rmSync(directory, { recursive: true, force: true });
        for (const [name, value] of previous) {
          if (value === undefined) delete process.env[name];
          else process.env[name] = value;
        }
      }
    }
  }
);
