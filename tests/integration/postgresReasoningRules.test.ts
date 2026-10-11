import assert from "node:assert/strict";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import {
  assertPostgresCatalogSnapshotCurrent,
  mutatePostgresCatalog,
  withPostgresCatalogSnapshot,
} from "../../src/lib/db/postgres/catalogSnapshot.ts";
import * as rules from "../../src/lib/db/reasoningRoutingRules.ts";

function input(
  patch: Partial<rules.ReasoningRoutingRuleInput> = {}
): rules.ReasoningRoutingRuleInput {
  return {
    name: "Rule",
    description: "",
    scope: "global",
    apiKeyId: null,
    comboId: null,
    connectionId: null,
    modelPattern: null,
    sourceEffort: "any",
    requestTags: [],
    tagMatchMode: "any",
    effortMode: "inherit",
    targetEffort: null,
    targetKind: "keep",
    targetModel: null,
    targetComboId: null,
    budgetAction: "preserve",
    budgetTokens: null,
    priority: 0,
    enabled: true,
    ...patch,
  };
}

// The shared runner supplies only a disposable TLS endpoint; no company database is used.
test(
  "PostgreSQL reasoning rules use selected durable authority and atomic references",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const database = await createTestPostgresDatabase({ maxConnections: 2 });
    const savedKey = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-postgres-reasoning-rule-key";
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    let second: typeof plane;
    const runtime = { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} };
    try {
      await migratePostgresSchema(database);
      // Isolated test schema only. Secrets must not retain another suite's encryption key.
      await database.transaction((tx) =>
        tx.query(
          "TRUNCATE omniroute.reasoning_routing_rules,omniroute.api_keys,omniroute.combos,omniroute.provider_connections,omniroute.key_value RESTART IDENTITY CASCADE"
        )
      );
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions({ maxConnections: 2 }),
        logger: { warn() {} },
        runtime,
      });
      second = await initializePostgresControlPlane({
        connection: await testPostgresOptions({ maxConnections: 2 }),
        logger: { warn() {} },
        runtime,
      });
      await t.test("rule normalization, isolated returned tags and enabled filtering", async () => {
        await plane!.run(async () => {
          const created = await rules.createReasoningRoutingRule(
            input({
              name: "Active",
              requestTags: [" X ", "x", "Y"],
              effortMode: "force",
              targetEffort: "high",
              budgetAction: "set",
              budgetTokens: 8192,
              priority: 3,
            })
          );
          assert.deepEqual(created.requestTags, ["x", "y"]);
          assert.equal(created.budgetTokens, 8192);
          created.requestTags.push("local-only");
          const current = (await rules.getReasoningRoutingRuleById(created.id))!;
          assert.deepEqual(current.requestTags, ["x", "y"]);
          assert.equal(current.budgetTokens, 8192);
          await rules.createReasoningRoutingRule(
            input({ name: "Disabled", enabled: false, priority: 4 })
          );
          assert.deepEqual(
            (await rules.getReasoningRoutingRules()).map((rule) => rule.name),
            ["Disabled", "Active"]
          );
          assert.deepEqual(
            (await rules.getReasoningRoutingRules({ enabledOnly: true })).map((rule) => rule.name),
            ["Active"]
          );
          await rules.deleteReasoningRoutingRule(created.id);
          for (const rule of await rules.getReasoningRoutingRules())
            await rules.deleteReasoningRoutingRule(rule.id);
        });
      });
      await t.test("invalid references fail atomically and messages match source", async () => {
        await plane!.run(async (binding) => {
          const bad = input({
            scope: "combo",
            comboId: "missing",
            targetKind: "combo",
            targetComboId: "missing-target",
          });
          assert.deepEqual(await rules.getReasoningRoutingRuleReferenceErrorsAsync(bad), [
            "The referenced source combo does not exist",
            "The referenced target combo does not exist",
          ]);
          assert.throws(
            () => rules.getReasoningRoutingRuleReferenceErrors(bad),
            /snapshot was not prepared/
          );
          await withPostgresCatalogSnapshot(binding.database!, async () => {
            assert.deepEqual(
              rules.getReasoningRoutingRuleReferenceErrors(bad),
              await rules.getReasoningRoutingRuleReferenceErrorsAsync(bad)
            );
          });
          await assert.rejects(rules.createReasoningRoutingRule(bad), /source combo.*target combo/);
          assert.equal((await rules.getReasoningRoutingRules()).length, 0);
          const created = await rules.createReasoningRoutingRule(input());
          await assert.rejects(
            rules.updateReasoningRoutingRule(created.id, {
              scope: "apiKey",
              apiKeyId: "missing-key",
            }),
            /API key does not exist/
          );
          assert.equal((await rules.getReasoningRoutingRuleById(created.id))!.scope, "global");
          assert.equal(await rules.updateReasoningRoutingRule("absent", { name: "ignored" }), null);
          assert.equal(await rules.deleteReasoningRoutingRule("absent"), false);
          assert.equal(await rules.deleteReasoningRoutingRule(created.id), true);
          assert.equal(await rules.deleteReasoningRoutingRule(created.id), false);
        });
      });
      await t.test("same-request mutations refresh prepared rows only after commit", async () => {
        await plane!.run((binding) =>
          withPostgresCatalogSnapshot(binding.database!, async () => {
            const before = (await rules.getReasoningRoutingRules()).length;
            const created = await rules.createReasoningRoutingRule(
              input({ name: "Read own write" })
            );
            assert.equal((await rules.getReasoningRoutingRules()).length, before + 1);
            const updated = await rules.updateReasoningRoutingRule(created.id, {
              effortMode: "force",
              targetEffort: "none",
              budgetAction: "set",
              budgetTokens: 64,
            });
            assert.equal(updated!.budgetAction, "remove");
            assert.equal(
              (await rules.getReasoningRoutingRuleById(created.id))!.targetEffort,
              "none"
            );
            await rules.deleteReasoningRoutingRule(created.id);
            assert.equal(await rules.getReasoningRoutingRuleById(created.id), null);
          })
        );
      });
      await t.test(
        "nested rule operations reuse the catalog transaction without a checkout",
        async () => {
          await plane!.run((binding) =>
            mutatePostgresCatalog(
              binding.database!,
              async () => {
                const created = await rules.createReasoningRoutingRule(input({ name: "Nested" }));
                assert.equal((await rules.getReasoningRoutingRuleById(created.id))!.name, "Nested");
                assert.equal((await rules.getReasoningRoutingRules())[0].id, created.id);
                await rules.updateReasoningRoutingRule(created.id, { name: "Nested updated" });
                assert.equal(
                  (await rules.getReasoningRoutingRuleById(created.id))!.name,
                  "Nested updated"
                );
                await rules.deleteReasoningRoutingRule(created.id);
                assert.equal(await rules.getReasoningRoutingRuleById(created.id), null);
              },
              { routing: true, providers: true }
            )
          );
        }
      );
      await t.test(
        "concurrent partial updates preserve both fields across runtime instances",
        async () => {
          const created = await plane!.run(() =>
            rules.createReasoningRoutingRule(input({ name: "Concurrent" }))
          );
          await Promise.all([
            plane!.run(() => rules.updateReasoningRoutingRule(created.id, { name: "Renamed" })),
            second!.run(() => rules.updateReasoningRoutingRule(created.id, { priority: 99 })),
          ]);
          await plane!.run(async () => {
            const current = (await rules.getReasoningRoutingRuleById(created.id))!;
            assert.equal(current.name, "Renamed");
            assert.equal(current.priority, 99);
            await rules.deleteReasoningRoutingRule(created.id);
          });
        }
      );
      await t.test(
        "another runtime cannot reuse a prepared view or process-global rule cache",
        async () => {
          await plane!.run((binding) =>
            withPostgresCatalogSnapshot(binding.database!, async () => {
              assert.equal((await rules.getReasoningRoutingRules()).length, 0);
              const created = await second!.run(() =>
                rules.createReasoningRoutingRule(input({ name: "Other runtime" }))
              );
              // The first request retains a coherent earlier authority; the second reads its own commit.
              assert.equal((await rules.getReasoningRoutingRules()).length, 0);
              assert.equal(
                (await second!.run(() => rules.getReasoningRoutingRules()))[0].id,
                created.id
              );
              await assert.rejects(assertPostgresCatalogSnapshotCurrent(), /policy changed/);
              await second!.run(() => rules.deleteReasoningRoutingRule(created.id));
            })
          );
        }
      );
      await t.test(
        "source key, source/target combo and connection deletion cascade their rules",
        async () => {
          await database.transaction(async (tx) => {
            const now = new Date().toISOString();
            await tx.query(
              "INSERT INTO omniroute.api_keys(id,name,key,created_at) VALUES('reasoning-key','Rule key','opaque-rule-test-key',$1)",
              [now]
            );
            await tx.query(
              "INSERT INTO omniroute.combos(id,name,data,created_at,updated_at) VALUES('source-combo','Source combo',$1,$2,$2),('target-combo','Target combo',$3,$2,$2)",
              [
                JSON.stringify({ id: "source-combo", name: "Source combo", models: [] }),
                now,
                JSON.stringify({ id: "target-combo", name: "Target combo", models: [] }),
              ]
            );
            await tx.query(
              "INSERT INTO omniroute.provider_connections(id,provider,created_at,updated_at) VALUES('reasoning-connection','openai',$1,$1)",
              [now]
            );
          });
          await plane!.run(async (binding) => {
            const keyRule = await rules.createReasoningRoutingRule(
              input({ scope: "apiKey", apiKeyId: "reasoning-key" })
            );
            const sourceRule = await rules.createReasoningRoutingRule(
              input({ scope: "combo", comboId: "source-combo" })
            );
            const targetRule = await rules.createReasoningRoutingRule(
              input({ targetKind: "combo", targetComboId: "target-combo" })
            );
            const connectionRule = await rules.createReasoningRoutingRule(
              input({ scope: "connection", connectionId: "reasoning-connection" })
            );
            await binding.apiKeys.deleteApiKey("reasoning-key");
            await binding.combos!.deleteById("source-combo");
            await binding.combos!.deleteById("target-combo");
            await binding.providers.deleteById("reasoning-connection");
            for (const rule of [keyRule, sourceRule, targetRule, connectionRule])
              assert.equal(await rules.getReasoningRoutingRuleById(rule.id), null);
            assert.equal((await rules.getReasoningRoutingRules()).length, 0);
          });
        }
      );
      await t.test(
        "rule create versus referenced combo deletion leaves no dangling policy",
        async () => {
          await plane!.run(async (binding) => {
            const combo = await binding.combos!.create({ name: "Race combo", models: [] });
            const results = await Promise.allSettled([
              rules.createReasoningRoutingRule(
                input({ scope: "combo", comboId: String(combo.id) })
              ),
              second!.run((other) => other.combos!.deleteById(String(combo.id))),
            ]);
            assert.equal(results[1].status, "fulfilled");
            if (results[0].status === "rejected")
              assert.match(String(results[0].reason), /source combo does not exist/);
            assert.equal((await rules.getReasoningRoutingRules()).length, 0);
          });
        }
      );
    } finally {
      await second?.close();
      await plane?.close();
      await database.close();
      if (savedKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = savedKey;
    }
  }
);
