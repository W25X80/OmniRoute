import { UnsupportedPersistenceDomainError } from "../../src/lib/db/controlPlaneContext.ts";
import { setProviderAliasAsync } from "../../src/lib/db/models/aliases.ts";
import { savePromptAsync } from "../../src/lib/db/prompts.ts";
import { rollbackPromptAsync } from "../../src/lib/db/prompts.ts";
import { upsertDiscoveryResultAsync } from "../../src/lib/db/discoveryResults.ts";
import { markVerifiedAsync } from "../../src/lib/db/discoveryResults.ts";
import { setModelCapabilityOverrideAsync } from "../../src/lib/db/modelCapabilityOverrides.ts";
import { setModelContextOverrideAsync } from "../../src/lib/db/modelContextOverrides.ts";
import { saveServiceModelsAsync } from "../../src/lib/db/serviceModels.ts";
import { saveModelsDevCapabilitiesAsync } from "../../src/lib/modelsDevSync.ts";
import assert from "node:assert/strict";
import test from "node:test";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { POSTGRES_CATALOG_SCHEMA_STATEMENTS } from "../../src/lib/db/postgres/catalogSchema.ts";
import {
  withPostgresCatalogSnapshot,
  catalogGeneration,
} from "../../src/lib/db/postgres/catalogSnapshot.ts";
import * as models from "../../src/lib/db/models.ts";
import { getProviderAliases, setProviderAlias } from "../../src/lib/db/models/aliases.ts";
import { savePrompt, getActivePrompt, listPromptVersions } from "../../src/lib/db/prompts.ts";
import { getDiscoveryResults } from "../../src/lib/db/discoveryResults.ts";
import {
  getModelCapabilityOverride,
  getReasoningEffortsOverride,
} from "../../src/lib/db/modelCapabilityOverrides.ts";
import { getModelContextOverride } from "../../src/lib/db/modelContextOverrides.ts";
import { setExcluded, listOverrides } from "../../src/lib/db/autoCandidateOverrides.ts";
import { getServiceModels } from "../../src/lib/db/serviceModels.ts";
import { getSyncedCapability, type ModelCapabilityEntry } from "../../src/lib/modelsDevSync.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

let database: PostgresDatabase;
let contender: PostgresDatabase;
let binding: PostgresControlPlaneBinding;
let otherBinding: PostgresControlPlaneBinding;

/** These tests select real native SQL; unrelated repositories are deliberately not used. */
function select(database: PostgresDatabase): PostgresControlPlaneBinding {
  return {
    database,
    databaseIdentity: Object.freeze({}),
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
}
function selected<T>(body: () => T | Promise<T>, current = binding): Promise<T> {
  return withPostgresControlPlane(current, () =>
    withPostgresCatalogSnapshot(current.database!, body)
  );
}
async function raw(namespace: string, key: string, value: string) {
  await database.transaction((transaction) =>
    transaction.query(
      "INSERT INTO omniroute.key_value(namespace,key,value) VALUES($1,$2,$3) ON CONFLICT(namespace,key) DO UPDATE SET value=EXCLUDED.value",
      [namespace, key, value]
    )
  );
}

if (!postgresTestsEnabled) {
  test("PostgreSQL catalog requires the isolated TLS runner", { skip: true }, () => {});
} else {
  test.before(async () => {
    database = await createTestPostgresDatabase();
    contender = await createTestPostgresDatabase();
    await migratePostgresSchema(database);
    await database.transaction(async (transaction) => {
      for (const statement of POSTGRES_CATALOG_SCHEMA_STATEMENTS)
        await transaction.query(statement);
    });
    binding = select(database);
    otherBinding = select(contender);
  });
  test.beforeEach(async () => {
    await database.transaction(async (transaction) => {
      await transaction.query("SELECT pg_advisory_xact_lock(18765,18)");
      await transaction.query("SELECT pg_advisory_xact_lock(18765,20)");
      await transaction.query("SELECT pg_advisory_xact_lock(18765,5)");
      await transaction.query(
        "TRUNCATE omniroute.model_capabilities,omniroute.model_capability_overrides,omniroute.model_context_overrides,omniroute.prompt_templates,omniroute.discovery_results,omniroute.tier_config,omniroute.auto_candidate_overrides RESTART IDENTITY"
      );
      await transaction.query("DELETE FROM omniroute.key_value WHERE namespace=ANY($1::text[])", [
        [
          "customModels",
          "modelCompatOverrides",
          "modelAliases",
          "managedModelAliasNames",
          "providerAliases",
          "syncedAvailableModels",
          "serviceModels",
        ],
      ]);
      await transaction.query(
        "UPDATE omniroute.api_key_policy_epoch SET generation=generation+1 WHERE singleton"
      );
    });
  });
  test.after(async () => {
    await contender?.close();
    await database?.close();
  });

  test("single-connection pool cleanup reuses the current catalog transaction", async () => {
    const single = await createTestPostgresDatabase({
      maxConnections: 1,
      connectionTimeoutMs: 500,
    });
    const current = select(single);
    try {
      await raw(
        "customModels",
        "pool-one",
        JSON.stringify([
          { id: "imported", source: "imported" },
          { id: "manual", source: "manual" },
        ])
      );
      await raw("syncedAvailableModels", "pool-one:deleted", '[{"id":"imported"}]');
      const result = await selected(
        () => models.cleanupProviderModelsAfterConnectionDelete("pool-one", "deleted"),
        current
      );
      assert.equal(result.remainingConnections, 0);
      assert.deepEqual(result.removedImportedModelIds, ["imported"]);
      assert.deepEqual(result.remainingSyncedModels, []);
      assert.deepEqual(
        (await selected(() => models.getCustomModels("pool-one"), current)).map(
          (model) => model.id
        ),
        ["manual"]
      );
    } finally {
      await single.close();
    }
  });

  test("original synchronous mutation names reject the native backend before SQLite access", async () => {
    await selected(() => {
      assert.throws(
        () => setProviderAlias("provider", "alias", "model"),
        UnsupportedPersistenceDomainError
      );
      assert.throws(() => savePrompt("prompt", "body"), UnsupportedPersistenceDomainError);
      assert.throws(
        () => models.setModelIsHidden("provider", "model", true),
        UnsupportedPersistenceDomainError
      );
    });
  });

  test("custom model replacement preserves operator metadata and modality visibility", async () => {
    await selected(async () => {
      await models.addCustomModel("provider", "model", "Human name");
      await models.updateCustomModel("provider", "model", {
        isHidden: false,
        supportsVision: true,
        preserveOpenAIDeveloperRole: false,
        upstreamHeaders: { "x-custom": "kept" },
      });
      await models.mergeModelCompatOverrideAsync("provider", "model", {
        isHidden: true,
        modality: "image",
      });
      await models.replaceCustomModels("provider", [{ id: "model", name: "Remote name" }]);
      const result = await models.getCustomModels("provider");
      assert.equal(result[0].supportsVision, true);
      assert.equal(result[0].preserveOpenAIDeveloperRole, false);
      assert.equal(models.getModelIsHidden("provider", "model", "image"), false);
      assert.equal(models.getModelUpstreamExtraHeaders("provider", "model")?.["x-custom"], "kept");
      await models.setModelIsHiddenAsync("provider", "only-image", true, "image");
      assert.equal(models.getModelIsHidden("provider", "only-image", "image"), true);
      assert.equal(models.getModelIsHidden("provider", "only-image", "chat"), false);
      assert.equal(
        models.getHiddenModelsByProvider("chat").get("provider")?.has("only-image") ?? false,
        false
      );
    });
  });

  test("source corrupt-row distinctions and unescaped LIKE matching remain visible", async () => {
    await raw("customModels", "provider", "{");
    await raw("syncedAvailableModels", "provider:broken", "{");
    await raw("syncedAvailableModels", "provider:ok", '[{"id":"good"}]');
    await selected(async () => {
      await assert.rejects(models.getCustomModels("provider"), SyntaxError);
      assert.deepEqual(
        await models.getSyncedAvailableModelsForConnection("provider", "broken"),
        []
      );
      await assert.rejects(models.getSyncedAvailableModels("provider"), SyntaxError);
      assert.equal(
        (await models.getSyncedAvailableModelsByConnection("provider"))[
          models.SYNCED_AVAILABLE_MODELS_MALFORMED
        ],
        true
      );
      assert.equal(await models.removeSyncedAvailableModel("provider", "good"), true);
    });
    await raw("syncedAvailableModels", "p_one:one", '[{"id":"a"}]');
    await raw("syncedAvailableModels", "pXone:two", '[{"id":"b"}]');
    await selected(async () =>
      assert.deepEqual(
        (await models.getSyncedAvailableModels("p_one")).map((model) => model.id),
        ["b", "a"]
      )
    );
  });

  test("independent pools serialize alias read-modify-write and advance durable policy", async () => {
    const before = await selected(() => catalogGeneration());
    await Promise.all([
      selected(() => setProviderAliasAsync("provider", "one", "upstream-one")),
      selected(() => setProviderAliasAsync("provider", "two", "upstream-two"), otherBinding),
    ]);
    assert.deepEqual(await selected(() => getProviderAliases("provider")), {
      one: "upstream-one",
      two: "upstream-two",
    });
    const after = await selected(() => catalogGeneration());
    assert.equal(BigInt(after) - BigInt(before), 2n);
  });

  test("native capabilities use overrides and reasoning tiers in the selected snapshot", async () => {
    const cap: ModelCapabilityEntry = {
      tool_call: true,
      reasoning: true,
      reasoning_efforts: ["low", "high"],
      attachment: true,
      structured_output: null,
      temperature: false,
      modalities_input: '["text","image"]',
      modalities_output: '["text"]',
      knowledge_cutoff: null,
      release_date: null,
      last_updated: null,
      status: null,
      family: null,
      open_weights: null,
      limit_context: 200000,
      limit_input: 190000,
      limit_output: 10000,
      interleaved_field: null,
    };
    await selected(async () => {
      await saveModelsDevCapabilitiesAsync({ provider: { reasoner: cap } });
      assert.equal(getSyncedCapability("provider", "reasoner")?.limit_context, 200000);
      assert.deepEqual(getSyncedCapability("provider", "reasoner")?.reasoning_efforts, [
        "low",
        "high",
      ]);
      assert.equal(
        await setModelCapabilityOverrideAsync("provider/reasoner", "max_output_tokens", 2000),
        true
      );
      assert.equal(
        await setModelCapabilityOverrideAsync("provider/reasoner", "reasoning_efforts", ["low"]),
        true
      );
      assert.equal(
        await setModelContextOverrideAsync("provider", "reasoner", 123456, "manual"),
        true
      );
      assert.equal(getModelCapabilityOverride("provider", "reasoner", "max_output_tokens"), 2000);
      assert.deepEqual(getReasoningEffortsOverride("provider", "reasoner"), ["low"]);
      assert.equal(getModelContextOverride("provider", "reasoner"), 123456);
      assert.equal(
        await setModelCapabilityOverrideAsync("provider/reasoner", "max_output_tokens", -1),
        false
      );
    });
  });

  test("prompt versions, nullable discovery uniqueness and exclusion identity preserve source behavior", async () => {
    await selected(async () => {
      const first = await savePromptAsync("prompt", "first");
      const second = await savePromptAsync("prompt", "second");
      assert.equal((await savePromptAsync("prompt", "first")).id, first.id);
      assert.equal(getActivePrompt("prompt")?.id, second.id);
      assert.equal((await rollbackPromptAsync("prompt", first.version))?.id, first.id);
      assert.deepEqual(
        listPromptVersions("prompt").map((prompt) => prompt.version),
        [2, 1]
      );
      const input = {
        providerId: "provider",
        method: "public_api" as const,
        authType: "none" as const,
        feasibility: 3,
        riskLevel: "low" as const,
        status: "pending" as const,
      };
      const found = await upsertDiscoveryResultAsync(input);
      await upsertDiscoveryResultAsync(input);
      assert.equal(getDiscoveryResults("provider").length, 2);
      assert.equal((await markVerifiedAsync(found.id!))?.status, "verified");
      const exclusion = await setExcluded("key", "auto", "connection", true);
      const visible = await setExcluded("key", "auto", "connection", false);
      assert.equal(visible.id, exclusion.id);
      assert.equal(visible.createdAt, exclusion.createdAt);
      assert.equal((await listOverrides("key", "auto"))[0].excluded, false);
      await saveServiceModelsAsync("service", [{ id: "one" }, { id: "two" }]);
      await saveServiceModelsAsync("service", [{ id: "two" }]);
      assert.deepEqual(
        getServiceModels("service").map((model) => [model.id, model.available]),
        [
          ["two", true],
          ["one", false],
        ]
      );
    });
  });
}
