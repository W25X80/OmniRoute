import assert from "node:assert/strict";
import test from "node:test";
import {
  __test_resetLearnedReasoningEffortCaps,
  recordLearnedReasoningEffort,
} from "@omniroute/open-sse/services/learnedReasoningEffortCaps.ts";
import { setCustomAliases } from "@omniroute/open-sse/services/modelDeprecation.ts";
import type { ApiKeyRepository } from "../../src/domain/persistence/apiKeyRepository.ts";
import type { ApiKeyGroupsRepository } from "../../src/domain/persistence/apiKeyGroupsRepository.ts";
import type { ProviderNodeRepository } from "../../src/domain/persistence/providerRepositories.ts";
import type { SettingsRepository } from "../../src/domain/persistence/settingsRepository.ts";
import { createPostgresApiKeyGroupsRepository } from "../../src/lib/db/postgres/apiKeyGroupsRepository.ts";
import {
  createPostgresApiKeyRepository,
  type PostgresApiKeyEffects,
  type PostgresApiKeyModelPolicy,
} from "../../src/lib/db/postgres/apiKeyRepository.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { createPostgresModelPolicy } from "../../src/lib/db/postgres/modelPolicy.ts";
import { createPostgresProviderNodeRepository } from "../../src/lib/db/postgres/providerNodesRepository.ts";
import { createPostgresSettingsRepository } from "../../src/lib/db/postgres/settingsRepository.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

const noKeyEffects: PostgresApiKeyEffects = {
  setNoLog() {},
  invalidateAuthCaches() {},
  invalidateModelCatalogCache() {},
  invalidateReasoningRoutingRuleCache() {},
  deleteRedisAuthCacheEntries() {},
  logAuditEvent() {},
};
let database: PostgresDatabase;
let nodes: ProviderNodeRepository;
let settings: SettingsRepository;
let policy: PostgresApiKeyModelPolicy;
let keys: ApiKeyRepository;
let groups: ApiKeyGroupsRepository;
const originalApiKeySecret = process.env.API_KEY_SECRET;

async function put(namespace: string, key: string, value: unknown, raw = false) {
  await database.transaction((transaction) =>
    transaction.query(
      `INSERT INTO omniroute.key_value(namespace,key,value) VALUES($1,$2,$3)
     ON CONFLICT(namespace,key) DO UPDATE SET value=EXCLUDED.value`,
      [namespace, key, raw ? value : JSON.stringify(value)]
    )
  );
}

async function reset() {
  __test_resetLearnedReasoningEffortCaps();
  setCustomAliases({});
  await database.transaction(async (transaction) => {
    await transaction.query("SELECT pg_advisory_xact_lock(18765,5)");
    await transaction.query(`TRUNCATE omniroute.provider_nodes, omniroute.api_keys,
      omniroute.key_groups, omniroute.group_model_permissions, omniroute.key_group_members
      RESTART IDENTITY CASCADE`);
    await transaction.query(`DELETE FROM omniroute.key_value WHERE namespace IN (
      'customModels','syncedAvailableModels','modelCompatOverrides','modelAliases','settings')`);
    // This composition retains its highest observed epoch; resetting rows must never
    // rewind durable policy time and make later decisions appear older than prior reads.
    await transaction.query(
      "UPDATE omniroute.api_key_policy_epoch SET generation=generation+1 WHERE singleton"
    );
  });
}

if (!postgresTestsEnabled) {
  test(
    "PostgreSQL model policy requires the isolated TLS integration runner",
    { skip: true },
    () => {}
  );
} else {
  test.before(async () => {
    process.env.API_KEY_SECRET ||= "isolated-model-policy-test-secret";
    database = await createTestPostgresDatabase();
    await migratePostgresSchema(database);
    nodes = createPostgresProviderNodeRepository(database, { afterNodeWrite() {} });
    settings = createPostgresSettingsRepository(
      database,
      {
        invalidateSettingsCache() {},
        bumpProxyConfigGeneration() {},
        applyRuntimeSettings() {},
        scheduleCatalogRevalidationAfterInit() {},
      },
      { environment: () => ({}) }
    );
    policy = createPostgresModelPolicy(database, nodes, settings);
    keys = createPostgresApiKeyRepository(database, {
      effects: noKeyEffects,
      modelPolicy: policy,
      async deleteDependents() {},
    });
    groups = createPostgresApiKeyGroupsRepository(database, noKeyEffects);
  });
  test.beforeEach(reset);
  test.after(async () => {
    __test_resetLearnedReasoningEffortCaps();
    setCustomAliases({});
    if (originalApiKeySecret === undefined) delete process.env.API_KEY_SECRET;
    else process.env.API_KEY_SECRET = originalApiKeySecret;
    await database?.close();
  });

  test("native publication uses custom and connection-scoped synced rows without registry fallback", async () => {
    assert.equal(await policy.findPublishedModel("openai", "gpt-4o"), null);
    await put("customModels", "provider", [{ id: "manual", supportedThinkingEfforts: ["high"] }]);
    await put("syncedAvailableModels", "provider:connection-1", [
      { model: " synced ", supportedThinkingEfforts: ["high", "low"] },
      { id: "duplicate", supportedThinkingEfforts: ["low"] },
      { id: "duplicate", supportedThinkingEfforts: ["high"] },
    ]);
    await put("syncedAvailableModels", "provider:connection-2", [{ id: "other" }]);
    await put("syncedAvailableModels", "provider:broken", "{", true);
    await put("syncedAvailableModels", "providerX:unrelated", [{ id: "foreign" }]);
    assert.deepEqual(await policy.findPublishedModel("provider", "manual"), {
      providerId: "provider",
      publishedModelId: "manual",
    });
    assert.deepEqual(await policy.findPublishedModel("provider", "synced-high"), {
      providerId: "provider",
      publishedModelId: "synced",
    });
    assert.deepEqual(await policy.findPublishedModel("provider", "other"), {
      providerId: "provider",
      publishedModelId: "other",
    });
    assert.equal(await policy.findPublishedModel("provider", "manual-high"), null);
    assert.equal(await policy.findPublishedModel("provider", "foreign"), null);
    assert.equal(await policy.findPublishedModel("provider", "duplicate-low"), null);
    assert.equal(await policy.findPublishedModel("provider", "manual-HIGH"), null);
    await put("syncedAvailableModels", "github:retired", [{ id: "gpt-4o" }]);
    assert.equal(await policy.findPublishedModel("github", "gpt-4o"), null);
    await put("customModels", "provider", "{", true);
    await assert.rejects(policy.findPublishedModel("provider", "synced"), SyntaxError);
  });

  test("native effort inheritance honors learned caps, exact models and dedicated-provider skips", async () => {
    await put("syncedAvailableModels", "provider:one", [
      { id: "reasoner", supportedThinkingEfforts: ["low", "high"] },
      { id: "reasoner-high", supportedThinkingEfforts: ["max"] },
    ]);
    recordLearnedReasoningEffort("connection", "reasoner", ["low"]);
    assert.equal(await policy.findPublishedModel("provider", "reasoner-high-high"), null);
    assert.deepEqual(await policy.findPublishedModel("provider", "reasoner-high"), {
      providerId: "provider",
      publishedModelId: "reasoner-high",
    });
    assert.deepEqual(await policy.findPublishedModel("provider", "reasoner-low"), {
      providerId: "provider",
      publishedModelId: "reasoner",
    });
    assert.deepEqual(await policy.findPublishedModel("provider", "reasoner-high-max"), {
      providerId: "provider",
      publishedModelId: "reasoner-high",
    });
    for (const provider of ["glm", "kimi-coding", "devin-cli"]) {
      await put("syncedAvailableModels", `${provider}:one`, [
        { id: "dedicated", supportedThinkingEfforts: ["high"] },
      ]);
      assert.equal(await policy.findPublishedModel(provider, "dedicated-high"), null);
    }
  });

  test("native canonical alias lookup preserves literal-first publication and node shadowing", async () => {
    await put("customModels", "soniox", [{ id: "tts" }]);
    assert.deepEqual(await policy.findPublishedModel("sx", "tts"), {
      providerId: "soniox",
      publishedModelId: "tts",
    });
    await put("customModels", "sx", [{ id: "tts" }]);
    assert.deepEqual(await policy.findPublishedModel("sx", "tts"), {
      providerId: "sx",
      publishedModelId: "tts",
    });
    assert.equal(await policy.canonicalProviderForAlias("llamacpp"), "llama-cpp");
    await nodes.create({
      id: "compatible-node",
      type: "openai-compatible",
      name: "node",
      prefix: "llamacpp",
    });
    assert.equal(await policy.canonicalProviderForAlias("llamacpp"), null);
    await put("customModels", "llama-cpp", [{ id: "canonical-only" }]);
    assert.equal(await policy.findPublishedModel("llamacpp", "canonical-only"), null);
    // Reserved registry prefixes are never handed to compatible provider nodes.
    await nodes.create({
      id: "reserved-node",
      type: "openai-compatible",
      name: "reserved",
      prefix: "cx",
    });
    assert.equal(await policy.canonicalProviderForAlias("cx"), "codex");
  });

  test("native hidden policy preserves custom override, alias union and chat modality precedence", async () => {
    await put("customModels", "soniox", [{ id: "Mixed", isHidden: false }]);
    await put("modelCompatOverrides", "soniox", [{ id: "Mixed", isHidden: true }]);
    assert.equal(await policy.isPublishedModelHidden("soniox", "sx", "mixed"), false);
    await put("customModels", "sx", [{ id: "mixed", isHidden: true }]);
    assert.equal(await policy.isPublishedModelHidden("soniox", "sx", "mixed"), true);
    await put("customModels", "sx", []);
    await put("customModels", "soniox", [{ id: "Mixed" }]);
    assert.equal(await policy.isPublishedModelHidden("soniox", "sx", "Mixed"), true);
    assert.equal(await policy.isPublishedModelHidden("soniox", "sx", "mixed"), false);
    await put("modelCompatOverrides", "soniox", [
      {
        id: "Mixed",
        isHidden: true,
        hiddenModalities: { chat: false, image: true },
        isDeleted: true,
      },
    ]);
    assert.equal(await policy.isPublishedModelHidden("soniox", "sx", "Mixed"), false);
    await put("modelCompatOverrides", "soniox", [
      { id: "Mixed", hiddenModalities: { chat: true } },
    ]);
    assert.equal(await policy.isPublishedModelHidden("soniox", "sx", "Mixed"), true);
    await put("customModels", "soniox", "{", true);
    assert.equal(await policy.isPublishedModelHidden("soniox", "sx", "Mixed"), true);
    await put("modelCompatOverrides", "soniox", "{", true);
    assert.equal(await policy.isPublishedModelHidden("soniox", "sx", "Mixed"), false);
  });

  test("native aliases and Claude preference read selected durable settings on every decision", async () => {
    setCustomAliases({ operator: "sqlite-backend/private" });
    assert.equal(await policy.resolveModelAlias("operator"), "operator");
    assert.equal(await policy.resolveModelAlias("gemini-pro"), "gemini-2.5-pro");
    await settings.update({
      modelAliases: { " operator ": " provider/first ", "gemini-pro": "provider/override" },
      preferClaudeCodeForUnprefixedClaudeModels: true,
    });
    assert.equal(await policy.resolveModelAlias("operator"), "provider/first");
    assert.equal(await policy.resolveModelAlias("gemini-pro"), "provider/override");
    assert.equal(await policy.preferClaudeCodeForUnprefixedClaudeModels(), true);
    await settings.update({
      modelAliases: { operator: "provider/second" },
      preferClaudeCodeForUnprefixedClaudeModels: false,
    });
    assert.equal(await policy.resolveModelAlias("operator"), "provider/second");
    assert.equal(await policy.preferClaudeCodeForUnprefixedClaudeModels(), false);
    // Namespace aliases belong to the routing resolver; the source API-key deprecation
    // helper reads settings.modelAliases only, so this row cannot replace that authority.
    await put("modelAliases", "operator", "provider/namespace");
    assert.equal(await policy.resolveModelAlias("operator"), "provider/second");
  });

  test("native API key gate inherits hidden/base blocks and canonical group denies through aliases", async () => {
    const key = await keys.createApiKey("policy", "model-policy-machine");
    await keys.updateApiKeyPermissions(key.id, { disableNonPublicModels: true });
    await put("syncedAvailableModels", "soniox:one", [
      { id: "tts", supportedThinkingEfforts: ["high"] },
    ]);
    assert.equal(await keys.isModelAllowedForKey(key.key, "sx/tts-high"), true);
    await keys.updateApiKeyPermissions(key.id, { blockedModels: ["soniox/tts"] });
    assert.equal(await keys.isModelAllowedForKey(key.key, "sx/tts-high"), false);
    await keys.updateApiKeyPermissions(key.id, { blockedModels: [] });
    await put("customModels", "sx", [{ id: "tts", isHidden: true }]);
    assert.equal(await keys.isModelAllowedForKey(key.key, "sx/tts-high"), false);
    await put("customModels", "sx", []);
    const group = await groups.createKeyGroup("canonical-deny");
    await groups.addKeyToGroup(key.id, group.id);
    const deny = await groups.addGroupPermission(group.id, "tts", "deny", "soniox");
    assert.equal(await keys.isModelAllowedForKey(key.key, "sx/tts"), false);
    assert.equal(await keys.isModelAllowedForKey(key.key, "sx/tts-high", "soniox/tts"), false);
    await groups.removeGroupPermission(deny.id);
    await groups.addGroupPermission(group.id, "*", "allow", "sx");
    assert.equal(await keys.isModelAllowedForKey(key.key, "sx/tts"), true);
  });

  test("native Claude preference controls bare-model wildcard publication exceptions", async () => {
    const key = await keys.createApiKey("claude-policy", "model-policy-machine");
    await keys.updateApiKeyPermissions(key.id, {
      disableNonPublicModels: true,
      modelAccessMode: "restricted",
      allowedModels: ["cc/*"],
    });
    assert.deepEqual((await keys.getApiKeyMetadata(key.key))?.allowedModels, ["cc/*"]);
    assert.equal(await keys.isModelAllowedForKey(key.key, "claude-sonnet-4-6"), false);
    await settings.update({ preferClaudeCodeForUnprefixedClaudeModels: true });
    assert.equal(await policy.preferClaudeCodeForUnprefixedClaudeModels(), true);
    assert.equal(await keys.isModelAllowedForKey(key.key, "claude-sonnet-4-6"), true);
    await settings.update({ preferClaudeCodeForUnprefixedClaudeModels: false });
    assert.equal(await policy.preferClaudeCodeForUnprefixedClaudeModels(), false);
    assert.equal(await keys.isModelAllowedForKey(key.key, "claude-sonnet-4-6"), false);
    assert.equal(await keys.isModelAllowedForKey(key.key, "cc/claude-sonnet-4-6"), true);
  });
}
