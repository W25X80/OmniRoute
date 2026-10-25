import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import type {
  ApiKeyRepository,
  ApiKeyPermissionsUpdate,
} from "../../../src/domain/persistence/apiKeyRepository.ts";
import type { ApiKeyGroupsRepository } from "../../../src/domain/persistence/apiKeyGroupsRepository.ts";
import { resolveSelfLoopBearer } from "../../../src/shared/middleware/chatAdmissionIdentity.ts";

export interface ApiKeyRepositoryHarness {
  keys: ApiKeyRepository;
  groups: ApiKeyGroupsRepository;
  reset(): Promise<void>;
}

export function registerApiKeyRepositoryConformance(
  createHarness: () => Promise<ApiKeyRepositoryHarness>
): void {
  test("API key repositories: identity, defaults, metadata, count and pagination", async () => {
    const h = await createHarness();
    await h.reset();
    await assert.rejects(h.keys.createApiKey("invalid", ""), /machineId is required/);
    const a = await h.keys.createApiKey("alpha", "0123456789abcdef", ["self:usage"]);
    const b = await h.keys.createApiKey("beta", "0123456789abcdef");
    assert.notEqual(a.key, b.key);
    assert.notEqual(a.id, b.id);
    assert.equal(await h.keys.getApiKeysCount(), 2);
    assert.deepEqual(
      (await h.keys.getApiKeys(1, 1)).map((key) => key.id),
      [b.id]
    );
    assert.equal(await h.keys.getApiKeyById("absent"), null);
    const row = await h.keys.getApiKeyById(a.id);
    assert.ok(row);
    assert.equal(row.key, a.key);
    assert.equal(row.modelAccessMode, "all");
    assert.deepEqual(row.allowedModels, []);
    assert.deepEqual(row.allowedCombos, ["combo/*"]);
    const metadata = await h.keys.getApiKeyMetadata(a.key);
    assert.ok(metadata);
    assert.equal(metadata.keyHash, createHash("sha256").update(a.key).digest("hex"));
    assert.equal(metadata.compressionEnabled, true);
    assert.equal(metadata.allowAutoCombos, true);
    assert.equal(metadata.maxSessions, 0);
    assert.equal(metadata.codexServiceMode, "inherit");
    assert.equal(metadata.usageLimitEnabled, false);
    assert.equal(metadata.dailyUsageLimitUsd, null);
    assert.equal(metadata.noLog, false);
    assert.equal(metadata.isActive, true);
    assert.equal(await h.keys.validateApiKey(a.key), true);
    assert.ok((await h.keys.getApiKeyById(a.id))?.lastUsedAt);
  });

  test("API key repositories: full permission encoding preserves nullable and policy fields", async () => {
    const h = await createHarness();
    await h.reset();
    const created = await h.keys.createApiKey("fields", "machine");
    const update: ApiKeyPermissionsUpdate = {
      name: "renamed",
      modelAccessMode: "restricted",
      allowedModels: ["cx/*"],
      blockedModels: ["cx/blocked"],
      allowedCombos: [],
      allowedConnections: ["connection"],
      allowedQuotas: ["pool"],
      noLog: true,
      autoResolve: true,
      accessSchedule: { enabled: true, from: "08:00", until: "18:00", days: [1, 2, 3], tz: "UTC" },
      maxRequestsPerDay: 123,
      maxRequestsPerMinute: 7,
      throttleDelayMs: 20,
      rateLimits: [{ limit: 10, window: 60 }],
      maxSessions: 2,
      scopes: ["self:usage", "manage"],
      proxyId: "proxy",
      allowedEndpoints: ["chat", "embeddings"],
      streamDefaultMode: "json",
      cacheDefaultMode: "bypass",
      codexServiceMode: "priority",
      disableNonPublicModels: true,
      allowUsageCommand: true,
      usageLimitEnabled: true,
      dailyUsageLimitUsd: 0.125,
      weeklyUsageLimitUsd: 1.5,
      chaosModeEnabled: true,
      compressionEnabled: false,
      allowAutoCombos: false,
      catalogScope: "models",
    };
    assert.equal(await h.keys.updateApiKeyPermissions(created.id, update), true);
    const metadata = await h.keys.getApiKeyMetadata(created.key);
    assert.ok(metadata);
    for (const [key, value] of Object.entries(update))
      assert.deepEqual(metadata[key as keyof typeof metadata], value, `roundtrip ${key}`);
    assert.equal(await h.keys.updateApiKeyPermissions(created.id, {}), false);
    assert.equal(await h.keys.updateApiKeyPermissions("absent", { noLog: true }), false);
    await h.keys.updateApiKeyPermissions(created.id, {
      accessSchedule: null,
      rateLimits: null,
      proxyId: null,
      dailyUsageLimitUsd: 0,
      weeklyUsageLimitUsd: -1,
      maxSessions: null,
    });
    const cleared = await h.keys.getApiKeyMetadata(created.key);
    assert.ok(cleared);
    assert.equal(cleared.accessSchedule, null);
    assert.equal(cleared.rateLimits, null);
    assert.equal(cleared.proxyId, null);
    assert.equal(cleared.dailyUsageLimitUsd, null);
    assert.equal(cleared.weeklyUsageLimitUsd, null);
    assert.equal(cleared.maxSessions, 0);
  });

  test("API key repositories: model normalization, alias candidates and deny precedence", async () => {
    const h = await createHarness();
    await h.reset();
    const created = await h.keys.createApiKey("models", "machine");
    assert.equal(await h.keys.isModelAllowedForKey(null, "model"), true);
    assert.equal(await h.keys.isModelAllowedForKey(created.key, null), false);
    assert.equal(await h.keys.isModelAllowedForKey("missing", "openai/model"), false);
    await h.keys.updateApiKeyPermissions(created.id, {
      modelAccessMode: "restricted",
      allowedModels: [],
    });
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "openai/model"), false);
    await h.keys.updateApiKeyPermissions(created.id, ["cx/*"]);
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "codex/model"), true);
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "no-think/codex/model"), true);
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "no-think/"), false);
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "no-think/openai/model"), false);
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "openai/model"), false);
    await h.keys.updateApiKeyPermissions(created.id, { blockedModels: ["codex/private*"] });
    assert.equal(
      await h.keys.isModelAllowedForKey(created.key, "no-think/codex/private-one"),
      false
    );
    assert.equal(
      await h.keys.isModelAllowedForKey(created.key, "cx/public", "codex/private-one"),
      false
    );
    assert.equal(await h.keys.isModelBlockedByPatterns(["cx/private*"], "codex/private-one"), true);
    await h.keys.updateApiKeyPermissions(created.id, {
      modelAccessMode: "all",
      allowedModels: ["ignored"],
    });
    assert.deepEqual((await h.keys.getApiKeyMetadata(created.key))?.allowedModels, []);
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "openai/model"), true);
  });

  test("API key repositories: lease invariants and internal key selection", async () => {
    const h = await createHarness();
    await h.reset();
    await assert.rejects(h.keys.createApiKey("invalid lease", "machine", ["lease:exclusive"]), {
      code: "LEASE_KEY_POLICY_INVALID",
    });
    const restricted = await h.keys.createApiKey("restricted", "machine", [], {
      modelAccessMode: "restricted",
      allowedModels: [],
    });
    const lease = await h.keys.createApiKey("lease", "machine", ["manage", "lease:exclusive"], {
      allowedConnections: ["one", "two"],
    });
    const regular = await h.keys.createApiKey("all", "machine");
    assert.equal(await h.keys.pickApiKeyForInternalUse(), regular.key);
    assert.deepEqual([...(await h.keys.getExclusiveLeaseConnectionIds())].sort(), ["one", "two"]);
    await assert.rejects(h.keys.updateApiKeyPermissions(lease.id, { allowedConnections: [] }), {
      code: "LEASE_KEY_POLICY_INVALID",
    });
    await assert.rejects(
      h.keys.updateApiKeyPermissions(restricted.id, { scopes: ["lease:exclusive"] }),
      { code: "LEASE_KEY_POLICY_INVALID" }
    );
    assert.deepEqual((await h.keys.getApiKeyMetadata(lease.key))?.allowedConnections, [
      "one",
      "two",
    ]);
    const manager = await h.keys.createApiKey("management", "machine", ["manage"]);
    assert.equal(await h.keys.pickApiKeyForInternalUse(), manager.key);
    await h.keys.revokeApiKey(manager.id);
    assert.equal(await h.keys.pickApiKeyForInternalUse(), regular.key);
  });

  test("API key repositories: lifecycle, rotation and idempotent row semantics", async () => {
    const h = await createHarness();
    await h.reset();
    const created = await h.keys.createApiKey("lifecycle", "machine");
    assert.equal(await h.keys.validateApiKey(created.key), true);
    await h.keys.setApiKeyExpiry(created.id, "2000-01-01T00:00:00.000Z");
    assert.equal(await h.keys.validateApiKey(created.key), false);
    await h.keys.setApiKeyExpiry(created.id, null);
    assert.equal(await h.keys.validateApiKey(created.key), true);
    await h.keys.updateApiKeyPermissions(created.id, { isBanned: true });
    assert.equal(await h.keys.validateApiKey(created.key), false);
    await h.keys.updateApiKeyPermissions(created.id, { isBanned: false, isActive: false });
    assert.equal(await h.keys.validateApiKey(created.key), false);
    await h.keys.updateApiKeyPermissions(created.id, { isActive: true });
    const rotated = await h.keys.regenerateApiKey(created.id);
    assert.ok(rotated);
    assert.notEqual(rotated.key, created.key);
    assert.equal(await h.keys.validateApiKey(created.key), false);
    assert.equal(await h.keys.getApiKeyMetadata(created.key), null);
    assert.equal(await h.keys.validateApiKey(rotated.key), true);
    assert.equal(await h.keys.revokeApiKey(created.id), true);
    const revoked = (await h.keys.getApiKeyMetadata(rotated.key))?.revokedAt;
    assert.ok(revoked);
    assert.equal(await h.keys.revokeApiKey(created.id), true);
    assert.equal((await h.keys.getApiKeyMetadata(rotated.key))?.revokedAt, revoked);
    assert.equal(await h.keys.validateApiKey(rotated.key), false);
    assert.equal(await h.keys.deleteApiKey(created.id), true);
    assert.equal(await h.keys.deleteApiKey(created.id), false);
    assert.equal(await h.keys.revokeApiKey("missing"), false);
    assert.equal(await h.keys.setApiKeyExpiry("missing", null), false);
    assert.equal(await h.keys.regenerateApiKey("missing"), null);
    assert.equal(await h.keys.getApiKeysCount(), 0);
  });

  test("API key repositories: env bootstrap and self-loop identity remain distinct", async () => {
    const h = await createHarness();
    await h.reset();
    const before = process.env.OMNIROUTE_API_KEY,
      router = process.env.ROUTER_API_KEY;
    try {
      process.env.OMNIROUTE_API_KEY = "conformance-env-key";
      delete process.env.ROUTER_API_KEY;
      assert.equal(await h.keys.validateApiKey("conformance-env-key"), true);
      const env = await h.keys.getApiKeyMetadata("conformance-env-key");
      assert.equal(env?.id, "env-key");
      assert.deepEqual(env?.scopes, ["manage"]);
      delete process.env.OMNIROUTE_API_KEY;
      const self = resolveSelfLoopBearer();
      assert.equal(await h.keys.validateApiKey(self), true);
      const meta = await h.keys.getApiKeyMetadata(self);
      assert.deepEqual(meta?.scopes, ["internal:self-loop"]);
      assert.deepEqual(meta?.allowedEndpoints, ["chat", "audio"]);
      assert.equal(await h.keys.validateApiKey("conformance-env-key"), false);
      assert.equal(await h.keys.getApiKeysCount(), 0);
    } finally {
      if (before === undefined) delete process.env.OMNIROUTE_API_KEY;
      else process.env.OMNIROUTE_API_KEY = before;
      if (router === undefined) delete process.env.ROUTER_API_KEY;
      else process.env.ROUTER_API_KEY = router;
    }
  });

  test("API key groups: membership, default denial, active groups and canonical provider denies", async () => {
    const h = await createHarness();
    await h.reset();
    const created = await h.keys.createApiKey("groups", "machine");
    const group = await h.groups.createKeyGroup("team", "description");
    assert.equal(await h.groups.addKeyToGroup(created.id, group.id), true);
    assert.equal(await h.groups.addKeyToGroup(created.id, group.id), true);
    assert.equal((await h.groups.getKeyGroupWithPermissions(group.id))?.memberCount, 1);
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "cx/public"), false);
    const allow = await h.groups.addGroupPermission(group.id, "*", "allow");
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "cx/public"), true);
    const deny = await h.groups.addGroupPermission(group.id, "private*", "deny", "codex");
    assert.equal(
      await h.keys.isModelAllowedForKey(created.key, "cx/public", "codex/private-one"),
      false
    );
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "cx/private-one"), false);
    assert.equal(await h.groups.removeGroupPermission(deny.id), true);
    await h.groups.updateKeyGroup(group.id, { isActive: false });
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "cx/private-one"), true);
    await h.groups.updateKeyGroup(group.id, { isActive: true });
    await h.keys.updateApiKeyPermissions(created.id, {
      modelAccessMode: "restricted",
      allowedModels: [],
    });
    assert.equal(await h.keys.isModelAllowedForKey(created.key, "cx/public"), false);
    assert.equal((await h.groups.getGroupPermissions(group.id))[0].id, allow.id);
    assert.equal(await h.groups.deleteKeyGroup(group.id), true);
    assert.deepEqual(await h.groups.getKeyGroupsForApiKey(created.id), []);
    assert.deepEqual(await h.groups.getGroupMembers(group.id), []);
    assert.equal(await h.groups.deleteKeyGroup(group.id), false);
  });
}
