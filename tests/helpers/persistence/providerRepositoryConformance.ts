import assert from "node:assert/strict";
import test from "node:test";
import type {
  ProviderConnectionRepository,
  ProviderNodeRepository,
  ProviderRecord,
} from "../../../src/domain/persistence/providerRepositories.ts";

export interface ProviderConformanceFixture {
  connections: ProviderConnectionRepository;
  nodes: ProviderNodeRepository;
  reset(): Promise<void>;
  updateStoredConnection(id: string, patch: ProviderRecord): Promise<void>;
}

function connectionId(connection: ProviderRecord | null): string {
  assert.ok(connection);
  assert.equal(typeof connection.id, "string");
  return String(connection.id);
}

export function registerProviderRepositoryConformance(
  name: string,
  fixture: () => Promise<ProviderConformanceFixture>
): void {
  async function fresh() {
    const value = await fixture();
    await value.reset();
    return value;
  }

  test(`${name}: connection reads preserve encryption, filters, projection and nullable overrides`, async () => {
    const { connections } = await fresh();
    const created = await connections.create({
      provider: "openai",
      authType: "apikey",
      name: "key",
      apiKey: "secret-a",
      maxConcurrent: null,
      group: "Zebra",
      quotaWindowThresholds: { daily: 0 },
      rateLimitOverrides: { rpm: 0, modelConcurrency: { model: 2 } },
    });
    const id = connectionId(created);
    assert.match(String(created?.createdAt), /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    assert.equal(created?.createdAt, created?.updatedAt);
    assert.equal(created?.apiKey, "secret-a");
    assert.match(String((await connections.listRaw())[0].apiKey), /^enc:v1:/);
    assert.equal((await connections.list())[0].apiKey, "secret-a");
    assert.equal((await connections.findById(id))?.apiKey, "secret-a");
    assert.equal((await connections.findById(id))?.maxConcurrent, null);
    assert.deepEqual((await connections.findById(id))?.quotaWindowThresholds, { daily: 0 });
    assert.deepEqual((await connections.findById(id))?.rateLimitOverrides, {
      rpm: 0,
      modelConcurrency: { model: 2 },
    });
    await connections.create({
      provider: "anthropic",
      authType: "oauth",
      email: "b@example.com",
      accessToken: "token-b",
      isActive: false,
      group: "alpha",
    });
    assert.equal(await connections.count(), 2);
    assert.equal(
      await connections.count({ isActive: true, provider: "openai", authType: "apikey" }),
      1
    );
    assert.equal((await connections.list({ isActive: false }))[0].provider, "anthropic");
    assert.equal(await connections.count({ provider: "OPENAI" }), 0);
    assert.deepEqual(await connections.listRaw({ provider: "openai" }, 1, 0, ["id", "group"]), [
      { id, group: "Zebra", quotaWindowThresholds: null, rateLimitOverrides: null },
    ]);
    assert.equal((await connections.listRaw({}, 0)).length, 0);
    assert.equal((await connections.listRaw({}, -1, -1)).length, 2);
    assert.equal((await connections.listRaw({}, undefined, 100)).length, 2);
    assert.deepEqual(await connections.distinctGroups(), ["Zebra", "alpha"]);
    assert.deepEqual(await connections.displayMetadata([id, "", id, "missing"]), [
      { id, name: "key", displayName: null, email: null },
    ]);
    await assert.rejects(
      connections.listRaw({}, undefined, undefined, ["id; DROP TABLE provider_connections"]),
      /invalid column/
    );
    assert.equal(await connections.findById("missing"), null);
  });

  test(`${name}: connection ordering keeps NULL and insertion ties and zero-priority promotion`, async () => {
    const { connections, updateStoredConnection } = await fresh();
    const first = connectionId(
      await connections.create({ provider: "order", authType: "access_token" })
    );
    const second = connectionId(
      await connections.create({ provider: "order", authType: "access_token" })
    );
    const third = connectionId(
      await connections.create({ provider: "order", authType: "access_token" })
    );
    await updateStoredConnection(first, { priority: 2, updated_at: "same" });
    await updateStoredConnection(second, { priority: 2, updated_at: "same" });
    await updateStoredConnection(third, { priority: null, updated_at: "same" });
    assert.deepEqual(
      (await connections.listRaw()).map((row) => row.id),
      [third, first, second]
    );
    await connections.reorder("order");
    assert.deepEqual(
      (await connections.listRaw()).map((row) => row.priority),
      [1, 2, 3]
    );
    assert.equal((await connections.update(second, { priority: 0 }))?.priority, 1);
    assert.equal((await connections.listRaw())[0].id, second);
    await connections.update(second, { priority: 99 });
    assert.equal((await connections.listRaw()).at(-1)?.priority, 99);
  });

  test(`${name}: hosted, local and web-cookie key identities preserve their distinct dedup rules`, async () => {
    const { connections } = await fresh();
    const hosted = connectionId(
      await connections.create({
        provider: "openai",
        authType: "apikey",
        name: "a",
        apiKey: "same",
      })
    );
    assert.equal(
      connectionId(
        await connections.create({
          provider: "openai",
          authType: "apikey",
          name: "b",
          apiKey: " same ",
        })
      ),
      hosted
    );
    assert.equal(
      connectionId(
        await connections.create({
          provider: "openai",
          authType: "apikey",
          name: "b",
          apiKey: "different",
        })
      ),
      hosted
    );
    const local = connectionId(
      await connections.create({
        provider: "lm-studio",
        authType: "apikey",
        apiKey: "local",
        providerSpecificData: { baseUrl: "http://one/v1/" },
      })
    );
    const otherLocal = connectionId(
      await connections.create({
        provider: "lm-studio",
        authType: "apikey",
        apiKey: "local",
        providerSpecificData: { baseUrl: "http://two/v1" },
      })
    );
    assert.notEqual(local, otherLocal);
    assert.equal(
      connectionId(
        await connections.create({
          provider: "lm-studio",
          authType: "apikey",
          apiKey: "local",
          providerSpecificData: { baseUrl: " http://one/v1 " },
        })
      ),
      local
    );
    const web = connectionId(
      await connections.create({
        provider: "grok-web",
        authType: "apikey",
        name: "shared",
        apiKey: "web-one",
      })
    );
    const webOther = connectionId(
      await connections.create({
        provider: "grok-web",
        authType: "apikey",
        name: "shared",
        apiKey: "web-two",
      })
    );
    assert.notEqual(web, webOther);
    assert.equal(
      connectionId(
        await connections.create({
          provider: "grok-web",
          authType: "apikey",
          name: "renamed",
          apiKey: "web-one",
        })
      ),
      web
    );
    assert.equal(await connections.count({ provider: "grok-web" }), 2);
  });

  test(`${name}: cookie credentials outrank names and bare access-token imports remain separate`, async () => {
    const { connections } = await fresh();
    const cookie = connectionId(
      await connections.create({
        provider: "grok-web",
        authType: "cookie",
        name: "same",
        providerSpecificData: { cookie: "first" },
      })
    );
    const other = connectionId(
      await connections.create({
        provider: "grok-web",
        authType: "cookie",
        name: "same",
        providerSpecificData: { cookie: "second" },
      })
    );
    assert.notEqual(cookie, other);
    assert.equal(
      connectionId(
        await connections.create({
          provider: "grok-web",
          authType: "cookie",
          name: "renamed",
          providerSpecificData: { cookie: "first" },
        })
      ),
      cookie
    );
    const firstToken = connectionId(
      await connections.create({
        provider: "codex",
        authType: "access_token",
        email: "same@example.com",
        accessToken: "same",
      })
    );
    assert.notEqual(
      connectionId(
        await connections.create({
          provider: "codex",
          authType: "access_token",
          email: "same@example.com",
          accessToken: "same",
        })
      ),
      firstToken
    );
  });

  test(`${name}: OAuth profile and organization identities coexist; Codex legacy identity promotes safely`, async () => {
    const { connections } = await fresh();
    const profile = connectionId(
      await connections.create({
        provider: "kiro",
        authType: "oauth",
        email: "a@example.com",
        providerSpecificData: { profileArn: "first" },
      })
    );
    assert.notEqual(
      connectionId(
        await connections.create({
          provider: "kiro",
          authType: "oauth",
          email: "a@example.com",
          providerSpecificData: { profileArn: "second" },
        })
      ),
      profile
    );
    assert.equal(
      connectionId(
        await connections.create({
          provider: "kiro",
          authType: "oauth",
          email: "a@example.com",
          providerSpecificData: { profileArn: "first" },
        })
      ),
      profile
    );
    const org = connectionId(
      await connections.create({
        provider: "claude",
        authType: "oauth",
        email: "a@example.com",
        providerSpecificData: { organizationUUID: "personal" },
      })
    );
    assert.notEqual(
      connectionId(
        await connections.create({
          provider: "claude",
          authType: "oauth",
          email: "a@example.com",
          providerSpecificData: { organizationUUID: "team" },
        })
      ),
      org
    );
    const legacy = await connections.create({
      provider: "codex",
      authType: "oauth",
      email: "legacy@example.com",
      accessToken: "token",
      providerSpecificData: { workspaceId: "workspace" },
    });
    const id = connectionId(legacy);
    const enriched = await connections.create({
      provider: "codex",
      authType: "oauth",
      email: "legacy@example.com",
      accessToken: "new-token",
      providerSpecificData: { workspaceId: "workspace", chatgptUserId: "first-user" },
    });
    assert.equal(connectionId(enriched), id);
    assert.equal(
      (enriched?.providerSpecificData as ProviderRecord).codexFingerprintSeed,
      (legacy?.providerSpecificData as ProviderRecord).codexFingerprintSeed
    );
    assert.notEqual(
      connectionId(
        await connections.create({
          provider: "codex",
          authType: "oauth",
          email: "legacy@example.com",
          providerSpecificData: { workspaceId: "workspace", chatgptUserId: "second-user" },
        })
      ),
      id
    );
    assert.equal(
      connectionId(
        await connections.create({
          provider: "codex",
          authType: "oauth",
          providerSpecificData: { workspaceId: "workspace", chatgptUserId: "first-user" },
        })
      ),
      id
    );
  });

  test(`${name}: partial writes preserve ciphertext and normalization; parent cooldown clear keeps quota evidence`, async () => {
    const { connections } = await fresh();
    const id = connectionId(
      await connections.create({
        provider: "codex",
        authType: "oauth",
        accessToken: "access",
        refreshToken: "refresh",
        idToken: "id",
        providerSpecificData: {
          codexScopeRateLimitedUntil: { codex: "future" },
          codexScopeRateLimitSource: { codex: "quota_reset" },
          codexQuotaState: { used: 1 },
        },
      })
    );
    const before = (await connections.listRaw())[0];
    await connections.update(id, { name: "new", rateLimitedUntil: null });
    const after = (await connections.listRaw())[0];
    for (const field of ["accessToken", "refreshToken", "idToken"])
      assert.equal(after[field], before[field]);
    const psd = after.providerSpecificData as ProviderRecord;
    assert.equal(psd.codexScopeRateLimitedUntil, undefined);
    assert.equal(psd.codexScopeRateLimitSource, undefined);
    assert.deepEqual(psd.codexQuotaState, { used: 1 });
    await connections.update(id, {
      accessToken: "replaced",
      refreshToken: null,
      maxConcurrent: null,
      rateLimitOverrides: null,
    });
    assert.equal((await connections.findById(id))?.accessToken, "replaced");
    assert.equal((await connections.listRaw())[0].refreshToken, undefined);
    assert.equal((await connections.listRaw())[0].maxConcurrent, null);
    await assert.rejects(
      connections.update(id, { quotaWindowThresholds: { daily: 101 } }),
      /rejected keys/
    );
    await assert.rejects(
      connections.update(id, { rateLimitOverrides: { rpm: -1 } }),
      /rejected keys/
    );
    assert.equal((await connections.findById(id))?.name, "new");
    await connections.mergeProviderSpecificData(id, { first: 1 });
    await connections.mergeProviderSpecificData(id, { second: 2 });
    assert.deepEqual((await connections.findById(id))?.providerSpecificData, {
      ...psd,
      first: 1,
      second: 2,
    });
  });

  test(`${name}: node partial updates preserve headers, zero quota hour and unambiguous resolution`, async () => {
    const { nodes } = await fresh();
    const id = "openai-compatible-00000000-0000-4000-8000-000000000001";
    await nodes.create({
      id,
      type: "openai-compatible",
      name: "node",
      customHeaders: { "X-Private": "value" },
      dailyQuotaResetHour: 0,
      iconUrl: "https://example.test/icon.png",
    });
    assert.equal(await nodes.count({ type: "openai-compatible" }), 1);
    assert.equal((await nodes.resolveForConnection("openai-compatible"))?.id, id);
    assert.deepEqual((await nodes.update(id, { name: "renamed" }))?.customHeaders, {
      "X-Private": "value",
    });
    assert.equal((await nodes.findById(id))?.dailyQuotaResetHour, 0);
    await nodes.update(id, { customHeaders: null, dailyQuotaResetHour: null, iconUrl: null });
    assert.equal((await nodes.findById(id))?.customHeaders, null);
    assert.equal((await nodes.findById(id))?.dailyQuotaResetHour, null);
    assert.equal((await nodes.findById(id))?.iconUrl, null);
    await nodes.create({
      id: "openai-compatible-00000000-0000-4000-8000-000000000002",
      type: "openai-compatible",
      name: "other",
    });
    assert.equal(await nodes.resolveForConnection("openai-compatible"), null);
    assert.equal((await nodes.resolveForConnection(id))?.id, id);
    assert.equal((await nodes.list({}, 1, 1)).length, 1);
    assert.equal((await nodes.deleteById(id))?.name, "renamed");
    assert.equal(await nodes.deleteById(id), null);
    assert.equal(await nodes.update("missing", { name: "missing" }), null);
  });

  test(`${name}: deletion modes preserve affected counts and only single deletion compacts priorities`, async () => {
    const { connections } = await fresh();
    const ids: string[] = [];
    for (let index = 0; index < 3; index++)
      ids.push(
        connectionId(await connections.create({ provider: "delete", authType: "access_token" }))
      );
    assert.equal(await connections.deleteByIds([ids[1], ids[1], "missing"]), 1);
    assert.deepEqual(
      (await connections.listRaw()).map((row) => row.priority),
      [1, 3]
    );
    assert.equal(await connections.deleteById(ids[0]), true);
    assert.equal((await connections.listRaw())[0].priority, 1);
    assert.equal(await connections.deleteById("missing"), false);
    assert.equal(await connections.deleteByIds([]), 0);
    assert.equal(await connections.deleteByProvider("delete"), 1);
    assert.equal(await connections.deleteByProvider("delete"), 0);
  });

  test(`${name}: active retirement policies preserve ciphertext, audited identities and canonical restoration`, async () => {
    const { connections } = await fresh();
    const policies = [
      {
        provider: " \tMSDESIGNER\n",
        source: "migration:retire-microsoft-designer-web",
        type: "provider_retired",
      },
      {
        provider: "\uFEFFFelo\u00a0",
        source: "migration:retire-felo-web",
        type: "provider_removed",
      },
      { provider: "\u2000QW\u3000", source: "migration:retire-qwen-web", type: "provider_removed" },
      { provider: "cgpt-web", source: "migration:retire-chatgpt-web", type: "provider_removed" },
    ];
    const ids = new Map<string, string>();
    for (const policy of policies) {
      const created = await connections.create({
        provider: policy.provider,
        authType: "apikey",
        apiKey: "retained",
        isActive: true,
      });
      const id = connectionId(created);
      ids.set(policy.source, id);
      assert.equal(created?.isActive, false);
      assert.equal(created?.testStatus, "unavailable");
      assert.equal(created?.lastErrorSource, policy.source);
      assert.equal(created?.lastErrorType, policy.type);
      const before = (await connections.listRaw({ provider: policy.provider }))[0];
      const updated = await connections.update(id, {
        isActive: true,
        testStatus: "active",
        lastError: null,
        lastErrorAt: null,
      });
      assert.equal(updated?.isActive, false);
      assert.equal(updated?.testStatus, "unavailable");
      assert.equal(updated?.lastErrorSource, policy.source);
      assert.equal(
        (await connections.listRaw({ provider: policy.provider }))[0].apiKey,
        before.apiKey
      );
    }
    const felo = ids.get("migration:retire-felo-web")!;
    await assert.rejects(connections.update(felo, { provider: "openai", isActive: true }));
    assert.equal((await connections.findById(felo))?.provider, "\uFEFFFelo\u00a0");
    const legacy = ids.get("migration:retire-chatgpt-web")!;
    await assert.rejects(connections.update(legacy, { provider: "openai", isActive: true }));
    assert.equal(
      (await connections.update(legacy, { provider: "chatgpt-web", isActive: true }))?.isActive,
      true
    );
    await assert.rejects(connections.update(legacy, { provider: "openai" }));
    // Qwen's current migration has a tombstone but no immutable-identity trigger.
    const qwen = ids.get("migration:retire-qwen-web")!;
    assert.equal(
      (await connections.update(qwen, { provider: "openai", isActive: true }))?.provider,
      "openai"
    );
    // One-time migration updates do not define permanent constraints on later row writes.
    assert.equal(
      (await connections.create({ provider: "raycast", authType: "apikey", apiKey: "later" }))
        ?.isActive,
      true
    );
    assert.equal(
      (
        await connections.create({
          provider: "chatgpt-web",
          authType: "apikey",
          apiKey: "canonical",
        })
      )?.isActive,
      true
    );
  });
}
