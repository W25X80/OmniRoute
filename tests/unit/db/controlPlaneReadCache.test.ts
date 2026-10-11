import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  UnsupportedPersistenceDomainError,
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";

const testDataDir = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-control-plane-cache-"));
const originalDataDir = process.env.DATA_DIR;
const originalInitialPassword = process.env.INITIAL_PASSWORD;
process.env.DATA_DIR = testDataDir;
delete process.env.INITIAL_PASSWORD;

const core = await import("../../../src/lib/db/core.ts");
const settingsDb = await import("../../../src/lib/db/settings.ts");
const cache = await import("../../../src/lib/db/readCache.ts");

test.after(() => {
  core.resetDbInstance();
  fs.rmSync(testDataDir, { recursive: true, force: true });
  if (originalDataDir === undefined) delete process.env.DATA_DIR;
  else process.env.DATA_DIR = originalDataDir;
  if (originalInitialPassword === undefined) delete process.env.INITIAL_PASSWORD;
  else process.env.INITIAL_PASSWORD = originalInitialPassword;
});

function selectedBinding(label: string) {
  let ready = true;
  const calls: Array<{ method: string; arguments: unknown[] }> = [];
  const unused = async (): Promise<never> => {
    throw new Error("Unexpected repository operation in cache dispatch test");
  };
  const binding: PostgresControlPlaneBinding = {
    assertReady() {
      if (!ready) throw new Error("Test control plane is not ready");
    },
    secrets: { get: unused, getOrCreate: unused },
    settings: {
      async get() {
        return { selectedLabel: label };
      },
      async getRevision() {
        return 42;
      },
      async update(updates, options) {
        calls.push({ method: "settings.update", arguments: [updates, options] });
        return { selectedLabel: label, ...updates };
      },
    },
    providers: {
      async list(filter) {
        calls.push({ method: "providers.list", arguments: [filter] });
        return [{ id: "connection", selectedLabel: label }];
      },
      async listRaw(filter) {
        calls.push({ method: "providers.listRaw", arguments: [filter] });
        return [{ id: "connection", rawLabel: label }];
      },
      async findById(id) {
        calls.push({ method: "providers.findById", arguments: [id] });
        return { id, selectedLabel: label };
      },
      count: unused,
      displayMetadata: unused,
      create: unused,
      update: unused,
      reorder: unused,
      deleteById: unused,
      deleteByIds: unused,
      deleteByProvider: unused,
      mergeProviderSpecificData: unused,
      touchLastUsed: unused,
      touchSyncedModelsAt: unused,
      resetBackoff: unused,
      distinctGroups: unused,
    },
    nodes: {
      async list(filter) {
        calls.push({ method: "nodes.list", arguments: [filter] });
        return [{ id: "node", selectedLabel: label }];
      },
      count: unused,
      findById: unused,
      resolveForConnection: unused,
      create: unused,
      update: unused,
      deleteById: unused,
    },
    apiKeys: {
      getApiKeys: unused,
      getApiKeysCount: unused,
      getApiKeyById: unused,
      createApiKey: unused,
      regenerateApiKey: unused,
      updateApiKeyPermissions: unused,
      deleteApiKey: unused,
      revokeApiKey: unused,
      setApiKeyExpiry: unused,
      validateApiKey: unused,
      getApiKeyMetadata: unused,
      isModelAllowedForKey: unused,
      isModelBlockedByPatterns: unused,
      pickApiKeyForInternalUse: unused,
      getExclusiveLeaseConnectionIds: unused,
      clearApiKeyCaches() {
        throw new Error("Unexpected API-key cache mutation");
      },
    },
    keyGroups: {
      getAllKeyGroups: unused,
      getKeyGroup: unused,
      getKeyGroupWithPermissions: unused,
      createKeyGroup: unused,
      updateKeyGroup: unused,
      deleteKeyGroup: unused,
      getGroupPermissions: unused,
      addGroupPermission: unused,
      removeGroupPermission: unused,
      getGroupMembers: unused,
      getKeyGroupsForApiKey: unused,
      addKeyToGroup: unused,
      removeKeyFromGroup: unused,
      checkKeyModelAccess: unused,
    },
  };
  return {
    binding,
    calls,
    close() {
      ready = false;
    },
  };
}

test("selected settings facade preserves arguments and delegates every settings operation", async () => {
  const selected = selectedBinding("selected");
  const updates = { cloudEnabled: true };
  const options = { expectedRevision: 41 };
  await withPostgresControlPlane(selected.binding, async () => {
    assert.deepEqual(await settingsDb.getSettings(), { selectedLabel: "selected" });
    assert.equal(await settingsDb.getSettingsRevision(), 42);
    assert.deepEqual(await settingsDb.updateSettings(updates, options), {
      selectedLabel: "selected",
      cloudEnabled: true,
    });
  });
  assert.equal(selected.calls[0].arguments[0], updates);
  assert.equal(selected.calls[0].arguments[1], options);
});

test("selected reads bypass prewarmed SQLite caches and stay isolated between concurrent scopes", async () => {
  await settingsDb.updateSettings({ selectedLabel: "sqlite" });
  cache.invalidateDbCache();
  const sqliteSettings = await cache.getCachedSettings();
  assert.equal(sqliteSettings.selectedLabel, "sqlite");
  assert.deepEqual(await cache.getCachedProviderConnections(), []);
  assert.deepEqual(await cache.getCachedRawProviderConnections(), []);
  assert.equal(await cache.getCachedProviderConnectionById("connection"), null);
  assert.deepEqual(await cache.getCachedProviderNodes(), []);

  const first = selectedBinding("first");
  const second = selectedBinding("second");
  const filter = { provider: "only-this-provider" };
  await Promise.all(
    [first, second].map(({ binding, calls }) =>
      withPostgresControlPlane(binding, async () => {
        const label = (await cache.getCachedSettings()).selectedLabel;
        assert.deepEqual(await cache.getCachedProviderConnections(filter), [
          { id: "connection", selectedLabel: label },
        ]);
        assert.deepEqual(await cache.getCachedRawProviderConnections(filter), [
          { id: "connection", rawLabel: label },
        ]);
        assert.deepEqual(await cache.getCachedProviderConnectionById("connection"), {
          id: "connection",
          selectedLabel: label,
        });
        assert.deepEqual(await cache.getCachedProviderNodes(filter), [
          { id: "node", selectedLabel: label },
        ]);
        assert.equal(calls[0].arguments[0], filter);
        assert.equal(calls[1].arguments[0], filter);
        assert.equal(calls[3].arguments[0], filter);
        assert.equal(label, binding === first.binding ? "first" : "second");
      })
    )
  );
  assert.equal(await cache.getCachedSettings(), sqliteSettings);
  assert.deepEqual(await cache.getCachedProviderConnections(), []);
});

test("pending SQLite cache loads cannot supply values to the selected control plane", async () => {
  cache.invalidateDbCache("settings");
  const sqlitePending = cache.getCachedSettings();
  const selected = selectedBinding("selected");
  await withPostgresControlPlane(selected.binding, async () => {
    assert.equal((await cache.getCachedSettings()).selectedLabel, "selected");
  });
  assert.equal((await sqlitePending).selectedLabel, "sqlite");
  assert.equal((await cache.getCachedSettings()).selectedLabel, "sqlite");
});

test("unsupported pricing and LKGP reject even when the SQLite cache is already populated", async () => {
  await cache.getCachedPricing();
  assert.equal(await cache.getCachedLKGP("cached", "model"), null);
  const selected = selectedBinding("selected");
  await withPostgresControlPlane(selected.binding, async () => {
    await assert.rejects(cache.getCachedPricing(), UnsupportedPersistenceDomainError);
    await assert.rejects(cache.getCachedLKGP("cached", "model"), UnsupportedPersistenceDomainError);
  });
});

test("every selected read checks readiness before serving a repository or a warmed cache", async () => {
  const selected = selectedBinding("selected");
  await withPostgresControlPlane(selected.binding, async () => {
    await cache.getCachedSettings();
    selected.close();
    for (const read of [
      settingsDb.getSettings,
      settingsDb.getSettingsRevision,
      cache.getCachedSettings,
      cache.getCachedPricing,
      cache.getCachedProviderConnections,
      cache.getCachedRawProviderConnections,
      () => cache.getCachedProviderConnectionById("connection"),
      cache.getCachedProviderNodes,
      () => cache.getCachedLKGP("cached", "model"),
    ]) {
      await assert.rejects(read(), /Test control plane is not ready/);
    }
  });
});
