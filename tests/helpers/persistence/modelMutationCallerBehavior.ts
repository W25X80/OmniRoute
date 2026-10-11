import assert from "node:assert/strict";
import type { TestContext } from "node:test";
import * as models from "../../../src/lib/db/models.ts";
import { createProviderConnection } from "../../../src/lib/db/providers.ts";
import * as managementRoute from "../../../src/app/api/provider-models/route.ts";
import * as testAllRoute from "../../../src/app/api/models/test-all/route.ts";
import { importManagedModels } from "../../../src/lib/providerModels/managedModelImport.ts";
import { makeManagementSessionRequest } from "../managementSession.ts";

type CatalogNamespace = "customModels" | "modelCompatOverrides" | "syncedAvailableModels";
export interface ModelMutationCallerFixture {
  prefix: string;
  selected<T>(body: () => T | Promise<T>): Promise<T>;
  readDurable(namespace: CatalogNamespace, key: string): Promise<string | undefined>;
}

type BatchBody = {
  results: Record<string, { status: string; hidden?: boolean; skipped?: boolean; error?: string }>;
};
type CompatEntry = ReturnType<typeof models.getModelCompatOverrides>[number];

async function durableRows(
  fixture: ModelMutationCallerFixture,
  namespace: CatalogNamespace,
  key: string
): Promise<Array<Record<string, unknown>>> {
  const value = await fixture.readDurable(namespace, key);
  assert.ok(value, `${namespace}/${key} must have committed bytes`);
  const rows: unknown = JSON.parse(value);
  assert.ok(Array.isArray(rows));
  return rows as Array<Record<string, unknown>>;
}

function requireCompat(rows: CompatEntry[], id: string): CompatEntry {
  const row = rows.find((entry) => entry.id === id);
  assert.ok(row, `compat override must exist for ${id}`);
  return row;
}

/** The same real public callers and durable observations qualify both storage paths. */
export async function runModelMutationCallerBehavior(
  t: TestContext,
  fixture: ModelMutationCallerFixture
): Promise<void> {
  const id = (suffix: string) => `${fixture.prefix}-${suffix}`;
  const originalFetch = globalThis.fetch;
  let fetchCalls = 0;
  globalThis.fetch = async () => {
    fetchCalls++;
    throw new Error("Model mutation caller fixture must not dispatch an HTTP request");
  };
  try {
    await t.test(
      "Test All commits a catalog-rejected model hide before reporting hidden",
      async () => {
        const connection = await fixture.selected(() =>
          createProviderConnection({
            provider: "codex",
            authType: "oauth",
            name: id("codex-account"),
            accessToken: "test-only-model-mutation-codex-token",
            isActive: true,
          })
        );
        assert.ok(connection);
        const connectionId = String(connection.id);
        await fixture.selected(() =>
          models.replaceSyncedAvailableModelsForConnection("codex", connectionId, [
            { id: id("advertised"), name: "Advertised fixture model" },
          ])
        );
        const modelId = id("unadvertised-hidden");
        const response = await fixture.selected(async () =>
          testAllRoute.POST(
            await makeManagementSessionRequest("http://localhost/api/models/test-all", {
              method: "POST",
              body: {
                providerId: "codex",
                connectionId,
                modelIds: [modelId],
                autoHideFailed: true,
              },
            })
          )
        );
        assert.equal(response.status, 200);
        const body = (await response.json()) as BatchBody;
        assert.equal(body.results[modelId]?.status, "error");
        assert.match(body.results[modelId]?.error ?? "", /not advertised for this account/);
        assert.equal(body.results[modelId]?.hidden, true);
        const stored = await durableRows(fixture, "modelCompatOverrides", "codex");
        assert.equal(stored.find((row) => row.id === modelId)?.isHidden, true);
        assert.equal(await fixture.selected(() => models.getModelIsHidden("codex", modelId)), true);

        const hiddenBytes = await fixture.readDurable("modelCompatOverrides", "codex");
        const visibleId = id("unadvertised-visible");
        const unhidden = await fixture.selected(async () =>
          testAllRoute.POST(
            await makeManagementSessionRequest("http://localhost/api/models/test-all", {
              method: "POST",
              body: {
                providerId: "codex",
                connectionId,
                modelIds: [visibleId],
                autoHideFailed: false,
              },
            })
          )
        );
        assert.equal(unhidden.status, 200);
        const visible = (await unhidden.json()) as BatchBody;
        assert.equal(visible.results[visibleId]?.status, "error");
        assert.notEqual(visible.results[visibleId]?.hidden, true);
        assert.equal(
          await fixture.selected(() => models.getModelIsHidden("codex", visibleId)),
          false
        );
        assert.equal(await fixture.readDurable("modelCompatOverrides", "codex"), hiddenBytes);
        assert.equal(fetchCalls, 0, "a nonempty Codex catalog rejects before sync or dispatch");
      }
    );

    await t.test(
      "Test All preserves the source web-session skip without a hidden write",
      async () => {
        await fixture.selected(() =>
          createProviderConnection({
            provider: "deepseek-web",
            authType: "apikey",
            name: id("web-account"),
            apiKey: "test-only-model-mutation-web-key",
            isActive: true,
          })
        );
        const modelId = id("skipped-web-model");
        const before = await fixture.readDurable("modelCompatOverrides", "deepseek-web");
        const response = await fixture.selected(async () =>
          testAllRoute.POST(
            await makeManagementSessionRequest("http://localhost/api/models/test-all", {
              method: "POST",
              body: { providerId: "deepseek-web", modelIds: [modelId], autoHideFailed: true },
            })
          )
        );
        assert.equal(response.status, 200);
        const body = (await response.json()) as BatchBody;
        assert.equal(body.results[modelId]?.status, "error");
        assert.equal(body.results[modelId]?.skipped, true);
        assert.notEqual(body.results[modelId]?.hidden, true);
        assert.equal(
          await fixture.selected(() => models.getModelIsHidden("deepseek-web", modelId)),
          false
        );
        assert.equal(await fixture.readDurable("modelCompatOverrides", "deepseek-web"), before);
        assert.equal(fetchCalls, 0);
      }
    );

    await t.test(
      "management PATCH merges missing-row visibility while preserving compatibility",
      async () => {
        const modelId = id("builtin");
        await fixture.selected(() =>
          models.mergeModelCompatOverrideAsync("openai", modelId, {
            normalizeToolCallId: true,
            preserveOpenAIDeveloperRole: false,
            upstreamHeaders: { "x-fixture-kept": "kept" },
            compatByProtocol: { claude: { preserveVideoUrl: true } },
          })
        );
        const url = `http://localhost/api/provider-models?provider=openai&modelId=${modelId}`;
        const response = await fixture.selected(async () =>
          managementRoute.PATCH(
            await makeManagementSessionRequest(url, {
              method: "PATCH",
              body: { isHidden: true, modality: "chat" },
            })
          )
        );
        assert.equal(response.status, 200);
        const body = (await response.json()) as { ok: boolean; updated: number };
        assert.equal(body.ok, true);
        assert.equal(body.updated, 1);
        const stored = await durableRows(fixture, "modelCompatOverrides", "openai");
        const row = stored.find((entry) => entry.id === modelId);
        assert.ok(row);
        assert.deepEqual(row.hiddenModalities, { chat: true });
        assert.equal(row.normalizeToolCallId, true);
        assert.equal(row.preserveOpenAIDeveloperRole, false);
        assert.deepEqual(row.upstreamHeaders, { "x-fixture-kept": "kept" });
        assert.deepEqual(row.compatByProtocol, { claude: { preserveVideoUrl: true } });
        await fixture.selected(async () => {
          assert.equal(models.getModelIsHidden("openai", modelId, "chat"), true);
          assert.equal(models.getModelIsHidden("openai", modelId, "images"), false);
          assert.equal(
            (await models.getCustomModels("openai")).some((entry) => entry.id === modelId),
            false
          );
          const get = await managementRoute.GET(
            await makeManagementSessionRequest(
              "http://localhost/api/provider-models?provider=openai"
            )
          );
          assert.equal(get.status, 200);
          const listed = (await get.json()) as { modelCompatOverrides: CompatEntry[] };
          assert.equal(requireCompat(listed.modelCompatOverrides, modelId).isHidden, true);
        });
      }
    );

    await t.test(
      "management PUT creates the source manual overlay and clears tri-state metadata",
      async () => {
        const modelId = id("manual-put");
        const put = async (body: Record<string, unknown>) =>
          fixture.selected(async () =>
            managementRoute.PUT(
              await makeManagementSessionRequest("http://localhost/api/provider-models", {
                method: "PUT",
                body: { provider: "openai", modelId, ...body },
              })
            )
          );
        const first = await put({
          modelName: "Manual fixture name",
          normalizeToolCallId: true,
          preserveOpenAIDeveloperRole: false,
          upstreamHeaders: { "x-fixture": "kept" },
          compatByProtocol: { openai: { preserveOpenAIDeveloperRole: true } },
        });
        assert.equal(first.status, 200);
        const created = (await first.json()) as { model: Record<string, unknown> };
        assert.equal(created.model.source, "manual");
        assert.equal(created.model.name, "Manual fixture name");
        assert.equal(created.model.preserveOpenAIDeveloperRole, false);
        const cleared = await put({ preserveOpenAIDeveloperRole: null, upstreamHeaders: null });
        assert.equal(cleared.status, 200);
        const stored = await durableRows(fixture, "customModels", "openai");
        const row = stored.find((entry) => entry.id === modelId);
        assert.ok(row);
        assert.equal(row.source, "manual");
        assert.equal(row.normalizeToolCallId, true);
        assert.equal(Object.hasOwn(row, "preserveOpenAIDeveloperRole"), false);
        assert.equal(Object.hasOwn(row, "upstreamHeaders"), false);
        assert.deepEqual(row.compatByProtocol, { openai: { preserveOpenAIDeveloperRole: true } });
      }
    );

    await t.test(
      "managed import carries removed chat-row compat and protects manual/media rows",
      async () => {
        const removedId = id("old-import");
        const manualId = id("manual-retained");
        const mediaId = id("speech-retained");
        const discoveredId = id("new-discovered");
        const connectionId = id("import-account");
        await fixture.selected(async () => {
          await models.replaceCustomModels("openai", [
            { id: removedId, name: "Old import", source: "imported", supportedEndpoints: ["chat"] },
            { id: manualId, name: "Operator name", source: "manual", supportedEndpoints: ["chat"] },
            {
              id: mediaId,
              name: "Speech import",
              source: "imported",
              supportedEndpoints: ["audio-transcriptions"],
            },
          ]);
          await models.updateCustomModel("openai", removedId, {
            isHidden: true,
            normalizeToolCallId: true,
            preserveOpenAIDeveloperRole: false,
            upstreamHeaders: { "x-import": "preserved" },
            compatByProtocol: { openai: { normalizeToolCallId: false } },
          });
          await models.mergeModelCompatOverrideAsync("openai", removedId, {
            compatByProtocol: { claude: { preserveVideoUrl: true } },
          });
          const result = await importManagedModels({
            providerId: "openai",
            connectionId,
            mode: "sync",
            fetchedModels: [{ id: discoveredId, name: "Discovered fixture" }],
          });
          assert.deepEqual(
            result.persistedModels.map((entry) => entry.id),
            [manualId, mediaId]
          );
          assert.equal(result.importedChanges.added, 1);
          assert.deepEqual(
            result.syncedAvailableModels.map((entry) => entry.id),
            [discoveredId]
          );
        });
        const custom = await durableRows(fixture, "customModels", "openai");
        assert.deepEqual(
          custom.map((entry) => entry.id),
          [manualId, mediaId]
        );
        assert.equal(custom[0].name, "Operator name");
        const stored = await durableRows(fixture, "modelCompatOverrides", "openai");
        const row = stored.find((entry) => entry.id === removedId);
        assert.ok(row);
        assert.equal(row.isHidden, true);
        assert.equal(row.normalizeToolCallId, true);
        assert.equal(row.preserveOpenAIDeveloperRole, false);
        assert.deepEqual(row.upstreamHeaders, { "x-import": "preserved" });
        assert.deepEqual(row.compatByProtocol, {
          claude: { preserveVideoUrl: true },
          openai: { normalizeToolCallId: false },
        });
        const synced = await durableRows(
          fixture,
          "syncedAvailableModels",
          `openai:${connectionId}`
        );
        assert.deepEqual(
          synced.map((entry) => entry.id),
          [discoveredId]
        );
        await fixture.selected(() => {
          assert.equal(models.getModelIsHidden("openai", removedId), true);
          const current = requireCompat(models.getModelCompatOverrides("openai"), removedId);
          assert.equal(current.preserveOpenAIDeveloperRole, false);
        });
        assert.equal(fetchCalls, 0);
      }
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
}
