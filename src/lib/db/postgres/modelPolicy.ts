import { getBuiltInAliases } from "@omniroute/open-sse/services/modelDeprecation.ts";
import { splitSyncedEffortSuffix } from "@omniroute/open-sse/services/model.ts";
import { getLearnedReasoningEffortForModel } from "@omniroute/open-sse/services/learnedReasoningEffortCaps.ts";
import { isSkippedEffortProvider } from "@omniroute/open-sse/utils/syncedEffortVariants.ts";
import type { ProviderNodeRepository } from "@/domain/persistence/providerRepositories";
import type { SettingsRepository } from "@/domain/persistence/settingsRepository";
import { resolveProviderId } from "@/shared/constants/providers";
import { isReservedProviderPrefix } from "@/shared/constants/reservedProviderPrefixes";
import { isHiddenForModality, type ModelHiddenFlags } from "@/shared/utils/modelVisibility";
import { asRecord } from "../models/shared";
import { normalizeSyncedAvailableModels, type SyncedAvailableModel } from "../models/synced";
import type { PostgresApiKeyModelPolicy } from "./apiKeyRepository";
import type { PostgresDatabase } from "./database";

interface CatalogRow {
  namespace: string;
  key: string;
  value: string;
}

function parseOrNull(value: string | undefined): unknown {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

/** Matches runtimeSettings' normalized custom deprecation-alias store. */
function settingsAliases(value: unknown): Record<string, string> {
  const record = asRecord(typeof value === "string" ? parseOrNull(value) : value);
  return Object.fromEntries(
    Object.entries(record)
      .map(([key, target]) => [key.trim(), typeof target === "string" ? target.trim() : ""])
      .filter(([key, target]) => key.length > 0 && target.length > 0)
  );
}

function syncedEffortBase(
  providerId: string,
  modelId: string,
  models: readonly SyncedAvailableModel[]
): string | null {
  if (isSkippedEffortProvider(providerId)) return null;
  for (const model of models) {
    if (!Array.isArray(model.supportedThinkingEfforts)) continue;
    const learned = getLearnedReasoningEffortForModel(model.id);
    const { baseModel, effort } = splitSyncedEffortSuffix(
      modelId,
      learned ? [...learned] : model.supportedThinkingEfforts
    );
    if (effort && baseModel === model.id) return model.id;
  }
  return null;
}

function hiddenForProvider(rows: CatalogRow[], providerId: string, modelId: string): boolean {
  const custom = parseOrNull(
    rows.find((row) => row.namespace === "customModels" && row.key === providerId)?.value
  );
  if (Array.isArray(custom)) {
    const records = custom.filter(
      (item): item is Record<string, unknown> =>
        Boolean(item) && typeof item === "object" && !Array.isArray(item)
    );
    const model =
      records.find((item) => item.id === modelId) ??
      records.find(
        (item) => typeof item.id === "string" && item.id.toLowerCase() === modelId.toLowerCase()
      );
    if (model && Object.hasOwn(model, "isHidden")) return Boolean(model.isHidden);
  }
  const compat = parseOrNull(
    rows.find((row) => row.namespace === "modelCompatOverrides" && row.key === providerId)?.value
  );
  if (!Array.isArray(compat)) return false;
  // readCompatList accepts object rows with string ids; the retired isDeleted tombstone
  // has no visibility effect. Exact id matching precedes the shared chat-scoped rule.
  const override = compat.find(
    (item) => item && typeof item === "object" && typeof item.id === "string" && item.id === modelId
  ) as ModelHiddenFlags | undefined;
  return isHiddenForModality(override, "chat");
}

/**
 * PostgreSQL-native catalog/alias authority used by the API-key model policy. Durable
 * inputs come only from this database and the selected node/settings repositories;
 * the static registry never substitutes for discovered/imported publication evidence.
 */
export function createPostgresModelPolicy(
  database: PostgresDatabase,
  providerNodesRepository: ProviderNodeRepository,
  settingsRepository: SettingsRepository
): PostgresApiKeyModelPolicy {
  async function canonicalProviderForAlias(prefix: string): Promise<string | null> {
    const canonical = resolveProviderId(prefix);
    if (canonical === prefix) return null;
    if (!isReservedProviderPrefix(prefix)) {
      const nodes = await providerNodesRepository.list();
      if (nodes.some((node) => node?.prefix === prefix || node?.id === prefix)) return null;
    }
    return canonical;
  }

  return {
    async preferClaudeCodeForUnprefixedClaudeModels() {
      try {
        const settings = await settingsRepository.get();
        if (typeof settings.preferClaudeCodeForUnprefixedClaudeModels === "boolean") {
          return settings.preferClaudeCodeForUnprefixedClaudeModels;
        }
      } catch {
        // Preserve the source's explicit environment fallback when settings are unavailable.
      }
      const value = process.env.OMNIROUTE_PREFER_CLAUDE_CODE_FOR_UNPREFIXED_CLAUDE_MODELS;
      return typeof value === "string" && /^(1|true|yes|on)$/i.test(value.trim());
    },

    async resolveModelAlias(modelId) {
      if (!modelId) return modelId;
      const aliases = settingsAliases((await settingsRepository.get()).modelAliases);
      // Read selected durable settings rather than another backend's process-global hydration.
      return aliases[modelId] || getBuiltInAliases()[modelId] || modelId;
    },

    canonicalProviderForAlias,

    async findPublishedModel(providerOrAlias, shortModelId) {
      const providerIds = [providerOrAlias];
      const canonical = await canonicalProviderForAlias(providerOrAlias);
      if (canonical) providerIds.push(canonical);
      const rows = await database.transaction(
        async (transaction) =>
          (
            await transaction.query<CatalogRow>(
              `SELECT namespace,key,value FROM omniroute.key_value
         WHERE (namespace='customModels' AND key=ANY($1::text[]))
            OR (namespace='syncedAvailableModels' AND EXISTS (
              SELECT 1 FROM unnest($1::text[]) provider(id) WHERE starts_with(key,provider.id || ':')
            )) ORDER BY namespace COLLATE "C",key COLLATE "C"`,
              [providerIds]
            )
          ).rows
      );
      for (const providerId of providerIds) {
        const prefix = `${providerId}:`;
        const byConnection: Record<string, SyncedAvailableModel[]> = {};
        for (const row of rows) {
          if (row.namespace !== "syncedAvailableModels" || !row.key.startsWith(prefix)) continue;
          const connectionId = row.key.slice(prefix.length);
          // The source's ordinary grouped Record does not enumerate its __proto__ key.
          if (connectionId === "__proto__") continue;
          byConnection[connectionId] = normalizeSyncedAvailableModels(
            parseOrNull(row.value),
            providerId
          );
        }
        const synced = Object.values(byConnection).flat();
        const value = rows.find(
          (row) => row.namespace === "customModels" && row.key === providerId
        )?.value;
        // Source custom publication parsing propagates corruption, unlike synced rows.
        const custom = value ? (JSON.parse(value) as SyncedAvailableModel[]) : [];
        const publishedModelId = synced.concat(custom).some((model) => model.id === shortModelId)
          ? shortModelId
          : syncedEffortBase(providerId, shortModelId, synced);
        if (publishedModelId) return { providerId, publishedModelId };
      }
      return null;
    },

    async isPublishedModelHidden(providerId, providerOrAlias, modelId) {
      const rows = await database.transaction(
        async (transaction) =>
          (
            await transaction.query<CatalogRow>(
              `SELECT namespace,key,value FROM omniroute.key_value
         WHERE namespace IN ('customModels','modelCompatOverrides') AND key=ANY($1::text[])`,
              [[providerId, providerOrAlias]]
            )
          ).rows
      );
      return (
        hiddenForProvider(rows, providerId, modelId) ||
        (providerId !== providerOrAlias && hiddenForProvider(rows, providerOrAlias, modelId))
      );
    },
  };
}
