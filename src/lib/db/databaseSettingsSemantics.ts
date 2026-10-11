import { DEFAULT_DATABASE_SETTINGS, type DatabaseSettings } from "@/types/databaseSettings";

export const DATABASE_SETTINGS_NAMESPACE = "databaseSettings";

export type UserDatabaseSettings = Omit<
  DatabaseSettings,
  "location" | "stats" | "backend" | "capabilities"
>;
export type DatabaseSettingsPatch = {
  [Section in keyof UserDatabaseSettings]?: Partial<UserDatabaseSettings[Section]>;
};
type DatabaseSettingsSection = keyof UserDatabaseSettings;

export const DATABASE_SETTINGS_SECTIONS = Object.keys(
  DEFAULT_DATABASE_SETTINGS
) as DatabaseSettingsSection[];

const LEGACY_FLAT_KEYS: {
  [TSection in DatabaseSettingsSection]: Partial<
    Record<keyof UserDatabaseSettings[TSection] & string, string[]>
  >;
} = {
  logs: {
    detailedLogsEnabled: ["detailedLogsEnabled"],
    callLogPipelineEnabled: ["callLogPipelineEnabled"],
    maxDetailSizeKb: ["maxDetailSizeKb"],
    ringBufferSize: ["ringBufferSize"],
  },
  backup: {
    autoBackupEnabled: ["autoBackupEnabled"],
    autoBackupFrequency: ["autoBackupFrequency"],
    keepLastNBackups: ["keepLastNBackups"],
  },
  cache: {
    semanticCacheEnabled: ["semanticCacheEnabled"],
    semanticCacheMaxSize: ["semanticCacheMaxSize"],
    semanticCacheTTL: ["semanticCacheTTL"],
    semanticCacheVectorEnabled: ["semanticCacheVectorEnabled"],
    semanticCacheBackend: ["semanticCacheBackend"],
    semanticCacheThreshold: ["semanticCacheThreshold"],
    semanticCacheEmbeddingProvider: ["semanticCacheEmbeddingProvider"],
    semanticCacheEmbeddingModel: ["semanticCacheEmbeddingModel"],
    semanticCacheEmbeddingDimension: ["semanticCacheEmbeddingDimension"],
    semanticCacheEmbeddingBaseUrl: ["semanticCacheEmbeddingBaseUrl"],
    semanticCacheEmbeddingApiKey: ["semanticCacheEmbeddingApiKey"],
    semanticCacheRedisUrl: ["semanticCacheRedisUrl"],
    semanticCacheRedisPrefix: ["semanticCacheRedisPrefix"],
    semanticCacheRequireZeroTemp: ["semanticCacheRequireZeroTemp"],
    promptCacheEnabled: ["promptCacheEnabled"],
    promptCacheStrategy: ["promptCacheStrategy"],
    alwaysPreserveClientCache: ["alwaysPreserveClientCache"],
    modelCatalogCacheTtlMs: ["modelCatalogCacheTtlMs"],
  },
  retention: {
    quotaSnapshots: ["quotaSnapshots"],
    compressionAnalytics: ["compressionAnalytics"],
    mcpAudit: ["mcpAudit"],
    configAudit: ["configAudit"],
    a2aEvents: ["a2aEvents"],
    callLogs: ["callLogs"],
    conversationTurnNodes: ["conversationTurnNodes"],
    usageHistory: ["usageHistory"],
    memoryEntries: ["memoryEntries"],
    domainCostHistory: ["domainCostHistory"],
    compressionCacheStats: ["compressionCacheStats"],
    xpAuditLog: ["xpAuditLog"],
    compressionRunTelemetry: ["compressionRunTelemetry"],
    autoCleanupEnabled: ["autoCleanupEnabled"],
  },
  aggregation: {
    enabled: ["aggregationEnabled", "enabled"],
    rawDataRetentionDays: ["rawDataRetentionDays"],
    granularity: ["granularity"],
  },
  optimization: {
    autoVacuumMode: ["autoVacuumMode"],
    scheduledVacuum: ["scheduledVacuum"],
    vacuumHour: ["vacuumHour"],
    pageSize: ["pageSize"],
    cacheSize: ["cacheSize"],
    optimizeOnStartup: ["optimizeOnStartup"],
  },
};

export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function cloneDefaultSettings(): UserDatabaseSettings {
  return structuredClone(DEFAULT_DATABASE_SETTINGS) as UserDatabaseSettings;
}

export function parseStoredValue(rawValue: unknown): unknown {
  if (typeof rawValue !== "string") return rawValue;

  try {
    return JSON.parse(rawValue);
  } catch {
    return rawValue;
  }
}

function toBooleanSetting(value: unknown): boolean | null {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return !Number.isNaN(value) && value !== 0;
  if (typeof value !== "string") return null;

  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return null;
}

export function normalizeOptimizationSettings(settings: UserDatabaseSettings) {
  const fallback = DEFAULT_DATABASE_SETTINGS.optimization.cacheSize;
  const numericCacheSize = Number(settings.optimization.cacheSize);
  settings.optimization.cacheSize =
    Number.isFinite(numericCacheSize) && numericCacheSize > 0
      ? Math.min(1000000, Math.floor(numericCacheSize))
      : fallback;
}

export function mergeSectionObject(
  target: UserDatabaseSettings,
  section: DatabaseSettingsSection,
  value: unknown
) {
  if (!isRecord(value)) return;

  const sectionTarget = target[section] as Record<string, unknown>;
  const defaultSection = DEFAULT_DATABASE_SETTINGS[section] as Record<string, unknown>;

  for (const key of Object.keys(defaultSection)) {
    if (value[key] !== undefined) {
      sectionTarget[key] = value[key];
    }
  }
}

export function mergeTopLevelSections(
  target: UserDatabaseSettings,
  values: Record<string, unknown>
) {
  for (const section of DATABASE_SETTINGS_SECTIONS) {
    mergeSectionObject(target, section, values[section]);
  }
}

export function mergeRuntimeLogSettings(
  target: UserDatabaseSettings,
  values: Record<string, unknown>
) {
  const pipelineEnabled = toBooleanSetting(values.call_log_pipeline_enabled);
  if (pipelineEnabled !== null) {
    target.logs.callLogPipelineEnabled = pipelineEnabled;
  }

  const legacyDetailedEnabled = toBooleanSetting(values.detailed_logs_enabled);
  if (legacyDetailedEnabled !== null) {
    target.logs.detailedLogsEnabled = legacyDetailedEnabled;
  }
}

export function mergeDatabaseSettingsNamespace(
  target: UserDatabaseSettings,
  values: Record<string, unknown>
) {
  for (const section of DATABASE_SETTINGS_SECTIONS) {
    const defaultSection = DEFAULT_DATABASE_SETTINGS[section] as Record<string, unknown>;
    const sectionTarget = target[section] as Record<string, unknown>;
    const flatAliases = LEGACY_FLAT_KEYS[section] as Partial<Record<string, string[]>>;

    for (const key of Object.keys(defaultSection)) {
      for (const alias of flatAliases[key] ?? []) {
        if (values[alias] !== undefined) {
          sectionTarget[key] = values[alias];
        }
      }

      const nestedKey = `${section}.${key}`;
      if (values[nestedKey] !== undefined) {
        sectionTarget[key] = values[nestedKey];
      }
    }
  }
}

export function resolveUserDatabaseSettings(
  mainSettings: Record<string, unknown>,
  namespaceSettings: Record<string, unknown>
): UserDatabaseSettings {
  const settings = cloneDefaultSettings();
  const nested = mainSettings[DATABASE_SETTINGS_NAMESPACE];
  if (isRecord(nested)) mergeTopLevelSections(settings, nested);
  mergeTopLevelSections(settings, mainSettings);
  mergeDatabaseSettingsNamespace(settings, namespaceSettings);
  mergeRuntimeLogSettings(settings, mainSettings);
  normalizeOptimizationSettings(settings);
  return settings;
}
