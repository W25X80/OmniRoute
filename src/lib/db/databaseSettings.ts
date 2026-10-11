import {
  readPostgresCatalogNamespace,
  refreshSelectedPostgresCatalogAfterCommit,
} from "./postgres/catalogSnapshot";
import fs from "node:fs";

import { type DatabaseSettings } from "@/types/databaseSettings";

import { backupDbFile, setDbBackupMaxFiles } from "./backup";
import { DATA_DIR, SQLITE_FILE, applyDatabaseOptimizationSettings, getDbInstance } from "./core";
import { invalidateDbCache } from "./readCache";
import {
  CACHE_SECRET_MASK,
  decryptCacheSecrets,
  encryptCacheValue,
  isCacheSecret,
  assertCacheCredentialEndpoint,
  encryptLegacyCacheSecretCopies,
} from "./cacheSecrets";
import { getDatabaseStats } from "./stats";
import { getState as getVacuumSchedulerState, refreshVacuumScheduler } from "./vacuumScheduler";

import {
  DATABASE_SETTINGS_NAMESPACE,
  DATABASE_SETTINGS_SECTIONS,
  cloneDefaultSettings,
  isRecord,
  mergeTopLevelSections,
  mergeDatabaseSettingsNamespace,
  mergeRuntimeLogSettings,
  normalizeOptimizationSettings,
  mergeSectionObject,
  parseStoredValue,
  type UserDatabaseSettings,
  type DatabaseSettingsPatch,
} from "./databaseSettingsSemantics";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { createPostgresOperationsRepository } from "./postgres/operationsRepository";
export type { UserDatabaseSettings, DatabaseSettingsPatch } from "./databaseSettingsSemantics";

function readNamespace(namespace: string): Record<string, unknown> {
  const catalogRows = readPostgresCatalogNamespace(namespace);
  const rows =
    catalogRows !== undefined
      ? catalogRows
      : (getDbInstance()
          .prepare("SELECT key, value FROM key_value WHERE namespace = ?")
          .all(namespace) as Array<{ key: string; value: string }>);

  const values: Record<string, unknown> = {};
  for (const row of rows) {
    values[row.key] = parseStoredValue(row.value);
  }
  return values;
}

function getWalSizeBytes(): number {
  if (!SQLITE_FILE) return 0;

  try {
    const walPath = `${SQLITE_FILE}-wal`;
    return fs.existsSync(walPath) ? fs.statSync(walPath).size : 0;
  } catch {
    return 0;
  }
}

function getSchemaVersion(): number {
  const db = getDbInstance();

  try {
    const row = db
      .prepare("SELECT MAX(CAST(version AS INTEGER)) AS version FROM _omniroute_migrations")
      .get() as { version: number | null } | undefined;
    return row?.version ?? 0;
  } catch {
    return 0;
  }
}

function getFreelistCount(): number {
  try {
    return getDbInstance().pragma("freelist_count", { simple: true }) as number;
  } catch {
    return 0;
  }
}

function getIntegrityCheck(): "ok" | "error" | null {
  try {
    const result = getDbInstance().pragma("quick_check", { simple: true }) as string;
    return result === "ok" ? "ok" : "error";
  } catch {
    return null;
  }
}

export function getUserDatabaseSettings(): UserDatabaseSettings {
  const settings = cloneDefaultSettings();
  const mainSettings = readNamespace("settings");
  const databaseSettingsValue = mainSettings[DATABASE_SETTINGS_NAMESPACE];

  if (isRecord(databaseSettingsValue)) {
    mergeTopLevelSections(settings, databaseSettingsValue);
  }

  mergeTopLevelSections(settings, mainSettings);
  mergeDatabaseSettingsNamespace(settings, readNamespace(DATABASE_SETTINGS_NAMESPACE));
  mergeRuntimeLogSettings(settings, mainSettings);
  normalizeOptimizationSettings(settings);
  decryptCacheSecrets(settings.cache as unknown as Record<string, unknown>);

  return settings;
}

export function getDatabaseSettings(): DatabaseSettings {
  const dbStats = getDatabaseStats();
  const vacuumState = getVacuumSchedulerState();

  return {
    ...getUserDatabaseSettings(),
    location: {
      databasePath: SQLITE_FILE ?? ":memory:",
      dataDir: DATA_DIR,
      walSizeBytes: getWalSizeBytes(),
      schemaVersion: getSchemaVersion(),
    },
    stats: {
      databaseSizeBytes: dbStats.totalSize,
      pageCount: dbStats.pageCount,
      freelistCount: getFreelistCount(),
      lastVacuumAt:
        vacuumState.lastRunAt !== null ? new Date(vacuumState.lastRunAt).toISOString() : null,
      lastOptimizationAt: null,
      integrityCheck: getIntegrityCheck(),
      autoVacuumDrift: vacuumState.autoVacuumDrift,
      lastReclaimedPages: vacuumState.lastReclaimedPages,
    },
  };
}

export function updateDatabaseSettings(
  updates: Partial<UserDatabaseSettings>
): UserDatabaseSettings {
  const nextSettings = getUserDatabaseSettings();
  assertCacheCredentialEndpoint(nextSettings.cache, updates.cache);
  const optimizationUpdated = updates.optimization !== undefined;

  for (const section of DATABASE_SETTINGS_SECTIONS) {
    if (updates[section] !== undefined) {
      const sectionUpdate = { ...updates[section] } as Record<string, unknown>;
      if (section === "cache") {
        for (const key of Object.keys(sectionUpdate)) {
          if (isCacheSecret(key) && sectionUpdate[key] === CACHE_SECRET_MASK)
            delete sectionUpdate[key];
        }
      }
      mergeSectionObject(nextSettings, section, sectionUpdate);
    }
  }
  normalizeOptimizationSettings(nextSettings);

  const db = getDbInstance();
  const insert = db.prepare(
    "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)"
  );
  const settingsInsert = db.prepare(
    "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('settings', ?, ?)"
  );

  const requestedLogs = updates.logs as Partial<UserDatabaseSettings["logs"]> | undefined;
  const pipelineEnabled = requestedLogs?.callLogPipelineEnabled;
  const detailedEnabled = requestedLogs?.detailedLogsEnabled;

  const requestedBackup = updates.backup as Partial<UserDatabaseSettings["backup"]> | undefined;
  if (requestedBackup?.keepLastNBackups !== undefined) {
    setDbBackupMaxFiles(nextSettings.backup.keepLastNBackups);
  }

  const tx = db.transaction(() => {
    for (const section of DATABASE_SETTINGS_SECTIONS) {
      const sectionValues = nextSettings[section] as Record<string, unknown>;

      for (const [key, value] of Object.entries(sectionValues)) {
        const stored = section === "cache" ? encryptCacheValue(key, value) : value;
        insert.run(
          DATABASE_SETTINGS_NAMESPACE,
          `${section}.${key}`,
          JSON.stringify(stored ?? null)
        );
      }
    }

    if (pipelineEnabled !== undefined) {
      settingsInsert.run("call_log_pipeline_enabled", JSON.stringify(Boolean(pipelineEnabled)));
    }
    encryptLegacyCacheSecretCopies(db);
  });
  tx();

  backupDbFile("pre-write");
  invalidateDbCache("settings");
  if (optimizationUpdated) {
    applyDatabaseOptimizationSettings(nextSettings.optimization);
    refreshVacuumScheduler();
  }

  return nextSettings;
}

/** Await persistence without changing the legacy synchronous SQLite API. */
export async function getUserDatabaseSettingsAsync(): Promise<UserDatabaseSettings> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getUserDatabaseSettings();
  return createPostgresOperationsRepository(database).getUserDatabaseSettings();
}

export async function getDatabaseSettingsAsync(): Promise<DatabaseSettings> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getDatabaseSettings();
  return createPostgresOperationsRepository(database).getDatabaseSettings();
}

export async function updateDatabaseSettingsAsync(
  updates: DatabaseSettingsPatch
): Promise<UserDatabaseSettings> {
  const database = getSelectedPostgresDatabase();
  if (!database) return updateDatabaseSettings(updates as Partial<UserDatabaseSettings>);
  const settings =
    await createPostgresOperationsRepository(database).updateDatabaseSettings(updates);
  invalidateDbCache("settings");
  await refreshSelectedPostgresCatalogAfterCommit();
  return settings;
}
