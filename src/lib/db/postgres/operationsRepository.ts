import { lockApiKeyPolicy, advanceApiKeyPolicyEpoch } from "./apiKeyPolicyState";
import { getSyntheticApiKeyIds } from "@/shared/constants/apiKeyIdentities";
import type { DatabaseSettings } from "@/types/databaseSettings";
import type { DatabaseStats } from "../stats";
import type { DbHealthCheckResult } from "../healthCheck";
import {
  DATABASE_SETTINGS_SECTIONS,
  mergeSectionObject,
  normalizeOptimizationSettings,
  parseStoredValue,
  resolveUserDatabaseSettings,
  type UserDatabaseSettings,
  type DatabaseSettingsPatch,
} from "../databaseSettingsSemantics";
import {
  assertCacheCredentialEndpoint,
  CACHE_SECRET_KEYS,
  CACHE_SECRET_MASK,
  decryptCacheSecrets,
  encryptCacheValue,
  isCacheSecret,
} from "../cacheSecrets";
import { PersistenceCapabilityError } from "../persistenceCapabilities";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";

type StoredRow = { namespace: string; key: string; value: string };

function namespaces(rows: StoredRow[]) {
  const main: Record<string, unknown> = {};
  const settings: Record<string, unknown> = {};
  for (const row of rows) {
    (row.namespace === "settings" ? main : settings)[row.key] = parseStoredValue(row.value);
  }
  return { main, settings };
}

async function readSettings(transaction: PostgresTransaction): Promise<UserDatabaseSettings> {
  const result = await transaction.query<StoredRow>(
    "SELECT namespace, key, value FROM omniroute.key_value WHERE namespace IN ('settings', 'databaseSettings')"
  );
  const values = namespaces(result.rows);
  const settings = resolveUserDatabaseSettings(values.main, values.settings);
  decryptCacheSecrets(settings.cache as unknown as Record<string, unknown>);
  return settings;
}

function protectCacheValue(key: string, value: unknown): unknown {
  const stored = encryptCacheValue(key, value);
  if (
    isCacheSecret(key) &&
    typeof value === "string" &&
    value &&
    (typeof stored !== "string" || !stored.startsWith("enc:v1:"))
  ) {
    throw new Error("PostgreSQL cache credentials require storage encryption");
  }
  return stored;
}

async function protectLegacyCopies(transaction: PostgresTransaction): Promise<void> {
  const keys = [
    "cache",
    "databaseSettings",
    ...CACHE_SECRET_KEYS,
    ...CACHE_SECRET_KEYS.map((key) => `cache.${key}`),
  ];
  const result = await transaction.query<StoredRow>(
    "SELECT namespace, key, value FROM omniroute.key_value WHERE namespace IN ('settings', 'databaseSettings') AND key = ANY($1::text[])",
    [keys]
  );
  for (const row of result.rows) {
    let value = parseStoredValue(row.value);
    if (value && typeof value === "object" && !Array.isArray(value)) {
      const record = value as Record<string, unknown>;
      const cache = row.key === "databaseSettings" ? record.cache : record;
      if (cache && typeof cache === "object" && !Array.isArray(cache)) {
        const fields = cache as Record<string, unknown>;
        for (const key of CACHE_SECRET_KEYS) {
          if (fields[key] !== undefined) fields[key] = protectCacheValue(key, fields[key]);
        }
      }
    } else value = protectCacheValue(row.key.replace(/^cache\./, ""), value);
    const encoded = JSON.stringify(value);
    if (encoded !== row.value) {
      await transaction.query(
        "UPDATE omniroute.key_value SET value = $1 WHERE namespace = $2 AND key = $3",
        [encoded, row.namespace, row.key]
      );
    }
  }
}

export const POSTGRES_OPERATION_CAPABILITIES = {
  backend: "postgresql",
  settings: true,
  statistics: true,
  applicationDiagnostics: true,
  databaseFileBackup: false,
  databaseFileRestore: false,
  databaseFileImport: false,
  sqliteOptimization: false,
  backupAuthority: "database-operator",
} as const;

async function readStats(transaction: PostgresTransaction): Promise<DatabaseStats> {
  const tables = await transaction.query<{ name: string; row_count: string; size: string }>(
    `SELECT c.relname AS name, GREATEST(c.reltuples, 0)::bigint::text AS row_count,
      pg_total_relation_size(c.oid)::text AS size
     FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'omniroute' AND c.relkind = 'r' ORDER BY c.relname`
  );
  const indexes = await transaction.query<{ name: string; tableName: string }>(
    `SELECT indexname AS name, tablename AS "tableName"
     FROM pg_catalog.pg_indexes WHERE schemaname = 'omniroute' ORDER BY indexname`
  );
  const tableStats = tables.rows.map((row) => ({
    name: row.name,
    rowCount: Number(row.row_count),
    rowCountApproximate: true,
    size: Number(row.size),
  }));
  return {
    backend: "postgresql",
    totalSize: tableStats.reduce((sum, table) => sum + table.size, 0),
    pageSize: null,
    pageCount: null,
    cacheSize: null,
    walSize: null,
    tables: tableStats,
    indexes: indexes.rows,
  };
}

export function createPostgresOperationsRepository(database: PostgresDatabase) {
  return {
    getUserDatabaseSettings() {
      return database.transaction(readSettings);
    },
    async getDatabaseSettings(): Promise<DatabaseSettings> {
      return database.transaction(async (transaction) => {
        const settings = await readSettings(transaction);
        const stats = await readStats(transaction);
        const version = await transaction.query<{ version: number }>(
          "SELECT COALESCE(MAX(version), 0) AS version FROM omniroute.schema_migrations"
        );
        return {
          ...settings,
          backend: "postgresql",
          capabilities: POSTGRES_OPERATION_CAPABILITIES,
          location: {
            databasePath: "postgresql:omniroute",
            dataDir: "",
            walSizeBytes: null,
            schemaVersion: version.rows[0]?.version ?? 0,
          },
          stats: {
            databaseSizeBytes: stats.totalSize,
            pageCount: null,
            freelistCount: null,
            lastVacuumAt: null,
            lastOptimizationAt: null,
            integrityCheck: null,
            autoVacuumDrift: null,
            lastReclaimedPages: null,
          },
        };
      });
    },
    async updateDatabaseSettings(updates: DatabaseSettingsPatch): Promise<UserDatabaseSettings> {
      if (updates.optimization !== undefined) {
        throw new PersistenceCapabilityError("sqlite-optimization");
      }
      if (updates.backup !== undefined) throw new PersistenceCapabilityError("sqlite-file-backup");
      return database.transaction(async (transaction) => {
        await acquirePostgresLock(transaction, "settings");
        await lockApiKeyPolicy(transaction);
        const settings = await readSettings(transaction);
        assertCacheCredentialEndpoint(settings.cache, updates.cache);
        for (const section of DATABASE_SETTINGS_SECTIONS) {
          if (updates[section] === undefined) continue;
          const patch = { ...updates[section] } as Record<string, unknown>;
          if (section === "cache") {
            for (const key of CACHE_SECRET_KEYS) {
              if (patch[key] === CACHE_SECRET_MASK) delete patch[key];
            }
          }
          mergeSectionObject(settings, section, patch);
        }
        normalizeOptimizationSettings(settings);
        for (const section of DATABASE_SETTINGS_SECTIONS) {
          if (section === "optimization" || section === "backup") continue;
          for (const [key, value] of Object.entries(settings[section])) {
            const stored = section === "cache" ? protectCacheValue(key, value) : value;
            await transaction.query(
              `INSERT INTO omniroute.key_value(namespace, key, value) VALUES ('databaseSettings', $1, $2)
               ON CONFLICT(namespace, key) DO UPDATE SET value = EXCLUDED.value`,
              [`${section}.${key}`, JSON.stringify(stored ?? null)]
            );
          }
        }
        if (updates.logs?.callLogPipelineEnabled !== undefined) {
          await transaction.query(
            `INSERT INTO omniroute.key_value(namespace, key, value) VALUES ('settings', 'call_log_pipeline_enabled', $1)
             ON CONFLICT(namespace, key) DO UPDATE SET value = EXCLUDED.value`,
            [JSON.stringify(Boolean(updates.logs.callLogPipelineEnabled))]
          );
        }
        await protectLegacyCopies(transaction);
        await advanceApiKeyPolicyEpoch(transaction);
        return settings;
      });
    },
    getDatabaseStats() {
      return database.transaction(readStats);
    },
    async healthCheck(options: { autoRepair?: boolean } = {}): Promise<DbHealthCheckResult> {
      if (options.autoRepair) throw new PersistenceCapabilityError("sqlite-integrity-repair");
      return database.transaction(async (transaction) => {
        await transaction.query("SELECT 1");
        const issues: DbHealthCheckResult["issues"] = [];
        const references = await transaction.query<{ table_name: string; count: string }>(
          `SELECT 'quota_snapshots' AS table_name, COUNT(*)::text AS count
           FROM omniroute.quota_snapshots q
           WHERE q.connection_id IS NOT NULL AND NOT EXISTS
             (SELECT 1 FROM omniroute.provider_connections p WHERE p.id = q.connection_id)
           UNION ALL
           SELECT 'domain_budgets', COUNT(*)::text FROM omniroute.domain_budgets b
           WHERE b.api_key_id IS NOT NULL AND NOT (b.api_key_id = ANY($1::text[])) AND NOT EXISTS
             (SELECT 1 FROM omniroute.api_keys k WHERE k.id = b.api_key_id)`,
          [[...getSyntheticApiKeyIds()]]
        );
        for (const row of references.rows) {
          if (Number(row.count) > 0)
            issues.push({
              type: "broken_reference",
              table: row.table_name,
              description: "Rows reference a missing connection or API key",
              count: Number(row.count),
            });
        }
        return {
          isHealthy: issues.length === 0,
          issues,
          repairedCount: 0,
          backupCreated: false,
          autoRepair: false,
          checkedAt: new Date().toISOString(),
          driver: { name: "postgresql", degraded: false },
          integrityScope: "connectivity-and-application-references",
        };
      });
    },
  };
}
