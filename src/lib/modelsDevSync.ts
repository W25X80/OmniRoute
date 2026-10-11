import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { getSelectedPersistenceIdentity } from "@/lib/db/controlPlaneContext";
import { createPersistencePoller, type PersistencePoller } from "@/lib/config/persistencePoller";
import { runPersistenceBackgroundWork } from "@/lib/db/persistenceRuntime";
import { assertSqliteBackendAllowed } from "./db/controlPlaneContext";
import { getSelectedPostgresDatabase } from "./db/controlPlaneContext";
import {
  catalogTableRows,
  catalogNamespaceRows,
  requirePostgresCatalogSnapshot,
  mutatePostgresCatalog,
  writePostgresCatalog,
  afterPostgresCatalogCommit,
} from "./db/postgres/catalogSnapshot";
/**
 * modelsDevSync.ts — Fetch model specs, pricing, and capabilities from models.dev
 *
 * models.dev (https://github.com/anomalyco/models.dev) is an open-source database
 * of AI model specifications maintained by the SST/OpenCode team (MIT license).
 *
 * API: https://models.dev/api.json
 * - 109 providers, 4,146+ models
 * - Data: pricing, capabilities, limits, modalities, metadata
 *
 * Resolution order (highest → lowest):
 *   1. User overrides (`pricing` namespace)
 *   2. models.dev sync (`models_dev_pricing` namespace)
 *   3. LiteLLM sync (`pricing_synced` namespace)
 *   4. Hardcoded defaults (`pricing.ts`)
 *
 * Settings UI (`modelsDevSyncEnabled`) controls the periodic sync by default.
 * `MODELS_DEV_SYNC_ENABLED=0|false|off|no` is a hard kill switch: it wins over
 * the DB setting so an operator can recover a wedged process (dashboard /
 * /healthz frozen on the same event loop — #10052) without the UI. Unset =
 * honor settings. `1|true|on|yes` forces sync on even if the setting is off.
 */

import { getDbInstance } from "./db/core";
import { invalidateDbCache, getModelCatalogCacheVersion } from "./db/readCache";
import { backupDbFile } from "./db/backup";

import {
  transformModelsDevToPricing,
  transformModelsDevToCapabilities,
} from "./modelsDevSync/transform";
import type {
  PricingModels,
  PricingByProvider,
  ModelCapabilityEntry,
  CapabilitiesByProvider,
  ModelsDevData,
} from "./modelsDevSync/transform";

// Re-export the pure transform layer (moved to ./modelsDevSync/transform)
// so this module's public API is unchanged.
export {
  mapProviderId,
  transformModelsDevToPricing,
  transformModelsDevToCapabilities,
} from "./modelsDevSync/transform";
export type {
  ModelCapabilityEntry,
  CapabilitiesByProvider,
  PricingByProvider,
} from "./modelsDevSync/transform";

// ─── Types ───────────────────────────────────────────────

interface SyncStatus {
  enabled: boolean;
  lastSync: string | null;
  lastSyncModelCount: number;
  lastSyncCapabilityCount: number;
  nextSync: string | null;
  intervalMs: number;
}

interface SyncResult {
  success: boolean;
  modelCount: number;
  providerCount: number;
  capabilityCount: number;
  dryRun: boolean;
  data?: { pricing: PricingByProvider; capabilities: CapabilitiesByProvider };
  error?: string;
}

// ─── Configuration ───────────────────────────────────────

const MODELS_DEV_API_URL = "https://models.dev/api.json";

const parsedInterval = parseInt(process.env.MODELS_DEV_SYNC_INTERVAL || "86400", 10);
const SYNC_INTERVAL_MS =
  Number.isFinite(parsedInterval) && parsedInterval > 0 ? parsedInterval * 1000 : 86400 * 1000;

/** Parse MODELS_DEV_SYNC_ENABLED. Invalid / empty → unset (honor DB settings). */
export function readModelsDevSyncEnvFlag(
  value: string | undefined = process.env.MODELS_DEV_SYNC_ENABLED
): "true" | "false" | "unset" {
  if (value == null) return "unset";
  const normalized = value.trim().toLowerCase();
  if (normalized === "") return "unset";
  if (normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on") {
    return "true";
  }
  if (normalized === "0" || normalized === "false" || normalized === "no" || normalized === "off") {
    return "false";
  }
  return "unset";
}

export function isModelsDevSyncEnvDisabled(): boolean {
  return readModelsDevSyncEnvFlag() === "false";
}

export function isModelsDevSyncEnvForcedOn(): boolean {
  return readModelsDevSyncEnvFlag() === "true";
}

// ─── Periodic sync state ─────────────────────────────────

let syncTimer: ReturnType<typeof setInterval> | null = null;
let activeSyncAbortController: AbortController | null = null;
let activeSyncPromise: Promise<SyncResult> | null = null;
let activePeriodicSyncToken: { stopped: boolean } | null = null;
let lastSyncTime: string | null = null;
let lastSyncModelCount = 0;
let lastSyncCapabilityCount = 0;
let activeSyncIntervalMs = SYNC_INTERVAL_MS;
let cachedData: ModelsDevData | null = null;
let cacheTime = 0;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours
let cachedCapabilities: CapabilitiesByProvider | null = null;
let cachedCapabilitiesLoadedAll = false;
const MODELS_DEV_ABORT_ERROR = "AbortError";

function createAbortError(): Error {
  const error = new Error("models.dev sync aborted");
  error.name = MODELS_DEV_ABORT_ERROR;
  return error;
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === MODELS_DEV_ABORT_ERROR;
}

function createAbortedSyncResult(dryRun: boolean): SyncResult {
  return {
    success: false,
    modelCount: 0,
    providerCount: 0,
    capabilityCount: 0,
    dryRun,
    error: "aborted",
  };
}

async function sleepWithAbort(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    throw createAbortError();
  }

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);

    const onAbort = () => {
      clearTimeout(timeout);
      reject(createAbortError());
    };

    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

// ─── Core: Fetch ─────────────────────────────────────────

/**
 * Fetch raw data from models.dev API.
 * Uses in-memory cache with 24h TTL to avoid repeated fetches.
 */
export async function fetchModelsDev(signal?: AbortSignal): Promise<ModelsDevData> {
  // Return cached data if still fresh
  if (cachedData && Date.now() - cacheTime < CACHE_TTL_MS) {
    return cachedData;
  }

  const response = await fetch(MODELS_DEV_API_URL, {
    signal: signal ?? AbortSignal.timeout(30000),
  });
  if (!response.ok) {
    throw new Error(`models.dev fetch failed [${response.status}]: ${response.statusText}`);
  }
  const text = await response.text();
  try {
    const data = JSON.parse(text) as ModelsDevData;
    cachedData = data;
    cacheTime = Date.now();
    return data;
  } catch {
    throw new Error(`models.dev returned invalid JSON (${text.slice(0, 100)}...)`);
  }
}

// ─── DB: models.dev pricing namespace ────────────────────

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

function parseReasoningEfforts(value: unknown): string[] | null {
  if (typeof value !== "string" || value.length === 0) return null;
  try {
    const parsed = JSON.parse(value);
    if (!Array.isArray(parsed)) return null;
    const efforts = parsed.filter(
      (entry): entry is string => typeof entry === "string" && entry.length > 0
    );
    return efforts.length > 0 ? efforts : null;
  } catch {
    return null;
  }
}

function mapCapabilityRecord(record: Record<string, unknown>): ModelCapabilityEntry {
  return {
    tool_call: record.tool_call === 1 ? true : record.tool_call === 0 ? false : null,
    reasoning: record.reasoning === 1 ? true : record.reasoning === 0 ? false : null,
    attachment: record.attachment === 1 ? true : record.attachment === 0 ? false : null,
    structured_output:
      record.structured_output === 1 ? true : record.structured_output === 0 ? false : null,
    temperature: record.temperature === 1 ? true : record.temperature === 0 ? false : null,
    modalities_input: typeof record.modalities_input === "string" ? record.modalities_input : "[]",
    modalities_output:
      typeof record.modalities_output === "string" ? record.modalities_output : "[]",
    knowledge_cutoff: typeof record.knowledge_cutoff === "string" ? record.knowledge_cutoff : null,
    release_date: typeof record.release_date === "string" ? record.release_date : null,
    last_updated: typeof record.last_updated === "string" ? record.last_updated : null,
    status: typeof record.status === "string" ? record.status : null,
    family: typeof record.family === "string" ? record.family : null,
    open_weights: record.open_weights === 1 ? true : record.open_weights === 0 ? false : null,
    limit_context: typeof record.limit_context === "number" ? record.limit_context : null,
    limit_input: typeof record.limit_input === "number" ? record.limit_input : null,
    limit_output: typeof record.limit_output === "number" ? record.limit_output : null,
    interleaved_field:
      typeof record.interleaved_field === "string" ? record.interleaved_field : null,
    reasoning_efforts: parseReasoningEfforts(record.reasoning_efforts),
  };
}

// #8697: getModelsDevPricing() re-ran the SELECT + JSON.parse of ~180 blobs on
// every call — called once per catalog model (up to ~6091x) instead of once per
// request, freezing the whole server 41-54s on a cold /v1/models rebuild.
// Memoized here, invalidated via the same modelCatalogCacheVersion signal
// save/clearModelsDevPricing already bump through invalidateDbCache("pricing") —
// reusing the existing pattern (getCachedRawProviderConnections et al. in
// db/readCache.ts) instead of introducing a new invalidation mechanism.
let pricingMemo: PricingByProvider | null = null;
let pricingMemoVersion = -1; // -1: never equals a real cacheVersion (starts at 0), guarantees a miss on the first call

/**
 * Read synced pricing from `models_dev_pricing` namespace.
 * Results are memoized until `saveModelsDevPricing` / `clearModelsDevPricing`.
 */
export function getModelsDevPricing(): PricingByProvider {
  if (getSelectedPostgresDatabase() && isModelsDevSyncEnvDisabled()) return {};
  if (getSelectedPostgresDatabase()) {
    requirePostgresCatalogSnapshot();
    const synced: PricingByProvider = {};
    for (const row of catalogNamespaceRows("models_dev_pricing")) {
      try {
        synced[row.key] = JSON.parse(row.value) as PricingModels;
      } catch {
        console.warn(`[PRICING_SYNC] Corrupted data for provider "${row.key}", skipping`);
      }
    }
    return synced;
  }

  // Kill switch: skip the SQL + JSON.parse scan entirely so a leftover
  // models_dev_pricing namespace cannot pin the event loop (#9685 / #10052).
  if (isModelsDevSyncEnvDisabled()) {
    return {};
  }

  const currentVersion = getModelCatalogCacheVersion();
  if (pricingMemo !== null && pricingMemoVersion === currentVersion) {
    return pricingMemo;
  }

  const db = getDbInstance();
  const rows = db
    .prepare("SELECT key, value FROM key_value WHERE namespace = 'models_dev_pricing'")
    .all();
  const synced: PricingByProvider = {};
  for (const row of rows) {
    const record = toRecord(row);
    const key = typeof record.key === "string" ? record.key : null;
    const rawValue = typeof record.value === "string" ? record.value : null;
    if (!key || rawValue === null) continue;
    try {
      synced[key] = JSON.parse(rawValue) as PricingModels;
    } catch {
      console.warn(`[MODELS_DEV] Corrupted pricing data for provider "${key}", skipping`);
    }
  }
  pricingMemo = synced;
  pricingMemoVersion = currentVersion;
  return synced;
}

/**
 * Save synced pricing to `models_dev_pricing` namespace (full replace).
 */
export async function saveModelsDevPricingAsync(data: PricingByProvider): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  if (catalogDatabase)
    return mutatePostgresCatalog(catalogDatabase, async () => {
      await writePostgresCatalog("DELETE FROM omniroute.key_value WHERE namespace=$1", [
        "models_dev_pricing",
      ]);
      const rows = Object.entries(data).map(([key, models]) => ({
        key,
        value: JSON.stringify(models),
      }));
      if (rows.length)
        await writePostgresCatalog(
          "INSERT INTO omniroute.key_value(namespace,key,value) SELECT $1, key,value FROM json_to_recordset($2::json) AS data(key text,value text)",
          ["models_dev_pricing", JSON.stringify(rows)]
        );
      afterPostgresCatalogCommit(() => invalidateDbCache("pricing"));
    });

  const db = getDbInstance();
  const del = db.prepare("DELETE FROM key_value WHERE namespace = 'models_dev_pricing'");
  const insert = db.prepare(
    "INSERT INTO key_value (namespace, key, value) VALUES ('models_dev_pricing', ?, ?)"
  );
  const tx = db.transaction(() => {
    del.run();
    for (const [provider, models] of Object.entries(data)) {
      insert.run(provider, JSON.stringify(models));
    }
  });
  tx();
  backupDbFile("pre-write");
  invalidateDbCache("pricing");
}

/**
 * Clear all models.dev synced pricing data.
 */
export async function clearModelsDevPricingAsync(): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  if (catalogDatabase)
    return mutatePostgresCatalog(catalogDatabase, async () => {
      await writePostgresCatalog("DELETE FROM omniroute.key_value WHERE namespace=$1", [
        "models_dev_pricing",
      ]);
      afterPostgresCatalogCommit(() => invalidateDbCache("pricing"));
    });

  const db = getDbInstance();
  db.prepare("DELETE FROM key_value WHERE namespace = 'models_dev_pricing'").run();
  backupDbFile("pre-write");
  invalidateDbCache("pricing");
}

// ─── DB: model_capabilities table ────────────────────────

/**
 * Ensure the model_capabilities table exists.
 * Call this before any capability operations.
 */
export function ensureCapabilitiesTable(): void {
  if (getSelectedPostgresDatabase()) return;

  const db = getDbInstance();
  db.exec(`
    CREATE TABLE IF NOT EXISTS model_capabilities (
      provider TEXT NOT NULL,
      model_id TEXT NOT NULL,
      tool_call BOOLEAN,
      reasoning BOOLEAN,
      attachment BOOLEAN,
      structured_output BOOLEAN,
      temperature BOOLEAN,
      modalities_input TEXT,
      modalities_output TEXT,
      knowledge_cutoff TEXT,
      release_date TEXT,
      last_updated TEXT,
      status TEXT,
      family TEXT,
      open_weights BOOLEAN,
      limit_context INTEGER,
      limit_input INTEGER,
      limit_output INTEGER,
      interleaved_field TEXT,
      reasoning_efforts TEXT,
      last_synced TEXT,
      PRIMARY KEY (provider, model_id)
    )
  `);
}

function defineEnumerableDataProperty<T extends object>(
  target: T,
  key: string,
  value: unknown
): void {
  Object.defineProperty(target, key, {
    value,
    writable: true,
    enumerable: true,
    configurable: true,
  });
}

function capabilitiesFromRows(rows: unknown[]): CapabilitiesByProvider {
  const result: CapabilitiesByProvider = {};

  for (const row of rows) {
    const record = toRecord(row);
    const prov = typeof record.provider === "string" ? record.provider : null;
    const mid = typeof record.model_id === "string" ? record.model_id : null;
    if (!prov || !mid) continue;

    if (!Object.hasOwn(result, prov)) {
      defineEnumerableDataProperty(result, prov, {});
    }
    defineEnumerableDataProperty(result[prov], mid, mapCapabilityRecord(record));
  }

  return result;
}

/**
 * Uncached full-table models.dev capability read for build-local snapshots.
 * Shares mapping with the ordinary all-row API but never mutates the module-global
 * `cachedCapabilities` / `cachedCapabilitiesLoadedAll` runtime cache.
 */
export function loadAllSyncedCapabilitiesUncached(): CapabilitiesByProvider {
  if (getSelectedPostgresDatabase()) {
    requirePostgresCatalogSnapshot();
    return capabilitiesFromRows(catalogTableRows("model_capabilities"));
  }

  const db = getDbInstance();
  ensureCapabilitiesTable();
  const rows = db.prepare("SELECT * FROM model_capabilities").all();
  return capabilitiesFromRows(rows);
}

/**
 * Reasoning tiers already resident in this process. A catalog/discovery read
 * must use this instead of `getSyncedCapabilities()` — that path opens SQLite
 * and runs migrations.
 */
export function peekCachedReasoningEfforts(provider: string, modelId: string): string[] | null {
  if (getSelectedPostgresDatabase()) {
    const efforts = getSyncedCapability(provider, modelId)?.reasoning_efforts;
    return efforts && efforts.length > 0 ? [...efforts] : null;
  }

  if (!cachedCapabilitiesLoadedAll || !cachedCapabilities) return null;
  const efforts = cachedCapabilities[provider]?.[modelId]?.reasoning_efforts;
  return efforts && efforts.length > 0 ? [...efforts] : null;
}

/**
 * Read synced capabilities from `model_capabilities` table.
 */
export function getSyncedCapabilities(provider?: string, modelId?: string): CapabilitiesByProvider {
  if (getSelectedPostgresDatabase()) {
    requirePostgresCatalogSnapshot();
    return capabilitiesFromRows(
      catalogTableRows("model_capabilities").filter(
        (row) => !provider || (row.provider === provider && (!modelId || row.model_id === modelId))
      )
    );
  }

  if (cachedCapabilitiesLoadedAll) {
    if (!provider) {
      return cachedCapabilities || {};
    }

    if (!modelId) {
      return cachedCapabilities?.[provider] ? { [provider]: cachedCapabilities[provider] } : {};
    }

    const providerCaps = cachedCapabilities?.[provider];
    return providerCaps?.[modelId] ? { [provider]: { [modelId]: providerCaps[modelId] } } : {};
  }

  const db = getDbInstance();
  ensureCapabilitiesTable();

  let query = "SELECT * FROM model_capabilities";
  const params: (string | number)[] = [];

  if (provider) {
    query += " WHERE provider = ?";
    params.push(provider);
    if (modelId) {
      query += " AND model_id = ?";
      params.push(modelId);
    }
  }

  const result = capabilitiesFromRows(db.prepare(query).all(...params));

  if (!provider && !modelId) {
    cachedCapabilities = result;
    cachedCapabilitiesLoadedAll = true;
  }

  return result;
}

/**
 * Resolved providers/aliases to also try when looking up a synced capability.
 * Required because models.dev has historically stored capability rows under the
 * alias side of an alias pair (e.g. "opencode-zen") while the catalog & combo
 * targets reference the canonical id (e.g. "opencode"). Without this fallback,
 * combos whose targets use the canonical id (e.g. "Opencode FREE Omni" → all
 * `opencode/...` models) end up with `context_length: null` in the catalog.
 */
const SYNCED_CAPABILITY_FALLBACK_ALIASES: Record<string, string[]> = {
  opencode: ["opencode-zen"],
  "opencode-zen": ["opencode"],
  "opencode-go": ["opencode-zen"],
};

function lookupSyncedCapabilityWithFallbacks(
  provider: string,
  lookup: (provider: string) => ModelCapabilityEntry | null
): ModelCapabilityEntry | null {
  const direct = lookup(provider);
  if (direct) return direct;

  const fallbacks = SYNCED_CAPABILITY_FALLBACK_ALIASES[provider];
  if (fallbacks) {
    for (const alt of fallbacks) {
      const found = lookup(alt);
      if (found) return found;
    }
  }

  return null;
}

export function getSyncedCapability(
  provider: string,
  modelId: string,
  bulk?: CapabilitiesByProvider | null
): ModelCapabilityEntry | null {
  if (getSelectedPostgresDatabase()) {
    if (!provider || !modelId) return null;
    if (bulk)
      return lookupSyncedCapabilityWithFallbacks(provider, (p) => bulk[p]?.[modelId] ?? null);
    requirePostgresCatalogSnapshot();
    const rows = catalogTableRows("model_capabilities");
    return lookupSyncedCapabilityWithFallbacks(provider, (p) => {
      const row = rows.find((row) => row.provider === p && row.model_id === modelId);
      return row ? mapCapabilityRecord(row) : null;
    });
  }

  if (!provider || !modelId) return null;

  if (bulk) {
    return lookupSyncedCapabilityWithFallbacks(provider, (p) => bulk[p]?.[modelId] ?? null);
  }

  // Fast path: every provider is in the in-memory cache, skip SQLite entirely.
  if (cachedCapabilitiesLoadedAll) {
    return lookupSyncedCapabilityWithFallbacks(
      provider,
      (p) => cachedCapabilities?.[p]?.[modelId] ?? null
    );
  }

  // Cold path: hit SQLite. Prepare the statement once, reuse for every alias.
  const db = getDbInstance();
  ensureCapabilitiesTable();
  const stmt = db.prepare(
    "SELECT * FROM model_capabilities WHERE provider = ? AND model_id = ? LIMIT 1"
  );
  return lookupSyncedCapabilityWithFallbacks(provider, (p) => {
    const row = stmt.get(p, modelId);
    if (!row) return null;
    return mapCapabilityRecord(toRecord(row));
  });
}

/**
 * Save synced capabilities to `model_capabilities` table (full replace).
 */
export async function saveModelsDevCapabilitiesAsync(data: CapabilitiesByProvider): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  if (catalogDatabase)
    return mutatePostgresCatalog(catalogDatabase, async () => {
      await writePostgresCatalog("DELETE FROM omniroute.model_capabilities");
      await insertNativeCapabilityRows(data, false);
      afterPostgresCatalogCommit(() => invalidateDbCache("model-capabilities"));
    });

  const db = getDbInstance();
  ensureCapabilitiesTable();

  const del = db.prepare("DELETE FROM model_capabilities");
  const insert = db.prepare(`
    INSERT INTO model_capabilities (
      provider, model_id, tool_call, reasoning, attachment, structured_output,
      temperature, modalities_input, modalities_output, knowledge_cutoff,
      release_date, last_updated, status, family, open_weights,
      limit_context, limit_input, limit_output, interleaved_field, reasoning_efforts, last_synced
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const now = new Date().toISOString();
  let changed = false;
  const tx = db.transaction(() => {
    if (del.run().changes > 0) changed = true;
    for (const [provider, models] of Object.entries(data)) {
      for (const [modelId, cap] of Object.entries(models)) {
        const info = insert.run(
          provider,
          modelId,
          cap.tool_call === null ? null : cap.tool_call ? 1 : 0,
          cap.reasoning === null ? null : cap.reasoning ? 1 : 0,
          cap.attachment === null ? null : cap.attachment ? 1 : 0,
          cap.structured_output === null ? null : cap.structured_output ? 1 : 0,
          cap.temperature === null ? null : cap.temperature ? 1 : 0,
          cap.modalities_input,
          cap.modalities_output,
          cap.knowledge_cutoff,
          cap.release_date,
          cap.last_updated,
          cap.status,
          cap.family,
          cap.open_weights === null ? null : cap.open_weights ? 1 : 0,
          cap.limit_context,
          cap.limit_input,
          cap.limit_output,
          cap.interleaved_field,
          cap.reasoning_efforts ? JSON.stringify(cap.reasoning_efforts) : null,
          now
        );
        if (info.changes > 0) changed = true;
      }
    }
  });
  tx();
  backupDbFile("pre-write");
  cachedCapabilities = data;
  cachedCapabilitiesLoadedAll = true;
  if (changed) invalidateDbCache("model-capabilities");
}

/**
 * Insert-or-replace one provider's capability rows without wiping the table.
 * Used by the OpenRouter live catalog walk so architecture.input_modalities
 * survive into the next combo LCD (#12613).
 */
export async function upsertSyncedCapabilitiesAsync(
  provider: string,
  models: Record<string, ModelCapabilityEntry>
): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  if (catalogDatabase) {
    if (!provider || Object.keys(models).length === 0) return;
    return mutatePostgresCatalog(catalogDatabase, async () => {
      await insertNativeCapabilityRows({ [provider]: models }, true);
      afterPostgresCatalogCommit(() => invalidateDbCache("model-capabilities"));
    });
  }

  if (!provider || Object.keys(models).length === 0) return;
  const db = getDbInstance();
  ensureCapabilitiesTable();
  const insert = db.prepare(`
    INSERT INTO model_capabilities (
      provider, model_id, tool_call, reasoning, attachment, structured_output,
      temperature, modalities_input, modalities_output, knowledge_cutoff,
      release_date, last_updated, status, family, open_weights,
      limit_context, limit_input, limit_output, interleaved_field, reasoning_efforts, last_synced
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(provider, model_id) DO UPDATE SET
      tool_call=excluded.tool_call,
      reasoning=excluded.reasoning,
      attachment=excluded.attachment,
      structured_output=excluded.structured_output,
      temperature=excluded.temperature,
      modalities_input=excluded.modalities_input,
      modalities_output=excluded.modalities_output,
      knowledge_cutoff=excluded.knowledge_cutoff,
      release_date=excluded.release_date,
      last_updated=excluded.last_updated,
      status=excluded.status,
      family=excluded.family,
      open_weights=excluded.open_weights,
      limit_context=excluded.limit_context,
      limit_input=excluded.limit_input,
      limit_output=excluded.limit_output,
      interleaved_field=excluded.interleaved_field,
      reasoning_efforts=excluded.reasoning_efforts,
      last_synced=excluded.last_synced
  `);
  const now = new Date().toISOString();
  let changed = false;
  const tx = db.transaction(() => {
    for (const [modelId, cap] of Object.entries(models)) {
      const info = insert.run(
        provider,
        modelId,
        cap.tool_call === null ? null : cap.tool_call ? 1 : 0,
        cap.reasoning === null ? null : cap.reasoning ? 1 : 0,
        cap.attachment === null ? null : cap.attachment ? 1 : 0,
        cap.structured_output === null ? null : cap.structured_output ? 1 : 0,
        cap.temperature === null ? null : cap.temperature ? 1 : 0,
        cap.modalities_input,
        cap.modalities_output,
        cap.knowledge_cutoff,
        cap.release_date,
        cap.last_updated,
        cap.status,
        cap.family,
        cap.open_weights === null ? null : cap.open_weights ? 1 : 0,
        cap.limit_context,
        cap.limit_input,
        cap.limit_output,
        cap.interleaved_field,
        cap.reasoning_efforts ? JSON.stringify(cap.reasoning_efforts) : null,
        now
      );
      if (info.changes > 0) changed = true;
    }
  });
  tx();
  if (cachedCapabilities) {
    cachedCapabilities[provider] = {
      ...(cachedCapabilities[provider] || {}),
      ...models,
    };
  }
  if (changed) invalidateDbCache("model-capabilities");
}

/**
 * Clear all synced capability data.
 */
export async function clearModelsDevCapabilitiesAsync(): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  if (catalogDatabase)
    return mutatePostgresCatalog(catalogDatabase, async () => {
      await writePostgresCatalog("DELETE FROM omniroute.model_capabilities");
      afterPostgresCatalogCommit(() => invalidateDbCache("model-capabilities"));
    });

  const db = getDbInstance();
  ensureCapabilitiesTable();
  const info = db.prepare("DELETE FROM model_capabilities").run();
  backupDbFile("pre-write");
  cachedCapabilities = {};
  cachedCapabilitiesLoadedAll = true;
  if (info.changes > 0) invalidateDbCache("model-capabilities");
}

// ─── Main sync function ──────────────────────────────────

/**
 * Fetch, transform, and save pricing + capabilities from models.dev.
 */
export async function syncModelsDev(opts?: {
  dryRun?: boolean;
  syncCapabilities?: boolean;
  maxRetries?: number;
  signal?: AbortSignal;
}): Promise<SyncResult> {
  const dryRun = opts?.dryRun ?? false;
  const syncCapabilities = opts?.syncCapabilities ?? true;
  const maxRetries = opts?.maxRetries ?? 3;
  const signal = opts?.signal;

  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    if (signal?.aborted) {
      return createAbortedSyncResult(dryRun);
    }

    try {
      const raw = await fetchModelsDev(signal);
      const pricing = transformModelsDevToPricing(raw);
      const capabilities = syncCapabilities ? transformModelsDevToCapabilities(raw) : {};

      const modelCount = Object.values(pricing).reduce(
        (sum, models) => sum + Object.keys(models).length,
        0
      );
      const providerCount = Object.keys(pricing).length;
      const capabilityCount = syncCapabilities
        ? Object.values(capabilities).reduce((sum, models) => sum + Object.keys(models).length, 0)
        : 0;

      if (signal?.aborted) {
        return createAbortedSyncResult(dryRun);
      }

      if (!dryRun) {
        await saveModelsDevPricingAsync(pricing);
        if (syncCapabilities) {
          ensureCapabilitiesTable();
          await saveModelsDevCapabilitiesAsync(capabilities);
        }
        lastSyncTime = new Date().toISOString();
        lastSyncModelCount = modelCount;
        lastSyncCapabilityCount = capabilityCount;
      }

      return {
        success: true,
        modelCount,
        providerCount,
        capabilityCount,
        dryRun,
        ...(dryRun ? { data: { pricing, capabilities } } : {}),
      };
    } catch (err) {
      if (signal?.aborted || isAbortError(err)) {
        return createAbortedSyncResult(dryRun);
      }

      lastError = err instanceof Error ? err : new Error(String(err));

      if (attempt < maxRetries) {
        const delayMs = Math.pow(2, attempt) * 1000; // Exponential backoff: 1s, 2s, 4s
        console.warn(
          `[MODELS_DEV] Sync attempt ${attempt + 1} failed, retrying in ${delayMs}ms:`,
          lastError.message
        );
        try {
          await sleepWithAbort(delayMs, signal);
        } catch (sleepError) {
          if (signal?.aborted || isAbortError(sleepError)) {
            return createAbortedSyncResult(dryRun);
          }
          throw sleepError;
        }
      }
    }
  }

  const message = lastError?.message || "Unknown error";
  console.warn(`[MODELS_DEV] Sync failed after ${maxRetries + 1} attempts:`, message);
  return {
    success: false,
    modelCount: 0,
    providerCount: 0,
    capabilityCount: 0,
    dryRun,
    error: message,
  };
}

// ─── Periodic sync ───────────────────────────────────────

/**
 * Start periodic models.dev sync (non-blocking).
 */
interface NativeSchedulerOwner {
  readonly identity: object;
  readonly poller: PersistencePoller;
  stopping?: Promise<void>;
  controller?: AbortController;
}
const NATIVE_SCHEDULER = Symbol.for("omniroute.modelsDevSync.postgresScheduler");
const nativeGlobals = globalThis as typeof globalThis & {
  [NATIVE_SCHEDULER]?: NativeSchedulerOwner;
};

function isNativeSchedulerRunning(): boolean {
  const owner = nativeGlobals[NATIVE_SCHEDULER];
  return Boolean(owner && !owner.stopping);
}

function stopNativePoller(): Promise<void> {
  const owner = nativeGlobals[NATIVE_SCHEDULER];
  if (!owner) return Promise.resolve();
  owner.controller?.abort();
  owner.stopping ??= owner.poller.stop().finally(() => {
    if (nativeGlobals[NATIVE_SCHEDULER] === owner) nativeGlobals[NATIVE_SCHEDULER] = undefined;
  });
  return owner.stopping;
}

export function startPeriodicSync(intervalMs?: number): void {
  if (parseDatabaseBackend() === "postgres") {
    const identity = getSelectedPersistenceIdentity();
    if (!identity) throw new Error("Native scheduler requires selected PostgreSQL persistence");
    const existing = nativeGlobals[NATIVE_SCHEDULER];
    if (existing) {
      if (existing.identity !== identity || existing.stopping)
        throw new Error("Native scheduler authority changed or is draining");
      return;
    }

    const interval = intervalMs ?? SYNC_INTERVAL_MS;
    activeSyncIntervalMs = interval;
    const owner: NativeSchedulerOwner = {
      identity,
      poller: createPersistencePoller({
        intervalMs: interval,
        initialDelayMs: 0,
        check: async (phase) => {
          await runPersistenceBackgroundWork(async () => {
            if (getSelectedPersistenceIdentity() !== identity)
              throw new Error("Native model-spec scheduler persistence identity changed");
            const controller = new AbortController();
            owner.controller = controller;
            try {
              const result = await syncModelsDev({ signal: controller.signal });
              if (result.success)
                console.log(
                  `[MODELS_DEV] ${phase === "start" ? "Initial" : "Periodic"} sync complete: ${result.modelCount} pricing entries`
                );
            } finally {
              if (owner.controller === controller) owner.controller = undefined;
            }
          });
        },
        onFailure: () => {
          console.warn("[MODELS_DEV] Native periodic sync failed");
        },
      }),
    };
    nativeGlobals[NATIVE_SCHEDULER] = owner;
    owner.poller.start();
    return;
  }
  if (syncTimer) return; // Already running

  const interval = intervalMs ?? SYNC_INTERVAL_MS;
  activeSyncIntervalMs = interval;
  const syncToken = { stopped: false };
  activePeriodicSyncToken = syncToken;
  console.log(`[MODELS_DEV] Starting periodic sync every ${interval / 1000}s`);

  const launchSync = async () => {
    if (syncToken.stopped) {
      return Promise.resolve(createAbortedSyncResult(false));
    }

    if (activeSyncPromise) return activeSyncPromise;

    const controller = new AbortController();
    activeSyncAbortController = controller;
    const promise = syncModelsDev({ signal: controller.signal }).finally(() => {
      if (activeSyncAbortController === controller) {
        activeSyncAbortController = null;
      }
      if (activeSyncPromise === promise) {
        activeSyncPromise = null;
      }
    });
    activeSyncPromise = promise;
    return promise;
  };

  // Initial sync (non-blocking)
  launchSync()
    .then((result) => {
      if (result.success) {
        console.log(
          `[MODELS_DEV] Initial sync complete: ${result.modelCount} pricing entries, ${result.capabilityCount} capabilities from ${result.providerCount} providers`
        );
      }
    })
    .catch((err) => {
      console.warn("[MODELS_DEV] Initial sync error:", err instanceof Error ? err.message : err);
    });

  syncTimer = setInterval(() => {
    launchSync()
      .then((result) => {
        if (result.success) {
          console.log(`[MODELS_DEV] Periodic sync complete: ${result.modelCount} pricing entries`);
        }
      })
      .catch((err) => {
        console.warn("[MODELS_DEV] Periodic sync error:", err instanceof Error ? err.message : err);
      });
  }, interval);

  if (syncTimer && typeof syncTimer === "object" && "unref" in syncTimer) {
    (syncTimer as { unref?: () => void }).unref?.();
  }
}

/**
 * Stop periodic sync and cleanup timer.
 */
export function stopPeriodicSync(): void {
  if (parseDatabaseBackend() === "postgres") {
    void stopNativePoller().catch(() => {});
    return;
  }
  if (activePeriodicSyncToken) {
    activePeriodicSyncToken.stopped = true;
    activePeriodicSyncToken = null;
  }

  if (activeSyncAbortController) {
    activeSyncAbortController.abort();
    activeSyncAbortController = null;
  }

  if (syncTimer) {
    clearInterval(syncTimer);
    syncTimer = null;
    console.log("[MODELS_DEV] Periodic sync stopped");
  }
}

/**
 * Get current sync status.
 */
export function getSyncStatus(): SyncStatus {
  // If the sync timer is active, it's enabled.
  const enabled = isNativeSchedulerRunning() || syncTimer !== null;
  return {
    enabled,
    lastSync: lastSyncTime,
    lastSyncModelCount,
    lastSyncCapabilityCount,
    nextSync:
      (isNativeSchedulerRunning() || syncTimer) && lastSyncTime
        ? new Date(new Date(lastSyncTime).getTime() + activeSyncIntervalMs).toISOString()
        : null,
    intervalMs: activeSyncIntervalMs,
  };
}

// ─── Init (called from instrumentation-node.ts) ───────────────────

/**
 * Initialize models.dev sync if enabled.
 */
export async function initModelsDevSync(): Promise<void> {
  if (isModelsDevSyncEnvDisabled()) {
    console.log("[MODELS_DEV] Disabled (MODELS_DEV_SYNC_ENABLED=0)");
    return;
  }

  const { getSettings } = await import("@/lib/db/settings");
  const settings = await getSettings();

  if (!isModelsDevSyncEnvForcedOn() && settings.modelsDevSyncEnabled !== true) {
    console.log("[MODELS_DEV] Disabled (enable via Settings > AI or MODELS_DEV_SYNC_ENABLED=1)");
    return;
  }

  const interval = settings.modelsDevSyncInterval as number | undefined;
  await startPeriodicSync(interval);
}

const NATIVE_CAPABILITY_COLUMNS = [
  "provider",
  "model_id",
  "tool_call",
  "reasoning",
  "attachment",
  "structured_output",
  "temperature",
  "modalities_input",
  "modalities_output",
  "knowledge_cutoff",
  "release_date",
  "last_updated",
  "status",
  "family",
  "open_weights",
  "limit_context",
  "limit_input",
  "limit_output",
  "interleaved_field",
  "reasoning_efforts",
  "last_synced",
] as const;
async function insertNativeCapabilityRows(
  data: CapabilitiesByProvider,
  upsert: boolean
): Promise<void> {
  const now = new Date().toISOString();
  const rows = Object.entries(data).flatMap(([provider, models]) =>
    Object.entries(models).map(([model_id, cap]) => ({
      provider,
      model_id,
      ...Object.fromEntries(
        (
          [
            "tool_call",
            "reasoning",
            "attachment",
            "structured_output",
            "temperature",
            "open_weights",
          ] as const
        ).map((key) => [key, cap[key] === null ? null : cap[key] ? 1 : 0])
      ),
      modalities_input: cap.modalities_input,
      modalities_output: cap.modalities_output,
      knowledge_cutoff: cap.knowledge_cutoff,
      release_date: cap.release_date,
      last_updated: cap.last_updated,
      status: cap.status,
      family: cap.family,
      limit_context: cap.limit_context,
      limit_input: cap.limit_input,
      limit_output: cap.limit_output,
      interleaved_field: cap.interleaved_field,
      reasoning_efforts: cap.reasoning_efforts ? JSON.stringify(cap.reasoning_efforts) : null,
      last_synced: now,
    }))
  );
  if (!rows.length) return;
  const columns = NATIVE_CAPABILITY_COLUMNS.join(",");
  const definitions = NATIVE_CAPABILITY_COLUMNS.map(
    (column) =>
      column +
      " " +
      ([
        "tool_call",
        "reasoning",
        "attachment",
        "structured_output",
        "temperature",
        "open_weights",
      ].includes(column)
        ? "integer"
        : ["limit_context", "limit_input", "limit_output"].includes(column)
          ? "double precision"
          : "text")
  ).join(",");
  const conflict = upsert
    ? " ON CONFLICT(provider,model_id) DO UPDATE SET " +
      NATIVE_CAPABILITY_COLUMNS.filter((column) => column !== "provider" && column !== "model_id")
        .map((column) => column + "=EXCLUDED." + column)
        .join(",")
    : "";
  await writePostgresCatalog(
    "INSERT INTO omniroute.model_capabilities(" +
      columns +
      ") SELECT " +
      columns +
      " FROM json_to_recordset($1::json) AS data(" +
      definitions +
      ")" +
      conflict,
    [JSON.stringify(rows)]
  );
}

export function saveModelsDevPricing(data: PricingByProvider): void {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  const del = db.prepare("DELETE FROM key_value WHERE namespace = 'models_dev_pricing'");
  const insert = db.prepare(
    "INSERT INTO key_value (namespace, key, value) VALUES ('models_dev_pricing', ?, ?)"
  );
  const tx = db.transaction(() => {
    del.run();
    for (const [provider, models] of Object.entries(data)) {
      insert.run(provider, JSON.stringify(models));
    }
  });
  tx();
  backupDbFile("pre-write");
  invalidateDbCache("pricing");
}

export function clearModelsDevPricing(): void {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  db.prepare("DELETE FROM key_value WHERE namespace = 'models_dev_pricing'").run();
  backupDbFile("pre-write");
  invalidateDbCache("pricing");
}

export function saveModelsDevCapabilities(data: CapabilitiesByProvider): void {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  ensureCapabilitiesTable();

  const del = db.prepare("DELETE FROM model_capabilities");
  const insert = db.prepare(`
    INSERT INTO model_capabilities (
      provider, model_id, tool_call, reasoning, attachment, structured_output,
      temperature, modalities_input, modalities_output, knowledge_cutoff,
      release_date, last_updated, status, family, open_weights,
      limit_context, limit_input, limit_output, interleaved_field, reasoning_efforts, last_synced
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const now = new Date().toISOString();
  let changed = false;
  const tx = db.transaction(() => {
    if (del.run().changes > 0) changed = true;
    for (const [provider, models] of Object.entries(data)) {
      for (const [modelId, cap] of Object.entries(models)) {
        const info = insert.run(
          provider,
          modelId,
          cap.tool_call === null ? null : cap.tool_call ? 1 : 0,
          cap.reasoning === null ? null : cap.reasoning ? 1 : 0,
          cap.attachment === null ? null : cap.attachment ? 1 : 0,
          cap.structured_output === null ? null : cap.structured_output ? 1 : 0,
          cap.temperature === null ? null : cap.temperature ? 1 : 0,
          cap.modalities_input,
          cap.modalities_output,
          cap.knowledge_cutoff,
          cap.release_date,
          cap.last_updated,
          cap.status,
          cap.family,
          cap.open_weights === null ? null : cap.open_weights ? 1 : 0,
          cap.limit_context,
          cap.limit_input,
          cap.limit_output,
          cap.interleaved_field,
          cap.reasoning_efforts ? JSON.stringify(cap.reasoning_efforts) : null,
          now
        );
        if (info.changes > 0) changed = true;
      }
    }
  });
  tx();
  backupDbFile("pre-write");
  cachedCapabilities = data;
  cachedCapabilitiesLoadedAll = true;
  if (changed) invalidateDbCache("model-capabilities");
}

export function upsertSyncedCapabilities(
  provider: string,
  models: Record<string, ModelCapabilityEntry>
): void {
  assertSqliteBackendAllowed();
  if (!provider || Object.keys(models).length === 0) return;
  const db = getDbInstance();
  ensureCapabilitiesTable();
  const insert = db.prepare(`
    INSERT INTO model_capabilities (
      provider, model_id, tool_call, reasoning, attachment, structured_output,
      temperature, modalities_input, modalities_output, knowledge_cutoff,
      release_date, last_updated, status, family, open_weights,
      limit_context, limit_input, limit_output, interleaved_field, reasoning_efforts, last_synced
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(provider, model_id) DO UPDATE SET
      tool_call=excluded.tool_call,
      reasoning=excluded.reasoning,
      attachment=excluded.attachment,
      structured_output=excluded.structured_output,
      temperature=excluded.temperature,
      modalities_input=excluded.modalities_input,
      modalities_output=excluded.modalities_output,
      knowledge_cutoff=excluded.knowledge_cutoff,
      release_date=excluded.release_date,
      last_updated=excluded.last_updated,
      status=excluded.status,
      family=excluded.family,
      open_weights=excluded.open_weights,
      limit_context=excluded.limit_context,
      limit_input=excluded.limit_input,
      limit_output=excluded.limit_output,
      interleaved_field=excluded.interleaved_field,
      reasoning_efforts=excluded.reasoning_efforts,
      last_synced=excluded.last_synced
  `);
  const now = new Date().toISOString();
  let changed = false;
  const tx = db.transaction(() => {
    for (const [modelId, cap] of Object.entries(models)) {
      const info = insert.run(
        provider,
        modelId,
        cap.tool_call === null ? null : cap.tool_call ? 1 : 0,
        cap.reasoning === null ? null : cap.reasoning ? 1 : 0,
        cap.attachment === null ? null : cap.attachment ? 1 : 0,
        cap.structured_output === null ? null : cap.structured_output ? 1 : 0,
        cap.temperature === null ? null : cap.temperature ? 1 : 0,
        cap.modalities_input,
        cap.modalities_output,
        cap.knowledge_cutoff,
        cap.release_date,
        cap.last_updated,
        cap.status,
        cap.family,
        cap.open_weights === null ? null : cap.open_weights ? 1 : 0,
        cap.limit_context,
        cap.limit_input,
        cap.limit_output,
        cap.interleaved_field,
        cap.reasoning_efforts ? JSON.stringify(cap.reasoning_efforts) : null,
        now
      );
      if (info.changes > 0) changed = true;
    }
  });
  tx();
  if (cachedCapabilities) {
    cachedCapabilities[provider] = {
      ...(cachedCapabilities[provider] || {}),
      ...models,
    };
  }
  if (changed) invalidateDbCache("model-capabilities");
}

export function clearModelsDevCapabilities(): void {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  ensureCapabilitiesTable();
  const info = db.prepare("DELETE FROM model_capabilities").run();
  backupDbFile("pre-write");
  cachedCapabilities = {};
  cachedCapabilitiesLoadedAll = true;
  if (info.changes > 0) invalidateDbCache("model-capabilities");
}

/** Keep the existing abort behavior, then await the accepted sync's actual completion. */
export async function stopPeriodicSyncAsync(): Promise<void> {
  if (parseDatabaseBackend() !== "postgres") return stopPeriodicSync();
  await stopNativePoller();
}
