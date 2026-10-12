/**
 * db/settings.js — Settings, pricing, and proxy config.
 */

import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase, getSelectedControlPlane } from "./controlPlaneContext";
import { resolvePostgresProxyForConnection } from "./postgres/proxyResolution";
import {
  getPostgresProxyConfig,
  setPostgresProxyForLevel,
  setPostgresProxyConfig,
} from "./postgres/proxyConfigRepository";
import { readPostgresSettingsSnapshot } from "./postgres/settingsSnapshot";
import {
  hasPostgresCatalogSnapshot,
  refreshPostgresCatalogSnapshot,
} from "./postgres/catalogSnapshot";
import { PostgresSettingsEffectsError } from "./postgres/settingsRepository";
import { backupDbFile } from "./backup";
import { PROVIDER_ID_TO_ALIAS } from "@omniroute/open-sse/config/providerModels.ts";
import {
  getProxyRefusalSeq,
  hasProxyRefusals,
  proxyEgressKey,
  proxySetAsideSeq,
} from "@omniroute/open-sse/utils/proxyRefusalMemory.ts";
import { isProxySkipRecentlyFailedEnabled } from "@/shared/utils/featureFlags";
import { invalidateDbCache } from "./readCache";
import { encrypt, decrypt } from "./encryption";
import { getProxyRegistryGeneration, resolveProxyForScopeFromRegistry } from "./proxies";
import {
  advanceProxyConfigGeneration,
  getProxyConfigGeneration,
  requiresStableEgress,
} from "./settings/stableEgressPolicy";
import { getComboModelProvider as getComboEntryProvider } from "@/lib/combos/steps";
import { decodeUserinfo } from "@/shared/utils/decodeUserinfo";
import { type JsonRecord, toRecord } from "./settings/shared";
import { resolveNoAuthSharedProviderProxy } from "./settings/noAuthProxyFallback";
import {
  interpretSettingsRows,
  needsHeadlessOnboarding,
  parseSettingsRevision,
  SETTINGS_REVISION_KEY,
  SettingsRevisionConflictError,
} from "./settings/persistenceSemantics";

export {
  SETTINGS_REVISION_KEY,
  SettingsRevisionConflictError,
} from "./settings/persistenceSemantics";

type ProxyValue = JsonRecord | string | null;
type ProxyResolutionResult = {
  proxy: ProxyValue;
  level: string;
  levelId: string | null;
  source?: string;
};
// State observed when a resolution started; an entry is stored only if it still holds.
type ProxyResolutionStamp = {
  generation: number;
  registryGeneration: number;
  // Proxy refusal memory sequence (see isCachedPoolMemberSetAside).
  refusalSeq: number;
};
type ProxyResolutionCacheEntry = ProxyResolutionStamp & { result: ProxyResolutionResult };

const PROXY_RESOLUTION_CACHE_MAX_ENTRIES = 100;

const proxyResolutionCache = new Map<string, ProxyResolutionCacheEntry>();

export function bumpProxyConfigGeneration() {
  advanceProxyConfigGeneration();
  proxyResolutionCache.clear();
}

function cacheProxyResolution(
  connectionId: string,
  stamp: ProxyResolutionStamp,
  result: ProxyResolutionResult
) {
  if (stamp.generation !== getProxyConfigGeneration()) return;
  if (stamp.registryGeneration !== getProxyRegistryGeneration()) return;
  if (proxyResolutionCache.size >= PROXY_RESOLUTION_CACHE_MAX_ENTRIES) {
    const oldestKey = proxyResolutionCache.keys().next().value;
    if (oldestKey) proxyResolutionCache.delete(oldestKey);
  }
  proxyResolutionCache.set(connectionId, { ...stamp, result });
}
type ProxyMap = Record<string, ProxyValue>;

interface ProxyConfig {
  global: ProxyValue;
  providers: ProxyMap;
  combos: ProxyMap;
  keys: ProxyMap;
  [key: string]: unknown;
}

function toProxyMap(value: unknown): ProxyMap {
  return value && typeof value === "object" ? (value as ProxyMap) : {};
}

function toProxyValue(value: unknown): ProxyValue {
  if (value === null || typeof value === "string") return value as string | null;
  if (value && typeof value === "object") return value as JsonRecord;
  return null;
}

// Legacy proxyConfig store (key_value namespace 'proxyConfig') predates the
// IPv6-only `family` directive, so its object configs have no family field.
// Default to "auto" so the family marker rides along the cascade end-to-end
// (consumed by proxyConfigToUrl). String configs are returned unchanged.
function withFamilyDefault(value: ProxyValue): ProxyValue {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const record = value as JsonRecord;
    if (typeof record.family === "string") return record;
    return { ...record, family: "auto" };
  }
  return value;
}

// ──────────────── Settings ────────────────

function readSettingsRevision(db: ReturnType<typeof getDbInstance>): number {
  const row = db
    .prepare("SELECT value FROM key_value WHERE namespace = 'settings' AND key = ?")
    .get(SETTINGS_REVISION_KEY) as { value?: string } | undefined;
  return parseSettingsRevision(row?.value);
}

export async function getSettingsRevision(): Promise<number> {
  const selected = getSelectedControlPlane();
  if (selected) return selected.settings.getRevision();
  return readSettingsRevision(getDbInstance());
}

export async function getSettings() {
  const selected = getSelectedControlPlane();
  if (selected) {
    const snapshot = readPostgresSettingsSnapshot();
    if (snapshot !== undefined) return snapshot;
    const result = await selected.settings.get();
    if (selected.database && hasPostgresCatalogSnapshot(selected.database)) {
      await refreshPostgresCatalogSnapshot(selected.database);
    }
    return result;
  }
  const db = getDbInstance();
  const rows = db.prepare("SELECT key, value FROM key_value WHERE namespace = 'settings'").all();
  const settings = interpretSettingsRows(rows, process.env, decrypt);

  // Auto-complete onboarding for pre-configured deployments (Docker/VM)
  // If INITIAL_PASSWORD is set via env, this is a headless deploy — skip the wizard
  if (needsHeadlessOnboarding(settings, process.env)) {
    settings.setupComplete = true;
    settings.requireLogin = true;
    db.prepare(
      "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('settings', 'setupComplete', 'true')"
    ).run();
    db.prepare(
      "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('settings', 'requireLogin', 'true')"
    ).run();
  }

  return settings;
}

export async function updateSettings(
  updates: Record<string, unknown>,
  options?: { expectedRevision?: number }
) {
  const selected = getSelectedControlPlane();
  if (selected) {
    const result = await selected.settings.update(updates, options);
    if (selected.database && hasPostgresCatalogSnapshot(selected.database)) {
      try {
        await refreshPostgresCatalogSnapshot(selected.database);
      } catch {
        throw new PostgresSettingsEffectsError(["catalog_snapshot"]);
      }
    }
    return result;
  }
  // Detect first-time setup completion before we overwrite settings.
  let setupJustCompleted = false;
  if (updates.setupComplete === true) {
    try {
      const prev = await getSettings();
      setupJustCompleted = prev.setupComplete !== true;
    } catch {
      setupJustCompleted = true;
    }
  }

  const db = getDbInstance();
  const insert = db.prepare(
    "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('settings', ?, ?)"
  );
  const tx = db.transaction(() => {
    const currentRevision = readSettingsRevision(db);
    if (options?.expectedRevision !== undefined && options.expectedRevision !== currentRevision) {
      throw new SettingsRevisionConflictError(currentRevision);
    }
    for (const [key, value] of Object.entries(updates)) {
      const toStore = key === "oidcClientSecret" ? encrypt(value as string) : value;
      insert.run(key, JSON.stringify(toStore));
    }
    insert.run(SETTINGS_REVISION_KEY, JSON.stringify(currentRevision + 1));
  });
  tx();
  backupDbFile("pre-write");
  invalidateDbCache("settings"); // Bust the read cache immediately

  // Bust proxy resolution cache when proxy toggle settings change
  const PROXY_TOGGLE_KEYS = ["proxyEnabled", "perKeyProxyEnabled"];
  if (Object.keys(updates).some((k) => PROXY_TOGGLE_KEYS.includes(k))) {
    bumpProxyConfigGeneration();
  }

  const nextSettings = await getSettings();

  try {
    const { applyRuntimeSettings } = await import("@/lib/config/runtimeSettings");
    await applyRuntimeSettings(nextSettings, { source: "settings:update" });
  } catch (error) {
    console.warn(
      "[HOT_RELOAD] Failed to apply runtime settings after update:",
      error instanceof Error ? error.message : error
    );
  }

  // Onboarding / setup finished → one-shot Codex catalog revalidation (init case).
  if (setupJustCompleted) {
    void import("@/shared/services/codexCatalogRevalidation")
      .then(({ scheduleCodexCatalogRevalidationAfterInit }) => {
        scheduleCodexCatalogRevalidationAfterInit();
      })
      .catch(() => {
        // non-fatal
      });
  }

  return nextSettings;
}

export async function isCloudEnabled() {
  const settings = await getSettings();
  return settings.cloudEnabled === true;
}

// ──────────────── Proxy Config ────────────────

const DEFAULT_PROXY_CONFIG: ProxyConfig = { global: null, providers: {}, combos: {}, keys: {} };
const ALIAS_TO_PROVIDER_ID = Object.entries(PROVIDER_ID_TO_ALIAS).reduce(
  (acc, [providerId, alias]) => {
    if (alias) acc[alias] = providerId;
    acc[providerId] = providerId;
    return acc;
  },
  {} as Record<string, string>
);

function resolveProviderAliasOrId(providerOrAlias: string): string {
  if (typeof providerOrAlias !== "string") return providerOrAlias;
  return ALIAS_TO_PROVIDER_ID[providerOrAlias] || providerOrAlias;
}

function getComboModelProvider(modelEntry: unknown): string | null {
  const providerOrAlias = getComboEntryProvider(modelEntry);
  return providerOrAlias ? resolveProviderAliasOrId(providerOrAlias) : null;
}

function migrateProxyEntry(value: unknown): JsonRecord | null {
  if (!value) return null;
  if (typeof value === "object") {
    const record = toRecord(value);
    if (record.type) return record;
  }
  if (typeof value !== "string") return null;

  try {
    const url = new URL(value);
    return {
      type: url.protocol.replace(":", "") || "http",
      host: url.hostname,
      port:
        url.port ||
        (url.protocol === "socks5:" ? "1080" : url.protocol === "https:" ? "443" : "8080"),
      username: url.username ? decodeUserinfo(url.username) : "",
      password: url.password ? decodeUserinfo(url.password) : "",
    };
  } catch {
    const parts = value.split(":");
    return {
      type: "http",
      host: parts[0] || value,
      port: parts[1] || "8080",
      username: "",
      password: "",
    };
  }
}

export async function getProxyConfig() {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) return getPostgresProxyConfig(postgres, migrateProxyEntry);
  const db = getDbInstance();
  const rows = db.prepare("SELECT key, value FROM key_value WHERE namespace = 'proxyConfig'").all();

  const raw: ProxyConfig = { ...DEFAULT_PROXY_CONFIG };
  for (const row of rows) {
    const record = toRecord(row);
    const key = typeof record.key === "string" ? record.key : null;
    const rawValue = typeof record.value === "string" ? record.value : null;
    if (!key || rawValue === null) continue;
    raw[key] = JSON.parse(rawValue);
  }

  let migrated = false;
  if (raw.global && typeof raw.global === "string") {
    raw.global = migrateProxyEntry(raw.global);
    migrated = true;
  }
  if (raw.providers) {
    for (const [k, v] of Object.entries(raw.providers)) {
      if (typeof v === "string") {
        raw.providers[k] = migrateProxyEntry(v);
        migrated = true;
      }
    }
  }

  if (migrated) {
    const insert = db.prepare(
      "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('proxyConfig', ?, ?)"
    );
    if (raw.global !== undefined) insert.run("global", JSON.stringify(raw.global));
    if (raw.providers) insert.run("providers", JSON.stringify(raw.providers));
  }

  return raw;
}

export async function getProxyForLevel(level: string, id?: string | null) {
  const config = await getProxyConfig();
  if (level === "global") return config.global || null;
  const map = toProxyMap(config[level + "s"] || config[level] || {});
  return (id ? map[id] : null) || null;
}

export async function setProxyForLevel(level: string, id: string | null, proxy: ProxyValue) {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const result = await setPostgresProxyForLevel(
      postgres,
      level,
      id,
      proxy,
      migrateProxyEntry,
      toProxyMap
    );
    bumpProxyConfigGeneration();
    return result;
  }
  const db = getDbInstance();
  const config = await getProxyConfig();

  if (level === "global") {
    config.global = proxy || null;
    db.prepare(
      "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('proxyConfig', 'global', ?)"
    ).run(JSON.stringify(config.global));
  } else {
    const mapKey = level + "s";
    const map = toProxyMap(config[mapKey] || {});
    if (proxy && id) {
      map[id] = proxy;
    } else {
      if (id) delete map[id];
    }
    config[mapKey] = map;
    db.prepare(
      "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('proxyConfig', ?, ?)"
    ).run(mapKey, JSON.stringify(map));
  }

  backupDbFile("pre-write");
  bumpProxyConfigGeneration();
  return config;
}

export async function deleteProxyForLevel(level: string, id: string | null) {
  return setProxyForLevel(level, id, null);
}

// With PROXY_SKIP_RECENTLY_FAILED on, a pool member set aside AFTER its resolution started is
// not re-served from the cache: the cascade runs again so the pool can pick another member.
// Once per set-aside event: the new entry records the sequence it started from, so a member
// the pool hands back anyway (every member set aside) is then served from the cache instead
// of costing a DB cascade on every request. Legacy single-proxy levels have no alternative
// and stay cached, like a result without a proxy. The flag is read last, only on a real hit.
function isCachedPoolMemberSetAside(entry: ProxyResolutionCacheEntry): boolean {
  const { result } = entry;
  if (!hasProxyRefusals() || result.source !== "registry" || result.proxy == null) return false;
  const setAsideSeq = proxySetAsideSeq(proxyEgressKey(result.proxy));
  if (setAsideSeq === null || setAsideSeq <= entry.refusalSeq) return false;
  return isProxySkipRecentlyFailedEnabled();
}

// The chat-path cache (below) exists so a hot connection does not pay the full resolution
// cascade on every request, but it must not FREEZE a rotating pool's choice: the registry
// resolver (resolveProxyForScopeFromRegistry, called directly by every #6365 rotation test)
// re-runs its strategy on every call and rotates correctly, while the cache here returned
// the same first-resolved member forever (#13575). A cached member is stale whenever it came
// from a live scope pool (source: "registry") and the connection is not in the two populations
// in the shared stable-egress policy: the caller then falls through to the full cascade,
// which re-invokes resolveProxyForScopeFromRegistry and applies the pool's own selection
// strategy (round-robin advances, sticky holds until its window elapses, random reshuffles) —
// no new strategy is introduced here.
function isCachedPoolMemberDue(
  entry: ProxyResolutionCacheEntry,
  db: ReturnType<typeof getDbInstance>,
  connectionId: string
): boolean {
  const { result } = entry;
  if (result.source !== "registry" || result.proxy == null) return false;
  const row = db
    .prepare("SELECT provider FROM provider_connections WHERE id = ?")
    .get(connectionId) as { provider?: string } | undefined;
  const provider = typeof row?.provider === "string" ? row.provider : null;
  return !requiresStableEgress(provider);
}

export async function resolveProxyForConnection(
  connectionId: string,
  apiKeyId?: string,
  providerId?: string
) {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    return resolvePostgresProxyForConnection(postgres, connectionId, apiKeyId, providerId);
  }
  const cacheKey = providerId
    ? `${connectionId}:${apiKeyId || ""}:${providerId}`
    : apiKeyId
      ? `${connectionId}:${apiKeyId}`
      : connectionId;
  const stamp: ProxyResolutionStamp = {
    generation: getProxyConfigGeneration(),
    registryGeneration: getProxyRegistryGeneration(),
    refusalSeq: getProxyRefusalSeq(),
  };
  const db = getDbInstance();
  const cached = proxyResolutionCache.get(cacheKey);
  if (
    cached &&
    cached.generation === stamp.generation &&
    cached.registryGeneration === stamp.registryGeneration &&
    !isCachedPoolMemberSetAside(cached) &&
    !isCachedPoolMemberDue(cached, db, connectionId)
  ) {
    return cached.result;
  }

  // Step 1: Check global proxyEnabled setting
  // Read only the proxyEnabled key for performance instead of loading all settings.
  let globalProxyEnabled = true;
  try {
    const proxyEnabledRow = db
      .prepare("SELECT value FROM key_value WHERE namespace = 'settings' AND key = 'proxyEnabled'")
      .get() as { value?: string } | undefined;
    if (proxyEnabledRow?.value) {
      globalProxyEnabled = JSON.parse(proxyEnabledRow.value) !== false;
    }
  } catch {
    // Default to true on read error
  }

  if (!globalProxyEnabled) {
    const result: ProxyResolutionResult = { proxy: null, level: "direct", levelId: null };
    // Do not cache the "direct" result when global toggle is off so that
    // toggling it back on takes effect immediately without a generation bump.
    return result;
  }

  let connectionRecord: JsonRecord | null = null;
  let connectionProvider: string | null = null;
  let connectionProxyEnabled = true;
  let connectionPerKeyProxyEnabled = false;

  const row = db
    .prepare(
      "SELECT provider, proxy_enabled, per_key_proxy_enabled FROM provider_connections WHERE id = ?"
    )
    .get(connectionId);
  if (row) {
    connectionRecord = toRecord(row);
    connectionProvider =
      typeof connectionRecord.provider === "string" ? connectionRecord.provider : null;
    connectionProxyEnabled = connectionRecord.proxy_enabled !== 0;
    connectionPerKeyProxyEnabled = connectionRecord.per_key_proxy_enabled === 1;
  }

  // A connection-level Proxy Off is explicit: it must bypass every stored proxy
  // source for this connection, including account, provider, global, and automatic
  // fallback candidates from the proxy pool.
  if (connectionRecord && !connectionProxyEnabled) {
    const result: ProxyResolutionResult = { proxy: null, level: "direct", levelId: null };
    cacheProxyResolution(cacheKey, stamp, result);
    return result;
  }

  // Step 1.5: Check global perKeyProxyEnabled setting
  let globalPerKeyProxyEnabled = false;
  try {
    const perKeyRow = db
      .prepare(
        "SELECT value FROM key_value WHERE namespace = 'settings' AND key = 'perKeyProxyEnabled'"
      )
      .get() as { value?: string } | undefined;
    if (perKeyRow?.value) {
      globalPerKeyProxyEnabled = JSON.parse(perKeyRow.value) !== false;
    }
  } catch {
    // Default to false on read error
  }

  const config = await getProxyConfig();

  // Step 2: API key-level proxy (only if per-key proxy is enabled globally or per-connection)
  if (apiKeyId) {
    // Check if per-key proxy is allowed: the global toggle is a true override —
    // when it is off, no connection's per-key assignment may apply, regardless
    // of that connection's own per_key_proxy_enabled flag (#8385).
    const perKeyEnabled = globalPerKeyProxyEnabled && connectionPerKeyProxyEnabled;

    if (perKeyEnabled) {
      try {
        const apiKeyRow = db.prepare("SELECT proxy_id FROM api_keys WHERE id = ?").get(apiKeyId) as
          { proxy_id?: string | null } | undefined;
        if (apiKeyRow?.proxy_id) {
          const proxyRow = db
            .prepare(
              "SELECT p.type, p.host, p.port, p.username, p.password, p.family FROM proxy_registry p WHERE p.id = ?"
            )
            .get(apiKeyRow.proxy_id) as
            | {
                type: string;
                host: string;
                port: number;
                username: string;
                password: string;
                family?: string;
              }
            | undefined;
          if (proxyRow) {
            const result = {
              proxy: {
                type: proxyRow.type,
                host: proxyRow.host,
                port: proxyRow.port,
                username: proxyRow.username,
                password: proxyRow.password,
                family: typeof proxyRow.family === "string" ? proxyRow.family : "auto",
              },
              level: "apiKey" as const,
              levelId: apiKeyId,
              source: "api_key" as const,
            };
            cacheProxyResolution(cacheKey, stamp, result);
            return result;
          }
        }
      } catch {
        // Fall through to existing resolution
      }
    }
  }

  // Step 3: Account-level registry
  const registryAccount = await resolveProxyForScopeFromRegistry("account", connectionId);
  if (registryAccount?.proxy) {
    cacheProxyResolution(cacheKey, stamp, registryAccount);
    return registryAccount;
  }

  // Step 4: Legacy key-level
  if (connectionId && config.keys?.[connectionId]) {
    const result = {
      proxy: withFamilyDefault(config.keys[connectionId]),
      level: "key",
      levelId: connectionId,
    };
    cacheProxyResolution(cacheKey, stamp, result);
    return result;
  }

  // Step 5: Use the connection's provider for provider/combo scoped proxies.
  if (connectionRecord) {
    // Step 6: Provider-level registry (only if proxy_enabled)
    if (connectionProvider && connectionProxyEnabled) {
      const registryProvider = await resolveProxyForScopeFromRegistry(
        "provider",
        connectionProvider
      );
      if (registryProvider?.proxy) {
        cacheProxyResolution(cacheKey, stamp, registryProvider);
        return registryProvider;
      }
    }

    // Step 7: Combo-level (only if proxy_enabled). For every combo whose model
    // list references this connection's provider, check the modern registry
    // (proxy_assignments, scope='combo') first — this is the assignment the
    // dashboard's Combo "Set Proxy" modal actually writes to (#7149, where the
    // registry write path and this read path had diverged, leaving combo-level
    // proxy assignment completely inert). Fall back to the legacy in-memory
    // combos map for any pre-existing legacy data.
    if (connectionProvider && connectionProxyEnabled) {
      const combos = db.prepare("SELECT id, data FROM combos").all();
      for (const comboRow of combos) {
        const comboRecord = toRecord(comboRow);
        const comboId = typeof comboRecord.id === "string" ? comboRecord.id : null;
        if (!comboId) continue;
        try {
          const comboRaw = typeof comboRecord.data === "string" ? comboRecord.data : null;
          if (!comboRaw) continue;
          const combo = toRecord(JSON.parse(comboRaw));
          const comboModels = Array.isArray(combo.models) ? combo.models : [];
          const usesProvider = comboModels.some(
            (entry) => getComboModelProvider(entry) === connectionProvider
          );
          if (!usesProvider) continue;

          const registryCombo = await resolveProxyForScopeFromRegistry("combo", comboId);
          if (registryCombo?.proxy) {
            cacheProxyResolution(cacheKey, stamp, registryCombo);
            return registryCombo;
          }

          if (config.combos?.[comboId]) {
            const result = {
              proxy: withFamilyDefault(config.combos[comboId]),
              level: "combo",
              levelId: comboId,
            };
            cacheProxyResolution(cacheKey, stamp, result);
            return result;
          }
        } catch {
          // Ignore malformed combo records during proxy resolution.
        }
      }
    }

    // Step 8: Legacy provider-level (only if proxy_enabled)
    if (connectionProvider && connectionProxyEnabled && config.providers?.[connectionProvider]) {
      const result = {
        proxy: withFamilyDefault(config.providers[connectionProvider]),
        level: "provider",
        levelId: connectionProvider,
      };
      cacheProxyResolution(cacheKey, stamp, result);
      return result;
    }
  }

  // Step 8.5 (#6272): no-auth providers (mimocode, opencode, ...) share a single
  // synthetic connectionId that never matches a `provider_connections` row, so
  // `connectionRecord` above is null and Steps 5-8 (which require it) never run for
  // them — a provider-level proxy assigned to a no-auth provider was silently
  // ignored. Best-effort fallback: scan the known no-auth provider ids directly.
  if (!connectionRecord) {
    const noAuthFallback = await resolveNoAuthSharedProviderProxy(config.providers, providerId);
    if (noAuthFallback) {
      cacheProxyResolution(cacheKey, stamp, noAuthFallback);
      return noAuthFallback;
    }
  }

  // Step 9: Global registry
  const registryGlobal = await resolveProxyForScopeFromRegistry("global");
  if (registryGlobal?.proxy) {
    cacheProxyResolution(cacheKey, stamp, registryGlobal);
    return registryGlobal;
  }

  // Step 10: Legacy global
  if (config.global) {
    const result = { proxy: withFamilyDefault(config.global), level: "global", levelId: null };
    cacheProxyResolution(cacheKey, stamp, result);
    return result;
  }

  // Step 11: Auto-selection fallback (only when global proxy is enabled)
  try {
    const { selectWorkingProxyFallback } = await import("@omniroute/open-sse/utils/proxyFallback");
    const fallback = await selectWorkingProxyFallback(connectionId);
    if (fallback) {
      // Auto-selected proxies are probed via a URL roundtrip that drops any
      // per-registry family policy, so default the family marker to "auto"
      // (no IPv6-only enforcement) when the fallback object omits it.
      const normalizedFallback =
        fallback.proxy && typeof fallback.proxy === "object"
          ? { ...fallback, proxy: withFamilyDefault(fallback.proxy as ProxyValue) }
          : fallback;
      cacheProxyResolution(cacheKey, stamp, normalizedFallback as ProxyResolutionResult);
      return normalizedFallback;
    }
  } catch (err) {
    console.warn({ err, connectionId }, "Proxy fallback auto-selection failed");
  }

  // Step 12: Return direct
  return { proxy: null, level: "direct", levelId: null };
}

export async function setProxyConfig(config: Record<string, unknown>) {
  if (config.level !== undefined) {
    const level = typeof config.level === "string" ? config.level : "global";
    const id = typeof config.id === "string" ? config.id : null;
    const proxy = (config.proxy as ProxyValue) || null;
    return setProxyForLevel(level, id, proxy);
  }

  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const result = await setPostgresProxyConfig(
      postgres,
      config,
      migrateProxyEntry,
      toProxyMap,
      toProxyValue
    );
    bumpProxyConfigGeneration();
    return result;
  }
  const db = getDbInstance();
  const current = await getProxyConfig();
  const insert = db.prepare(
    "INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('proxyConfig', ?, ?)"
  );

  const tx = db.transaction(() => {
    if (config.global !== undefined) {
      current.global = toProxyValue(config.global);
      insert.run("global", JSON.stringify(current.global));
    }
    for (const mapKey of ["providers", "combos", "keys"]) {
      if (config[mapKey]) {
        const merged = { ...toProxyMap(current[mapKey]), ...toProxyMap(config[mapKey]) };
        for (const [k, v] of Object.entries(merged)) {
          if (!v) delete merged[k];
        }
        current[mapKey] = merged;
        insert.run(mapKey, JSON.stringify(merged));
      }
    }
  });
  tx();

  backupDbFile("pre-write");
  bumpProxyConfigGeneration();
  return current;
}

// ──────────────── Re-exports from leaf modules ────────────────

export {
  type PricingSource,
  type PricingSourceMap,
  getPricing,
  getPricingWithSources,
  getPricingForModel,
  updatePricing,
  resetPricing,
  resetAllPricing,
} from "./settings/pricing";

export {
  type LKGPRecord,
  getLKGP,
  setLKGP,
  clearAllLKGP,
  clearLKGP,
  deleteLKGPByComboName,
  deleteLKGPRowsByComboName,
  deleteLKGPByConnectionIds,
} from "./settings/lkgp";

export {
  type CacheTrendPoint,
  getCacheMetrics,
  updateCacheMetrics,
  getCacheTrend,
  resetCacheMetrics,
} from "./settings/cacheMetrics";
export { getCachedSettings } from "./readCache";
