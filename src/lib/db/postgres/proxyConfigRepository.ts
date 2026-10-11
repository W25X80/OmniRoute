import type { PostgresDatabase, PostgresTransaction } from "./database";
import { lockProxyState } from "./proxyRepository";

type ProxyValue = Record<string, unknown> | string | null;
type ProxyMap = Record<string, ProxyValue>;
export interface StoredProxyConfig {
  global: ProxyValue;
  providers: ProxyMap;
  combos: ProxyMap;
  keys: ProxyMap;
  [key: string]: unknown;
}
type NormalizeLegacy = (value: unknown) => Record<string, unknown> | null;

async function load(tx: PostgresTransaction): Promise<StoredProxyConfig> {
  const config: StoredProxyConfig = { global: null, providers: {}, combos: {}, keys: {} };
  for (const row of (
    await tx.query<{ key: string; value: string }>(
      "SELECT key,value FROM omniroute.key_value WHERE namespace='proxyConfig'"
    )
  ).rows)
    config[row.key] = JSON.parse(row.value);
  return config;
}
function normalize(config: StoredProxyConfig, convert: NormalizeLegacy): boolean {
  let changed = false;
  if (config.global && typeof config.global === "string") {
    config.global = convert(config.global);
    changed = true;
  }
  if (config.providers) {
    for (const [key, value] of Object.entries(config.providers)) {
      if (typeof value === "string") {
        config.providers[key] = convert(value);
        changed = true;
      }
    }
  }
  return changed;
}
async function put(tx: PostgresTransaction, key: string, value: unknown): Promise<void> {
  await tx.query(
    "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('proxyConfig',$1,$2) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value",
    [key, JSON.stringify(value)]
  );
}
async function loadNormalized(tx: PostgresTransaction, convert: NormalizeLegacy) {
  const config = await load(tx);
  if (normalize(config, convert)) {
    if (config.global !== undefined) await put(tx, "global", config.global);
    if (config.providers) await put(tx, "providers", config.providers);
  }
  return config;
}

/** The source pure URL converter is supplied by settings.ts, preserving decodeUserinfo semantics. */
export async function getPostgresProxyConfig(
  database: PostgresDatabase,
  convert: NormalizeLegacy
): Promise<StoredProxyConfig> {
  const current = await database.transaction(load);
  if (!normalize(current, convert)) return current;
  // Re-read under the writer lock so a concurrent edit is never overwritten by lazy normalization.
  return database.transaction(async (tx) => {
    await lockProxyState(tx);
    return loadNormalized(tx, convert);
  });
}

export function setPostgresProxyForLevel(
  database: PostgresDatabase,
  level: string,
  id: string | null,
  value: ProxyValue,
  convert: NormalizeLegacy,
  toMap: (value: unknown) => ProxyMap
): Promise<StoredProxyConfig> {
  return database.transaction(async (tx) => {
    await lockProxyState(tx);
    const config = await loadNormalized(tx, convert);
    if (level === "global") {
      config.global = value || null;
      await put(tx, "global", config.global);
    } else {
      const key = level + "s",
        map = toMap(config[key] || {});
      if (value && id) map[id] = value;
      else if (id) delete map[id];
      config[key] = map;
      await put(tx, key, map);
    }
    return config;
  });
}

export function setPostgresProxyConfig(
  database: PostgresDatabase,
  patch: Record<string, unknown>,
  convert: NormalizeLegacy,
  toMap: (value: unknown) => ProxyMap,
  toValue: (value: unknown) => ProxyValue
): Promise<StoredProxyConfig> {
  return database.transaction(async (tx) => {
    await lockProxyState(tx);
    const current = await loadNormalized(tx, convert);
    if (patch.global !== undefined) {
      current.global = toValue(patch.global);
      await put(tx, "global", current.global);
    }
    for (const key of ["providers", "combos", "keys"]) {
      if (!patch[key]) continue;
      const merged = { ...toMap(current[key]), ...toMap(patch[key]) };
      for (const [id, value] of Object.entries(merged)) if (!value) delete merged[id];
      current[key] = merged;
      await put(tx, key, merged);
    }
    return current;
  });
}
