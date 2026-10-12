import { getComboModelProvider } from "@/lib/combos/steps";
import { PROVIDER_ID_TO_ALIAS } from "@omniroute/open-sse/config/providerModels.ts";
import { hasPostgresCatalogSnapshot, withPostgresCatalogSnapshot } from "./catalogSnapshot";
import type { PostgresDatabase } from "./database";
import { createPostgresProxyRepository } from "./proxyRepository";
import { resolveNoAuthSharedProviderProxy } from "../settings/noAuthProxyFallback";
import { getSelectedPersistenceIdentity } from "../controlPlaneContext";
import { getProxyRegistryGeneration } from "../proxies/registryGeneration";
import { getProxyConfigGeneration, requiresStableEgress } from "../settings/stableEgressPolicy";
type Value = Record<string, unknown> | string | null;
interface Resolution {
  proxy: Value;
  level: string;
  levelId: string | null;
  source?: string;
}
interface PinStamp {
  config: number;
  registry: number;
}
interface StablePins {
  members: Map<string, PinStamp & { proxyId: string }>;
  queues: Array<Promise<void>>;
}
const PIN_LIMIT = 100;
const PIN_QUEUE_STRIPES = 64;
const pinsKey = Symbol.for("omniroute.postgres.stable-proxy-pins.v1");
const processState = globalThis as typeof globalThis & {
  [pinsKey]?: WeakMap<object, StablePins>;
};
const pinsByAuthority = (processState[pinsKey] ??= new WeakMap<object, StablePins>());

function pinStamp(): PinStamp {
  return { config: getProxyConfigGeneration(), registry: getProxyRegistryGeneration() };
}
function sameStamp(left: PinStamp, right: PinStamp): boolean {
  return left.config === right.config && left.registry === right.registry;
}

async function resolveStableScope(
  database: PostgresDatabase,
  repository: ReturnType<typeof createPostgresProxyRepository>,
  owner: readonly (string | undefined)[],
  stamp: PinStamp,
  scope: string,
  scopeId?: string | null
) {
  const identity = getSelectedPersistenceIdentity() ?? database;
  let pins = pinsByAuthority.get(identity);
  if (!pins) {
    pins = {
      members: new Map(),
      queues: Array.from({ length: PIN_QUEUE_STRIPES }, () => Promise.resolve()),
    };
    pinsByAuthority.set(identity, pins);
  }
  const key = JSON.stringify([...owner, scope, scopeId ?? null]);
  let hash = 0;
  for (let i = 0; i < key.length; i++) hash = (Math.imul(hash, 31) + key.charCodeAt(i)) | 0;
  const stripe = (hash >>> 0) % PIN_QUEUE_STRIPES;
  const previous = pins.queues[stripe];
  let release!: () => void;
  pins.queues[stripe] = new Promise<void>((resolve) => {
    release = resolve;
  });
  // Fixed lock stripes bound coordination state. Collisions only serialize stable
  // selections; ordinary providers never wait here. Waiters belong to active calls.
  await previous;
  try {
    const existing = pins.members.get(key);
    const preferred =
      existing && sameStamp(existing, stamp) && sameStamp(stamp, pinStamp())
        ? existing.proxyId
        : undefined;
    const selected = await repository.resolveScopeWithPreference(scope, scopeId, preferred);
    // Publish identity only after acknowledged COMMIT and only in the generation
    // that admitted this selection. A late old receipt cannot replace a newer pin.
    if (sameStamp(stamp, pinStamp())) {
      pins.members.delete(key);
      if (selected.proxyId) {
        pins.members.set(key, { ...stamp, proxyId: selected.proxyId });
        if (pins.members.size > PIN_LIMIT) pins.members.delete(pins.members.keys().next().value!);
      }
    }
    return selected.resolution;
  } finally {
    release();
  }
}
const ALIAS_TO_PROVIDER_ID: Record<string, string> = {};
for (const [provider, alias] of Object.entries(PROVIDER_ID_TO_ALIAS)) {
  if (alias) ALIAS_TO_PROVIDER_ID[alias] = provider;
  ALIAS_TO_PROVIDER_ID[provider] = provider;
}
function resolveProviderAliasOrId(value: string) {
  return ALIAS_TO_PROVIDER_ID[value] || value;
}
function family(value: Value): Value {
  return value &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    typeof value.family !== "string"
    ? { ...value, family: "auto" }
    : value;
}
function parse(value: string | undefined): unknown {
  try {
    return value === undefined ? undefined : JSON.parse(value);
  } catch {
    return undefined;
  }
}
/** Proxy policy is read on every call; stable pins retain only a checked member ID. */
function perKeyProxyEnabled(value: string | undefined): boolean {
  if (!value) return false;
  try {
    return JSON.parse(value) !== false;
  } catch {
    return false;
  }
}

async function resolveProxyForConnection(
  database: PostgresDatabase,
  connectionId: string,
  apiKeyId?: string,
  providerId?: string
): Promise<Resolution> {
  const stamp = pinStamp();
  const state = await database.transaction(async (tx) => {
    const rows = (
      await tx.query<{ namespace: string; key: string; value: string }>(
        "SELECT namespace,key,value FROM omniroute.key_value WHERE namespace='settings' AND key IN('proxyEnabled','perKeyProxyEnabled')"
      )
    ).rows;
    const settings = new Map(
      rows.filter((r) => r.namespace === "settings").map((r) => [r.key, r.value])
    );
    const connection = (
      await tx.query<{
        provider: string;
        proxy_enabled: number;
        per_key_proxy_enabled: number;
      }>(
        "SELECT provider,proxy_enabled,per_key_proxy_enabled FROM omniroute.provider_connections WHERE id=$1",
        [connectionId]
      )
    ).rows[0];
    const apiKey = apiKeyId
      ? (
          await tx.query<{ proxy_id: string | null }>(
            "SELECT proxy_id FROM omniroute.api_keys WHERE id=$1",
            [apiKeyId]
          )
        ).rows[0]
      : undefined;
    const keyProxy = apiKey?.proxy_id
      ? (
          await tx.query(
            "SELECT type,host,port,username,password,family FROM omniroute.proxy_registry WHERE id=$1",
            [apiKey.proxy_id]
          )
        ).rows[0]
      : undefined;
    const combos = connection?.provider
      ? (
          await tx.query<{ id: string; data: string }>(
            "SELECT id,data FROM omniroute.combos ORDER BY insertion_order"
          )
        ).rows
      : [];
    return {
      globalEnabled: parse(settings.get("proxyEnabled")) !== false,
      perKeyEnabled: perKeyProxyEnabled(settings.get("perKeyProxyEnabled")),
      connection,
      keyProxy,
      combos,
    };
  });
  const direct: Resolution = { proxy: null, level: "direct", levelId: null };
  if (!state.globalEnabled || state.connection?.proxy_enabled === 0) return direct;
  const { getProxyConfig } = await import("../settings");
  const config = await getProxyConfig();
  if (
    apiKeyId &&
    state.perKeyEnabled &&
    state.connection?.per_key_proxy_enabled === 1 &&
    state.keyProxy
  )
    return {
      proxy: family(state.keyProxy),
      level: "apiKey",
      levelId: apiKeyId,
      source: "api_key",
    };
  const repo = createPostgresProxyRepository(database);
  const provider = state.connection?.provider;
  const resolveScope = (scope: string, scopeId?: string | null) =>
    requiresStableEgress(provider ?? null)
      ? resolveStableScope(
          database,
          repo,
          [connectionId, apiKeyId, provider],
          stamp,
          scope,
          scopeId
        )
      : repo.resolveScope(scope, scopeId);
  const account = await resolveScope("account", connectionId);
  if (account?.proxy) return account as Resolution;
  const keys = config.keys as Record<string, Value> | undefined;
  if (connectionId && keys?.[connectionId])
    return {
      proxy: family(keys[connectionId]),
      level: "key",
      levelId: connectionId,
    };
  const providers = config.providers as Record<string, Value> | undefined;
  if (provider) {
    const registry = await resolveScope("provider", provider);
    if (registry?.proxy) return registry as Resolution;
    for (const row of state.combos) {
      const value = parse(row.data);
      if (!value || typeof value !== "object") continue;
      const combo = value as { models?: unknown };
      const models = Array.isArray(combo.models) ? combo.models : [];
      if (
        !models.some((entry) => {
          const alias = getComboModelProvider(entry);
          return alias && resolveProviderAliasOrId(alias) === provider;
        })
      )
        continue;
      const registryCombo = await resolveScope("combo", row.id);
      if (registryCombo?.proxy) return registryCombo as Resolution;
      const combos = config.combos as Record<string, Value> | undefined;
      if (combos?.[row.id])
        return {
          proxy: family(combos[row.id]),
          level: "combo",
          levelId: row.id,
        };
    }
    if (providers?.[provider])
      return {
        proxy: family(providers[provider]),
        level: "provider",
        levelId: provider,
      };
  } else {
    const fallback = await resolveNoAuthSharedProviderProxy(providers, providerId);
    if (fallback) return fallback;
  }
  const global = await resolveScope("global");
  if (global?.proxy) return global as Resolution;
  if (config.global)
    return {
      proxy: family(config.global as Value),
      level: "global",
      levelId: null,
    };
  const { selectWorkingProxyFallback } = await import("@omniroute/open-sse/utils/proxyFallback");
  const fallback = await selectWorkingProxyFallback(connectionId);
  return fallback
    ? ({ ...fallback, proxy: family(fallback.proxy as Value) } as Resolution)
    : direct;
}

async function resolveProxyForProvider(database: PostgresDatabase, providerId: string) {
  const enabled = await database.transaction(
    async (tx) =>
      (
        await tx.query<{ value: string }>(
          "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key='proxyEnabled'"
        )
      ).rows[0]
  );
  if (parse(enabled?.value) === false) return null;
  const repository = createPostgresProxyRepository(database);
  const scoped = await repository.resolveScope("provider", providerId);
  if (scoped?.proxy) return scoped.proxy;
  const { getProxyForLevel } = await import("../settings");
  const legacyProvider = await getProxyForLevel("provider", providerId);
  if (legacyProvider && typeof legacyProvider === "object" && legacyProvider.host)
    return {
      type: legacyProvider.type,
      host: legacyProvider.host,
      port: legacyProvider.port,
      username: legacyProvider.username,
      password: legacyProvider.password,
    };
  const global = await repository.resolveScope("global");
  if (global?.proxy) return global.proxy;
  const legacyGlobal = await getProxyForLevel("global");
  if (legacyGlobal && typeof legacyGlobal === "object" && legacyGlobal.host)
    return {
      type: legacyGlobal.type,
      host: legacyGlobal.host,
      port: legacyGlobal.port,
      username: legacyGlobal.username,
      password: legacyGlobal.password,
    };
  return null;
}

export function resolvePostgresProxyForConnection(
  database: PostgresDatabase,
  connectionId: string,
  apiKeyId?: string,
  providerId?: string
): Promise<Resolution> {
  const resolve = () => resolveProxyForConnection(database, connectionId, apiKeyId, providerId);
  return hasPostgresCatalogSnapshot(database)
    ? resolve()
    : withPostgresCatalogSnapshot(database, resolve);
}

export function resolvePostgresProxyForProvider(database: PostgresDatabase, providerId: string) {
  const resolve = () => resolveProxyForProvider(database, providerId);
  return hasPostgresCatalogSnapshot(database)
    ? resolve()
    : withPostgresCatalogSnapshot(database, resolve);
}
