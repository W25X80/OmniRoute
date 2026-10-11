import type {
  RadarCache,
  RadarSettings,
  RadarReferralsCache,
  RadarOffersCache,
  RadarIntelCache,
  RadarLocalModelState,
  RadarLocalMergeState,
  RadarLocalModelOverridePatch,
} from "../radar";
import { normalizeRadarIdentity, normalizeDisplayName } from "../radar/persistenceSemantics";
import { decrypt } from "../encryption";
import type { PostgresTransaction } from "./database";
import { first, rows, lockServices, protectSecret } from "./serviceQuery";

export async function getRadarCache(transaction: PostgresTransaction): Promise<RadarCache | null> {
  return (
    (await first<RadarCache>(
      transaction,
      `SELECT version, generated_at AS "generatedAt", tier, payload, signature, fetched_at AS "fetchedAt" FROM omniroute.radar_feed_cache WHERE id = 1`
    )) ?? null
  );
}
export async function setRadarCache(
  transaction: PostgresTransaction,
  entry: Parameters<typeof import("../radar").setRadarCache>[0]
): Promise<void> {
  await lockServices(transaction, "radar");
  await transaction.query(
    `INSERT INTO omniroute.radar_feed_cache (id, version, generated_at, tier, payload, signature, fetched_at)
    VALUES (1, $1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO UPDATE SET version = excluded.version,
    generated_at = excluded.generated_at, tier = excluded.tier, payload = excluded.payload,
    signature = excluded.signature, fetched_at = excluded.fetched_at`,
    [
      entry.version,
      entry.generatedAt ?? null,
      entry.tier,
      entry.payload,
      entry.signature,
      entry.fetchedAt ?? new Date().toISOString(),
    ]
  );
}
export async function getRadarSettings(transaction: PostgresTransaction): Promise<RadarSettings> {
  const row = await first<{
    opt_in: number;
    supporter_key_encrypted: string | null;
    updated_at: string;
  }>(
    transaction,
    "SELECT opt_in, supporter_key_encrypted, updated_at FROM omniroute.radar_settings WHERE id = 1"
  );
  if (!row) throw new Error("Radar settings were not initialized");
  return {
    optIn: row.opt_in === 1,
    supporterKey: decrypt(row.supporter_key_encrypted) ?? null,
    updatedAt: row.updated_at,
  };
}
export async function setRadarOptIn(
  transaction: PostgresTransaction,
  optIn: boolean
): Promise<void> {
  await transaction.query(
    "UPDATE omniroute.radar_settings SET opt_in = $1, updated_at = $2 WHERE id = 1",
    [optIn ? 1 : 0, new Date().toISOString()]
  );
}
export async function setRadarKey(
  transaction: PostgresTransaction,
  key: string | null
): Promise<void> {
  await lockServices(transaction, "radar");
  await transaction.query(
    "UPDATE omniroute.radar_settings SET supporter_key_encrypted = $1, updated_at = $2 WHERE id = 1",
    [protectSecret(key), new Date().toISOString()]
  );
  for (const table of [
    "radar_feed_cache",
    "radar_referrals_cache",
    "radar_offers_cache",
    "radar_intel_cache",
  ])
    await transaction.query(`DELETE FROM omniroute.${table} WHERE id = 1`);
}
export async function getRadarReferralsCache(
  transaction: PostgresTransaction
): Promise<RadarReferralsCache | null> {
  return (
    (await first<RadarReferralsCache>(
      transaction,
      `SELECT generated_at AS "generatedAt", tier, payload, signature, fetched_at AS "fetchedAt" FROM omniroute.radar_referrals_cache WHERE id = 1`
    )) ?? null
  );
}
export async function setRadarReferralsCache(
  transaction: PostgresTransaction,
  entry: Parameters<typeof import("../radar").setRadarReferralsCache>[0]
): Promise<void> {
  await lockServices(transaction, "radar");
  await transaction.query(
    `INSERT INTO omniroute.radar_referrals_cache (id, generated_at, tier, payload, signature, fetched_at)
    VALUES (1, $1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET generated_at = excluded.generated_at,
    tier = excluded.tier, payload = excluded.payload, signature = excluded.signature, fetched_at = excluded.fetched_at`,
    [
      entry.generatedAt,
      entry.tier,
      entry.payload,
      entry.signature,
      entry.fetchedAt ?? new Date().toISOString(),
    ]
  );
}
export async function getRadarOffersCache(
  transaction: PostgresTransaction
): Promise<RadarOffersCache | null> {
  return (
    (await first<RadarOffersCache>(
      transaction,
      `SELECT version, tier, payload, signature, fetched_at AS "fetchedAt" FROM omniroute.radar_offers_cache WHERE id = 1`
    )) ?? null
  );
}
export async function setRadarOffersCache(
  transaction: PostgresTransaction,
  entry: Parameters<typeof import("../radar").setRadarOffersCache>[0]
): Promise<void> {
  await lockServices(transaction, "radar");
  await transaction.query(
    `INSERT INTO omniroute.radar_offers_cache (id, version, tier, payload, signature, fetched_at)
    VALUES (1, $1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET version = excluded.version,
    tier = excluded.tier, payload = excluded.payload, signature = excluded.signature, fetched_at = excluded.fetched_at`,
    [
      entry.version,
      entry.tier,
      entry.payload,
      entry.signature,
      entry.fetchedAt ?? new Date().toISOString(),
    ]
  );
}
export async function getRadarIntelCache(
  transaction: PostgresTransaction
): Promise<RadarIntelCache | null> {
  return (
    (await first<RadarIntelCache>(
      transaction,
      `SELECT version, tier, payload, signature, supporter_identity AS "supporterIdentity", fetched_at AS "fetchedAt" FROM omniroute.radar_intel_cache WHERE id = 1`
    )) ?? null
  );
}
export async function setRadarIntelCache(
  transaction: PostgresTransaction,
  entry: Parameters<typeof import("../radar").setRadarIntelCache>[0]
): Promise<void> {
  await lockServices(transaction, "radar");
  await transaction.query(
    `INSERT INTO omniroute.radar_intel_cache (id, version, tier, payload, signature, supporter_identity, fetched_at)
    VALUES (1, $1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO UPDATE SET version = excluded.version,
    tier = excluded.tier, payload = excluded.payload, signature = excluded.signature, supporter_identity = excluded.supporter_identity, fetched_at = excluded.fetched_at`,
    [
      entry.version,
      entry.tier,
      entry.payload,
      entry.signature,
      entry.supporterIdentity,
      entry.fetchedAt ?? new Date().toISOString(),
    ]
  );
}
function mapLocal(row: Record<string, unknown>): RadarLocalModelState {
  return {
    provider: String(row.provider),
    modelId: String(row.model_id),
    displayName: row.display_name as string | null,
    enabled: row.enabled === null ? null : row.enabled === 1,
    tombstoned: row.tombstoned === 1,
    updatedAt: String(row.updated_at),
  };
}
export async function listRadarLocalModelState(
  transaction: PostgresTransaction
): Promise<RadarLocalModelState[]> {
  return (
    await rows(
      transaction,
      'SELECT * FROM omniroute.radar_local_model_state ORDER BY provider COLLATE "C", model_id COLLATE "C"'
    )
  ).map(mapLocal);
}
async function getLocal(
  transaction: PostgresTransaction,
  provider: string,
  modelId: string
): Promise<RadarLocalModelState | null> {
  const row = await first(
    transaction,
    "SELECT * FROM omniroute.radar_local_model_state WHERE provider = $1 AND model_id = $2",
    [provider, modelId]
  );
  return row ? mapLocal(row) : null;
}
async function storeLocal(
  transaction: PostgresTransaction,
  input: Omit<RadarLocalModelState, "updatedAt">
): Promise<void> {
  if (input.displayName === null && input.enabled === null && !input.tombstoned) {
    await transaction.query(
      "DELETE FROM omniroute.radar_local_model_state WHERE provider = $1 AND model_id = $2",
      [input.provider, input.modelId]
    );
    return;
  }
  await transaction.query(
    `INSERT INTO omniroute.radar_local_model_state (provider, model_id, display_name, enabled, tombstoned, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (provider, model_id) DO UPDATE SET display_name = excluded.display_name,
    enabled = excluded.enabled, tombstoned = excluded.tombstoned, updated_at = excluded.updated_at`,
    [
      input.provider,
      input.modelId,
      input.displayName,
      input.enabled === null ? null : input.enabled ? 1 : 0,
      input.tombstoned ? 1 : 0,
      new Date().toISOString(),
    ]
  );
}
export async function setRadarLocalModelOverride(
  transaction: PostgresTransaction,
  provider: unknown,
  modelId: unknown,
  patch: RadarLocalModelOverridePatch
): Promise<boolean> {
  const identity = normalizeRadarIdentity(provider, modelId);
  if (!identity || !patch || typeof patch !== "object") return false;
  const keys = Object.keys(patch);
  if (
    !keys.length ||
    keys.some((key) => key !== "displayName" && key !== "enabled") ||
    (Object.hasOwn(patch, "enabled") &&
      patch.enabled !== null &&
      typeof patch.enabled !== "boolean")
  )
    return false;
  const displayName = normalizeDisplayName(patch.displayName);
  if (Object.hasOwn(patch, "displayName") && displayName === undefined) return false;
  await lockServices(transaction, "radar");
  const current = await getLocal(transaction, identity.provider, identity.modelId);
  await storeLocal(transaction, {
    ...identity,
    displayName: Object.hasOwn(patch, "displayName")
      ? (displayName ?? null)
      : (current?.displayName ?? null),
    enabled: Object.hasOwn(patch, "enabled") ? (patch.enabled ?? null) : (current?.enabled ?? null),
    tombstoned: current?.tombstoned ?? false,
  });
  return true;
}
export async function clearRadarLocalModelOverride(
  transaction: PostgresTransaction,
  provider: unknown,
  modelId: unknown
): Promise<boolean> {
  const identity = normalizeRadarIdentity(provider, modelId);
  if (!identity) return false;
  await lockServices(transaction, "radar");
  const current = await getLocal(transaction, identity.provider, identity.modelId);
  await storeLocal(transaction, {
    ...identity,
    displayName: null,
    enabled: null,
    tombstoned: current?.tombstoned ?? false,
  });
  return true;
}
export async function setRadarModelTombstone(
  transaction: PostgresTransaction,
  provider: unknown,
  modelId: unknown,
  tombstoned: boolean
): Promise<boolean> {
  const identity = normalizeRadarIdentity(provider, modelId);
  if (!identity || typeof tombstoned !== "boolean") return false;
  await lockServices(transaction, "radar");
  const current = await getLocal(transaction, identity.provider, identity.modelId);
  await storeLocal(transaction, {
    ...identity,
    displayName: current?.displayName ?? null,
    enabled: current?.enabled ?? null,
    tombstoned,
  });
  return true;
}
export async function getRadarLocalMergeState(
  transaction: PostgresTransaction
): Promise<RadarLocalMergeState> {
  const localOverrides = new Map<string, { displayName?: string; enabled?: boolean }>();
  const tombstones = new Set<string>();
  for (const state of await listRadarLocalModelState(transaction)) {
    const key = `${state.provider}:${state.modelId}`;
    const override: { displayName?: string; enabled?: boolean } = {};
    if (state.displayName !== null) override.displayName = state.displayName;
    if (state.enabled !== null) override.enabled = state.enabled;
    if (Object.keys(override).length) localOverrides.set(key, override);
    if (state.tombstoned) tombstones.add(key);
  }
  return { localOverrides, tombstones };
}
