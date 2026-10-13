import type { PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";

export interface ApiKeyAuditEvent {
  action: string;
  target: string;
  details?: Record<string, unknown>;
}

/** Mandatory composition callbacks. Inert callbacks are permitted only in isolated tests. */
export interface PostgresApiKeyEffects {
  setNoLog(id: string, noLog: boolean): void;
  /** Shared publication sink when multiple callbacks wrap the same privacy cache. */
  privacyAuthority?: object;
  invalidateAuthCaches(): void | Promise<void>;
  invalidateModelCatalogCache(): void | Promise<void>;
  invalidateReasoningRoutingRuleCache(): void | Promise<void>;
  deleteRedisAuthCacheEntries(...hashes: string[]): void | Promise<void>;
  logAuditEvent(event: ApiKeyAuditEvent): void | Promise<void>;
}

export interface ApiKeyEffectsReceipt {
  hashes: string[];
  noLog?: { id: string; value: boolean };
  catalog?: boolean;
  reasoning?: boolean;
  audit?: ApiKeyAuditEvent[];
}

export class PostgresApiKeyEffectsError extends Error {
  readonly committed = true;
  constructor(readonly failedEffects: readonly string[]) {
    super("API key policy mutation committed but application effects failed");
    this.name = "PostgresApiKeyEffectsError";
  }
}

export function lockApiKeyPolicy(transaction: PostgresTransaction, shared = false) {
  return acquirePostgresLock(transaction, "keyPolicy", shared ? "shared" : "exclusive");
}

export interface ApiKeyPolicyVersion {
  authorityId: string;
  generation: string;
}

function policyVersion(
  row: { authority_id: string; generation: string } | undefined
): ApiKeyPolicyVersion {
  if (!row) throw new Error("API key policy epoch is unavailable");
  return { authorityId: row.authority_id, generation: row.generation };
}

/** Durable database identity and generation are read in one native SQL statement. */
export async function readApiKeyPolicyVersion(
  transaction: PostgresTransaction
): Promise<ApiKeyPolicyVersion> {
  const { rows } = await transaction.query<{ authority_id: string; generation: string }>(
    "SELECT authority_id,generation FROM omniroute.api_key_policy_epoch WHERE singleton"
  );
  return policyVersion(rows[0]);
}

/** Compatibility for owners that need only the durable generation. */
export async function readApiKeyPolicyEpoch(transaction: PostgresTransaction): Promise<string> {
  return (await readApiKeyPolicyVersion(transaction)).generation;
}

interface PolicyObservation {
  generation?: bigint;
  privacyPublications: Map<string, bigint>;
}

const observedVersions = new WeakMap<object, Map<string, PolicyObservation>>();

function policyObservation(
  effects: Pick<PostgresApiKeyEffects, "setNoLog" | "privacyAuthority">,
  authorityId: string
): PolicyObservation {
  const authority = effects.privacyAuthority ?? effects.setNoLog;
  let versions = observedVersions.get(authority);
  if (!versions) {
    versions = new Map<string, PolicyObservation>();
    observedVersions.set(authority, versions);
  }
  let observation = versions.get(authorityId);
  if (!observation) {
    observation = { privacyPublications: new Map<string, bigint>() };
    versions.set(authorityId, observation);
  }
  return observation;
}

/** Independent pools share ordering only when they publish to the same sink and durable authority. */
export function observeApiKeyPolicyVersion(
  effects: Pick<PostgresApiKeyEffects, "setNoLog" | "privacyAuthority">,
  version: ApiKeyPolicyVersion
): boolean {
  const observation = policyObservation(effects, version.authorityId);
  const current = BigInt(version.generation);
  if (observation.generation !== undefined && current < observation.generation) return false;
  observation.generation = current;
  return true;
}

/** Only a successful publication for this key proves that an older privacy value is obsolete. */
export function publishApiKeyNoLog(
  effects: Pick<PostgresApiKeyEffects, "setNoLog" | "privacyAuthority">,
  version: ApiKeyPolicyVersion,
  id: string,
  value: boolean
): boolean {
  const observation = policyObservation(effects, version.authorityId);
  const current = BigInt(version.generation);
  const highest = observation.privacyPublications.get(id);
  if (highest !== undefined && current < highest) return false;
  effects.setNoLog(id, value);
  observation.privacyPublications.set(id, current);
  return true;
}

export function sameApiKeyPolicyVersion(
  left: ApiKeyPolicyVersion,
  right: ApiKeyPolicyVersion
): boolean {
  return left.authorityId === right.authorityId && left.generation === right.generation;
}

export async function advanceApiKeyPolicyVersion(
  transaction: PostgresTransaction
): Promise<ApiKeyPolicyVersion> {
  const result = await transaction.query<{ authority_id: string; generation: string }>(
    "UPDATE omniroute.api_key_policy_epoch SET generation = generation + 1 WHERE singleton RETURNING authority_id,generation"
  );
  if (result.rowCount !== 1) throw new Error("API key policy epoch is unavailable");
  return policyVersion(result.rows[0]);
}

/** Compatibility for existing transactional settings/catalog mutation owners. */
export async function advanceApiKeyPolicyEpoch(transaction: PostgresTransaction): Promise<string> {
  return (await advanceApiKeyPolicyVersion(transaction)).generation;
}

/** All independent effects are attempted only after acknowledged COMMIT. */
export async function applyApiKeyEffects(
  effects: PostgresApiKeyEffects,
  receipt: ApiKeyEffectsReceipt,
  noLogEffect?: (id: string, value: boolean) => void | Promise<void>
): Promise<void> {
  const failed: string[] = [];
  async function attempt(name: string, effect: () => void | Promise<void>) {
    try {
      await effect();
    } catch {
      failed.push(name);
    }
  }
  await attempt("auth_caches", () => effects.invalidateAuthCaches());
  if (receipt.noLog)
    await attempt("no_log", () =>
      (noLogEffect || effects.setNoLog)(receipt.noLog!.id, receipt.noLog!.value)
    );
  if (receipt.catalog) await attempt("model_catalog", () => effects.invalidateModelCatalogCache());
  if (receipt.reasoning)
    await attempt("reasoning_routing", () => effects.invalidateReasoningRoutingRuleCache());
  await attempt("redis_auth", () => effects.deleteRedisAuthCacheEntries(...receipt.hashes));
  for (const event of receipt.audit || [])
    await attempt("audit", () => effects.logAuditEvent(event));
  if (failed.length) throw new PostgresApiKeyEffectsError([...new Set(failed)]);
}
