import { createHash, randomUUID } from "node:crypto";
import type {
  ApiKeyRepository,
  ApiKeyMetadata,
  ApiKeyPermissionsUpdate,
} from "@/domain/persistence/apiKeyRepository";
import { ALL_COMBOS_ACCESS_RULE } from "@/shared/constants/comboAccess";
import { SYNTHETIC_ENV_API_KEY_ID } from "@/shared/constants/apiKeyIdentities";
import { getProviderAlias, resolveProviderId } from "@/shared/constants/providers";
import { parseApiKeyCodexServiceMode } from "@/shared/constants/codexServiceMode";
import { timingSafeCompare } from "@/shared/utils/timingSafeCompare";
import { isSelfLoopBearer, selfLoopKeyOverrides } from "../apiKeys/selfLoopKey";
import { normalizeApiKeyPermissionsUpdate } from "../apiKeys/permissionsUpdate";
import {
  apiKeyLifecycleAllows,
  assertExclusiveLeaseKeyPolicy,
  hydrateApiKeyMetadata,
  hydrateApiKeyRecord,
} from "../apiKeys/persistenceSemantics";
import { evaluateKeyGroupAccess } from "../apiKeys/groupSemantics";
import {
  parseCacheDefaultMode,
  parseStreamDefaultMode,
  parseStringList,
  parseAllowedConnections,
} from "../apiKeys/rowParsers";
import { parseNullablePositiveNumber } from "../apiKeyUsageLimitFields";
import {
  CLAUDE_CODE_PROVIDER_PREFIXES,
  addModelCandidate,
  addProviderAliasScopedCandidates,
  hasClaudeCodeWildcardPermission,
  isPotentialUnprefixedClaudeCodeModel,
  modelPatternMatches,
  stripExtendedContextSuffix,
} from "../apiKeys/modelPermissions";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";
import { loadKeyGroupPolicy, type KeyGroupPolicy } from "./apiKeyGroupsRepository";
import {
  advanceApiKeyPolicyVersion,
  applyApiKeyEffects,
  lockApiKeyPolicy,
  readApiKeyPolicyVersion,
  observeApiKeyPolicyVersion,
  publishApiKeyNoLog,
  sameApiKeyPolicyVersion,
  type ApiKeyPolicyVersion,
  type ApiKeyAuditEvent,
  type ApiKeyEffectsReceipt,
  type PostgresApiKeyEffects,
} from "./apiKeyPolicyState";

export { PostgresApiKeyEffectsError } from "./apiKeyPolicyState";
export type { PostgresApiKeyEffects } from "./apiKeyPolicyState";

/** These catalog owners must use the selected backend; no SQLite fallback is admitted. */
export interface PostgresApiKeyModelPolicy {
  preferClaudeCodeForUnprefixedClaudeModels(): Promise<boolean>;
  resolveModelAlias(modelId: string): string | null | Promise<string | null>;
  findPublishedModel(
    providerOrAlias: string,
    shortModelId: string
  ): Promise<{ providerId: string; publishedModelId: string } | null>;
  isPublishedModelHidden(
    providerId: string,
    providerOrAlias: string,
    modelId: string
  ): boolean | Promise<boolean>;
  canonicalProviderForAlias(prefix: string): Promise<string | null>;
}

export interface PostgresApiKeyDependencies {
  effects: PostgresApiKeyEffects;
  modelPolicy: PostgresApiKeyModelPolicy;
  /** Native budgets/cost/reasoning-policy cleanup in the key DELETE transaction. */
  deleteDependents(transaction: PostgresTransaction, id: string): Promise<void>;
}

export interface PostgresApiKeyOptions {
  generateApiKeyWithMachine?(machineId: string): { key: string; keyId: string };
}

interface KeySnapshot {
  row: Record<string, unknown> | undefined;
  version: ApiKeyPolicyVersion;
  groups: KeyGroupPolicy;
}

const LAST_USED_UPDATE_TTL = 5 * 60 * 1000;
const EXCLUSIVE_LEASE_SCOPE = "lease:exclusive";

function hashKey(key: string): string {
  // High-entropy API-token identity, matching the SQLite SHA-256 representation.
  return createHash("sha256").update(key).digest("hex");
}

function syntheticMetadata(key: string): ApiKeyMetadata | null {
  const envKey = process.env.OMNIROUTE_API_KEY || process.env.ROUTER_API_KEY;
  if (!(envKey && timingSafeCompare(key, envKey)) && !isSelfLoopBearer(key)) return null;
  return {
    ...hydrateApiKeyMetadata({
      id: SYNTHETIC_ENV_API_KEY_ID,
      name: "Environment Key",
      machine_id: "server-env",
      model_access_mode: "all",
      auto_resolve: 1,
      allowed_combos: JSON.stringify([ALL_COMBOS_ACCESS_RULE]),
      scopes: '["manage"]',
      key_hash: null,
    }),
    ...selfLoopKeyOverrides(key),
  };
}

/** Encodes only known source columns; update names never enter SQL as caller text. */
function encodePermissionUpdate(update: ApiKeyPermissionsUpdate): Map<string, unknown> {
  const result = new Map<string, unknown>();
  const direct = {
    name: "name",
    modelAccessMode: "model_access_mode",
    maxRequestsPerDay: "max_requests_per_day",
    maxRequestsPerMinute: "max_requests_per_minute",
    throttleDelayMs: "throttle_delay_ms",
    expiresAt: "expires_at",
    catalogScope: "catalog_scope",
  } as const;
  for (const [field, column] of Object.entries(direct)) {
    const value = update[field as keyof ApiKeyPermissionsUpdate];
    if (value !== undefined) result.set(column, value);
  }
  const lists = {
    allowedModels: "allowed_models",
    blockedModels: "blocked_models",
    allowedCombos: "allowed_combos",
    allowedConnections: "allowed_connections",
    allowedQuotas: "allowed_quotas",
    scopes: "scopes",
    allowedEndpoints: "allowed_endpoints",
  } as const;
  for (const [field, column] of Object.entries(lists)) {
    const value = update[field as keyof ApiKeyPermissionsUpdate];
    if (value !== undefined)
      result.set(
        column,
        JSON.stringify(
          Array.isArray(value) ? value.filter((entry) => typeof entry === "string") : []
        )
      );
  }
  const booleans = {
    noLog: "no_log",
    autoResolve: "auto_resolve",
    isActive: "is_active",
    isBanned: "is_banned",
    disableNonPublicModels: "disable_non_public_models",
    allowUsageCommand: "allow_usage_command",
    chaosModeEnabled: "chaos_mode_enabled",
    compressionEnabled: "compression_enabled",
    allowAutoCombos: "allow_auto_combos",
    usageLimitEnabled: "usage_limit_enabled",
  } as const;
  for (const [field, column] of Object.entries(booleans)) {
    const value = update[field as keyof ApiKeyPermissionsUpdate];
    if (value !== undefined)
      result.set(column, field === "usageLimitEnabled" ? (value === true ? 1 : 0) : value ? 1 : 0);
  }
  for (const [field, column] of [
    ["accessSchedule", "access_schedule"],
    ["rateLimits", "rate_limits"],
  ] as const) {
    const value = update[field];
    if (value !== undefined) result.set(column, value === null ? null : JSON.stringify(value));
  }
  if (update.maxSessions !== undefined)
    result.set(
      "max_sessions",
      typeof update.maxSessions === "number" ? Math.max(0, update.maxSessions) : 0
    );
  if (update.proxyId !== undefined)
    result.set(
      "proxy_id",
      typeof update.proxyId === "string" && update.proxyId.trim() !== "" ? update.proxyId : null
    );
  if (update.streamDefaultMode !== undefined)
    result.set("stream_default_mode", parseStreamDefaultMode(update.streamDefaultMode));
  if (update.cacheDefaultMode !== undefined)
    result.set("cache_default_mode", parseCacheDefaultMode(update.cacheDefaultMode));
  if (update.codexServiceMode !== undefined)
    result.set("codex_service_mode", parseApiKeyCodexServiceMode(update.codexServiceMode));
  if (update.dailyUsageLimitUsd !== undefined)
    result.set("daily_usage_limit_usd", parseNullablePositiveNumber(update.dailyUsageLimitUsd));
  if (update.weeklyUsageLimitUsd !== undefined)
    result.set("weekly_usage_limit_usd", parseNullablePositiveNumber(update.weeklyUsageLimitUsd));
  return result;
}

export function createPostgresApiKeyRepository(
  database: PostgresDatabase,
  dependencies: PostgresApiKeyDependencies,
  repositoryOptions: PostgresApiKeyOptions = {}
): ApiKeyRepository {
  const { effects, modelPolicy } = dependencies;
  for (const name of [
    "setNoLog",
    "invalidateAuthCaches",
    "invalidateModelCatalogCache",
    "invalidateReasoningRoutingRuleCache",
    "deleteRedisAuthCacheEntries",
    "logAuditEvent",
  ] as const) {
    if (typeof effects?.[name] !== "function")
      throw new Error(`API key effect ${name} is required`);
  }
  if (typeof dependencies.deleteDependents !== "function")
    throw new Error("API key dependent cleanup is required");

  function read<T>(body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
    return database.transaction(async (tx) => {
      await lockApiKeyPolicy(tx, true);
      return body(tx);
    });
  }
  async function keyById(tx: PostgresTransaction, id: string) {
    return (await tx.query("SELECT * FROM omniroute.api_keys WHERE id=$1", [id])).rows[0];
  }
  async function snapshot(key: string, groups = false): Promise<KeySnapshot> {
    return read(async (tx) => {
      const version = await readApiKeyPolicyVersion(tx);
      const { rows } = await tx.query(
        "SELECT * FROM omniroute.api_keys WHERE key=$1 OR key_hash=$2 ORDER BY insertion_order LIMIT 1",
        [key, hashKey(key)]
      );
      return {
        row: rows[0],
        version,
        groups:
          groups && rows[0]
            ? await loadKeyGroupPolicy(tx, String(rows[0].id))
            : { hasGroups: false, permissions: [] },
      };
    });
  }
  function isCurrent(version: ApiKeyPolicyVersion): Promise<boolean> {
    return read((tx) => readApiKeyPolicyVersion(tx)).then(
      (latest) =>
        observeApiKeyPolicyVersion(effects, latest) && sameApiKeyPolicyVersion(latest, version)
    );
  }
  async function synchronizeNoLog(id: string): Promise<void> {
    const snapshot = await read(async (tx) => ({
      row: await keyById(tx, id),
      version: await readApiKeyPolicyVersion(tx),
    }));
    // A read's COMMIT acknowledgement can arrive late. Only a newer successful privacy
    // publication for this same key makes its old value obsolete; other keys stay independent.
    observeApiKeyPolicyVersion(effects, snapshot.version);
    publishApiKeyNoLog(
      effects,
      snapshot.version,
      id,
      snapshot.row ? hydrateApiKeyMetadata(snapshot.row).noLog : false
    );
  }
  async function mutation<T>(
    body: (tx: PostgresTransaction) => Promise<{ value: T; effects: ApiKeyEffectsReceipt | null }>
  ): Promise<T> {
    const receipt = await database.transaction(async (tx) => {
      await lockApiKeyPolicy(tx);
      const result = await body(tx);
      const version = result.effects ? await advanceApiKeyPolicyVersion(tx) : null;
      return { ...result, version };
    });
    if (receipt.version) observeApiKeyPolicyVersion(effects, receipt.version);
    if (receipt.effects)
      await applyApiKeyEffects(effects, receipt.effects, (id) => synchronizeNoLog(id));
    return receipt.value;
  }
  function rowHashes(row: Record<string, unknown> | undefined): string[] {
    return typeof row?.key_hash === "string" && row.key_hash ? [row.key_hash] : [];
  }
  async function candidates(modelId: string): Promise<string[]> {
    const found = new Set<string>();
    addModelCandidate(found, modelId);
    const clean = stripExtendedContextSuffix(modelId.trim());
    if (!clean) return [...found];
    if (clean.includes("/")) {
      const slash = clean.indexOf("/"),
        provider = clean.slice(0, slash),
        model = clean.slice(slash + 1);
      if (CLAUDE_CODE_PROVIDER_PREFIXES.has(provider) && model) {
        addModelCandidate(found, model);
        addModelCandidate(found, `cc/${model}`);
        addModelCandidate(found, `claude/${model}`);
      }
      if (model)
        addProviderAliasScopedCandidates(
          found,
          provider,
          model,
          resolveProviderId,
          getProviderAlias
        );
    } else if (
      isPotentialUnprefixedClaudeCodeModel(clean) &&
      (await modelPolicy.preferClaudeCodeForUnprefixedClaudeModels())
    ) {
      addModelCandidate(found, `cc/${clean}`);
      addModelCandidate(found, `claude/${clean}`);
    }
    return [...found];
  }
  async function publishedTarget(modelId: string) {
    const clean = stripExtendedContextSuffix(modelId.trim());
    if (clean.includes("/")) {
      const slash = clean.indexOf("/"),
        provider = clean.slice(0, slash),
        model = clean.slice(slash + 1);
      return model
        ? {
            providerId: CLAUDE_CODE_PROVIDER_PREFIXES.has(provider) ? "claude" : provider,
            modelId: model,
          }
        : null;
    }
    return isPotentialUnprefixedClaudeCodeModel(clean) &&
      (await modelPolicy.preferClaudeCodeForUnprefixedClaudeModels())
      ? { providerId: "claude", modelId: clean }
      : null;
  }
  const repository: ApiKeyRepository = {
    async getApiKeys(limit, offset) {
      const current = await read(async (tx) => {
        const suffix = limit === undefined ? "" : limit < 0 ? " OFFSET $1" : " LIMIT $1 OFFSET $2";
        const values =
          limit === undefined
            ? []
            : limit < 0
              ? [Math.max(0, offset ?? 0)]
              : [limit, Math.max(0, offset ?? 0)];
        const rows = (
          await tx.query(
            `SELECT * FROM omniroute.api_keys ORDER BY created_at COLLATE "C",insertion_order${suffix}`,
            values
          )
        ).rows;
        return { rows, version: await readApiKeyPolicyVersion(tx) };
      });
      const records = current.rows.map(hydrateApiKeyRecord);
      observeApiKeyPolicyVersion(effects, current.version);
      for (const record of records)
        publishApiKeyNoLog(effects, current.version, record.id, record.noLog);
      return records;
    },
    getApiKeysCount: () =>
      read(async (tx) =>
        Number(
          (await tx.query<{ count: string }>("SELECT count(*) AS count FROM omniroute.api_keys"))
            .rows[0].count
        )
      ),
    async getApiKeyById(id) {
      const current = await read(async (tx) => ({
        row: await keyById(tx, id),
        version: await readApiKeyPolicyVersion(tx),
      }));
      if (!current.row) return null;
      const record = hydrateApiKeyRecord(current.row);
      observeApiKeyPolicyVersion(effects, current.version);
      publishApiKeyNoLog(effects, current.version, id, record.noLog);
      return record;
    },
    async createApiKey(name, machineId, scopes = [], options = {}) {
      if (!machineId) throw new Error("machineId is required");
      const normalized = normalizeApiKeyPermissionsUpdate({
        modelAccessMode: options.modelAccessMode,
        allowedModels: options.allowedModels,
      });
      const connections = options.allowedConnections ?? [];
      assertExclusiveLeaseKeyPolicy(scopes, connections);
      const generator =
        repositoryOptions.generateApiKeyWithMachine ||
        (await import("@/shared/utils/apiKey")).generateApiKeyWithMachine;
      const key = generator(machineId).key,
        id = randomUUID(),
        now = new Date().toISOString();
      return mutation(async (tx) => {
        const { rows } = await tx.query(
          `INSERT INTO omniroute.api_keys(id,name,key,machine_id,model_access_mode,allowed_models,allowed_combos,allowed_connections,no_log,created_at,key_prefix,key_hash,scopes,expires_at)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,0,$9,$10,$11,$12,$13) RETURNING *`,
          [
            id,
            name,
            key,
            machineId,
            normalized.modelAccessMode ?? "all",
            JSON.stringify(normalized.allowedModels ?? []),
            JSON.stringify(options.allowedCombos ?? [ALL_COMBOS_ACCESS_RULE]),
            JSON.stringify(connections),
            now,
            key.slice(0, 12),
            hashKey(key),
            JSON.stringify(scopes),
            options.expiresAt ?? null,
          ]
        );
        return {
          value: hydrateApiKeyRecord(rows[0]),
          effects: { hashes: [hashKey(key)], noLog: { id, value: false }, catalog: true },
        };
      });
    },
    async regenerateApiKey(id) {
      const generator =
        repositoryOptions.generateApiKeyWithMachine ||
        (await import("@/shared/utils/apiKey")).generateApiKeyWithMachine;
      return mutation(async (tx) => {
        const row = await keyById(tx, id);
        if (!row) return { value: null, effects: null };
        const key = generator(String(row.machine_id || "0000000000000000")).key;
        await tx.query(
          "UPDATE omniroute.api_keys SET key=$1,key_hash=$2,key_prefix=$3 WHERE id=$4",
          [key, hashKey(key), key.slice(0, 12), id]
        );
        return {
          value: { id, key },
          effects: {
            hashes: [...rowHashes(row), hashKey(key)],
            audit: [
              {
                action: "apiKey.regenerate",
                target: id,
                details: { name: String(row.name || "") },
              },
            ],
          },
        };
      });
    },
    async updateApiKeyPermissions(id, update) {
      const normalized = normalizeApiKeyPermissionsUpdate(update),
        fields = encodePermissionUpdate(normalized);
      if (!fields.size) return false;
      return mutation(async (tx) => {
        const row = await keyById(tx, id);
        if (!row) return { value: false, effects: null };
        const previousScopes = parseStringList(row.scopes),
          nextScopes =
            normalized.scopes === undefined ? previousScopes : parseStringList(normalized.scopes);
        const connections =
          normalized.allowedConnections === undefined
            ? parseAllowedConnections(row.allowed_connections)
            : normalized.allowedConnections;
        assertExclusiveLeaseKeyPolicy(nextScopes, connections);
        const values: unknown[] = [id],
          sets: string[] = [];
        for (const [column, value] of fields) {
          values.push(value);
          sets.push(`${column}=$${values.length}`);
        }
        await tx.query(`UPDATE omniroute.api_keys SET ${sets.join(",")} WHERE id=$1`, values);
        const audit: ApiKeyAuditEvent[] = [];
        if (normalized.isBanned !== undefined)
          audit.push({ action: normalized.isBanned ? "apiKey.ban" : "apiKey.unban", target: id });
        if (normalized.isActive !== undefined)
          audit.push({
            action: normalized.isActive ? "apiKey.activate" : "apiKey.deactivate",
            target: id,
          });
        if (normalized.scopes !== undefined) {
          const had = previousScopes.includes("manage"),
            has = nextScopes.includes("manage");
          const action =
            !had && has
              ? "apiKey.scopes.grant"
              : had && !has
                ? "apiKey.scopes.revoke"
                : previousScopes.length !== nextScopes.length ||
                    previousScopes.some((scope) => !nextScopes.includes(scope)) ||
                    nextScopes.some((scope) => !previousScopes.includes(scope))
                  ? "apiKey.scopes.update"
                  : null;
          if (action)
            audit.push({
              action,
              target: id,
              details: { scopes: nextScopes, previous: previousScopes },
            });
        }
        const catalog = [
          "modelAccessMode",
          "allowedModels",
          "blockedModels",
          "allowedCombos",
          "allowedConnections",
          "allowedQuotas",
          "disableNonPublicModels",
          "allowAutoCombos",
          "catalogScope",
        ].some((field) => normalized[field as keyof ApiKeyPermissionsUpdate] !== undefined);
        return {
          value: true,
          effects: {
            hashes: rowHashes(row),
            catalog,
            audit,
            ...(normalized.noLog === undefined ? {} : { noLog: { id, value: normalized.noLog } }),
          },
        };
      });
    },
    deleteApiKey: (id) =>
      mutation(async (tx) => {
        const row = await keyById(tx, id);
        if (!row) return { value: false, effects: null };
        await acquirePostgresLock(tx, "accounting");
        await tx.query("DELETE FROM omniroute.api_keys WHERE id=$1", [id]);
        await dependencies.deleteDependents(tx, id);
        return {
          value: true,
          effects: {
            hashes: rowHashes(row),
            noLog: { id, value: false },
            catalog: true,
            reasoning: true,
          },
        };
      }),
    revokeApiKey: (id) =>
      mutation(async (tx) => {
        const row = await keyById(tx, id);
        if (!row) return { value: false, effects: null };
        await tx.query(
          "UPDATE omniroute.api_keys SET revoked_at=COALESCE(revoked_at,$1),is_active=0 WHERE id=$2",
          [new Date().toISOString(), id]
        );
        return { value: true, effects: { hashes: rowHashes(row), catalog: true } };
      }),
    setApiKeyExpiry: (id, expiry) =>
      mutation(async (tx) => {
        const row = await keyById(tx, id);
        if (!row) return { value: false, effects: null };
        await tx.query("UPDATE omniroute.api_keys SET expires_at=$1 WHERE id=$2", [expiry, id]);
        return { value: true, effects: { hashes: rowHashes(row), catalog: true } };
      }),
    async validateApiKey(key) {
      if (!key || typeof key !== "string") return false;
      if (syntheticMetadata(key)) return true;
      const current = await snapshot(key);
      if (!current.row || !apiKeyLifecycleAllows(current.row, Date.now())) return false;
      const id = String(current.row.id);
      // A separate native write rechecks identity and lifecycle before marking use. No local
      // positive auth cache or Redis SET can outlive a rotate/revoke from another replica.
      const accepted = await database.transaction(async (tx) => {
        await lockApiKeyPolicy(tx);
        if (!sameApiKeyPolicyVersion(await readApiKeyPolicyVersion(tx), current.version))
          return false;
        const row = await keyById(tx, id);
        const now = Date.now();
        if (!row || row.key !== current.row!.key || !apiKeyLifecycleAllows(row, now)) return false;
        const last = typeof row.last_used_at === "string" ? Date.parse(row.last_used_at) : NaN;
        if (!Number.isFinite(last) || now - last >= LAST_USED_UPDATE_TTL)
          await tx.query("UPDATE omniroute.api_keys SET last_used_at=$1 WHERE id=$2", [
            new Date(now).toISOString(),
            id,
          ]);
        return true;
      });
      return accepted && (await isCurrent(current.version));
    },
    async getApiKeyMetadata(key) {
      if (!key || typeof key !== "string") return null;
      const synthetic = syntheticMetadata(key);
      if (synthetic) return synthetic;
      const current = await snapshot(key);
      if (!current.row || !(await isCurrent(current.version))) return null;
      const result = hydrateApiKeyMetadata(current.row);
      if (!result.id) return null;
      publishApiKeyNoLog(effects, current.version, result.id, result.noLog);
      return result;
    },
    async isModelBlockedByPatterns(patterns, modelId) {
      if (!patterns?.length) return false;
      const targets = await candidates(modelId);
      return patterns.some((pattern) => modelPatternMatches(pattern, targets));
    },
    async isModelAllowedForKey(key, modelId, resolvedModelId) {
      if (!key) return true;
      if (!modelId) return false;
      const synthetic = syntheticMetadata(key),
        current = synthetic ? null : await snapshot(key, true);
      if (!synthetic && (!current?.row || !apiKeyLifecycleAllows(current.row, Date.now())))
        return false;
      const metadata = synthetic || hydrateApiKeyMetadata(current!.row!);
      const targets = [
        ...new Set([
          ...(await candidates(modelId)),
          ...(resolvedModelId ? await candidates(resolvedModelId) : []),
        ]),
      ];
      if (metadata.blockedModels.some((pattern) => modelPatternMatches(pattern, targets)))
        return false;
      if (metadata.disableNonPublicModels) {
        const effective =
          resolvedModelId || (await modelPolicy.resolveModelAlias(modelId)) || modelId;
        const publication = resolvedModelId ? await candidates(resolvedModelId) : targets;
        if (!hasClaudeCodeWildcardPermission(metadata.allowedModels, publication)) {
          const lookup = await publishedTarget(effective),
            provider = lookup?.providerId || effective.split("/")[0],
            model = lookup?.modelId || effective.split("/").slice(1).join("/");
          if (!provider || !model) return false;
          const published = await modelPolicy.findPublishedModel(provider, model);
          if (!published) return false;
          if (published.publishedModelId !== model && metadata.blockedModels.length) {
            const baseCandidates = await candidates(
              `${published.providerId}/${published.publishedModelId}`
            );
            if (
              metadata.blockedModels.some((pattern) => modelPatternMatches(pattern, baseCandidates))
            )
              return false;
          }
          if (
            await modelPolicy.isPublishedModelHidden(
              published.providerId,
              provider,
              published.publishedModelId
            )
          )
            return false;
        }
      }
      let allowed =
        metadata.allowedModels.length === 0
          ? metadata.modelAccessMode !== "restricted"
          : metadata.allowedModels.some((pattern) => modelPatternMatches(pattern, targets));
      if (current) {
        const groups = current.groups;
        for (const target of new Set([modelId, ...(resolvedModelId ? [resolvedModelId] : [])])) {
          const prefixed = target.includes("/"),
            provider = prefixed ? target.split("/")[0] : undefined,
            model = prefixed ? target.split("/").slice(1).join("/") : target;
          if (
            !evaluateKeyGroupAccess(groups.hasGroups, groups.permissions, model, provider)
              .allowed ||
            !evaluateKeyGroupAccess(groups.hasGroups, groups.permissions, target, provider).allowed
          )
            allowed = false;
          if (allowed && provider) {
            const canonical = await modelPolicy.canonicalProviderForAlias(provider);
            if (
              canonical &&
              (evaluateKeyGroupAccess(groups.hasGroups, groups.permissions, model, canonical)
                .deniedBy ||
                evaluateKeyGroupAccess(
                  groups.hasGroups,
                  groups.permissions,
                  `${canonical}/${model}`,
                  canonical
                ).deniedBy)
            )
              allowed = false;
          }
        }
        return allowed && (await isCurrent(current.version));
      }
      return allowed;
    },
    async pickApiKeyForInternalUse() {
      const keys = await repository.getApiKeys();
      const usable = keys.filter(
        (k) =>
          k.key &&
          k.isActive !== false &&
          !k.revokedAt &&
          !k.isBanned &&
          !k.scopes.includes(EXCLUSIVE_LEASE_SCOPE)
      );
      const manage = usable.find((k) => k.scopes.includes("manage"));
      if (manage) return manage.key;
      const all = usable.find((k) => k.modelAccessMode !== "restricted" && !k.allowedModels.length);
      if (all) return all.key;
      // SQLite currently compares only numeric lastUsedAt; persisted ISO text ties in insertion order.
      return usable[0]?.key ?? null;
    },
    getExclusiveLeaseConnectionIds: () =>
      read(async (tx) => {
        const { rows } = await tx.query(
          `SELECT allowed_connections FROM omniroute.api_keys WHERE is_active != 0 AND is_banned != 1 AND revoked_at IS NULL
        AND (expires_at IS NULL OR expires_at > $1) AND scopes LIKE $2`,
          [new Date().toISOString(), `%"${EXCLUSIVE_LEASE_SCOPE}"%`]
        );
        return new Set(rows.flatMap((row) => parseAllowedConnections(row.allowed_connections)));
      }),
    // There are deliberately no positive permission caches in this repository.
    clearApiKeyCaches() {},
  };
  return repository;
}
