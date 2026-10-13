import { randomUUID, randomInt } from "node:crypto";
import { isProxyRegistryStatus } from "@/shared/constants/proxyRegistryStatus";
import {
  proxyEgressKey,
  isProxyAvoided,
  hasProxyRefusals,
} from "@omniroute/open-sse/utils/proxyRefusalMemory.ts";
import {
  isProxySkipRecentlyFailedEnabled,
  isProxyPoolSharedEgressOrderEnabled,
  isOperatorEgressEnabled,
} from "@/shared/utils/featureFlags";
import { isEgressBucketedLockScope } from "@omniroute/open-sse/config/providerErrorRules.ts";
import { maybeEmitPoolExhausted } from "@/lib/proxyEvents/proxyTransitionBridge";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import {
  mapProxyRow,
  mapAssignmentRow,
  normalizeScope,
  normalizeAssignmentScopeId,
  isScopeIdMissing,
  toRegistryProxyResolution,
  redactProxySecrets,
  toLegacyProxyLevel,
} from "../proxies/mappers";
import type {
  ProxyPayload,
  ProxyAssignmentPayload,
  ProxyRegistryRecord,
  ProxyScope,
  JsonRecord,
} from "../proxies/types";
import {
  normalizeRotationStrategy,
  normalizeRotationScopeId,
  rankPoolCandidates,
} from "../proxies/rotation";
import { PROXY_ALIVE_PREDICATE } from "../proxies/guards";
import { bumpProxyRegistryGeneration } from "../proxies/registryGeneration";
import { readPostgresEgressAddressSet } from "./proxyOperatorEgressRepository";
import { acquirePostgresLock } from "./locks";
const PROXY_UPDATE_ORDER = `CASE WHEN pg_input_is_valid(updated_at, 'timestamptz') THEN date_trunc('second', updated_at::timestamptz) END DESC NULLS LAST,name COLLATE "C",insertion_order`;
const PROXY_COLUMNS =
  "id,name,type,host,port,username,password,region,notes,status,source,family,subscription_id,created_at,updated_at";
const ASSIGNMENT_COLUMNS = "id,proxy_id,scope,scope_id,position,created_at,updated_at";
interface PreferredMember {
  preferredId?: string;
  selectedId?: string;
}

export async function lockProxyState(tx: PostgresTransaction, shared = false): Promise<void> {
  await acquirePostgresLock(tx, "proxies", shared ? "shared" : "exclusive");
}

function scopeIdentity(scope: string, scopeId?: string | null): [ProxyScope, string | null] {
  const normalized = normalizeScope(scope);
  const id = normalizeAssignmentScopeId(normalized, scopeId);
  if (isScopeIdMissing(normalized, id))
    throw new Error("scopeId is required for non-global proxy assignments");
  return [normalized, id];
}

async function readProxy(tx: PostgresTransaction, id: string, secrets = false) {
  const { rows } = await tx.query(
    `SELECT ${PROXY_COLUMNS} FROM omniroute.proxy_registry WHERE id=$1`,
    [id]
  );
  if (!rows[0]) return null;
  const proxy = mapProxyRow(rows[0]);
  return secrets ? proxy : redactProxySecrets(proxy);
}

async function insertProxy(
  tx: PostgresTransaction,
  id: string,
  payload: ProxyPayload,
  now: string
) {
  await tx.query(
    `INSERT INTO omniroute.proxy_registry (${PROXY_COLUMNS}) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
    [
      id,
      payload.name,
      payload.type,
      payload.host,
      Number(payload.port),
      payload.username || "",
      payload.password || "",
      payload.region || null,
      payload.notes || null,
      payload.status || "active",
      payload.source || "manual",
      payload.family || "auto",
      payload.subscriptionId ?? null,
      now,
      now,
    ]
  );
}

async function updateProxy(
  tx: PostgresTransaction,
  id: string,
  existing: ProxyRegistryRecord,
  payload: Partial<ProxyPayload>,
  now: string
) {
  const merged = {
    ...existing,
    ...payload,
    username: typeof payload.username === "string" ? payload.username.trim() : existing.username,
    password: typeof payload.password === "string" ? payload.password.trim() : existing.password,
    subscriptionId:
      payload.subscriptionId === undefined ? existing.subscriptionId : payload.subscriptionId,
  };
  await tx.query(
    `UPDATE omniroute.proxy_registry SET name=$2,type=$3,host=$4,port=$5,username=$6,password=$7,region=$8,notes=$9,status=$10,source=$11,family=$12,subscription_id=$13,updated_at=$14 WHERE id=$1`,
    [
      id,
      merged.name,
      merged.type,
      merged.host,
      Number(merged.port),
      merged.username || "",
      merged.password || "",
      merged.region || null,
      merged.notes || null,
      merged.status || "active",
      merged.source || "manual",
      merged.family || "auto",
      merged.subscriptionId ?? null,
      now,
    ]
  );
}

async function readAssignment(
  tx: PostgresTransaction,
  scope: string,
  scopeId: string | null,
  proxyId?: string
) {
  const { rows } = await tx.query(
    `SELECT ${ASSIGNMENT_COLUMNS} FROM omniroute.proxy_assignments WHERE scope=$1 AND scope_id IS NOT DISTINCT FROM $2 ${proxyId ? "AND proxy_id=$3" : ""} ORDER BY id LIMIT 1`,
    proxyId ? [scope, scopeId, proxyId] : [scope, scopeId]
  );
  return rows[0] ? mapAssignmentRow(rows[0]) : null;
}

async function replaceAssignment(
  tx: PostgresTransaction,
  scope: string,
  scopeId: string | null,
  proxyId: string,
  now: string
) {
  await tx.query(
    "DELETE FROM omniroute.proxy_assignments WHERE scope=$1 AND scope_id IS NOT DISTINCT FROM $2",
    [scope, scopeId]
  );
  await tx.query(
    "INSERT INTO omniroute.proxy_assignments(proxy_id,scope,scope_id,position,created_at,updated_at) VALUES($1,$2,$3,0,$4,$4)",
    [proxyId, scope, scopeId, now]
  );
}

async function clearLegacyAssignment(
  tx: PostgresTransaction,
  assignment: ProxyAssignmentPayload
): Promise<boolean> {
  const [scope, id] = scopeIdentity(assignment.scope, assignment.scopeId);
  const level = toLegacyProxyLevel(scope);
  const key = level === "global" ? "global" : `${level}s`;
  const { rows } = await tx.query<{ value: string }>(
    "SELECT value FROM omniroute.key_value WHERE namespace='proxyConfig' AND key=$1",
    [key]
  );
  if (!rows[0]) return false;
  let value: unknown;
  try {
    value = JSON.parse(rows[0].value);
  } catch {
    value = undefined;
  }
  if (level === "global") {
    if (value === null) return false;
    value = null;
  } else {
    const record =
      value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : {};
    if (value === record && !Object.prototype.hasOwnProperty.call(record, id!)) return false;
    delete record[id!];
    value = record;
  }
  await tx.query(
    "UPDATE omniroute.key_value SET value=$2 WHERE namespace='proxyConfig' AND key=$1",
    [key, JSON.stringify(value)]
  );
  return true;
}

async function committedMutation<T>(
  database: PostgresDatabase,
  body: (tx: PostgresTransaction) => Promise<T>
): Promise<T> {
  const result = await database.transaction(async (tx) => {
    await lockProxyState(tx);
    return body(tx);
  });
  bumpProxyRegistryGeneration();
  return result;
}

export function createPostgresProxyRepository(database: PostgresDatabase) {
  async function whereUsed(tx: PostgresTransaction, proxyId: string) {
    const assignments = (
      await tx.query(
        `SELECT ${ASSIGNMENT_COLUMNS} FROM omniroute.proxy_assignments WHERE proxy_id=$1 ORDER BY scope,scope_id NULLS FIRST,id`,
        [proxyId]
      )
    ).rows.map(mapAssignmentRow);
    const connections = (
      await tx.query<{
        id: string;
        provider: string;
        provider_specific_data: string | null;
      }>(
        "SELECT id,provider,provider_specific_data FROM omniroute.provider_connections ORDER BY insertion_order"
      )
    ).rows;
    const accountReferences: Array<{
      connectionId: string;
      provider: string;
      accountCount: number;
    }> = [];
    let accountReferenceCount = 0;
    for (const connection of connections) {
      let value: unknown;
      try {
        value = JSON.parse(connection.provider_specific_data || "null");
      } catch {
        continue;
      }
      const refs =
        value && typeof value === "object"
          ? (value as { accountProxies?: unknown }).accountProxies
          : null;
      if (!Array.isArray(refs)) continue;
      const accountCount = refs.filter(
        (entry) => entry && typeof entry === "object" && entry.proxyId === proxyId
      ).length;
      if (accountCount) {
        accountReferences.push({
          connectionId: connection.id,
          provider: connection.provider,
          accountCount,
        });
        accountReferenceCount += accountCount;
      }
    }
    return {
      count: assignments.length + accountReferenceCount,
      assignmentCount: assignments.length,
      assignments,
      accountReferenceCount,
      accountReferences,
    };
  }

  async function resolve(
    tx: PostgresTransaction,
    scope: ProxyScope,
    id: string | null,
    anyId: boolean,
    notifications: Array<{ scope: string; candidates: JsonRecord[] }>,
    preference?: PreferredMember
  ) {
    const candidates = (
      await tx.query<JsonRecord>(
        `SELECT p.id,p.name,p.type,p.host,p.port,p.username,p.password,p.notes,p.family,a.position AS __pos,a.id AS __aid FROM omniroute.proxy_assignments a JOIN omniroute.proxy_registry p ON p.id=a.proxy_id WHERE a.scope=$1 ${anyId ? "" : "AND a.scope_id IS NOT DISTINCT FROM $2"} AND ${PROXY_ALIVE_PREDICATE} ORDER BY a.position,a.id`,
        anyId ? [scope] : [scope, id]
      )
    ).rows;
    if (!candidates.length) return null;
    notifications.push({ scope, candidates });
    const selected = (row: JsonRecord) => {
      if (preference) preference.selectedId = String(row.id);
      return toRegistryProxyResolution(row, scope, scope === "global" ? null : id);
    };
    const preferred = preference?.preferredId
      ? candidates.find((row) => row.id === preference.preferredId)
      : undefined;
    // A stable egress pins identity, never credentials or policy. Every lookup reads
    // current alive membership; a refused member yields to an eligible alternative.
    if (
      preferred &&
      (!isProxySkipRecentlyFailedEnabled() ||
        !isProxyAvoided(proxyEgressKey(preferred)) ||
        candidates.every((row) => isProxyAvoided(proxyEgressKey(row))))
    )
      return selected(preferred);
    if (candidates.length === 1) return selected(candidates[0]);
    const rotationId = normalizeRotationScopeId(scope, id);
    const now = new Date().toISOString();
    await tx.query(
      "INSERT INTO omniroute.proxy_scope_rotation(scope,scope_id,strategy,cursor,updated_at) VALUES($1,$2,'round-robin',0,$3) ON CONFLICT(scope,scope_id) DO NOTHING",
      [scope, rotationId, now]
    );
    const state = (
      await tx.query<{
        strategy: string;
        cursor: string;
        sticky_window_minutes: number;
        rotated_at: string | null;
      }>(
        "SELECT strategy,cursor,sticky_window_minutes,rotated_at FROM omniroute.proxy_scope_rotation WHERE scope=$1 AND scope_id=$2 FOR UPDATE",
        [scope, rotationId]
      )
    ).rows[0];
    const strategy = normalizeRotationStrategy(state.strategy);
    let cursor = Number(state.cursor) || 0;
    const expired =
      !state.rotated_at ||
      !Number.isFinite(Date.parse(state.rotated_at)) ||
      Date.now() - Date.parse(state.rotated_at) >=
        (Number(state.sticky_window_minutes) || 30) * 60_000;
    if (strategy === "sticky" && !expired) {
      let index = ((cursor % candidates.length) + candidates.length) % candidates.length;
      if (hasProxyRefusals() && isProxySkipRecentlyFailedEnabled()) {
        const eligible = candidates
          .map((row, i) => (isProxyAvoided(proxyEgressKey(row)) ? -1 : i))
          .filter((i) => i >= 0);
        if (eligible.length && eligible.length < candidates.length) {
          for (let step = 0; step < candidates.length; step++) {
            const next = (index + step) % candidates.length;
            if (eligible.includes(next)) {
              index = next;
              break;
            }
          }
        }
      }
      return selected(candidates[index]);
    }
    let provider: string | null = scope === "provider" ? id : null;
    if (
      scope === "account" &&
      id &&
      isProxySkipRecentlyFailedEnabled() &&
      isProxyPoolSharedEgressOrderEnabled() &&
      hasProxyRefusals()
    )
      provider =
        (
          await tx.query<{ provider: string }>(
            "SELECT provider FROM omniroute.provider_connections WHERE id=$1",
            [id]
          )
        ).rows[0]?.provider ?? null;
    const addressSets = new Map<unknown, Set<string>>();
    const hot = new Set<string>();
    if (
      isProxySkipRecentlyFailedEnabled() &&
      isProxyPoolSharedEgressOrderEnabled() &&
      isEgressBucketedLockScope(provider) &&
      hasProxyRefusals()
    ) {
      for (const candidate of candidates) {
        const set = (
          await readPostgresEgressAddressSet(
            tx,
            String(candidate.host || ""),
            Number(candidate.port),
            isOperatorEgressEnabled()
          )
        ).addresses;
        addressSets.set(candidate, set);
        if (isProxyAvoided(proxyEgressKey(candidate))) for (const address of set) hot.add(address);
      }
    }
    const ranked = isProxySkipRecentlyFailedEnabled()
      ? rankPoolCandidates(candidates, {
          sharesHotEgress: (candidate) =>
            [...(addressSets.get(candidate) ?? [])].some((address) => hot.has(address)),
        })
      : [...candidates];
    const eligible =
      hasProxyRefusals() && isProxySkipRecentlyFailedEnabled()
        ? ranked.map((r, i) => (isProxyAvoided(proxyEgressKey(r)) ? -1 : i)).filter((i) => i >= 0)
        : [];
    const chooseFrom =
      eligible.length && eligible.length < ranked.length ? eligible : ranked.map((_, i) => i);
    let index: number;
    if (strategy === "random") index = chooseFrom[randomInt(chooseFrom.length)];
    else if (strategy === "latency") {
      const hours = parseInt(process.env.PROXY_LATENCY_WINDOW_HOURS ?? "3", 10);
      const since = new Date(Date.now() - hours * 3_600_000).toISOString();
      const latency = (
        await tx.query<{
          proxy_host: string;
          proxy_port: number;
          avg_latency: number | null;
        }>(
          "SELECT proxy_host,proxy_port,AVG(latency_ms) AS avg_latency FROM omniroute.proxy_logs WHERE timestamp >= $1 AND (attempt_issue IS NULL OR attempt_issue <> 'abandoned') GROUP BY proxy_host,proxy_port",
          [since]
        )
      ).rows;
      const map = new Map(
        latency
          .filter((r) => r.avg_latency != null)
          .map((r) => [`${r.proxy_host}:${r.proxy_port}`, Number(r.avg_latency)])
      );
      index = [...chooseFrom].sort(
        (a, b) =>
          (map.get(`${ranked[a].host}:${ranked[a].port}`) ?? -1) -
          (map.get(`${ranked[b].host}:${ranked[b].port}`) ?? -1)
      )[0];
    } else {
      if (strategy === "sticky" && expired) cursor++;
      index = ((cursor % ranked.length) + ranked.length) % ranked.length;
      for (let step = 0; step < ranked.length; step++) {
        const next = (index + step) % ranked.length;
        if (chooseFrom.includes(next)) {
          index = next;
          break;
        }
      }
      const advance =
        strategy === "sticky"
          ? cursor
          : cursor +
            ((index -
              (((cursor % ranked.length) + ranked.length) % ranked.length) +
              ranked.length) %
              ranked.length) +
            1;
      if (strategy !== "sticky" || expired)
        await tx.query(
          strategy === "sticky"
            ? "UPDATE omniroute.proxy_scope_rotation SET cursor=$3,rotated_at=$4,updated_at=$4 WHERE scope=$1 AND scope_id=$2"
            : "UPDATE omniroute.proxy_scope_rotation SET cursor=$3,updated_at=$4 WHERE scope=$1 AND scope_id=$2",
          [scope, rotationId, advance, now]
        );
    }
    return selected(ranked[index]);
  }

  async function resolveScope(
    scope: string,
    scopeId?: string | null,
    preference?: PreferredMember
  ) {
    const notifications: Array<{ scope: string; candidates: JsonRecord[] }> = [];
    const selected = await database.transaction(async (tx) => {
      await lockProxyState(tx, true);
      const s = normalizeScope(scope);
      if (s !== "global" && !scopeId) return null;
      return resolve(
        tx,
        s,
        s === "global" ? null : scopeId!,
        s === "global",
        notifications,
        preference
      );
    });
    for (const notification of notifications)
      maybeEmitPoolExhausted(
        notification.scope,
        notification.candidates,
        (row) => proxyEgressKey(row),
        (key) => isProxyAvoided(key)
      );
    return selected;
  }

  return {
    list: (options?: { includeSecrets?: boolean; limit?: number; offset?: number }) =>
      database.transaction(async (tx) => {
        const rows = (
          await tx.query(
            `SELECT ${PROXY_COLUMNS} FROM omniroute.proxy_registry ORDER BY ${PROXY_UPDATE_ORDER} ${options?.limit !== undefined ? "LIMIT CASE WHEN $1::bigint < 0 THEN NULL ELSE $1::bigint END OFFSET GREATEST($2::bigint,0)" : ""}`,
            options?.limit !== undefined ? [options.limit, options.offset ?? 0] : []
          )
        ).rows;
        const total = Number(
          (
            await tx.query<{ count: string }>(
              "SELECT count(*) AS count FROM omniroute.proxy_registry"
            )
          ).rows[0].count
        );
        const proxies = rows.map(mapProxyRow);
        return {
          items: options?.includeSecrets === true ? proxies : proxies.map(redactProxySecrets),
          total,
        };
      }),
    get: (id: string, options?: { includeSecrets?: boolean }) =>
      database.transaction((tx) => readProxy(tx, id, options?.includeSecrets === true)),
    create: (payload: ProxyPayload) =>
      committedMutation(database, async (tx) => {
        const id = randomUUID();
        await insertProxy(tx, id, payload, new Date().toISOString());
        return readProxy(tx, id);
      }),
    upsert: (payload: ProxyPayload, options?: { claimOwnership?: boolean }) =>
      committedMutation(database, async (tx) => {
        const existing = (
          await tx.query<{ id: string; subscription_id: string | null }>(
            "SELECT id,subscription_id FROM omniroute.proxy_registry WHERE host=$1 AND port=$2 AND username=$3 ORDER BY insertion_order LIMIT 1",
            [(payload.host || "").trim(), Number(payload.port), (payload.username || "").trim()]
          )
        ).rows[0];
        if (existing) {
          if (
            options?.claimOwnership === false &&
            (existing.subscription_id ?? null) !== (payload.subscriptionId ?? null)
          )
            return { proxy: null, action: "skipped" as const };
          const row = await readProxy(tx, existing.id, true);
          const { status, ...rest } = payload;
          await updateProxy(
            tx,
            existing.id,
            row!,
            isProxyRegistryStatus(status) ? { ...rest, status } : rest,
            new Date().toISOString()
          );
          return {
            proxy: await readProxy(tx, existing.id),
            action: "updated" as const,
          };
        }
        const id = randomUUID();
        await insertProxy(tx, id, payload, new Date().toISOString());
        return { proxy: await readProxy(tx, id), action: "created" as const };
      }),
    update: (id: string, payload: Partial<ProxyPayload>) =>
      committedMutation(database, async (tx) => {
        const existing = await readProxy(tx, id, true);
        if (!existing) return null;
        await updateProxy(tx, id, existing, payload, new Date().toISOString());
        return readProxy(tx, id);
      }),
    mutateAndAssign: (
      id: string | null,
      payload: ProxyPayload | Partial<ProxyPayload>,
      assignment: ProxyAssignmentPayload
    ) =>
      committedMutation(database, async (tx) => {
        const now = new Date().toISOString();
        const proxyId = id ?? randomUUID();
        if (id) {
          const existing = await readProxy(tx, id, true);
          if (!existing) return null;
          await updateProxy(tx, id, existing, payload, now);
        } else await insertProxy(tx, proxyId, payload as ProxyPayload, now);
        const [scope, scopeId] = scopeIdentity(assignment.scope, assignment.scopeId);
        await replaceAssignment(tx, scope, scopeId, proxyId, now);
        await clearLegacyAssignment(tx, assignment);
        return {
          proxy: (await readProxy(tx, proxyId))!,
          assignment: await readAssignment(tx, scope, scopeId),
        };
      }),
    assignments: (filters?: { proxyId?: string; scope?: string }) =>
      database.transaction(async (tx) =>
        (
          await tx.query(
            `SELECT ${ASSIGNMENT_COLUMNS} FROM omniroute.proxy_assignments ${filters?.proxyId ? "WHERE proxy_id=$1" : filters?.scope ? "WHERE scope=$1" : ""} ORDER BY scope,scope_id NULLS FIRST,id`,
            filters?.proxyId
              ? [filters.proxyId]
              : filters?.scope
                ? [normalizeScope(filters.scope)]
                : []
          )
        ).rows.map(mapAssignmentRow)
      ),
    whereUsed: (id: string) => database.transaction((tx) => whereUsed(tx, id)),
    assign: (scope: string, scopeId: string | null, proxyId: string | null) =>
      committedMutation(database, async (tx) => {
        const [s, id] = scopeIdentity(scope, scopeId);
        if (!proxyId) {
          await tx.query(
            "DELETE FROM omniroute.proxy_assignments WHERE scope=$1 AND scope_id IS NOT DISTINCT FROM $2",
            [s, id]
          );
          await tx.query(
            "DELETE FROM omniroute.proxy_scope_rotation WHERE scope=$1 AND scope_id=$2",
            [s, id ?? ""]
          );
          return null;
        }
        if (!(await readProxy(tx, proxyId, true)))
          throw Object.assign(new Error(`Proxy not found: ${proxyId}`), {
            status: 404,
          });
        const now = new Date().toISOString();
        await replaceAssignment(tx, s, id, proxyId, now);
        await tx.query(
          "UPDATE omniroute.proxy_scope_rotation SET cursor=0,rotated_at=NULL,updated_at=$3 WHERE scope=$1 AND scope_id=$2",
          [s, id ?? "", now]
        );
        return readAssignment(tx, s, id);
      }),
    addToPool: (scope: string, scopeId: string | null, proxyIds: string[]) =>
      committedMutation(database, async (tx) => {
        const [s, id] = scopeIdentity(scope, scopeId);
        const unique = [...new Set(proxyIds.filter(Boolean))];
        let added = 0;
        let position =
          Number(
            (
              await tx.query<{ max: number | null }>(
                "SELECT max(position) AS max FROM omniroute.proxy_assignments WHERE scope=$1 AND scope_id IS NOT DISTINCT FROM $2",
                [s, id]
              )
            ).rows[0].max ?? -1
          ) + 1;
        const now = new Date().toISOString();
        for (const proxyId of unique) {
          if (!(await readProxy(tx, proxyId, true)))
            throw Object.assign(new Error(`Proxy not found: ${proxyId}`), {
              status: 404,
            });
          if (await readAssignment(tx, s, id, proxyId)) {
            position++;
            continue;
          }
          await tx.query(
            "INSERT INTO omniroute.proxy_assignments(proxy_id,scope,scope_id,position,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$5)",
            [proxyId, s, id, position++, now]
          );
          added++;
        }
        return added;
      }),
    removeFromPool: (scope: string, scopeId: string | null, proxyId: string) =>
      committedMutation(database, async (tx) => {
        const s = normalizeScope(scope);
        const id = normalizeAssignmentScopeId(s, scopeId);
        return (
          (
            await tx.query(
              "DELETE FROM omniroute.proxy_assignments WHERE scope=$1 AND scope_id IS NOT DISTINCT FROM $2 AND proxy_id=$3",
              [s, id, proxyId]
            )
          ).rowCount! > 0
        );
      }),
    setStrategy: (
      scope: string,
      scopeId: string | null,
      strategy: string,
      options?: { stickyWindowMinutes?: number }
    ) =>
      committedMutation(database, async (tx) => {
        const s = normalizeScope(scope),
          id = normalizeRotationScopeId(s, scopeId),
          normalized = normalizeRotationStrategy(strategy),
          now = new Date().toISOString();
        const window =
          options?.stickyWindowMinutes !== undefined && Number.isFinite(options.stickyWindowMinutes)
            ? Math.max(1, Math.floor(options.stickyWindowMinutes))
            : null;
        await tx.query(
          "INSERT INTO omniroute.proxy_scope_rotation(scope,scope_id,strategy,sticky_window_minutes,updated_at) VALUES($1,$2,$3,COALESCE($4::double precision,30),$5) ON CONFLICT(scope,scope_id) DO UPDATE SET strategy=excluded.strategy,sticky_window_minutes=COALESCE($4::double precision,omniroute.proxy_scope_rotation.sticky_window_minutes),updated_at=excluded.updated_at",
          [s, id, normalized, window, now]
        );
        return normalized;
      }),
    delete: (id: string, options?: { force?: boolean }) =>
      committedMutation(database, async (tx) => {
        const usage = await whereUsed(tx, id);
        if (!options?.force && usage.count)
          throw Object.assign(
            new Error(
              "Proxy is still in use. Remove assignments or account references first, or use force=true"
            ),
            { status: 409, code: "proxy_in_use", details: usage }
          );
        if (options?.force && usage.count)
          await tx.query("DELETE FROM omniroute.proxy_assignments WHERE proxy_id=$1", [id]);
        return (
          (await tx.query("DELETE FROM omniroute.proxy_registry WHERE id=$1", [id])).rowCount! > 0
        );
      }),
    pool: (scope: string, scopeId?: string | null) =>
      database.transaction(async (tx) => {
        const s = normalizeScope(scope);
        const id = normalizeAssignmentScopeId(s, scopeId);
        return (
          await tx.query(
            `SELECT ${ASSIGNMENT_COLUMNS} FROM omniroute.proxy_assignments WHERE scope=$1 AND scope_id IS NOT DISTINCT FROM $2 ORDER BY position,created_at,id`,
            [s, id]
          )
        ).rows.map(mapAssignmentRow);
      }),
    strategy: (scope: string, scopeId?: string | null) =>
      database.transaction(async (tx) => {
        const s = normalizeScope(scope);
        return normalizeRotationStrategy(
          (
            await tx.query<{ strategy: string }>(
              "SELECT strategy FROM omniroute.proxy_scope_rotation WHERE scope=$1 AND scope_id=$2",
              [s, normalizeRotationScopeId(s, scopeId)]
            )
          ).rows[0]?.strategy
        );
      }),
    resolveScope,
    resolveScopeWithPreference: async (
      scope: string,
      scopeId?: string | null,
      preferredId?: string
    ) => {
      const preference: PreferredMember = { preferredId };
      const resolution = await resolveScope(scope, scopeId, preference);
      return { resolution, proxyId: preference.selectedId };
    },
    resolveConnection: async (connectionId: string) => {
      const notifications: Array<{ scope: string; candidates: JsonRecord[] }> = [];
      const selected = await database.transaction(async (tx) => {
        await lockProxyState(tx, true);
        const account = await resolve(tx, "account", connectionId, false, notifications);
        if (account) return account;
        const connection = (
          await tx.query<{ provider: string }>(
            "SELECT provider FROM omniroute.provider_connections WHERE id=$1",
            [connectionId]
          )
        ).rows[0];
        if (connection?.provider) {
          const provider = await resolve(tx, "provider", connection.provider, false, notifications);
          if (provider) return provider;
        }
        return resolve(tx, "global", null, true, notifications);
      });
      for (const notification of notifications)
        maybeEmitPoolExhausted(
          notification.scope,
          notification.candidates,
          (row) => proxyEgressKey(row),
          (key) => isProxyAvoided(key)
        );
      return selected;
    },
    egressRows: (scope: string, scopeId: string | null) =>
      database.transaction(
        async (tx) =>
          (
            await tx.query<JsonRecord>(
              `SELECT p.id,p.name,p.type,p.host,p.port,p.username,p.password,p.notes,p.family,a.position AS __pos,a.id AS __aid FROM omniroute.proxy_assignments a JOIN omniroute.proxy_registry p ON p.id=a.proxy_id WHERE a.scope=$1 ${scopeId === null ? "" : "AND a.scope_id IS NOT DISTINCT FROM $2"} AND ${PROXY_ALIVE_PREDICATE} ORDER BY a.position,a.id`,
              scopeId === null ? [scope] : [scope, scopeId]
            )
          ).rows
      ),
    blockingConnection: (connectionId: string, providerId?: string, comboName?: string | null) =>
      database.transaction(async (tx) => {
        const setting = (
          await tx.query<{ value: string }>(
            "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key='proxyEnabled'"
          )
        ).rows[0];
        try {
          if (setting && JSON.parse(setting.value) === false) return false;
        } catch {}
        const conn = (
          await tx.query<{ provider: string; proxy_enabled: number }>(
            "SELECT provider,proxy_enabled FROM omniroute.provider_connections WHERE id=$1",
            [connectionId]
          )
        ).rows[0];
        if (conn?.proxy_enabled === 0) return false;
        return (
          (
            await tx.query(
              `SELECT 1 FROM omniroute.proxy_assignments a JOIN omniroute.proxy_registry p ON p.id=a.proxy_id WHERE ((a.scope='account' AND a.scope_id=$1) OR (a.scope='provider' AND a.scope_id=$2) OR a.scope='global' OR (a.scope='combo' AND a.scope_id=$3)) AND NOT ${PROXY_ALIVE_PREDICATE} LIMIT 1`,
              [connectionId, conn?.provider ?? providerId ?? null, comboName ?? null]
            )
          ).rows.length > 0
        );
      }),
    blockingProvider: (provider: string) =>
      database.transaction(async (tx) => {
        const setting = (
          await tx.query<{ value: string }>(
            "SELECT value FROM omniroute.key_value WHERE namespace='settings' AND key='proxyEnabled'"
          )
        ).rows[0];
        try {
          if (setting && JSON.parse(setting.value) === false) return false;
        } catch {}
        const row = (
          await tx.query<{ assigned: boolean; alive: boolean }>(
            `SELECT EXISTS(SELECT 1 FROM omniroute.proxy_assignments a WHERE (a.scope='provider' AND a.scope_id=$1) OR a.scope='global') AS assigned,EXISTS(SELECT 1 FROM omniroute.proxy_assignments a JOIN omniroute.proxy_registry p ON p.id=a.proxy_id WHERE ((a.scope='provider' AND a.scope_id=$1) OR a.scope='global') AND ${PROXY_ALIVE_PREDICATE}) AS alive`,
            [provider]
          )
        ).rows[0];
        return row.assigned && !row.alive;
      }),
  };
}
