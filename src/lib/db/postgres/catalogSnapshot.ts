import { AsyncLocalStorage } from "node:async_hooks";
import {
  getSelectedPostgresDatabase,
  getSelectedPersistenceIdentity,
} from "../controlPlaneContext";
import { invalidateModelCatalogCache } from "../readCache";
import type { PostgresDatabase, PostgresTransaction, PostgresResult } from "./database";
import { advanceApiKeyPolicyEpoch, lockApiKeyPolicy } from "./apiKeyPolicyState";

export type CatalogRow = Record<string, unknown>;
export interface CatalogValueRow {
  namespace: string;
  key: string;
  value: string;
}
export const CATALOG_TABLES = [
  "model_capabilities",
  "model_capability_overrides",
  "model_context_overrides",
  "prompt_templates",
  "discovery_results",
  "tier_config",
  "auto_candidate_overrides",
  "provider_connections",
  "provider_nodes",
  "upstream_proxy_config",
  "plugins",
  "exclusive_connection_leases",
  "reasoning_routing_rules",
  "api_keys",
  "combos",
  "model_intelligence",
] as const;
export type CatalogTable = (typeof CATALOG_TABLES)[number];
const CATALOG_NAMESPACES = [
  "customModels",
  "syncedAvailableModels",
  "modelCompatOverrides",
  "modelAliases",
  "managedModelAliasNames",
  "providerAliases",
  "mitmAlias",
  "serviceModels",
  "provider_param_filters",
  "interception_rules",
  "ccDiscoveryAliases",
  "functionalGatewayMirrors",
  "models_dev_pricing",
  "pricing",
  "litellm_pricing",
  "pricing_synced",
  "feature_flags",
  "settings",
  "databaseSettings",
  "proxyConfig",
  "pricing_sync_status",
  "arena_elo",
  "ipFilter",
] as const;

interface SnapshotRows {
  values: CatalogValueRow[];
  tables: Record<CatalogTable, CatalogRow[]>;
  generation: string;
  authority: string;
}
interface CatalogScope {
  readonly database: PostgresDatabase;
  readonly identity: object;
  readonly environment: Readonly<Record<string, string | undefined>>;
  rows: SnapshotRows;
  valid: boolean;
}
interface MutationScope {
  readonly database: PostgresDatabase;
  readonly identity: object;
  readonly transaction: PostgresTransaction;
  readonly afterCommit: Set<() => void | Promise<void>>;
  active: boolean;
  changed: boolean;
  validation: boolean;
  providers: boolean;
  routing: boolean;
}
// Shared process identities preserve request authority across separately bundled Next modules.
const stateKey = Symbol.for("omniroute.postgres.catalog-scopes.v1");
const processState = globalThis as typeof globalThis & {
  [stateKey]?: {
    snapshots: AsyncLocalStorage<CatalogScope>;
    mutations: AsyncLocalStorage<MutationScope>;
  };
};
const shared = (processState[stateKey] ??= {
  snapshots: new AsyncLocalStorage<CatalogScope>(),
  mutations: new AsyncLocalStorage<MutationScope>(),
});
const { snapshots, mutations } = shared;

// Every subquery runs under ONE PostgreSQL statement snapshot. Identifiers come from this
// closed domain list, never caller input. This is not a driver or SQL-dialect adapter.
const SNAPSHOT_SQL = `SELECT
  COALESCE((SELECT json_agg(k ORDER BY namespace COLLATE "C",key COLLATE "C")
    FROM (SELECT namespace,key,value FROM omniroute.key_value WHERE namespace=ANY($1::text[])) k),'[]') AS values,
  json_build_object(${CATALOG_TABLES.map((table) => `'${table}',COALESCE((SELECT json_agg(t) FROM ${table === "api_keys" || table === "combos" ? `(SELECT id FROM omniroute.${table})` : `omniroute.${table}`} t${table === "exclusive_connection_leases" ? " WHERE state='ACTIVE'" : ""}),'[]')`).join(",")}) AS tables,
  (SELECT generation::text FROM omniroute.api_key_policy_epoch WHERE singleton) AS generation,
  (SELECT authority_id FROM omniroute.api_key_policy_epoch WHERE singleton) AS authority`;

async function load(transaction: PostgresTransaction): Promise<SnapshotRows> {
  const result = await transaction.query<SnapshotRows>(SNAPSHOT_SQL, [[...CATALOG_NAMESPACES]]);
  if (!result.rows[0]?.generation) throw new Error("Catalog policy generation is unavailable");
  return result.rows[0];
}

function identity(database: PostgresDatabase): object {
  return getSelectedPersistenceIdentity() ?? database;
}

function scope(database: PostgresDatabase): CatalogScope {
  const current = snapshots.getStore();
  if (!current || !current.valid || current.identity !== identity(database))
    throw new Error("Selected PostgreSQL catalog snapshot was not prepared");
  return current;
}

/** Called by the selected app/request/job boundary before any synchronous pure getter. */
export async function withPostgresCatalogSnapshot<T>(
  database: PostgresDatabase,
  body: () => T | Promise<T>
): Promise<T> {
  const rows = await database.transaction(load);
  return snapshots.run(
    {
      database,
      identity: identity(database),
      environment: Object.freeze({ ...process.env }),
      rows,
      valid: true,
    },
    body
  );
}

export function hasPostgresCatalogSnapshot(database: PostgresDatabase): boolean {
  const current = snapshots.getStore();
  return Boolean(current?.valid && current.identity === identity(database));
}

export function requirePostgresCatalogSnapshot(): void {
  const database = getSelectedPostgresDatabase();
  if (database) scope(database);
}

export function readPostgresCatalogNamespace(
  namespace: string
): readonly { key: string; value: string }[] | undefined {
  return getSelectedPostgresDatabase() ? catalogNamespaceRows(namespace) : undefined;
}

export function catalogNamespaceRows(namespace: string): CatalogValueRow[] {
  const database = getSelectedPostgresDatabase();
  if (!database) throw new Error("PostgreSQL catalog owner is not selected");
  return scope(database)
    .rows.values.filter((row) => row.namespace === namespace)
    .map((row) => ({ ...row }));
}

export function catalogValue(namespace: string, key: string): { value: string } | undefined {
  const row = catalogNamespaceRows(namespace).find((item) => item.key === key);
  return row ? { value: row.value } : undefined;
}

export function catalogTableRows<Row = CatalogRow>(table: CatalogTable): Row[] {
  const database = getSelectedPostgresDatabase();
  if (!database) throw new Error("PostgreSQL catalog owner is not selected");
  // Callers get private row copies and cannot mutate the prepared authority.
  return structuredClone(scope(database).rows.tables[table]) as unknown as Row[];
}

/** Internal opaque read token; validates the current selected/prepared authority first. */
export function catalogSnapshotReadIdentity(): object {
  const database = getSelectedPostgresDatabase();
  if (!database) throw new Error("PostgreSQL catalog owner is not selected");
  return scope(database).rows;
}

export function catalogGeneration(): string {
  const database = getSelectedPostgresDatabase();
  if (!database) throw new Error("PostgreSQL catalog owner is not selected");
  return scope(database).rows.generation;
}

export function compareCatalogText(left: unknown, right: unknown): number {
  return Buffer.compare(Buffer.from(String(left ?? "")), Buffer.from(String(right ?? "")));
}

/** Source LIKE domain syntax: ASCII case-folded %/_ matching, no SQL or eval execution. */
export function catalogKeyMatches(key: string, pattern: string): boolean {
  const fold = (value: string) => value.replace(/[A-Z]/g, (char) => char.toLowerCase());
  const value = [...fold(key)],
    rule = [...fold(pattern)];
  let at = 0,
    patternAt = 0,
    star = -1,
    retryAt = 0;
  while (at < value.length) {
    if (rule[patternAt] === "_" || rule[patternAt] === value[at]) {
      at++;
      patternAt++;
    } else if (rule[patternAt] === "%") {
      star = patternAt++;
      retryAt = at;
    } else if (star !== -1) {
      patternAt = star + 1;
      at = ++retryAt;
    } else return false;
  }
  while (rule[patternAt] === "%") patternAt++;
  return patternAt === rule.length;
}

export function isPostgresCatalogMutation(database: PostgresDatabase): boolean {
  const current = mutations.getStore();
  return Boolean(current?.active && current.identity === identity(database));
}

/** Catalog (18), shared KV (20), then policy (5). Nested domain writes reuse this scope. */
export async function mutatePostgresCatalog<T>(
  database: PostgresDatabase,
  body: () => T | Promise<T>,
  options: {
    validation?: boolean;
    providers?: boolean;
    routing?: boolean;
  } = {}
): Promise<T> {
  if (isPostgresCatalogMutation(database)) {
    if (
      (options.validation && !mutations.getStore()?.validation) ||
      (options.providers && !mutations.getStore()?.providers) ||
      (options.routing && !mutations.getStore()?.routing)
    )
      throw new Error("Validation requires provider and lease locks before catalog locks");
    return body();
  }
  const parent = snapshots.getStore();
  const receipt = await database.transaction(async (transaction) => {
    if (options.routing) await transaction.query("SELECT pg_advisory_xact_lock(18765,2)");
    if (options.validation || options.providers)
      await transaction.query("SELECT pg_advisory_xact_lock(18765,4)");
    if (options.validation) await transaction.query("SELECT pg_advisory_xact_lock(18765,7)");
    await transaction.query("SELECT pg_advisory_xact_lock(18765,18)");
    await transaction.query("SELECT pg_advisory_xact_lock(18765,20)");
    await lockApiKeyPolicy(transaction);
    const current: MutationScope = {
      database,
      identity: identity(database),
      transaction,
      active: true,
      changed: false,
      validation: options.validation === true,
      providers: options.providers === true || options.validation === true,
      routing: options.routing === true,
      afterCommit: new Set(),
    };
    const view: CatalogScope = {
      database,
      identity: identity(database),
      environment:
        parent?.identity === identity(database)
          ? parent.environment
          : Object.freeze({ ...process.env }),
      rows: await load(transaction),
      valid: true,
    };
    try {
      const value = await snapshots.run(view, () => mutations.run(current, body));
      if (current.changed) {
        view.rows.generation = await advanceApiKeyPolicyEpoch(transaction);
      }
      return {
        value,
        rows: view.rows,
        changed: current.changed,
        effects: current.afterCommit,
      };
    } finally {
      current.active = false;
    }
  });
  // No refreshed data or external effect is published before acknowledged COMMIT.
  if (parent?.identity === identity(database)) {
    if (parent.rows.authority !== receipt.rows.authority) parent.valid = false;
    else if (BigInt(receipt.rows.generation) >= BigInt(parent.rows.generation))
      parent.rows = receipt.rows;
  }
  const failed: string[] = [];
  const attempt = async (name: string, effect: () => void | Promise<void>) => {
    try {
      await effect();
    } catch {
      failed.push(name);
    }
  };
  if (receipt.changed) await attempt("invalidate-model-catalog", invalidateModelCatalogCache);
  for (const effect of receipt.effects) await attempt(effect.name || "catalog-effect", effect);
  if (failed.length) throw new PostgresCatalogEffectsError(failed);
  return receipt.value;
}

/** Internal native statement primitive for this catalog domain; no SQLite query interface. */
export async function writePostgresCatalog<Row = CatalogRow>(
  text: string,
  values: unknown[] = []
): Promise<PostgresResult<Row>> {
  const database = getSelectedPostgresDatabase();
  const current = mutations.getStore();
  if (!database || !current?.active || current.identity !== identity(database))
    throw new Error("Catalog write requires its native mutation scope");
  const result = await current.transaction.query<Row>(text, values);
  current.changed ||= (result.rowCount ?? 0) > 0;
  // Read-own-writes for pure getter calls later in this same domain operation.
  scope(database).rows = await load(current.transaction);
  return result;
}

export function afterPostgresCatalogCommit(effect: () => void | Promise<void>): void {
  const current = mutations.getStore();
  if (!current?.active) throw new Error("Catalog effect requires its native mutation scope");
  current.afterCommit.add(effect);
}

/** Catalog-owned KV writes use PostgreSQL UPSERT, never SQLite replacement semantics. */
export async function putCatalogValue(
  namespace: string,
  key: string,
  value: string
): Promise<{ changes: number }> {
  const result = await writePostgresCatalog(
    "INSERT INTO omniroute.key_value(namespace,key,value) VALUES($1,$2,$3) " +
      "ON CONFLICT(namespace,key) DO UPDATE SET value=EXCLUDED.value",
    [namespace, key, value]
  );
  return { changes: result.rowCount ?? 0 };
}

export async function deleteCatalogValue(
  namespace: string,
  key: string
): Promise<{ changes: number }> {
  const result = await writePostgresCatalog(
    "DELETE FROM omniroute.key_value WHERE namespace=$1 AND key=$2",
    [namespace, key]
  );
  return { changes: result.rowCount ?? 0 };
}

export async function updateCatalogValue(
  namespace: string,
  key: string,
  value: string
): Promise<{ changes: number }> {
  const result = await writePostgresCatalog(
    "UPDATE omniroute.key_value SET value=$3 WHERE namespace=$1 AND key=$2",
    [namespace, key, value]
  );
  return { changes: result.rowCount ?? 0 };
}

/** Preserve source LIKE matching exactly; do not reinterpret unescaped provider IDs. */
export async function deleteCatalogMatchingKeys(
  namespace: string,
  predicate: (key: string) => boolean
): Promise<{ changes: number }> {
  const keys = catalogNamespaceRows(namespace)
    .filter((row) => predicate(row.key))
    .map((row) => row.key);
  if (!keys.length) return { changes: 0 };
  const result = await writePostgresCatalog(
    "DELETE FROM omniroute.key_value WHERE namespace=$1 AND key=ANY($2::text[])",
    [namespace, keys]
  );
  return { changes: result.rowCount ?? 0 };
}

export class PostgresCatalogEffectsError extends Error {
  readonly committed = true;
  constructor(readonly failedEffects: readonly string[]) {
    super("Catalog mutation committed but application effects failed");
    this.name = "PostgresCatalogEffectsError";
  }
}

export interface PostgresCatalogVersion {
  readonly identity: object;
  readonly authority: string;
  readonly generation: string;
}

/** Bind derived values before yielding; acknowledged writes may replace the scope's rows. */
export function capturePostgresCatalogVersion(): PostgresCatalogVersion {
  const database = getSelectedPostgresDatabase();
  if (!database) throw new Error("PostgreSQL catalog owner is not selected");
  const current = scope(database);
  return Object.freeze({
    identity: current.identity,
    authority: current.rows.authority,
    generation: current.rows.generation,
  });
}

/** A long catalog build may yield; never return its stale view after policy publication. */
export async function assertPostgresCatalogSnapshotCurrent(
  expected?: PostgresCatalogVersion
): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return;
  const current = scope(database);
  const captured = expected ?? capturePostgresCatalogVersion();
  if (captured.identity !== current.identity || captured.authority !== current.rows.authority)
    throw new Error("Catalog authority changed during request preparation");
  const version = await database.transaction(async (transaction) => {
    const result = await transaction.query<{
      authority_id: string;
      generation: string;
    }>("SELECT authority_id,generation FROM omniroute.api_key_policy_epoch WHERE singleton");
    return result.rows[0];
  });
  if (
    !version ||
    version.authority_id !== captured.authority ||
    version.generation !== captured.generation
  )
    throw new Error("Catalog policy changed during request preparation");
}

/** Pure leaves resolve environment flags from the same request authority as catalog rows. */
export function catalogEnvironmentValue(name: string): string | undefined {
  const database = getSelectedPostgresDatabase();
  return database ? scope(database).environment[name] : process.env[name];
}

/** Native DB-domain read in the current transaction; no second pool lease or SQL adapter. */
export async function readPostgresCatalog<Row = CatalogRow>(
  text: string,
  values: unknown[] = []
): Promise<PostgresResult<Row>> {
  const database = getSelectedPostgresDatabase();
  const current = mutations.getStore();
  if (!database || !current?.active || current.identity !== identity(database))
    throw new Error("Catalog read requires its native mutation scope");
  return current.transaction.query<Row>(text, values);
}

/** Refresh after another native facade commits, without republishing an older receipt. */
export async function refreshPostgresCatalogSnapshot(database: PostgresDatabase): Promise<void> {
  const current = scope(database);
  try {
    const mutation = mutations.getStore();
    const rows =
      mutation?.active && mutation.identity === identity(database)
        ? await load(mutation.transaction)
        : await database.transaction(load);
    if (rows.authority !== current.rows.authority)
      throw new Error("Catalog authority changed during refresh");
    if (BigInt(rows.generation) >= BigInt(current.rows.generation)) current.rows = rows;
  } catch (error) {
    current.valid = false;
    throw error;
  }
}

/** Acknowledged facade writes must refresh their request view without disguising commit failure. */
export async function refreshSelectedPostgresCatalogAfterCommit(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database || !hasPostgresCatalogSnapshot(database)) return;
  try {
    await refreshPostgresCatalogSnapshot(database);
  } catch {
    throw new PostgresCatalogEffectsError(["catalog_snapshot"]);
  }
}
