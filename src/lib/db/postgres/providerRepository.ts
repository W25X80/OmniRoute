import { v4 as uuidv4 } from "uuid";
import type {
  ProviderConnectionRepository,
  ProviderConnectionUpdateOptions,
  ProviderRecord,
} from "@/domain/persistence/providerRepositories";
import { rowToCamel, toSnakeCase } from "../caseMapping";
import { decryptConnectionFields } from "../encryption";
import { isConnectionRuntimeStateUpdate } from "../readCache";
import { toRecord, toStringOrNull } from "../providers/columns";
import { createLazyRowProxy } from "../providers/lazyConnectionView";
import {
  buildInsertConnectionRowParams,
  buildNewProviderConnection,
  buildUpdateConnectionRowParams,
  normalizeConnectionProviderSpecificData,
} from "../providers/persistenceSemantics";
import { parseProviderSpecificData } from "../webSessionDedup";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { advanceApiKeyPolicyEpoch, lockApiKeyPolicy } from "./apiKeyPolicyState";
import {
  connectionPersistence,
  connectionRequiresPolicyReadback,
  findExistingConnection,
  lockProviders,
  mergeConnectionPatch,
  normalizeConnectionResult,
  pageSql,
  parseConnectionRow,
  PROVIDER_CONNECTION_ORDER,
  PROVIDER_CONNECTION_PROJECTION_COLUMNS,
} from "./providerConnectionSemantics";

export interface ProviderDeletionInput {
  mode: "single" | "ids" | "provider";
  requestedIds: string[];
  existingRows: ProviderRecord[];
  provider?: string;
}

export interface ProviderConnectionChange {
  kind: "create" | "upsert" | "update" | "delete" | "merge" | "reset";
  id?: string;
  patch?: ProviderRecord;
  options?: ProviderConnectionUpdateOptions;
  deletion?: ProviderDeletionInput;
}

/** Required callbacks bind adjacent durable state and application caches to this backend. */
export interface PostgresProviderEffects {
  assertApiKeyIsNotManagementPassword(apiKey: unknown): Promise<void>;
  reconcileCodexUsageHistory(
    transaction: PostgresTransaction,
    input: {
      connectionId: string;
      existing: ProviderRecord;
      merged: ProviderRecord;
      matchedExistingCodexByWorkspace?: boolean;
    }
  ): Promise<void>;
  deleteConnectionState(
    transaction: PostgresTransaction,
    input: ProviderDeletionInput
  ): Promise<void>;
  afterConnectionWrite(change: ProviderConnectionChange): void | Promise<void>;
  afterNodeWrite(change: {
    kind: "create" | "update" | "delete";
    id: string;
  }): void | Promise<void>;
}

export class PostgresProviderEffectsError extends Error {
  readonly committed = true;

  constructor(readonly operation: string) {
    super("Provider write committed but application effects failed");
    this.name = "PostgresProviderEffectsError";
  }
}

export async function providerEffectsAfterCommit(
  operation: string,
  effect: () => void | Promise<void>
): Promise<void> {
  try {
    await effect();
  } catch {
    throw new PostgresProviderEffectsError(operation);
  }
}

function connectionPredicate(filter: ProviderRecord, values: unknown[]): string {
  const predicates: string[] = [];
  for (const [key, column] of [
    ["provider", "provider"],
    ["authType", "auth_type"],
  ] as const) {
    if (!filter[key]) continue;
    values.push(filter[key]);
    predicates.push(`${column} = $${values.length}`);
  }
  if (filter.isActive !== undefined) {
    values.push(filter.isActive ? 1 : 0);
    predicates.push(`is_active = $${values.length}`);
  }
  return predicates.length ? ` WHERE ${predicates.join(" AND ")}` : "";
}

async function readConnection(transaction: PostgresTransaction, id: string, locked = false) {
  const { rows } = await transaction.query<ProviderRecord>(
    `SELECT * FROM omniroute.provider_connections WHERE id=$1${locked ? " FOR UPDATE" : ""}`,
    [id]
  );
  return rows[0] ?? null;
}

async function writeConnection(
  transaction: PostgresTransaction,
  data: ProviderRecord,
  insert: boolean,
  id: string
): Promise<void> {
  const params: ProviderRecord = insert
    ? buildInsertConnectionRowParams(data)
    : buildUpdateConnectionRowParams(id, data, data.updatedAt);
  const keys = Object.keys(params);
  const columns = keys.map((key) => `"${toSnakeCase(key)}"`);
  const values = keys.map((key) =>
    typeof params[key] === "number" && Number.isNaN(params[key]) ? null : params[key]
  );
  if (insert) {
    await transaction.query(
      `INSERT INTO omniroute.provider_connections (${columns.join(", ")})
       VALUES (${keys.map((_, index) => `$${index + 1}`).join(", ")})`,
      values
    );
  } else {
    values.push(id);
    await transaction.query(
      `UPDATE omniroute.provider_connections SET ${columns
        .map((column, index) => `${column}=$${index + 1}`)
        .join(", ")} WHERE id=$${values.length}`,
      values
    );
  }
}

async function reorder(transaction: PostgresTransaction, provider: unknown): Promise<number> {
  const { rows } = await transaction.query<{ id: string }>(
    `SELECT id FROM omniroute.provider_connections WHERE provider=$1
     ORDER BY ${PROVIDER_CONNECTION_ORDER} FOR UPDATE`,
    [provider]
  );
  for (const [index, row] of rows.entries()) {
    await transaction.query("UPDATE omniroute.provider_connections SET priority=$1 WHERE id=$2", [
      index + 1,
      row.id,
    ]);
  }
  return rows.length;
}

export function createPostgresProviderConnectionRepository(
  database: PostgresDatabase,
  effects: PostgresProviderEffects
): ProviderConnectionRepository {
  async function listRaw(
    filter: ProviderRecord = {},
    limit?: number,
    offset?: number,
    columns?: string[]
  ) {
    const invalid =
      columns?.filter((column) => !PROVIDER_CONNECTION_PROJECTION_COLUMNS.has(column)) ?? [];
    if (invalid.length)
      throw new Error(`getProviderConnections: invalid column(s) requested: ${invalid.join(", ")}`);
    const projection = columns?.length ? columns.map((column) => `"${column}"`).join(", ") : "*";
    const values: unknown[] = [];
    const predicate = connectionPredicate(filter, values);
    const page = pageSql(limit, offset, values);
    return database.transaction(async (transaction) => {
      const result = await transaction.query<ProviderRecord>(
        `SELECT ${projection} FROM omniroute.provider_connections${predicate}
         ORDER BY ${PROVIDER_CONNECTION_ORDER}${page}`,
        values
      );
      return result.rows.map(parseConnectionRow);
    });
  }

  async function after(change: ProviderConnectionChange): Promise<void> {
    await providerEffectsAfterCommit(change.kind, () => effects.afterConnectionWrite(change));
  }

  async function remove(input: Omit<ProviderDeletionInput, "existingRows">): Promise<number> {
    if (input.mode === "ids" && input.requestedIds.length === 0) return 0;
    const receipt = await database.transaction(async (transaction) => {
      await lockProviders(transaction);
      await lockApiKeyPolicy(transaction);
      const predicate = input.mode === "provider" ? "provider=$1" : "id=ANY($1::text[])";
      const values = [input.mode === "provider" ? input.provider : input.requestedIds];
      const { rows } = await transaction.query<ProviderRecord>(
        `SELECT * FROM omniroute.provider_connections WHERE ${predicate} ORDER BY insertion_order FOR UPDATE`,
        values
      );
      if (input.mode === "single" && rows.length === 0) return null;
      const deletion: ProviderDeletionInput = { ...input, existingRows: rows };
      if (input.mode === "provider") deletion.requestedIds = rows.map((row) => String(row.id));
      await transaction.query(
        "DELETE FROM omniroute.proxy_assignments WHERE scope='account' AND scope_id=ANY($1::text[])",
        [deletion.requestedIds]
      );
      await effects.deleteConnectionState(transaction, deletion);
      const result = await transaction.query(
        `DELETE FROM omniroute.provider_connections WHERE ${predicate}`,
        values
      );
      if (input.mode === "single") await reorder(transaction, rows[0].provider);
      const count = result.rowCount ?? 0;
      if (count > 0) await advanceApiKeyPolicyEpoch(transaction);
      return { deletion, count };
    });
    if (!receipt) return 0;
    await after({ kind: "delete", deletion: receipt.deletion });
    return receipt.count;
  }

  return {
    listRaw,
    list: async (filter, limit, offset, columns) =>
      (await listRaw(filter, limit, offset, columns)).map(createLazyRowProxy),
    count: (filter = {}) =>
      database.transaction(async (transaction) => {
        const values: unknown[] = [];
        const result = await transaction.query<{ count: string }>(
          `SELECT count(*) AS count FROM omniroute.provider_connections${connectionPredicate(filter, values)}`,
          values
        );
        return Number(result.rows[0].count);
      }),
    findById: (id) =>
      database.transaction(async (transaction) => {
        const row = await readConnection(transaction, id);
        return row ? decryptConnectionFields(parseConnectionRow(row)) : null;
      }),
    displayMetadata: (connectionIds) => {
      const ids = [...new Set(connectionIds.filter((id) => id.length > 0))];
      if (!ids.length) return Promise.resolve([]);
      return database.transaction(async (transaction) => {
        const { rows } = await transaction.query<ProviderRecord>(
          `SELECT id,name,display_name,email FROM omniroute.provider_connections
           WHERE id=ANY($1::text[]) ORDER BY id COLLATE "C"`,
          [ids]
        );
        return rows.map((row) => ({
          id: String(row.id),
          name: toStringOrNull(row.name),
          displayName: toStringOrNull(row.display_name),
          email: toStringOrNull(row.email),
        }));
      });
    },
    async create(data) {
      await effects.assertApiKeyIsNotManagementPassword(data.apiKey);
      const receipt = await database.transaction(async (transaction) => {
        await lockProviders(transaction);
        await lockApiKeyPolicy(transaction);
        const now = new Date().toISOString();
        const normalized = toRecord(
          normalizeConnectionProviderSpecificData(
            toStringOrNull(data.provider),
            data.providerSpecificData,
            data
          )
        );
        const { rows } = await transaction.query<ProviderRecord>(
          "SELECT * FROM omniroute.provider_connections WHERE provider=$1 ORDER BY insertion_order FOR UPDATE",
          [data.provider]
        );
        const { existing, promoted } = findExistingConnection(rows, data, normalized);
        if (existing) {
          const id = toStringOrNull(existing.id);
          if (!id) return null;
          const raw = toRecord(rowToCamel(existing));
          delete raw.insertionOrder;
          const merged = mergeConnectionPatch(
            decryptConnectionFields({ ...raw }),
            data,
            now,
            false
          );
          if (promoted)
            await effects.reconcileCodexUsageHistory(transaction, {
              connectionId: id,
              existing,
              merged,
              matchedExistingCodexByWorkspace: true,
            });
          await writeConnection(transaction, connectionPersistence(merged, data, raw), false, id);
          await advanceApiKeyPolicyEpoch(transaction);
          const policyRow = connectionRequiresPolicyReadback(merged.provider)
            ? await readConnection(transaction, id)
            : null;
          return {
            connection: policyRow
              ? decryptConnectionFields(parseConnectionRow(policyRow))
              : normalizeConnectionResult(merged),
            kind: "upsert" as const,
            id,
          };
        }
        const max = await transaction.query<{ max: number | null }>(
          "SELECT MAX(priority) AS max FROM omniroute.provider_connections WHERE provider=$1",
          [data.provider]
        );
        const id = uuidv4();
        const connection = buildNewProviderConnection(
          data,
          normalized,
          now,
          id,
          max.rows[0].max ?? 0
        );
        await writeConnection(transaction, connectionPersistence(connection, data), true, id);
        await reorder(transaction, data.provider);
        await advanceApiKeyPolicyEpoch(transaction);
        const policyRow = connectionRequiresPolicyReadback(data.provider)
          ? await readConnection(transaction, id)
          : null;
        return {
          connection: policyRow
            ? decryptConnectionFields(parseConnectionRow(policyRow))
            : normalizeConnectionResult(connection),
          kind: "create" as const,
          id,
        };
      });
      if (!receipt) return null;
      await after({ kind: receipt.kind, id: receipt.id, patch: data });
      return receipt.connection;
    },
    async update(id, data, options) {
      const candidate = await database.transaction((transaction) =>
        readConnection(transaction, id)
      );
      if (!candidate) return null;
      // The guard is outside the transaction; row merge happens only after it completes.
      await effects.assertApiKeyIsNotManagementPassword(data.apiKey);
      // Match the source catalog invalidation predicate, including its explicit caller voucher.
      const publishesCatalog = !options?.skipModelCatalog && !isConnectionRuntimeStateUpdate(data);
      const connection = await database.transaction(async (transaction) => {
        await lockProviders(transaction);
        if (publishesCatalog) await lockApiKeyPolicy(transaction);
        const existing = await readConnection(transaction, id, true);
        if (!existing) return null;
        const raw = toRecord(rowToCamel(existing));
        delete raw.insertionOrder;
        const merged = mergeConnectionPatch(raw, data, new Date().toISOString(), true);
        await effects.reconcileCodexUsageHistory(transaction, {
          connectionId: id,
          existing,
          merged,
        });
        await writeConnection(transaction, connectionPersistence(merged, data, raw), false, id);
        if (publishesCatalog) await advanceApiKeyPolicyEpoch(transaction);
        if (data.priority === 0) {
          await reorder(transaction, existing.provider);
          const row = await readConnection(transaction, id);
          return row ? decryptConnectionFields(parseConnectionRow(row)) : null;
        }
        if (connectionRequiresPolicyReadback(merged.provider)) {
          const row = await readConnection(transaction, id);
          if (row) return decryptConnectionFields(parseConnectionRow(row));
        }
        return normalizeConnectionResult(merged);
      });
      if (connection) await after({ kind: "update", id, patch: data, options });
      return connection;
    },
    reorder: (provider) =>
      database.transaction(async (transaction) => {
        await lockProviders(transaction);
        await lockApiKeyPolicy(transaction);
        const count = await reorder(transaction, provider);
        if (count > 0) await advanceApiKeyPolicyEpoch(transaction);
      }),
    deleteById: async (id) => (await remove({ mode: "single", requestedIds: [id] })) > 0,
    deleteByIds: (ids) => remove({ mode: "ids", requestedIds: ids }),
    deleteByProvider: (provider) => remove({ mode: "provider", requestedIds: [], provider }),
    async mergeProviderSpecificData(id, patch) {
      const wrote = await database.transaction(async (transaction) => {
        await lockProviders(transaction);
        const existing = await readConnection(transaction, id, true);
        if (!existing) return false;
        const merged = { ...parseProviderSpecificData(existing.provider_specific_data), ...patch };
        await transaction.query(
          "UPDATE omniroute.provider_connections SET provider_specific_data=$1,updated_at=$2 WHERE id=$3",
          [JSON.stringify(merged), new Date().toISOString(), id]
        );
        return true;
      });
      if (wrote) await after({ kind: "merge", id, patch, options: { skipModelCatalog: true } });
    },
    async touchLastUsed(id, consecutiveUseCount) {
      if (!id) return;
      await database.transaction(async (transaction) => {
        await lockProviders(transaction);
        const now = new Date().toISOString();
        await transaction.query(
          "UPDATE omniroute.provider_connections SET last_used_at=$1,consecutive_use_count=$2,updated_at=$1 WHERE id=$3",
          [now, Number.isNaN(consecutiveUseCount) ? null : consecutiveUseCount, id]
        );
      });
    },
    async touchSyncedModelsAt(id) {
      if (!id) return;
      await database.transaction(async (transaction) => {
        await lockProviders(transaction);
        await lockApiKeyPolicy(transaction);
        const result = await transaction.query(
          "UPDATE omniroute.provider_connections SET synced_models_at=$1,updated_at=$1 WHERE id=$2",
          [new Date().toISOString(), id]
        );
        if ((result.rowCount ?? 0) > 0) await advanceApiKeyPolicyEpoch(transaction);
      });
    },
    async resetBackoff(id) {
      if (!id) return;
      await database.transaction(async (transaction) => {
        await lockProviders(transaction);
        await transaction.query(
          `UPDATE omniroute.provider_connections SET backoff_level=0,
          test_status='active',last_error=NULL,last_error_at=NULL,last_error_type=NULL,
          last_error_source=NULL,error_code=NULL,updated_at=$1 WHERE id=$2`,
          [new Date().toISOString(), id]
        );
      });
      await after({ kind: "reset", id, options: { skipModelCatalog: true } });
    },
    distinctGroups: () =>
      database.transaction(async (transaction) => {
        const { rows } = await transaction.query<{ group: string }>(
          'SELECT DISTINCT "group" COLLATE "C" AS "group" FROM omniroute.provider_connections WHERE "group" IS NOT NULL ORDER BY "group"'
        );
        return rows.map((row) => String(row.group)).filter(Boolean);
      }),
  };
}
