import { v4 as uuidv4 } from "uuid";
import type {
  ProviderNodeRepository,
  ProviderRecord,
} from "@/domain/persistence/providerRepositories";
import { rowToCamel, toSnakeCase } from "../caseMapping";
import { selectProviderNodeForConnection } from "../providerNodeSelect";
import { toRecord } from "../providers/columns";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { lockProviders, pageSql } from "./providerConnectionSemantics";
import { providerEffectsAfterCommit, type PostgresProviderEffects } from "./providerRepository";

function quotaHour(value: unknown): number | null {
  const parsed = value === 0 || value ? Number(value) : null;
  return typeof parsed === "number" && Number.isNaN(parsed) ? null : parsed;
}

function nodeParams(record: ProviderRecord, id: unknown): ProviderRecord {
  return {
    id,
    type: record.type,
    name: record.name,
    prefix: record.prefix || null,
    apiType: record.apiType || null,
    baseUrl: record.baseUrl || null,
    chatPath: record.chatPath || null,
    modelsPath: record.modelsPath || null,
    iconUrl: record.iconUrl || null,
    customHeadersJson: record.customHeadersJson || null,
    dailyQuotaResetTimezone: record.dailyQuotaResetTimezone || null,
    dailyQuotaResetHour: quotaHour(record.dailyQuotaResetHour),
    updatedAt: record.updatedAt,
  };
}

function parsedHeaders(record: ProviderRecord, storedJson: unknown): ProviderRecord {
  const result: ProviderRecord = { ...record, customHeaders: null };
  if (typeof storedJson === "string" && storedJson) {
    try {
      result.customHeaders = JSON.parse(storedJson);
    } catch {
      result.customHeaders = null;
    }
  }
  delete result.customHeadersJson;
  return result;
}

async function persist(transaction: PostgresTransaction, params: ProviderRecord, insert: boolean) {
  const keys = Object.keys(params);
  const columns = keys.map((key) => `"${toSnakeCase(key)}"`);
  const values = keys.map((key) => params[key]);
  if (insert) {
    await transaction.query(
      `INSERT INTO omniroute.provider_nodes (${columns.join(",")})
       VALUES(${keys.map((_, index) => `$${index + 1}`).join(",")})`,
      values
    );
  } else {
    values.push(params.id);
    await transaction.query(
      `UPDATE omniroute.provider_nodes SET ${columns
        .map((column, index) => `${column}=$${index + 1}`)
        .join(",")} WHERE id=$${values.length}`,
      values
    );
  }
}

export function createPostgresProviderNodeRepository(
  database: PostgresDatabase,
  effects: Pick<PostgresProviderEffects, "afterNodeWrite">,
  policy?: { markPolicyChanged(transaction: PostgresTransaction): Promise<void> }
): ProviderNodeRepository {
  async function after(kind: "create" | "update" | "delete", id: string): Promise<void> {
    await providerEffectsAfterCommit(`node_${kind}`, () => effects.afterNodeWrite({ kind, id }));
  }

  async function list(filter: ProviderRecord = {}, limit?: number, offset?: number) {
    const values: unknown[] = filter.type ? [filter.type] : [];
    const predicate = filter.type ? " WHERE type=$1" : "";
    const page = pageSql(limit, offset, values);
    return database.transaction(async (transaction) => {
      const { rows } = await transaction.query(
        `SELECT * FROM omniroute.provider_nodes${predicate} ORDER BY id ASC${page}`,
        values
      );
      return rows.map((row) => toRecord(rowToCamel(row)));
    });
  }

  return {
    list,
    count: (filter = {}) =>
      database.transaction(async (transaction) => {
        const { rows } = await transaction.query<{ count: string }>(
          `SELECT count(*) AS count FROM omniroute.provider_nodes${filter.type ? " WHERE type=$1" : ""}`,
          filter.type ? [filter.type] : []
        );
        return Number(rows[0].count);
      }),
    findById: (id) =>
      database.transaction(async (transaction) => {
        const { rows } = await transaction.query(
          "SELECT * FROM omniroute.provider_nodes WHERE id=$1",
          [id]
        );
        return rows[0] ? rowToCamel(rows[0]) : null;
      }),
    resolveForConnection: async (idOrType) =>
      selectProviderNodeForConnection(idOrType, await list()),
    async create(data) {
      const node = await database.transaction(async (transaction) => {
        await lockProviders(transaction);
        const now = new Date().toISOString();
        const params: ProviderRecord = {
          ...nodeParams(
            {
              ...data,
              updatedAt: now,
              customHeadersJson: data.customHeaders ? JSON.stringify(data.customHeaders) : null,
            },
            data.id || uuidv4()
          ),
          createdAt: now,
        };
        await policy?.markPolicyChanged(transaction);
        await persist(transaction, params, true);
        return parsedHeaders(params, params.customHeadersJson);
      });
      await after("create", String(node.id));
      return node;
    },
    async update(id, data) {
      const node = await database.transaction(async (transaction) => {
        await lockProviders(transaction);
        const { rows } = await transaction.query<ProviderRecord>(
          "SELECT * FROM omniroute.provider_nodes WHERE id=$1 FOR UPDATE",
          [id]
        );
        if (!rows[0]) return null;
        const merged: ProviderRecord = {
          ...toRecord(rowToCamel(rows[0])),
          ...data,
          updatedAt: new Date().toISOString(),
          customHeadersJson:
            data.customHeaders !== undefined
              ? data.customHeaders
                ? JSON.stringify(data.customHeaders)
                : null
              : typeof rows[0].custom_headers_json === "string"
                ? rows[0].custom_headers_json
                : null,
        };
        await policy?.markPolicyChanged(transaction);
        await persist(transaction, nodeParams(merged, id), false);
        return parsedHeaders(merged, merged.customHeadersJson);
      });
      if (node) await after("update", id);
      return node;
    },
    async deleteById(id) {
      const node = await database.transaction(async (transaction) => {
        await lockProviders(transaction);
        const { rows } = await transaction.query(
          "SELECT * FROM omniroute.provider_nodes WHERE id=$1 FOR UPDATE",
          [id]
        );
        if (!rows[0]) return null;
        await policy?.markPolicyChanged(transaction);
        await transaction.query("DELETE FROM omniroute.provider_nodes WHERE id=$1", [id]);
        return rowToCamel(rows[0]);
      });
      if (node) await after("delete", id);
      return node;
    },
  };
}
