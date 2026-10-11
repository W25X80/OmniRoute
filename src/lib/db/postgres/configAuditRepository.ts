import type {
  ConfigAuditLogOptions,
  ConfigAuditRow,
  configAuditEntryParams,
} from "../configAuditModel";
import { isPostgresCatalogMutation, readPostgresCatalog } from "./catalogSnapshot";
import type { PostgresDatabase, PostgresResult } from "./database";

export const CONFIG_AUDIT_AFTER_COMMIT_MESSAGE =
  "Configuration audit writes must run after the catalog transaction commits";

function order(prefix: "" | "page." = ""): string {
  // Source-generated ISO timestamps sort at SQLite datetime's whole-second precision.
  return `CASE WHEN pg_input_is_valid(${prefix}timestamp,'timestamptz')
    THEN date_trunc('second',${prefix}timestamp::timestamptz) END DESC NULLS LAST,
    ${prefix}id COLLATE "C" DESC`;
}

function filters(options?: ConfigAuditLogOptions): { where: string; values: unknown[] } {
  const values: unknown[] = [],
    clauses: string[] = [];
  const fields = [
    ["target", options?.target],
    ["target_id", options?.targetId],
    ["action", options?.action],
    ["source", options?.source],
  ] as const;
  for (const [column, value] of fields) {
    if (value) {
      values.push(value);
      clauses.push(`${column} COLLATE "C"=$${values.length} COLLATE "C"`);
    }
  }
  if (options?.since) {
    values.push(options.since);
    clauses.push(`timestamp COLLATE "C">=$${values.length} COLLATE "C"`);
  }
  return { where: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "", values };
}

/** Configuration change journal; no settings mutation or rollback is performed here. */
export function createPostgresConfigAuditRepository(database: PostgresDatabase) {
  function query<Row>(text: string, values: unknown[] = []): Promise<PostgresResult<Row>> {
    return isPostgresCatalogMutation(database)
      ? readPostgresCatalog<Row>(text, values)
      : database.transaction((transaction) => transaction.query<Row>(text, values));
  }
  function assertStandaloneWrite(): void {
    if (isPostgresCatalogMutation(database)) throw new Error(CONFIG_AUDIT_AFTER_COMMIT_MESSAGE);
  }
  return {
    async insert(params: ReturnType<typeof configAuditEntryParams>): Promise<void> {
      assertStandaloneWrite();
      await query(
        `INSERT INTO omniroute.config_audit_log
         (id,timestamp,action,target,target_id,target_name,before_json,after_json,diff_json,source,note)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
        [
          params.id,
          params.timestamp,
          params.action,
          params.target,
          params.targetId,
          params.targetName,
          params.beforeJson,
          params.afterJson,
          params.diffJson,
          params.source,
          params.note,
        ]
      );
    },
    async list(
      options?: ConfigAuditLogOptions
    ): Promise<{ rows: ConfigAuditRow[]; total: number }> {
      const { where, values } = filters(options);
      const limit = options?.limit ?? 50,
        offset = options?.offset ?? 0;
      values.push(
        Number.isInteger(limit) && limit < 0 ? null : limit,
        Number.isInteger(offset) && offset < 0 ? 0 : offset
      );
      const result = await query<{ total: string; rows: ConfigAuditRow[] }>(
        `WITH filtered AS (SELECT * FROM omniroute.config_audit_log ${where}),
        page AS (SELECT * FROM filtered ORDER BY ${order()}
          LIMIT $${values.length - 1}::bigint OFFSET $${values.length}::bigint)
        SELECT (SELECT count(*)::text FROM filtered) AS total,
          COALESCE((SELECT json_agg(page ORDER BY ${order("page.")}) FROM page),'[]') AS rows`,
        values
      );
      return { rows: result.rows[0].rows, total: Number(result.rows[0].total) };
    },
    async find(id: string): Promise<ConfigAuditRow | null> {
      const result = await query<ConfigAuditRow>(
        'SELECT * FROM omniroute.config_audit_log WHERE id COLLATE "C"=$1 COLLATE "C"',
        [id]
      );
      return result.rows[0] ?? null;
    },
    async all(): Promise<ConfigAuditRow[]> {
      return (
        await query<ConfigAuditRow>(`SELECT * FROM omniroute.config_audit_log ORDER BY ${order()}`)
      ).rows;
    },
    async clear(): Promise<void> {
      assertStandaloneWrite();
      await query("DELETE FROM omniroute.config_audit_log");
    },
  };
}
