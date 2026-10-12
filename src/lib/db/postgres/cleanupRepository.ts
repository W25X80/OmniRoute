import { artifactHasSqlOwner, lockArtifactPublication } from "./artifactPublicationRepository";
import type { DeleteByPeriodTarget } from "../cleanup/usagePurge";
import type { PostgresDatabase, PostgresTransaction } from "./database";

const RETENTION_COLUMNS: Readonly<Record<string, string>> = {
  quota_snapshots: "created_at",
  call_logs: "timestamp",
  usage_history: "timestamp",
  daily_usage_summary: "date",
  hourly_usage_summary: "date_hour",
  request_detail_logs: "timestamp",
  proxy_logs: "timestamp",
  relay_logs: "created_at",
  compression_analytics: "timestamp",
  compression_engine_breakdown: "timestamp",
  compression_run_telemetry: "timestamp",
  mcp_tool_audit: "created_at",
  config_audit_log: "timestamp",
  a2a_task_events: "created_at",
  memories: "created_at",
  domain_cost_history: "timestamp",
  compression_cache_stats: "created_at",
  xp_audit_log: "created_at",
  routing_decisions: "created_at",
  quota_consumption: "updated_at",
  conversation_turn_nodes: "last_seen_at",
  agentic_conversations: "last_seen_at",
};

function qualify(table: string, column?: string): string {
  if (!(table in RETENTION_COLUMNS) || (column && RETENTION_COLUMNS[table] !== column)) {
    throw new Error("Unknown PostgreSQL retention target");
  }
  return `omniroute."${table}"`;
}

function cutoffValue(target: DeleteByPeriodTarget, cutoff: string): string | number {
  const time = new Date(cutoff).getTime();
  if (!Number.isFinite(time)) throw new Error("Invalid retention cutoff");
  switch (target.cutoff) {
    case "date":
      return cutoff.slice(0, 10);
    case "dateHour":
      return `${cutoff.slice(0, 10)} ${cutoff.slice(11, 13)}:00:00`;
    case "epochMs":
      return time;
    case "epochSeconds":
      return Math.floor(time / 1000);
    default:
      return cutoff;
  }
}

async function exists(transaction: PostgresTransaction, table: string): Promise<boolean> {
  qualify(table);
  const result = await transaction.query<{ present: boolean }>(
    "SELECT to_regclass($1) IS NOT NULL AS present",
    [`omniroute.${table}`]
  );
  return result.rows[0]?.present === true;
}

/** Persist references in the same transaction that removes their last log row. */
export async function queuePostgresArtifactDeletions(
  transaction: PostgresTransaction,
  paths: string[]
): Promise<void> {
  if (paths.length === 0) return;
  await transaction.query(
    `INSERT INTO omniroute.artifact_deletion_queue(relpath)
     SELECT DISTINCT unnest($1::text[]) ON CONFLICT(relpath) DO UPDATE
       SET publication_state='delete_pending',next_attempt_at=statement_timestamp()`,
    [paths]
  );
}

/** Classify last references while the caller holds the call-history insertion fence. */
async function unreferencedCallLogArtifacts(
  transaction: PostgresTransaction,
  paths: string[]
): Promise<string[]> {
  if (paths.length === 0) return [];
  const result = await transaction.query<{ relpath: string }>(
    `SELECT DISTINCT relpath FROM unnest($1::text[]) AS removed(relpath)
     WHERE NOT EXISTS(SELECT 1 FROM omniroute.call_logs WHERE artifact_relpath=removed.relpath
       UNION ALL SELECT 1 FROM omniroute.call_logs_v1_legacy WHERE artifact_relpath=removed.relpath)`,
    [paths]
  );
  return result.rows.map((row) => row.relpath);
}

export function createPostgresCleanupRepository(database: PostgresDatabase) {
  return {
    async pendingArtifactDeletions(limit = 1000): Promise<string[]> {
      const bounded = Number.isInteger(limit) ? Math.max(1, Math.min(1000, limit)) : 1000;
      return database.transaction(async (transaction) => {
        const result = await transaction.query<{ relpath: string }>(
          `SELECT relpath FROM omniroute.artifact_deletion_queue
           WHERE publication_state<>'attached' AND next_attempt_at<=statement_timestamp()
           ORDER BY next_attempt_at,created_at,relpath LIMIT $1`,
          [bounded]
        );
        return result.rows.map((row) => row.relpath);
      });
    },
    async acknowledgeArtifactDeletion(path: string): Promise<boolean> {
      return database.transaction(async (transaction) => {
        await lockArtifactPublication(transaction);
        if (await artifactHasSqlOwner(transaction, path)) return false;
        const result = await transaction.query(
          `DELETE FROM omniroute.artifact_deletion_queue WHERE relpath=$1
           AND publication_state='delete_pending' AND upload_state IN ('acknowledged','not_sent')`,
          [path]
        );
        return result.rowCount === 1;
      });
    },
    exists(table: string) {
      return database.transaction((transaction) => exists(transaction, table));
    },
    async deleteBefore(target: DeleteByPeriodTarget, cutoff: string): Promise<number> {
      const table = qualify(target.table, target.column);
      const value = cutoffValue(target, cutoff);
      let deleted = 0;
      // Each statement is atomic and independently bounded by the database timeout.
      // A backlog exceeding this pass is resumed by the next scheduled cleanup.
      for (let batch = 0; batch < 100; batch++) {
        const count = await database.transaction(async (transaction) => {
          if (target.table === "domain_cost_history") {
            await transaction.query("SELECT pg_advisory_xact_lock(18765,8)");
          }
          if (!(await exists(transaction, target.table))) return 0;
          const result = await transaction.query(
            `WITH expired AS (SELECT ctid FROM ${table} WHERE ${target.cutoff === "iso" ? `"${target.column}"::timestamptz < $1::timestamptz` : `"${target.column}" < $1`} LIMIT 1000)
             DELETE FROM ${table} WHERE ctid IN (SELECT ctid FROM expired)`,
            [value]
          );
          return result.rowCount ?? 0;
        });
        deleted += count;
        if (count < 1000) break;
      }
      return deleted;
    },
    async deleteOrphanConversations(cutoff: string): Promise<number> {
      let deleted = 0;
      for (let batch = 0; batch < 100; batch++) {
        const count = await database.transaction(async (transaction) => {
          if (
            !(await exists(transaction, "agentic_conversations")) ||
            !(await exists(transaction, "conversation_turn_nodes"))
          )
            return 0;
          const result = await transaction.query(
            `WITH expired AS (
               SELECT ctid FROM omniroute.agentic_conversations a
               WHERE last_seen_at::timestamptz < $1::timestamptz AND NOT EXISTS
                 (SELECT 1 FROM omniroute.conversation_turn_nodes n WHERE n.conversation_id = a.id)
               LIMIT 1000)
             DELETE FROM omniroute.agentic_conversations WHERE ctid IN (SELECT ctid FROM expired)`,
            [cutoff]
          );
          return result.rowCount ?? 0;
        });
        deleted += count;
        if (count < 1000) break;
      }
      return deleted;
    },
    async deleteCallLogs(cutoff?: string): Promise<{ deleted: number; artifacts: string[] }> {
      let deleted = 0;
      const artifacts: string[] = [];
      for (let batch = 0; batch < 100; batch++) {
        const result = await database.transaction(async (transaction) => {
          await transaction.query("SELECT pg_advisory_xact_lock(18765,8)");
          const rows = await transaction.query<{ artifact_relpath: string | null }>(
            `WITH expired AS (SELECT ctid FROM omniroute.call_logs
               ${cutoff ? "WHERE timestamp::timestamptz < $1::timestamptz" : ""} LIMIT 1000)
             DELETE FROM omniroute.call_logs WHERE ctid IN (SELECT ctid FROM expired)
             RETURNING artifact_relpath`,
            cutoff ? [cutoff] : []
          );
          const unreferenced = await unreferencedCallLogArtifacts(
            transaction,
            rows.rows.flatMap((row) => (row.artifact_relpath ? [row.artifact_relpath] : []))
          );
          await queuePostgresArtifactDeletions(transaction, unreferenced);
          return { deleted: rows.rowCount ?? 0, artifacts: unreferenced };
        });
        deleted += result.deleted;
        artifacts.push(...result.artifacts);
        if (result.deleted < 1000) break;
      }
      return { deleted, artifacts };
    },
    async deleteAll(table: string): Promise<number> {
      const qualified = qualify(table);
      return database.transaction(async (transaction) => {
        if (!(await exists(transaction, table))) return 0;
        const result = await transaction.query(`DELETE FROM ${qualified}`);
        return result.rowCount ?? 0;
      });
    },
    async reset(
      targets: Array<DeleteByPeriodTarget & { resultKey: string; allOnly?: boolean }>,
      cutoff?: string
    ): Promise<{ counts: Record<string, number>; artifacts: string[] }> {
      return database.transaction(async (transaction) => {
        await transaction.query("SELECT pg_advisory_xact_lock(18765, 7)");
        if (
          targets.some((target) => target.table === "call_logs" && (!cutoff || !target.allOnly))
        ) {
          await transaction.query("SELECT pg_advisory_xact_lock(18765,8)");
        }
        const counts: Record<string, number> = {};
        const artifacts: string[] = [];
        for (const target of targets) {
          if (cutoff && target.allOnly) continue;
          const table = qualify(target.table, target.column);
          if (!(await exists(transaction, target.table))) {
            counts[target.resultKey] = 0;
            continue;
          }
          const result = await transaction.query<{ artifact_relpath: string | null }>(
            `DELETE FROM ${table} ${cutoff ? (target.cutoff === "iso" ? `WHERE "${target.column}"::timestamptz < $1::timestamptz` : `WHERE "${target.column}" < $1`) : ""}
             ${target.table === "call_logs" ? "RETURNING artifact_relpath" : ""}`,
            cutoff ? [cutoffValue(target, cutoff)] : []
          );
          counts[target.resultKey] = result.rowCount ?? 0;
          if (target.table === "call_logs") {
            for (const row of result.rows)
              if (row.artifact_relpath) artifacts.push(row.artifact_relpath);
          }
        }
        const unreferenced = await unreferencedCallLogArtifacts(transaction, artifacts);
        await queuePostgresArtifactDeletions(transaction, unreferenced);
        return { counts, artifacts: unreferenced };
      });
    },
  };
}
