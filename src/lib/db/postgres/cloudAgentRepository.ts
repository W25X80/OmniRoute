import type { CloudAgentTaskRow } from "../cloudAgentTasks";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";
const COLUMNS =
  "id,provider_id,external_id,status,prompt,source,options,result,activities,error,created_at,updated_at,completed_at";
const UPDATABLE = new Set([
  "status",
  "prompt",
  "source",
  "options",
  "result",
  "activities",
  "error",
  "completed_at",
]);
export function createPostgresCloudAgentRepository(database: PostgresDatabase) {
  function operation<T>(write: boolean, body: (tx: PostgresTransaction) => Promise<T>) {
    return database.transaction(async (tx) => {
      await acquirePostgresLock(tx, "cloudAgents", write ? "exclusive" : "shared");
      return body(tx);
    });
  }
  return {
    assertCloudAgentTaskTable: () =>
      operation(false, async (tx) => {
        await tx.query("SELECT 1 FROM omniroute.cloud_agent_tasks LIMIT 0");
      }),
    insertCloudAgentTask: (row: CloudAgentTaskRow) =>
      operation(true, async (tx) => {
        await tx.query(
          `INSERT INTO omniroute.cloud_agent_tasks(${COLUMNS}) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
          [
            row.id,
            row.provider_id,
            row.external_id,
            row.status,
            row.prompt,
            row.source,
            row.options,
            row.result,
            row.activities,
            row.error,
            row.created_at,
            row.updated_at,
            row.completed_at,
          ]
        );
      }),
    updateCloudAgentTask: (id: string, patch: Partial<Omit<CloudAgentTaskRow, "id">>) =>
      operation(true, async (tx) => {
        const values: unknown[] = [id],
          fields: string[] = [];
        for (const [field, value] of Object.entries(patch))
          if (UPDATABLE.has(field)) {
            values.push(value);
            fields.push(`${field}=$${values.length}`);
          }
        if (!fields.length) return null;
        return (
          (
            await tx.query<{ status: string }>(
              `UPDATE omniroute.cloud_agent_tasks SET ${fields.join(",")},updated_at=to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') WHERE id=$1 RETURNING status`,
              values
            )
          ).rows[0]?.status ?? null
        );
      }),
    getCloudAgentTaskById: (id: string) =>
      operation(
        false,
        async (tx) =>
          (
            await tx.query<CloudAgentTaskRow>(
              `SELECT ${COLUMNS} FROM omniroute.cloud_agent_tasks WHERE id=$1`,
              [id]
            )
          ).rows[0] ?? null
      ),
    getCloudAgentTasksByProvider: (provider: string, limit = 50) =>
      operation(
        false,
        async (tx) =>
          (
            await tx.query<CloudAgentTaskRow>(
              `SELECT ${COLUMNS} FROM omniroute.cloud_agent_tasks WHERE provider_id=$1 ORDER BY created_at DESC,insertion_order LIMIT $2`,
              [provider, limit]
            )
          ).rows
      ),
    getCloudAgentTasksByStatus: (status: string, limit = 50) =>
      operation(
        false,
        async (tx) =>
          (
            await tx.query<CloudAgentTaskRow>(
              `SELECT ${COLUMNS} FROM omniroute.cloud_agent_tasks WHERE status=$1 ORDER BY created_at DESC,insertion_order LIMIT $2`,
              [status, limit]
            )
          ).rows
      ),
    getAllCloudAgentTasks: (limit = 100) =>
      operation(
        false,
        async (tx) =>
          (
            await tx.query<CloudAgentTaskRow>(
              `SELECT ${COLUMNS} FROM omniroute.cloud_agent_tasks ORDER BY created_at DESC,insertion_order LIMIT $1`,
              [limit]
            )
          ).rows
      ),
    deleteCloudAgentTask: (id: string) =>
      operation(true, async (tx) => {
        await tx.query("DELETE FROM omniroute.cloud_agent_tasks WHERE id=$1", [id]);
      }),
    readCloudAgentCredential: (provider: string) =>
      operation(
        false,
        async (tx) =>
          (
            await tx.query<{ api_key_encrypted: string; base_url: string | null }>(
              "SELECT api_key_encrypted,base_url FROM omniroute.cloud_agent_credentials WHERE provider_id=$1",
              [provider]
            )
          ).rows[0] ?? null
      ),
    listCloudAgentCredentialRows: () =>
      operation(
        false,
        async (tx) =>
          (
            await tx.query<{
              provider_id: string;
              api_key_encrypted: string;
              base_url: string | null;
              updated_at: string;
            }>(
              "SELECT provider_id,api_key_encrypted,base_url,updated_at FROM omniroute.cloud_agent_credentials ORDER BY insertion_order"
            )
          ).rows
      ),
    saveCloudAgentCredential: (provider: string, encrypted: string, baseUrl?: string) =>
      operation(true, async (tx) => {
        await tx.query(
          `INSERT INTO omniroute.cloud_agent_credentials(provider_id,api_key_encrypted,base_url,updated_at)
      VALUES($1,$2,$3,to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')) ON CONFLICT(provider_id) DO UPDATE SET api_key_encrypted=excluded.api_key_encrypted,base_url=excluded.base_url,updated_at=excluded.updated_at`,
          [provider, encrypted, baseUrl ?? null]
        );
      }),
    deleteCloudAgentCredential: (provider: string) =>
      operation(true, async (tx) => {
        await tx.query("DELETE FROM omniroute.cloud_agent_credentials WHERE provider_id=$1", [
          provider,
        ]);
      }),
  };
}
