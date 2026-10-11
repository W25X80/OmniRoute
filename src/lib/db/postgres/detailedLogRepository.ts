import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import type { PostgresDatabase } from "./database";
import { queryUsageRows, safeUsageNumber, bindSelectedUsageRepository } from "./usageQueries";
export class PostgresDetailedLogRepository {
  constructor(private readonly database: PostgresDatabase) {}
  async save(parameters: unknown[], apiKeyId?: string | null): Promise<void> {
    await this.database.transaction(async (transaction) => {
      await transaction.query("SELECT pg_advisory_xact_lock_shared(18765,5)");
      if (apiKeyId) {
        const row = (
          await transaction.query<{ no_log: number }>(
            "SELECT no_log FROM omniroute.api_keys WHERE id=$1",
            [apiKeyId]
          )
        ).rows[0];
        if (!row || row.no_log === 1) return;
      }
      await transaction.query("SELECT pg_advisory_xact_lock(18765,8)");
      await transaction.query(
        `INSERT INTO omniroute.request_detail_logs
    (id,call_log_id,timestamp,client_request,translated_request,provider_response,client_response,provider,model,source_format,target_format,duration_ms)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
        parameters
      );
      await transaction.query(`DELETE FROM omniroute.request_detail_logs WHERE id IN
    (SELECT id FROM omniroute.request_detail_logs ORDER BY timestamp DESC,id DESC OFFSET 500)`);
    });
  }
  async list(limit: number, offset: number): Promise<Record<string, unknown>[]> {
    return queryUsageRows<Record<string, unknown>>(
      this.database,
      "SELECT * FROM omniroute.request_detail_logs ORDER BY timestamp DESC,id DESC LIMIT $1 OFFSET $2",
      [limit, offset],
      ["duration_ms"]
    );
  }
  async byId(id: string): Promise<Record<string, unknown> | undefined> {
    return (
      await queryUsageRows<Record<string, unknown>>(
        this.database,
        "SELECT * FROM omniroute.request_detail_logs WHERE id=$1",
        [id],
        ["duration_ms"]
      )
    )[0];
  }
  async byCallLogId(id: string): Promise<Record<string, unknown> | undefined> {
    return (
      await queryUsageRows<Record<string, unknown>>(
        this.database,
        "SELECT * FROM omniroute.request_detail_logs WHERE call_log_id=$1 ORDER BY timestamp DESC,id DESC LIMIT 1",
        [id],
        ["duration_ms"]
      )
    )[0];
  }
  async count(): Promise<number> {
    return safeUsageNumber(
      (
        await queryUsageRows<{ count: string }>(
          this.database,
          "SELECT COUNT(*) AS count FROM omniroute.request_detail_logs"
        )
      )[0].count
    );
  }
}
export function getSelectedDetailedLogRepository(): PostgresDetailedLogRepository | undefined {
  const database = getSelectedPostgresDatabase();
  return database
    ? bindSelectedUsageRepository(new PostgresDetailedLogRepository(database))
    : undefined;
}
