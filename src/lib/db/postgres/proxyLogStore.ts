import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import type { ProxyLogEntry } from "../../proxyLogger";
import type { PostgresDatabase } from "./database";
import { queryUsageRows, bindSelectedUsageRepository } from "./usageQueries";

export class PostgresProxyLogStore {
  constructor(private readonly database: PostgresDatabase) {}
  async append(entries: ProxyLogEntry[]): Promise<void> {
    await this.database.transaction(async (transaction) => {
      for (const item of entries) {
        await transaction.query(
          `INSERT INTO omniroute.proxy_logs(id,timestamp,status,proxy_type,proxy_host,proxy_port,proxy_name,
            level,level_id,provider,target_url,public_ip,egress_ip,latency_ms,error,connection_id,combo_id,
            account,rotation_account,correlation_id,tls_fingerprint,upstream_status,attempt_number,
            attempt_issue,headers_ms,first_chunk_ms)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)
           ON CONFLICT(id) DO NOTHING`,
          [
            item.id,
            item.timestamp,
            item.status,
            item.proxy?.type || null,
            item.proxy?.host || null,
            item.proxy?.port ? Number(item.proxy.port) : null,
            item.proxy?.name || null,
            item.level,
            item.levelId,
            item.provider,
            item.targetUrl,
            item.clientIp,
            item.egressIp,
            item.latencyMs,
            item.error,
            item.connectionId,
            item.comboId,
            item.account,
            item.rotationAccount,
            item.correlationId,
            item.tlsFingerprint ? 1 : 0,
            item.upstreamStatus,
            item.attemptNumber,
            item.attemptIssue,
            item.headersMs,
            item.firstChunkMs,
          ]
        );
      }
    });
  }
  async list(limit = 200): Promise<Record<string, unknown>[]> {
    return queryUsageRows(
      this.database,
      "SELECT * FROM omniroute.proxy_logs ORDER BY timestamp DESC,id DESC LIMIT $1",
      [limit],
      [
        "proxy_port",
        "latency_ms",
        "tls_fingerprint",
        "upstream_status",
        "attempt_number",
        "headers_ms",
        "first_chunk_ms",
      ]
    );
  }
  async clear(): Promise<void> {
    await this.database.transaction((transaction) =>
      transaction.query("DELETE FROM omniroute.proxy_logs")
    );
  }
}
export function getSelectedProxyLogStore(): PostgresProxyLogStore | undefined {
  const database = getSelectedPostgresDatabase();
  return database ? bindSelectedUsageRepository(new PostgresProxyLogStore(database)) : undefined;
}
