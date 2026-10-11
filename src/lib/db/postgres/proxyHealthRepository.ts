import type { PostgresDatabase } from "./database";
import type { ProviderUpstreamRow } from "../proxyHealth";
export async function getPostgresProviderUpstreamSummary(
  database: PostgresDatabase,
  since: string
) {
  return database.transaction(async (tx) => {
    const rows = (
      await tx.query(
        "SELECT\n         provider,\n         proxy_host AS exit_host,\n         proxy_port AS exit_port,\n         COUNT(*) AS attempts,\n         SUM(CASE WHEN upstream_status IS NOT NULL THEN 1 ELSE 0 END) AS measured,\n         SUM(CASE WHEN upstream_status >= 500 AND upstream_status < 600 THEN 1 ELSE 0 END) AS server_errors\n       FROM omniroute.proxy_logs\n       WHERE timestamp >= $1\n         AND provider IS NOT NULL AND TRIM(provider) <> ''\n         AND (target_url NOT LIKE '%/connection-test' OR target_url IS NULL)\n       GROUP BY provider, exit_host, exit_port",
        [since]
      )
    ).rows as Array<{
      provider: string;
      exit_host: string | null;
      exit_port: number | null;
      attempts: number;
      measured: number;
      server_errors: number;
    }>;

    const byProvider = new Map<string, ProviderUpstreamRow>();
    for (const row of rows) {
      const provider = row.provider;
      let entry = byProvider.get(provider);
      if (!entry) {
        entry = {
          provider,
          attempts: 0,
          measured: 0,
          serverErrors: 0,
          exits: [],
        };
        byProvider.set(provider, entry);
      }
      const attempts = Number(row.attempts || 0);
      const measured = Number(row.measured || 0);
      const serverErrors = Number(row.server_errors || 0);
      entry.attempts += attempts;
      entry.measured += measured;
      entry.serverErrors += serverErrors;
      entry.exits.push({
        exit: `${row.exit_host ?? "?"}:${row.exit_port ?? "?"}`,
        attempts,
        serverErrors,
      });
    }
    return [...byProvider.values()].sort((a, b) => a.provider.localeCompare(b.provider));
  });
}

export async function getPostgresProxyHealthStats(
  database: PostgresDatabase,
  options?: { hours?: number }
) {
  return database.transaction(async (tx) => {
    const hours = Math.max(1, Math.min(24 * 30, Number(options?.hours || 24)));
    const sinceIso = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();

    const rows = (
      await tx.query(
        "SELECT\n         p.id as proxy_id,\n         p.name as proxy_name,\n         p.type as proxy_type,\n         p.host as proxy_host,\n         p.port as proxy_port,\n         p.status as proxy_status,\n         COUNT(l.id) as total_requests,\n         SUM(CASE WHEN l.status = 'success' THEN 1 ELSE 0 END) as success_count,\n         SUM(CASE WHEN l.status = 'error' THEN 1 ELSE 0 END) as error_count,\n         SUM(CASE WHEN l.status = 'timeout' THEN 1 ELSE 0 END) as timeout_count,\n         SUM(CASE WHEN l.id IS NOT NULL AND l.target_url LIKE '%/connection-test' THEN 1 ELSE 0 END) as connection_tests,\n         SUM(CASE WHEN l.id IS NOT NULL AND l.target_url LIKE '%/connection-test' AND l.status = 'success' THEN 1 ELSE 0 END) as connection_test_success,\n         SUM(CASE WHEN l.id IS NOT NULL AND (l.target_url NOT LIKE '%/connection-test' OR l.target_url IS NULL) THEN 1 ELSE 0 END) as real_requests,\n         SUM(CASE WHEN l.id IS NOT NULL AND (l.target_url NOT LIKE '%/connection-test' OR l.target_url IS NULL) AND l.upstream_status IS NOT NULL THEN 1 ELSE 0 END) as measured_requests,\n         SUM(CASE WHEN l.id IS NOT NULL AND (l.target_url NOT LIKE '%/connection-test' OR l.target_url IS NULL) AND (l.upstream_status IS NOT NULL OR l.status = 'success') THEN 1 ELSE 0 END) as transport_ok,\n         SUM(CASE WHEN l.id IS NOT NULL AND (l.target_url NOT LIKE '%/connection-test' OR l.target_url IS NULL) AND l.status IN ('error', 'timeout') AND l.upstream_status IS NULL AND NOT (COALESCE(l.attempt_issue, '') = 'abandoned' AND COALESCE(l.error, '') LIKE 'OpencodeHeadersWaitTimeout:%') AND NOT (l.attempt_issue IS NOT NULL AND COALESCE(l.error, '') LIKE '%Request aborted%') THEN 1 ELSE 0 END) as transport_failures,\n         SUM(CASE WHEN l.id IS NOT NULL AND (l.target_url NOT LIKE '%/connection-test' OR l.target_url IS NULL) AND l.status IN ('error', 'timeout') AND l.upstream_status IS NULL AND COALESCE(l.attempt_issue, '') = 'abandoned' AND COALESCE(l.error, '') LIKE 'OpencodeHeadersWaitTimeout:%' THEN 1 ELSE 0 END) as slow_abandoned,\n         SUM(CASE WHEN l.id IS NOT NULL AND (l.target_url NOT LIKE '%/connection-test' OR l.target_url IS NULL) AND l.status IN ('error', 'timeout') AND l.upstream_status IS NULL AND l.attempt_issue IS NOT NULL AND COALESCE(l.error, '') LIKE '%Request aborted%' THEN 1 ELSE 0 END) as client_aborted,\n         SUM(CASE WHEN l.id IS NOT NULL AND (l.target_url NOT LIKE '%/connection-test' OR l.target_url IS NULL) AND l.upstream_status >= 400 AND l.upstream_status < 500 THEN 1 ELSE 0 END) as upstream_4xx,\n         SUM(CASE WHEN l.id IS NOT NULL AND (l.target_url NOT LIKE '%/connection-test' OR l.target_url IS NULL) AND l.upstream_status >= 500 AND l.upstream_status < 600 THEN 1 ELSE 0 END) as upstream_5xx,\n         AVG(CASE WHEN l.latency_ms IS NOT NULL THEN l.latency_ms END) as avg_latency_ms,\n         MAX(l.timestamp) as last_seen_at\n       FROM omniroute.proxy_registry p\n       LEFT JOIN omniroute.proxy_logs l\n         ON l.proxy_host = p.host\n        AND l.proxy_type = p.type\n        AND l.proxy_port = p.port\n        AND l.timestamp >= $1\n       GROUP BY p.id, p.name, p.type, p.host, p.port\n       ORDER BY p.name ASC",
        [sinceIso]
      )
    ).rows as Array<Record<string, unknown>>;

    return rows.map((row) => {
      const total = Number(row.total_requests || 0);
      const success = Number(row.success_count || 0);
      const error = Number(row.error_count || 0);
      const timeout = Number(row.timeout_count || 0);
      const successRate = total > 0 ? Math.round((success / total) * 10000) / 100 : null;
      const realRequests = Number(row.real_requests || 0);
      const transportOk = Number(row.transport_ok || 0);
      const transportFailures = Number(row.transport_failures || 0);
      const slowAbandoned = Number(row.slow_abandoned || 0);
      const clientAborted = Number(row.client_aborted || 0);
      const measuredRequests = Number(row.measured_requests || 0);
      const transportRate =
        !measuredRequests || transportOk + transportFailures === 0
          ? null
          : Math.round((transportOk / (transportOk + transportFailures)) * 10000) / 100;

      return {
        proxyId: String(row.proxy_id || ""),
        name: String(row.proxy_name || ""),
        type: String(row.proxy_type || "http"),
        host: String(row.proxy_host || ""),
        port: Number(row.proxy_port || 0),
        status: String(row.proxy_status || "active"),
        totalRequests: total,
        successCount: success,
        errorCount: error,
        timeoutCount: timeout,
        successRate,
        connectionTests: Number(row.connection_tests || 0),
        connectionTestSuccess: Number(row.connection_test_success || 0),
        realRequests,
        measuredRequests,
        measured: measuredRequests > 0,
        transportOk,
        transportFailures,
        slowAbandoned,
        clientAborted,
        transportRate,
        upstream4xx: Number(row.upstream_4xx || 0),
        upstream5xx: Number(row.upstream_5xx || 0),
        avgLatencyMs:
          row.avg_latency_ms === null || row.avg_latency_ms === undefined
            ? null
            : Math.round(Number(row.avg_latency_ms)),
        lastSeenAt: row.last_seen_at ? String(row.last_seen_at) : null,
      };
    });
  });
}
