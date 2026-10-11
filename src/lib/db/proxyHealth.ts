import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { getPostgresProviderUpstreamSummary } from "./postgres/proxyHealthRepository";
/**
 * Upstream failure spread grouped by provider, read from the proxy log. One row
 * per provider with real (non connection-test) requests since `since`, and per
 * exit (distinct proxy host + port) the attempts and 5xx counts behind that
 * provider. Providers with a null or blank name are excluded: they can never
 * own a regime line. Only numbers leave this function.
 */
export type ProviderUpstreamExit = {
  exit: string;
  attempts: number;
  serverErrors: number;
};

export type ProviderUpstreamRow = {
  provider: string;
  attempts: number;
  measured: number;
  serverErrors: number;
  exits: ProviderUpstreamExit[];
};

export function getProviderUpstreamSummary(since: string): ProviderUpstreamRow[] {
  const db = getDbInstance();
  const rows = db
    .prepare(
      `SELECT
         provider,
         proxy_host AS exit_host,
         proxy_port AS exit_port,
         COUNT(*) AS attempts,
         SUM(CASE WHEN upstream_status IS NOT NULL THEN 1 ELSE 0 END) AS measured,
         SUM(CASE WHEN upstream_status >= 500 AND upstream_status < 600 THEN 1 ELSE 0 END) AS server_errors
       FROM proxy_logs
       WHERE timestamp >= ?
         AND provider IS NOT NULL AND TRIM(provider) <> ''
         AND (target_url NOT LIKE '%/connection-test' OR target_url IS NULL)
       GROUP BY provider, exit_host, exit_port`
    )
    .all(since) as Array<{
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
}

export async function getProviderUpstreamSummaryAsync(
  since: string
): Promise<ProviderUpstreamRow[]> {
  const postgres = getSelectedPostgresDatabase();
  return postgres
    ? getPostgresProviderUpstreamSummary(postgres, since)
    : getProviderUpstreamSummary(since);
}
