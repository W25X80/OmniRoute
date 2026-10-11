import type { PostgresDatabase } from "./database";
import { queryUsageRows, USAGE_NUMERIC_FIELDS } from "./usageQueries";
/**
 * Read-only auto-routing aggregations over `call_logs`.
 * `requested_model` keeps the client auto/* id after routing resolves a target.
 */

// ---------------------------------------------------------------------------
// Auto-routing analytics
// ---------------------------------------------------------------------------

export interface AutoRoutingTotalResult {
  count: number;
}

/**
 * Returns the number of call-log rows requested through auto/ prefix models.
 */
export async function getAutoRoutingTotalCount(
  database: PostgresDatabase
): Promise<AutoRoutingTotalResult> {
  const row = (
    await queryUsageRows(
      database,
      `
      SELECT COUNT(*) as count
      FROM omniroute.call_logs
      WHERE requested_model = 'auto' OR requested_model LIKE 'auto/%'
    `,
      [],
      USAGE_NUMERIC_FIELDS
    )
  )[0] as AutoRoutingTotalResult | undefined;
  return row ?? { count: 0 };
}

export interface AutoRoutingVariantRow {
  variant: string;
  count: number;
}

/**
 * Returns per-variant request counts from the client-requested model id.
 */

export async function getAutoRoutingVariantBreakdown(
  database: PostgresDatabase
): Promise<AutoRoutingVariantRow[]> {
  return (await queryUsageRows(
    database,
    `
      SELECT
        CASE
          WHEN requested_model = 'auto' THEN 'default'
          WHEN requested_model LIKE 'auto/%' THEN SUBSTR(requested_model, 6)
          ELSE 'other'
        END as variant,
        COUNT(*) as count
      FROM omniroute.call_logs
      WHERE requested_model = 'auto' OR requested_model LIKE 'auto/%'
      GROUP BY variant
      ORDER BY count DESC
    `,
    [],
    USAGE_NUMERIC_FIELDS
  )) as AutoRoutingVariantRow[];
}

export interface AutoRoutingTopProviderRow {
  provider: string;
  count: number;
}

/**
 * Returns the top 10 providers used for auto/ prefix requests.
 */

export async function getAutoRoutingTopProviders(
  database: PostgresDatabase
): Promise<AutoRoutingTopProviderRow[]> {
  return (await queryUsageRows(
    database,
    `
      SELECT provider, COUNT(*) as count
      FROM omniroute.call_logs
      WHERE (requested_model = 'auto' OR requested_model LIKE 'auto/%')
        AND provider IS NOT NULL
        AND TRIM(provider) != ''
      GROUP BY provider
      ORDER BY count DESC
      LIMIT 10
      `,
    [],
    USAGE_NUMERIC_FIELDS
  )) as AutoRoutingTopProviderRow[];
}
