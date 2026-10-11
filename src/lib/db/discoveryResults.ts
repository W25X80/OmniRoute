import { assertSqliteBackendAllowed } from "./controlPlaneContext";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import {
  catalogTableRows,
  compareCatalogText,
  requirePostgresCatalogSnapshot,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  writePostgresCatalog,
} from "./postgres/catalogSnapshot";
/**
 * Database module: Discovery Results
 *
 * CRUD for the automated provider-discovery tool (opt-in, default off). Rows
 * live in the `discovery_results` table (migration 074). The Discovery service
 * (`src/lib/discovery/`) writes findings here via {@link upsertDiscoveryResult};
 * the `/api/discovery/*` routes read/verify/delete them.
 *
 * See `_tasks/features-v3.8.42/gaps/DISCOVERY_TOOL_DESIGN.md` for the design.
 */

import { getDbInstance } from "./core";

export type DiscoveryMethod = "free_tier" | "web_cookie" | "auto_register" | "trial" | "public_api";
export type DiscoveryAuthType = "none" | "cookie" | "api_key" | "oauth";
export type DiscoveryRiskLevel = "none" | "low" | "medium" | "high" | "critical";
export type DiscoveryStatus = "pending" | "testing" | "verified" | "rejected";

export interface DiscoveryResult {
  id?: number;
  providerId: string;
  method: DiscoveryMethod;
  endpoint?: string | null;
  authType: DiscoveryAuthType;
  models?: string[];
  rateLimit?: string | null;
  feasibility: number;
  riskLevel: DiscoveryRiskLevel;
  status: DiscoveryStatus;
  notes?: string | null;
  discoveredAt?: string;
  verifiedAt?: string | null;
}

interface DiscoveryRow {
  id: number;
  provider_id: string;
  method: string;
  endpoint: string | null;
  auth_type: string | null;
  models: string | null;
  rate_limit: string | null;
  feasibility: number | null;
  risk_level: string | null;
  status: string;
  notes: string | null;
  discovered_at: string;
  verified_at: string | null;
}

function rowToResult(row: DiscoveryRow): DiscoveryResult {
  let models: string[] | undefined;
  if (row.models) {
    try {
      const parsed = JSON.parse(row.models);
      if (Array.isArray(parsed)) models = parsed.map(String);
    } catch {
      models = undefined;
    }
  }
  return {
    id: row.id,
    providerId: row.provider_id,
    method: row.method as DiscoveryMethod,
    endpoint: row.endpoint,
    authType: (row.auth_type as DiscoveryAuthType) ?? "none",
    models,
    rateLimit: row.rate_limit,
    feasibility: row.feasibility ?? 0,
    riskLevel: (row.risk_level as DiscoveryRiskLevel) ?? "none",
    status: row.status as DiscoveryStatus,
    notes: row.notes,
    discoveredAt: row.discovered_at,
    verifiedAt: row.verified_at,
  };
}

/**
 * Insert or update a discovery finding. Uniqueness is keyed on
 * `(provider_id, method, endpoint)` (the table's UNIQUE constraint), so
 * re-discovering the same endpoint updates the existing row rather than
 * duplicating it. Returns the persisted row (with its id).
 */
export async function upsertDiscoveryResultAsync(
  result: DiscoveryResult
): Promise<DiscoveryResult> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const models = result.models ? JSON.stringify(result.models) : null;
    getSelectedPostgresDatabase()
      ? {
          changes:
            (
              await writePostgresCatalog(
                "INSERT INTO omniroute.discovery_results\n       (provider_id, method, endpoint, auth_type, models, rate_limit, feasibility, risk_level, status, notes)\n     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)\n     ON CONFLICT(provider_id, method, endpoint) DO UPDATE SET\n       auth_type = excluded.auth_type,\n       models = excluded.models,\n       rate_limit = excluded.rate_limit,\n       feasibility = excluded.feasibility,\n       risk_level = excluded.risk_level,\n       status = excluded.status,\n       notes = excluded.notes",
                [
                  result.providerId,
                  result.method,
                  result.endpoint ?? null,
                  result.authType,
                  models,
                  result.rateLimit ?? null,
                  result.feasibility,
                  result.riskLevel,
                  result.status,
                  result.notes ?? null,
                ]
              )
            ).rowCount ?? 0,
        }
      : db!
          .prepare(
            `INSERT INTO discovery_results
       (provider_id, method, endpoint, auth_type, models, rate_limit, feasibility, risk_level, status, notes)
     VALUES (@provider_id, @method, @endpoint, @auth_type, @models, @rate_limit, @feasibility, @risk_level, @status, @notes)
     ON CONFLICT(provider_id, method, endpoint) DO UPDATE SET
       auth_type = excluded.auth_type,
       models = excluded.models,
       rate_limit = excluded.rate_limit,
       feasibility = excluded.feasibility,
       risk_level = excluded.risk_level,
       status = excluded.status,
       notes = excluded.notes`
          )
          .run({
            provider_id: result.providerId,
            method: result.method,
            endpoint: result.endpoint ?? null,
            auth_type: result.authType,
            models,
            rate_limit: result.rateLimit ?? null,
            feasibility: result.feasibility,
            risk_level: result.riskLevel,
            status: result.status,
            notes: result.notes ?? null,
          });

    const row = (
      getSelectedPostgresDatabase()
        ? catalogTableRows<DiscoveryRow>("discovery_results")
            .filter(
              (row) =>
                row.provider_id === result.providerId &&
                row.method === result.method &&
                (row.endpoint ?? "") === (result.endpoint ?? "")
            )
            .sort((a, b) => a.id - b.id)[0]
        : db
            .prepare(
              `SELECT * FROM discovery_results
       WHERE provider_id = ? AND method = ? AND ifnull(endpoint, '') = ifnull(?, '')`
            )
            .get(result.providerId, result.method, result.endpoint ?? null)
    ) as DiscoveryRow | undefined;
    // The row was just written, so it must exist.
    return rowToResult(row!);
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

/**
 * List discovery results, optionally filtered to a single provider. Newest
 * findings first.
 */
export function getDiscoveryResults(providerId?: string): DiscoveryResult[] {
  requirePostgresCatalogSnapshot();

  const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
  const rows = providerId
    ? ((getSelectedPostgresDatabase()
        ? catalogTableRows<DiscoveryRow>("discovery_results")
            .filter((row) => row.provider_id === providerId)
            .sort((a, b) => compareCatalogText(b.discovered_at, a.discovered_at) || b.id - a.id)
        : db
            .prepare(
              "SELECT * FROM discovery_results WHERE provider_id = ? ORDER BY discovered_at DESC, id DESC"
            )
            .all(providerId)) as DiscoveryRow[])
    : ((getSelectedPostgresDatabase()
        ? catalogTableRows<DiscoveryRow>("discovery_results").sort(
            (a, b) => compareCatalogText(b.discovered_at, a.discovered_at) || b.id - a.id
          )
        : db
            .prepare("SELECT * FROM discovery_results ORDER BY discovered_at DESC, id DESC")
            .all()) as DiscoveryRow[]);
  return rows.map(rowToResult);
}

export function getDiscoveryResultById(id: number): DiscoveryResult | null {
  requirePostgresCatalogSnapshot();

  const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
  const row = (
    getSelectedPostgresDatabase()
      ? catalogTableRows<DiscoveryRow>("discovery_results").filter((row) => row.id === id)[0]
      : db!.prepare("SELECT * FROM discovery_results WHERE id = ?").get(id)
  ) as DiscoveryRow | undefined;
  return row ? rowToResult(row) : null;
}

/**
 * Mark a finding as verified, stamping `verified_at`. Returns the updated row,
 * or null if no row with that id exists.
 */
export async function markVerifiedAsync(id: number): Promise<DiscoveryResult | null> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const info = getSelectedPostgresDatabase()
      ? {
          changes:
            (
              await writePostgresCatalog(
                "UPDATE omniroute.discovery_results SET status = 'verified', verified_at = to_char(timezone('UTC',clock_timestamp()),'YYYY-MM-DD HH24:MI:SS') WHERE id = $1",
                [id]
              )
            ).rowCount ?? 0,
        }
      : db
          .prepare(
            "UPDATE discovery_results SET status = 'verified', verified_at = datetime('now') WHERE id = ?"
          )
          .run(id);
    if (info.changes === 0) return null;
    return getDiscoveryResultById(id);
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

/**
 * Delete a finding. Returns true if a row was removed, false if the id was not
 * found.
 */
export async function deleteDiscoveryResultAsync(id: number): Promise<boolean> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const info = getSelectedPostgresDatabase()
      ? {
          changes:
            (
              await writePostgresCatalog("DELETE FROM omniroute.discovery_results WHERE id = $1", [
                id,
              ])
            ).rowCount ?? 0,
        }
      : db!.prepare("DELETE FROM discovery_results WHERE id = ?").run(id);
    return info.changes > 0;
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

export function upsertDiscoveryResult(result: DiscoveryResult): DiscoveryResult {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  const models = result.models ? JSON.stringify(result.models) : null;
  db.prepare(
    `INSERT INTO discovery_results
       (provider_id, method, endpoint, auth_type, models, rate_limit, feasibility, risk_level, status, notes)
     VALUES (@provider_id, @method, @endpoint, @auth_type, @models, @rate_limit, @feasibility, @risk_level, @status, @notes)
     ON CONFLICT(provider_id, method, endpoint) DO UPDATE SET
       auth_type = excluded.auth_type,
       models = excluded.models,
       rate_limit = excluded.rate_limit,
       feasibility = excluded.feasibility,
       risk_level = excluded.risk_level,
       status = excluded.status,
       notes = excluded.notes`
  ).run({
    provider_id: result.providerId,
    method: result.method,
    endpoint: result.endpoint ?? null,
    auth_type: result.authType,
    models,
    rate_limit: result.rateLimit ?? null,
    feasibility: result.feasibility,
    risk_level: result.riskLevel,
    status: result.status,
    notes: result.notes ?? null,
  });

  const row = db
    .prepare(
      `SELECT * FROM discovery_results
       WHERE provider_id = ? AND method = ? AND ifnull(endpoint, '') = ifnull(?, '')`
    )
    .get(result.providerId, result.method, result.endpoint ?? null) as DiscoveryRow | undefined;
  // The row was just written, so it must exist.
  return rowToResult(row!);
}

export function markVerified(id: number): DiscoveryResult | null {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  const info = db
    .prepare(
      "UPDATE discovery_results SET status = 'verified', verified_at = datetime('now') WHERE id = ?"
    )
    .run(id);
  if (info.changes === 0) return null;
  return getDiscoveryResultById(id);
}

export function deleteDiscoveryResult(id: number): boolean {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  const info = db.prepare("DELETE FROM discovery_results WHERE id = ?").run(id);
  return info.changes > 0;
}
