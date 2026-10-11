import { randomUUID } from "node:crypto";
import type { JobRecord, JobRun } from "../../jobRegistry/core";
import type { PlaygroundPresetListItem } from "../playgroundPresets";
import type { DiscoveryResult } from "../discovery";
import type {
  LogExportDestinationRow,
  CreateLogExportDestinationInput,
  UpdateLogExportDestinationInput,
} from "../logExportDestinations";
import { decrypt } from "../encryption";
import type { PostgresTransaction } from "./database";
import { affected, first, rows, safeInteger, lockServices, protectSecret } from "./serviceQuery";

type Row = Record<string, unknown>;
function parseObject(value: unknown): Record<string, unknown> {
  if (typeof value !== "string") return {};
  try {
    const parsed: unknown = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
}
function mapJob(row: Row): JobRecord {
  return {
    id: String(row.id),
    type: row.type as JobRecord["type"],
    cron: row.cron as string | null,
    intervalMs: row.interval_ms as number | null,
    enabled: row.enabled === 1,
    envFlag: row.env_flag as string | null,
    config: parseObject(row.config),
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
  };
}
function mapRun(row: Row): JobRun {
  return {
    id: safeInteger(row.id),
    jobId: String(row.job_id),
    startedAt: String(row.started_at),
    finishedAt: row.finished_at as string | null,
    status: row.status as JobRun["status"],
    errorMessage: row.error_message as string | null,
    recordsAffected: safeInteger(row.records_affected ?? 0),
    durationMs: row.duration_ms as number | null,
  };
}
export async function getAllJobs(transaction: PostgresTransaction): Promise<JobRecord[]> {
  return (await rows(transaction, 'SELECT * FROM omniroute.jobs ORDER BY id COLLATE "C"')).map(
    mapJob
  );
}
export async function getJob(
  transaction: PostgresTransaction,
  id: string
): Promise<JobRecord | null> {
  const row = await first(transaction, "SELECT * FROM omniroute.jobs WHERE id = $1", [id]);
  return row ? mapJob(row) : null;
}
export async function upsertJob(transaction: PostgresTransaction, job: JobRecord): Promise<void> {
  await transaction.query(
    `INSERT INTO omniroute.jobs (id, type, cron, interval_ms, enabled, env_flag, config, created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (id) DO UPDATE SET
    type = excluded.type, cron = excluded.cron, interval_ms = excluded.interval_ms,
    env_flag = excluded.env_flag, config = excluded.config, updated_at = excluded.updated_at`,
    [
      job.id,
      job.type,
      job.cron,
      job.intervalMs,
      job.enabled ? 1 : 0,
      job.envFlag,
      JSON.stringify(job.config ?? {}),
      job.createdAt,
      new Date().toISOString(),
    ]
  );
}
export async function updateJobEnabled(
  transaction: PostgresTransaction,
  id: string,
  enabled: boolean
): Promise<void> {
  await transaction.query("UPDATE omniroute.jobs SET enabled = $1, updated_at = $2 WHERE id = $3", [
    enabled ? 1 : 0,
    new Date().toISOString(),
    id,
  ]);
}
export async function recordRun(
  transaction: PostgresTransaction,
  jobId: string,
  status: JobRun["status"],
  opts: import("../jobRegistryDb").RecordRunOptions = {}
): Promise<void> {
  const startedAt = opts.startedAt ?? new Date().toISOString();
  const finishedAt =
    status === "running"
      ? null
      : opts.startedAt && opts.durationMs != null
        ? new Date(new Date(opts.startedAt).getTime() + opts.durationMs).toISOString()
        : new Date().toISOString();
  await transaction.query(
    `INSERT INTO omniroute.job_runs (job_id, started_at, finished_at, status, error_message, records_affected, duration_ms)
    VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [
      jobId,
      startedAt,
      finishedAt,
      status,
      opts.errorMessage ?? null,
      opts.recordsAffected ?? 0,
      opts.durationMs ?? null,
    ]
  );
}
export async function pruneRuns(
  transaction: PostgresTransaction,
  jobId: string,
  maxRuns = 100,
  maxDays = 30
): Promise<void> {
  await transaction.query(
    `DELETE FROM omniroute.job_runs WHERE job_id = $1 AND id NOT IN
    (SELECT id FROM omniroute.job_runs WHERE job_id = $1 ORDER BY started_at COLLATE "C" DESC, id DESC LIMIT CASE WHEN $2::bigint < 0 THEN NULL ELSE $2::bigint END)
    AND started_at < $3`,
    [jobId, maxRuns, new Date(Date.now() - maxDays * 86400000).toISOString()]
  );
}
export async function getRuns(
  transaction: PostgresTransaction,
  jobId: string,
  limit = 20
): Promise<JobRun[]> {
  return (
    await rows(
      transaction,
      'SELECT * FROM omniroute.job_runs WHERE job_id = $1 ORDER BY started_at COLLATE "C" DESC, id DESC LIMIT CASE WHEN $2::bigint < 0 THEN NULL ELSE $2::bigint END',
      [jobId, limit]
    )
  ).map(mapRun);
}
export async function cleanupOrphanedRuns(
  transaction: PostgresTransaction,
  timeoutMinutes = 5
): Promise<void> {
  await transaction.query(
    "UPDATE omniroute.job_runs SET status = 'failure', error_message = 'orphaned: exceeded timeout', finished_at = $1 WHERE status = 'running' AND started_at < $2",
    [new Date().toISOString(), new Date(Date.now() - timeoutMinutes * 60000).toISOString()]
  );
}
function mapPreset(row: Row): PlaygroundPresetListItem {
  return {
    id: String(row.id),
    name: String(row.name),
    endpoint: String(row.endpoint),
    model: String(row.model),
    system: row.system as string | null,
    params: parseObject(row.params_json),
    created_at: String(row.created_at),
  };
}
export async function listPlaygroundPresets(
  transaction: PostgresTransaction,
  options?: { limit?: number; offset?: number }
) {
  const page =
    options?.limit === undefined
      ? ""
      : " LIMIT CASE WHEN $1::bigint < 0 THEN NULL ELSE $1::bigint END OFFSET GREATEST($2::bigint, 0)";
  const items = (
    await rows(
      transaction,
      `SELECT * FROM omniroute.playground_presets ORDER BY created_at COLLATE "C" DESC, id COLLATE "C" DESC${page}`,
      page ? [options!.limit, options!.offset ?? 0] : []
    )
  ).map(mapPreset);
  const total = safeInteger(
    (
      await first<{ count: string }>(
        transaction,
        "SELECT count(*) AS count FROM omniroute.playground_presets"
      )
    )?.count ?? 0
  );
  return { items, total };
}
export async function getPlaygroundPreset(
  transaction: PostgresTransaction,
  id: string
): Promise<PlaygroundPresetListItem | null> {
  const row = await first(transaction, "SELECT * FROM omniroute.playground_presets WHERE id = $1", [
    id,
  ]);
  return row ? mapPreset(row) : null;
}
export async function createPlaygroundPreset(
  transaction: PostgresTransaction,
  input: Parameters<typeof import("../playgroundPresets").createPlaygroundPreset>[0]
) {
  const id = randomUUID();
  await transaction.query(
    "INSERT INTO omniroute.playground_presets (id, name, endpoint, model, system, params_json) VALUES ($1, $2, $3, $4, $5, $6)",
    [
      id,
      input.name,
      input.endpoint,
      input.model,
      input.system ?? null,
      JSON.stringify(input.params ?? {}),
    ]
  );
  return (await getPlaygroundPreset(transaction, id))!;
}
export async function updatePlaygroundPreset(
  transaction: PostgresTransaction,
  id: string,
  patch: Parameters<typeof import("../playgroundPresets").updatePlaygroundPreset>[1]
) {
  await lockServices(transaction, "presets");
  const existing = await getPlaygroundPreset(transaction, id);
  if (!existing) return null;
  const columns: Row = {};
  for (const key of ["name", "endpoint", "model"] as const)
    if (patch[key] !== undefined) columns[key] = patch[key];
  if ("system" in patch) columns.system = patch.system ?? null;
  if (patch.params !== undefined) columns.params_json = JSON.stringify(patch.params);
  const keys = Object.keys(columns);
  if (keys.length)
    await transaction.query(
      `UPDATE omniroute.playground_presets SET ${keys.map((key, index) => `${key} = $${index + 1}`).join(", ")} WHERE id = $${keys.length + 1}`,
      [...Object.values(columns), id]
    );
  return getPlaygroundPreset(transaction, id);
}
export async function deletePlaygroundPreset(transaction: PostgresTransaction, id: string) {
  return (
    (await affected(transaction, "DELETE FROM omniroute.playground_presets WHERE id = $1", [id])) >
    0
  );
}
async function getCliValue(
  transaction: PostgresTransaction,
  namespace: string,
  toolId: string
): Promise<string | null> {
  return (
    (
      await first<{ value: string }>(
        transaction,
        "SELECT value FROM omniroute.key_value WHERE namespace = $1 AND key = $2",
        [namespace, toolId]
      )
    )?.value ?? null
  );
}
export async function saveCliToolLastConfigured(
  transaction: PostgresTransaction,
  toolId: string,
  timestamp = new Date().toISOString()
) {
  await transaction.query(
    "INSERT INTO omniroute.key_value (namespace, key, value) VALUES ('cliToolLastConfig', $1, $2) ON CONFLICT (namespace, key) DO UPDATE SET value = excluded.value",
    [toolId, JSON.stringify(timestamp)]
  );
}
export async function getCliToolLastConfigured(
  transaction: PostgresTransaction,
  toolId: string
): Promise<string | null> {
  const raw = await getCliValue(transaction, "cliToolLastConfig", toolId);
  if (raw === null) return null;
  try {
    const value: unknown = JSON.parse(raw);
    return typeof value === "string" ? value : null;
  } catch {
    return null;
  }
}
export async function getAllCliToolLastConfigured(
  transaction: PostgresTransaction
): Promise<Record<string, string>> {
  const result: Record<string, string> = {};
  for (const row of await rows<{ key: string; value: string }>(
    transaction,
    "SELECT key, value FROM omniroute.key_value WHERE namespace = 'cliToolLastConfig'"
  )) {
    try {
      const value: unknown = JSON.parse(row.value);
      if (typeof value === "string") result[row.key] = value;
    } catch {
      /* Preserve corrupt-row skip behavior. */
    }
  }
  return result;
}
export async function deleteCliToolLastConfigured(
  transaction: PostgresTransaction,
  toolId: string
) {
  await transaction.query(
    "DELETE FROM omniroute.key_value WHERE namespace = 'cliToolLastConfig' AND key = $1",
    [toolId]
  );
}
export async function saveCliToolInitialConfig(
  transaction: PostgresTransaction,
  toolId: string,
  config: Record<string, unknown>
): Promise<boolean> {
  return (
    (await affected(
      transaction,
      "INSERT INTO omniroute.key_value (namespace, key, value) VALUES ('cliToolInitialConfig', $1, $2) ON CONFLICT (namespace, key) DO NOTHING",
      [toolId, protectSecret(JSON.stringify(config))]
    )) > 0
  );
}
export async function getCliToolInitialConfig(
  transaction: PostgresTransaction,
  toolId: string
): Promise<Record<string, unknown> | null> {
  const raw = await getCliValue(transaction, "cliToolInitialConfig", toolId);
  if (raw === null) return null;
  const decoded = decrypt(raw);
  if (!decoded) return null;
  try {
    const value: unknown = JSON.parse(decoded);
    return value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}
export async function deleteCliToolInitialConfig(transaction: PostgresTransaction, toolId: string) {
  await transaction.query(
    "DELETE FROM omniroute.key_value WHERE namespace = 'cliToolInitialConfig' AND key = $1",
    [toolId]
  );
}
function mapDiscovery(row: Row): DiscoveryResult {
  return {
    id: safeInteger(row.id),
    providerId: row.provider_id as string,
    method: row.method as DiscoveryResult["method"],
    authType: row.auth_type as DiscoveryResult["authType"],
    endpoint: row.endpoint as string,
    modelsJson: row.models_json as string,
    rateLimit: row.rate_limit as string,
    feasibility: row.feasibility as number,
    riskLevel: row.risk_level as DiscoveryResult["riskLevel"],
    status: row.status as DiscoveryResult["status"],
    notes: row.notes as string,
    discoveredAt: row.discovered_at as string,
    verifiedAt: row.verified_at as string,
  };
}
export async function insertDiscoveryResult(
  transaction: PostgresTransaction,
  input: DiscoveryResult
): Promise<number> {
  const row = await first<{ id: string }>(
    transaction,
    `INSERT INTO omniroute.discovery_results
    (provider_id, method, auth_type, endpoint, models, rate_limit, feasibility, risk_level, status, notes, discovered_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id`,
    [
      input.providerId,
      input.method,
      input.authType,
      input.endpoint ?? null,
      input.modelsJson ?? "[]",
      input.rateLimit ?? null,
      input.feasibility ?? 0,
      input.riskLevel ?? "none",
      input.status ?? "pending",
      input.notes ?? null,
      new Date().toISOString(),
    ]
  );
  return safeInteger(row?.id);
}
export async function listDiscoveryResults(
  transaction: PostgresTransaction,
  status?: string
): Promise<DiscoveryResult[]> {
  return (
    await rows(
      transaction,
      `SELECT *, models AS models_json FROM omniroute.discovery_results ${status ? "WHERE status = $1" : ""} ORDER BY discovered_at COLLATE "C" DESC, id DESC`,
      status ? [status] : []
    )
  ).map(mapDiscovery);
}
export async function getDiscoveryResultById(
  transaction: PostgresTransaction,
  id: number
): Promise<DiscoveryResult | null> {
  const row = await first(
    transaction,
    "SELECT *, models AS models_json FROM omniroute.discovery_results WHERE id = $1",
    [id]
  );
  return row ? mapDiscovery(row) : null;
}
export async function updateDiscoveryStatus(
  transaction: PostgresTransaction,
  id: number,
  status: string,
  notes?: string
) {
  return (
    (await affected(
      transaction,
      "UPDATE omniroute.discovery_results SET status = $1, notes = COALESCE($2, notes), verified_at = CASE WHEN $1 = 'verified' THEN $3 ELSE verified_at END, updated_at = $3 WHERE id = $4",
      [status, notes ?? null, new Date().toISOString(), id]
    )) > 0
  );
}
export async function deleteDiscoveryResult(transaction: PostgresTransaction, id: number) {
  return (
    (await affected(transaction, "DELETE FROM omniroute.discovery_results WHERE id = $1", [id])) > 0
  );
}
function mapDestination(row: Row): LogExportDestinationRow {
  return {
    id: String(row.id),
    name: String(row.name),
    type: String(row.type),
    enabled: row.enabled === 1,
    config: parseObject(row.config),
    batchSize: safeInteger(row.batch_size),
    includeBodies: row.include_bodies === 1,
    maxBodyBytes: safeInteger(row.max_body_bytes ?? 262144),
    maxRowsPerRun: safeInteger(row.max_rows_per_run),
    cursorRowId: safeInteger(row.cursor_row_id ?? 0),
    exportedTotal: safeInteger(row.exported_total ?? 0),
    lastRunAt: row.last_run_at as string | null,
    lastStatus: row.last_status as LogExportDestinationRow["lastStatus"],
    lastError: row.last_error as string | null,
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
  };
}
export async function getLogExportDestinations(
  transaction: PostgresTransaction
): Promise<LogExportDestinationRow[]> {
  return (
    await rows(
      transaction,
      'SELECT * FROM omniroute.log_export_destinations ORDER BY created_at COLLATE "C" ASC, id COLLATE "C" ASC'
    )
  ).map(mapDestination);
}
export async function getEnabledLogExportDestinations(
  transaction: PostgresTransaction
): Promise<LogExportDestinationRow[]> {
  return (
    await rows(
      transaction,
      'SELECT * FROM omniroute.log_export_destinations WHERE enabled = 1 ORDER BY created_at COLLATE "C" ASC, id COLLATE "C" ASC'
    )
  ).map(mapDestination);
}
export async function getLogExportDestination(
  transaction: PostgresTransaction,
  id: string
): Promise<LogExportDestinationRow | null> {
  const row = await first(
    transaction,
    "SELECT * FROM omniroute.log_export_destinations WHERE id = $1",
    [id]
  );
  return row ? mapDestination(row) : null;
}
export async function createLogExportDestination(
  transaction: PostgresTransaction,
  input: CreateLogExportDestinationInput
): Promise<LogExportDestinationRow> {
  const id = randomUUID();
  const now = new Date().toISOString();
  await transaction.query(
    `INSERT INTO omniroute.log_export_destinations (id, name, type, enabled, config, batch_size, include_bodies, max_body_bytes, max_rows_per_run, cursor_row_id, exported_total, created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, 0, $10, $10)`,
    [
      id,
      input.name,
      input.type,
      input.enabled ? 1 : 0,
      JSON.stringify(input.config ?? {}),
      input.batchSize ?? 500,
      input.includeBodies ? 1 : 0,
      input.maxBodyBytes ?? 262144,
      input.maxRowsPerRun ?? 10000,
      now,
    ]
  );
  return (await getLogExportDestination(transaction, id))!;
}
export async function updateLogExportDestination(
  transaction: PostgresTransaction,
  id: string,
  input: UpdateLogExportDestinationInput
): Promise<LogExportDestinationRow | null> {
  await lockServices(transaction, "log-export");
  const existing = await getLogExportDestination(transaction, id);
  if (!existing) return null;
  await transaction.query(
    `UPDATE omniroute.log_export_destinations SET name = $1, enabled = $2, config = $3, batch_size = $4,
    include_bodies = $5, max_body_bytes = $6, max_rows_per_run = $7, updated_at = $8 WHERE id = $9`,
    [
      input.name ?? existing.name,
      (input.enabled ?? existing.enabled) ? 1 : 0,
      JSON.stringify(input.config ?? existing.config),
      input.batchSize ?? existing.batchSize,
      (input.includeBodies ?? existing.includeBodies) ? 1 : 0,
      input.maxBodyBytes ?? existing.maxBodyBytes,
      input.maxRowsPerRun ?? existing.maxRowsPerRun,
      new Date().toISOString(),
      id,
    ]
  );
  return getLogExportDestination(transaction, id);
}
export async function deleteLogExportDestination(
  transaction: PostgresTransaction,
  id: string
): Promise<boolean> {
  return (
    (await affected(transaction, "DELETE FROM omniroute.log_export_destinations WHERE id = $1", [
      id,
    ])) > 0
  );
}
export async function advanceLogExportCursor(
  transaction: PostgresTransaction,
  id: string,
  cursorRowId: number,
  exported: number
): Promise<void> {
  safeInteger(cursorRowId);
  safeInteger(exported);
  await transaction.query(
    `UPDATE omniroute.log_export_destinations SET cursor_row_id = $1,
    exported_total = exported_total + $2, updated_at = $3 WHERE id = $4 AND cursor_row_id < $1`,
    [cursorRowId, exported, new Date().toISOString(), id]
  );
}
export async function resetLogExportCursor(
  transaction: PostgresTransaction,
  id: string,
  cursorRowId = 0
): Promise<void> {
  safeInteger(cursorRowId);
  await transaction.query(
    "UPDATE omniroute.log_export_destinations SET cursor_row_id = $1, updated_at = $2 WHERE id = $3",
    [cursorRowId, new Date().toISOString(), id]
  );
}
export async function recordLogExportRun(
  transaction: PostgresTransaction,
  id: string,
  status: "success" | "failure",
  error: string | null
): Promise<void> {
  await transaction.query(
    "UPDATE omniroute.log_export_destinations SET last_run_at = $1, last_status = $2, last_error = $3, updated_at = $1 WHERE id = $4",
    [new Date().toISOString(), status, error, id]
  );
}
