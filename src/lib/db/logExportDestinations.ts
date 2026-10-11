import { getSelectedPostgresDatabase } from "./controlPlaneContext";
/**
 * Log-export destination persistence (migration 170).
 *
 * snake_case in SQLite, camelCase in the returned objects. `config` is a JSON string
 * whose secret keys are already ciphertext when they reach this module — encryption
 * lives in src/lib/logExport/secrets.ts, next to the destination registry that knows
 * which keys are secret.
 */

import { randomUUID } from "node:crypto";
import { getDbInstance } from "./core";

export interface LogExportDestinationRow {
  id: string;
  name: string;
  type: string;
  enabled: boolean;
  config: Record<string, unknown>;
  batchSize: number;
  includeBodies: boolean;
  maxBodyBytes: number;
  maxRowsPerRun: number;
  cursorRowId: number;
  exportedTotal: number;
  lastRunAt: string | null;
  lastStatus: "success" | "failure" | null;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface CreateLogExportDestinationInput {
  name: string;
  type: string;
  enabled?: boolean;
  config: Record<string, unknown>;
  batchSize?: number;
  includeBodies?: boolean;
  maxBodyBytes?: number;
  maxRowsPerRun?: number;
}

export interface UpdateLogExportDestinationInput {
  name?: string;
  enabled?: boolean;
  config?: Record<string, unknown>;
  batchSize?: number;
  includeBodies?: boolean;
  maxBodyBytes?: number;
  maxRowsPerRun?: number;
}

function mapRow(row: any): LogExportDestinationRow {
  let config: Record<string, unknown> = {};
  if (row.config) {
    try {
      const parsed = JSON.parse(row.config);
      if (parsed && typeof parsed === "object") config = parsed as Record<string, unknown>;
    } catch {
      config = {};
    }
  }
  return {
    id: row.id,
    name: row.name,
    type: row.type,
    enabled: row.enabled === 1,
    config,
    batchSize: row.batch_size,
    includeBodies: row.include_bodies === 1,
    maxBodyBytes: row.max_body_bytes ?? 262_144,
    maxRowsPerRun: row.max_rows_per_run,
    cursorRowId: row.cursor_row_id ?? 0,
    exportedTotal: row.exported_total ?? 0,
    lastRunAt: row.last_run_at ?? null,
    lastStatus: row.last_status ?? null,
    lastError: row.last_error ?? null,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export function getLogExportDestinations(): LogExportDestinationRow[] {
  const db = getDbInstance();
  const rows = db.prepare("SELECT * FROM log_export_destinations ORDER BY created_at ASC").all();
  return rows.map(mapRow);
}

export function getEnabledLogExportDestinations(): LogExportDestinationRow[] {
  const db = getDbInstance();
  const rows = db
    .prepare("SELECT * FROM log_export_destinations WHERE enabled = 1 ORDER BY created_at ASC")
    .all();
  return rows.map(mapRow);
}

export function getLogExportDestination(id: string): LogExportDestinationRow | null {
  const db = getDbInstance();
  const row = db.prepare("SELECT * FROM log_export_destinations WHERE id = ?").get(id);
  return row ? mapRow(row) : null;
}

export function createLogExportDestination(
  input: CreateLogExportDestinationInput
): LogExportDestinationRow {
  const db = getDbInstance();
  const now = new Date().toISOString();
  const id = randomUUID();
  db.prepare(
    `INSERT INTO log_export_destinations
       (id, name, type, enabled, config, batch_size, include_bodies, max_body_bytes,
        max_rows_per_run, cursor_row_id, exported_total, created_at, updated_at)
     VALUES (@id, @name, @type, @enabled, @config, @batchSize, @includeBodies, @maxBodyBytes,
             @maxRowsPerRun, 0, 0, @now, @now)`
  ).run({
    id,
    name: input.name,
    type: input.type,
    enabled: input.enabled ? 1 : 0,
    config: JSON.stringify(input.config ?? {}),
    batchSize: input.batchSize ?? 500,
    includeBodies: input.includeBodies ? 1 : 0,
    maxBodyBytes: input.maxBodyBytes ?? 262_144,
    maxRowsPerRun: input.maxRowsPerRun ?? 10_000,
    now,
  });
  return getLogExportDestination(id) as LogExportDestinationRow;
}

export function updateLogExportDestination(
  id: string,
  input: UpdateLogExportDestinationInput
): LogExportDestinationRow | null {
  const existing = getLogExportDestination(id);
  if (!existing) return null;

  const db = getDbInstance();
  db.prepare(
    `UPDATE log_export_destinations
        SET name = @name,
            enabled = @enabled,
            config = @config,
            batch_size = @batchSize,
            include_bodies = @includeBodies,
            max_body_bytes = @maxBodyBytes,
            max_rows_per_run = @maxRowsPerRun,
            updated_at = @now
      WHERE id = @id`
  ).run({
    id,
    name: input.name ?? existing.name,
    enabled: (input.enabled ?? existing.enabled) ? 1 : 0,
    config: JSON.stringify(input.config ?? existing.config),
    batchSize: input.batchSize ?? existing.batchSize,
    includeBodies: (input.includeBodies ?? existing.includeBodies) ? 1 : 0,
    maxBodyBytes: input.maxBodyBytes ?? existing.maxBodyBytes,
    maxRowsPerRun: input.maxRowsPerRun ?? existing.maxRowsPerRun,
    now: new Date().toISOString(),
  });
  return getLogExportDestination(id);
}

export function deleteLogExportDestination(id: string): boolean {
  const db = getDbInstance();
  const result = db.prepare("DELETE FROM log_export_destinations WHERE id = ?").run(id);
  return result.changes > 0;
}

/** Advance the cursor after a batch is durably accepted by the destination. */
export function advanceLogExportCursor(id: string, cursorRowId: number, exported: number): void {
  const db = getDbInstance();
  db.prepare(
    `UPDATE log_export_destinations
        SET cursor_row_id = @cursorRowId,
            exported_total = exported_total + @exported,
            updated_at = @now
      WHERE id = @id`
  ).run({ id, cursorRowId, exported, now: new Date().toISOString() });
}

/**
 * Rewind the cursor. Used by the truncation guard: when `call_logs` has been purged
 * the surviving max(rowid) can be lower than the stored cursor, which would otherwise
 * make every future row invisible to the exporter.
 */
export function resetLogExportCursor(id: string, cursorRowId = 0): void {
  const db = getDbInstance();
  db.prepare(
    `UPDATE log_export_destinations
        SET cursor_row_id = @cursorRowId, updated_at = @now
      WHERE id = @id`
  ).run({ id, cursorRowId, now: new Date().toISOString() });
}

export function recordLogExportRun(
  id: string,
  status: "success" | "failure",
  error: string | null
): void {
  const db = getDbInstance();
  db.prepare(
    `UPDATE log_export_destinations
        SET last_run_at = @now, last_status = @status, last_error = @error, updated_at = @now
      WHERE id = @id`
  ).run({ id, status, error, now: new Date().toISOString() });
}

export async function getLogExportDestinationsAsync(
  ...args: Parameters<typeof getLogExportDestinations>
): Promise<ReturnType<typeof getLogExportDestinations>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.getLogExportDestinations(transaction, ...args)
    );
  }
  return getLogExportDestinations(...args);
}

export async function getEnabledLogExportDestinationsAsync(
  ...args: Parameters<typeof getEnabledLogExportDestinations>
): Promise<ReturnType<typeof getEnabledLogExportDestinations>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.getEnabledLogExportDestinations(transaction, ...args)
    );
  }
  return getEnabledLogExportDestinations(...args);
}

export async function getLogExportDestinationAsync(
  ...args: Parameters<typeof getLogExportDestination>
): Promise<ReturnType<typeof getLogExportDestination>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.getLogExportDestination(transaction, ...args)
    );
  }
  return getLogExportDestination(...args);
}

export async function createLogExportDestinationAsync(
  ...args: Parameters<typeof createLogExportDestination>
): Promise<ReturnType<typeof createLogExportDestination>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.createLogExportDestination(transaction, ...args)
    );
  }
  return createLogExportDestination(...args);
}

export async function updateLogExportDestinationAsync(
  ...args: Parameters<typeof updateLogExportDestination>
): Promise<ReturnType<typeof updateLogExportDestination>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.updateLogExportDestination(transaction, ...args)
    );
  }
  return updateLogExportDestination(...args);
}

export async function deleteLogExportDestinationAsync(
  ...args: Parameters<typeof deleteLogExportDestination>
): Promise<ReturnType<typeof deleteLogExportDestination>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.deleteLogExportDestination(transaction, ...args)
    );
  }
  return deleteLogExportDestination(...args);
}

export async function advanceLogExportCursorAsync(
  ...args: Parameters<typeof advanceLogExportCursor>
): Promise<ReturnType<typeof advanceLogExportCursor>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.advanceLogExportCursor(transaction, ...args)
    );
  }
  return advanceLogExportCursor(...args);
}

export async function resetLogExportCursorAsync(
  ...args: Parameters<typeof resetLogExportCursor>
): Promise<ReturnType<typeof resetLogExportCursor>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) => native.resetLogExportCursor(transaction, ...args));
  }
  return resetLogExportCursor(...args);
}

export async function recordLogExportRunAsync(
  ...args: Parameters<typeof recordLogExportRun>
): Promise<ReturnType<typeof recordLogExportRun>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) => native.recordLogExportRun(transaction, ...args));
  }
  return recordLogExportRun(...args);
}

/** PostgreSQL export identities are not reused by retention deletes. */
export function needsLogExportCursorReconciliation(): boolean {
  return getSelectedPostgresDatabase() === undefined;
}
