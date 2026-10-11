/**
 * Configuration Audit Trail
 *
 * Records every change to provider connections, combos, and routing
 * policies with before/after snapshots and diff detection.
 * Enables rollback to previous configurations when changes cause issues.
 *
 * Each entry captures:
 * - What changed (target type + ID)
 * - Who/what triggered the change (source)
 * - Before/after state snapshots
 * - Computed diff summary
 * - Optional human notes
 */

import { randomUUID } from "node:crypto";
import { getDbInstance } from "../lib/db/core";
import { getSelectedPostgresDatabase } from "../lib/db/controlPlaneContext";
import {
  configAuditEntryParams,
  createConfigAuditEntry,
  rowToConfigAuditEntry as rowToEntry,
  summarizeConfigAuditRows,
  type ConfigAuditRow,
  type ConfigAuditLogOptions,
  type ConfigAuditSummary,
} from "../lib/db/configAuditModel";
import { createPostgresConfigAuditRepository } from "../lib/db/postgres/configAuditRepository";
export { computeDiff } from "../lib/db/configAuditModel";
export type { ConfigAuditLogOptions, ConfigAuditSummary } from "../lib/db/configAuditModel";

/** Types of configuration entities that can be audited */
export type AuditTarget = "provider" | "combo" | "policy" | "connection" | "settings";

/** How the change was triggered */
export type AuditSource = "dashboard" | "api" | "sync" | "auto-healing" | "cli" | "mcp";

/** Type of change */
export type AuditAction = "create" | "update" | "delete" | "enable" | "disable";

/** A single audit log entry */
export interface ConfigAuditEntry {
  /** Unique entry ID */
  id: string;
  /** ISO timestamp of the change */
  timestamp: string;
  /** Type of change */
  action: AuditAction;
  /** What type of entity was changed */
  target: AuditTarget;
  /** ID of the changed entity */
  targetId: string;
  /** Human-readable name of the entity */
  targetName: string;
  /** State before the change (null for creates) */
  before: Record<string, unknown> | null;
  /** State after the change (null for deletes) */
  after: Record<string, unknown> | null;
  /** How the change was triggered */
  source: AuditSource;
  /** Computed diff summary */
  diff: ConfigDiff;
  /** Optional human note */
  note: string | null;
}

/** Computed diff between two states */
export interface ConfigDiff {
  /** Keys that were added */
  added: string[];
  /** Keys that were removed */
  removed: string[];
  /** Keys whose values changed */
  changed: Array<{ key: string; from: unknown; to: unknown }>;
  /** True if the states are identical */
  isEmpty: boolean;
}

/** Configuration snapshot for export/import */
export interface ConfigSnapshot {
  /** ISO timestamp when snapshot was taken */
  timestamp: string;
  /** Semantic version tag */
  version: string;
  /** Description of the snapshot */
  description: string;
  /** Full configuration data */
  data: Record<string, unknown>;
}

// ── SQLite-backed store ───────────────────────────────────────────────────────

let idCounter = 0;

function generateId(): string {
  idCounter++;
  const ts = Date.now().toString(36);
  const seq = idCounter.toString(36).padStart(4, "0");
  return `audit-${ts}-${seq}`;
}

function db() {
  return getDbInstance();
}

/**
 * Record a configuration change in the audit log.
 */
export function recordChange(
  action: AuditAction,
  target: AuditTarget,
  targetId: string,
  targetName: string,
  before: Record<string, unknown> | null,
  after: Record<string, unknown> | null,
  source: AuditSource,
  note?: string | null
): ConfigAuditEntry {
  const entry = createConfigAuditEntry(
    generateId(),
    action,
    target,
    targetId,
    targetName,
    before,
    after,
    source,
    note
  );

  db()
    .prepare(
      `INSERT INTO config_audit_log
       (id, timestamp, action, target, target_id, target_name, before_json, after_json, diff_json, source, note)
     VALUES
       (@id, @timestamp, @action, @target, @targetId, @targetName, @beforeJson, @afterJson, @diffJson, @source, @note)`
    )
    .run(configAuditEntryParams(entry));

  return entry;
}

/**
 * Get audit entries, optionally filtered.
 */
export function getAuditLog(options?: ConfigAuditLogOptions): {
  entries: ConfigAuditEntry[];
  total: number;
} {
  const where: string[] = [];
  const params: Record<string, unknown> = {};

  if (options?.target) {
    where.push("target = @target");
    params.target = options.target;
  }
  if (options?.targetId) {
    where.push("target_id = @targetId");
    params.targetId = options.targetId;
  }
  if (options?.action) {
    where.push("action = @action");
    params.action = options.action;
  }
  if (options?.source) {
    where.push("source = @source");
    params.source = options.source;
  }
  if (options?.since) {
    where.push("timestamp >= @since");
    params.since = options.since;
  }

  const whereSql = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";

  const totalRow = db()
    .prepare(`SELECT COUNT(*) AS c FROM config_audit_log ${whereSql}`)
    .get(params) as { c: number };
  const total = totalRow.c;

  const offset = options?.offset ?? 0;
  const limit = options?.limit ?? 50;

  const rows = db()
    .prepare(
      `SELECT * FROM config_audit_log ${whereSql} ORDER BY datetime(timestamp) DESC, id DESC LIMIT @limit OFFSET @offset`
    )
    .all({ ...params, limit, offset }) as ConfigAuditRow[];

  return { entries: rows.map(rowToEntry), total };
}

/**
 * Get a specific audit entry by ID.
 */
export function getAuditEntry(id: string): ConfigAuditEntry | null {
  const row = db().prepare("SELECT * FROM config_audit_log WHERE id = @id").get({ id }) as
    ConfigAuditRow | undefined;
  return row ? rowToEntry(row) : null;
}

/**
 * Get the state of an entity before a specific audit entry.
 * Enables rollback by returning the `before` snapshot.
 */
export function getRollbackState(entryId: string): Record<string, unknown> | null {
  const entry = getAuditEntry(entryId);
  if (!entry) return null;
  return entry.before;
}

/**
 * Create a full configuration snapshot for export.
 */
export function createSnapshot(
  version: string,
  description: string,
  configData: Record<string, unknown>
): ConfigSnapshot {
  return {
    timestamp: new Date().toISOString(),
    version,
    description,
    data: JSON.parse(JSON.stringify(configData)), // deep clone
  };
}

/**
 * Get summary statistics of the audit log.
 */
export function getAuditSummary(): ConfigAuditSummary {
  const rows = db()
    .prepare("SELECT * FROM config_audit_log ORDER BY datetime(timestamp) DESC, id DESC")
    .all() as ConfigAuditRow[];
  return summarizeConfigAuditRows(rows);
}

/**
 * Reset the audit log. Useful for testing.
 */
export function resetAuditLog(): void {
  db().prepare("DELETE FROM config_audit_log").run();
  idCounter = 0;
}

/** Native asynchronous siblings; synchronous SQLite callers retain their existing contract. */
export async function recordChangeAsync(
  action: AuditAction,
  target: AuditTarget,
  targetId: string,
  targetName: string,
  before: Record<string, unknown> | null,
  after: Record<string, unknown> | null,
  source: AuditSource,
  note?: string | null
): Promise<ConfigAuditEntry> {
  const database = getSelectedPostgresDatabase();
  if (!database)
    return recordChange(action, target, targetId, targetName, before, after, source, note);
  const id = `audit-${Date.now().toString(36)}-${randomUUID()}`;
  const entry = createConfigAuditEntry(
    id,
    action,
    target,
    targetId,
    targetName,
    before,
    after,
    source,
    note
  );
  // Capture JSON before the first await, as the synchronous source writer does.
  await createPostgresConfigAuditRepository(database).insert(configAuditEntryParams(entry));
  return entry;
}
export async function getAuditLogAsync(
  options?: ConfigAuditLogOptions
): Promise<{ entries: ConfigAuditEntry[]; total: number }> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getAuditLog(options);
  const result = await createPostgresConfigAuditRepository(database).list(options);
  return { entries: result.rows.map(rowToEntry), total: result.total };
}
export async function getAuditEntryAsync(id: string): Promise<ConfigAuditEntry | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getAuditEntry(id);
  const row = await createPostgresConfigAuditRepository(database).find(id);
  return row ? rowToEntry(row) : null;
}
export async function getRollbackStateAsync(
  entryId: string
): Promise<Record<string, unknown> | null> {
  const entry = await getAuditEntryAsync(entryId);
  return entry ? entry.before : null;
}
export async function getAuditSummaryAsync(): Promise<ConfigAuditSummary> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getAuditSummary();
  return summarizeConfigAuditRows(await createPostgresConfigAuditRepository(database).all());
}
export async function resetAuditLogAsync(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return resetAuditLog();
  await createPostgresConfigAuditRepository(database).clear();
}
