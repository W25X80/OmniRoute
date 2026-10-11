import type {
  AuditAction,
  AuditSource,
  AuditTarget,
  ConfigAuditEntry,
  ConfigDiff,
} from "../../domain/configAudit";

export interface ConfigAuditRow {
  id: string;
  timestamp: string;
  action: string;
  target: string;
  target_id: string;
  target_name: string;
  before_json: string | null;
  after_json: string | null;
  diff_json: string;
  source: string;
  note: string | null;
}

export function rowToConfigAuditEntry(row: ConfigAuditRow): ConfigAuditEntry {
  return {
    id: row.id,
    timestamp: row.timestamp,
    action: row.action as AuditAction,
    target: row.target as AuditTarget,
    targetId: row.target_id,
    targetName: row.target_name,
    before:
      row.before_json === null
        ? null
        : (JSON.parse(row.before_json) as Record<string, unknown> | null),
    after:
      row.after_json === null
        ? null
        : (JSON.parse(row.after_json) as Record<string, unknown> | null),
    source: row.source as AuditSource,
    diff: JSON.parse(row.diff_json) as ConfigDiff,
    note: row.note,
  };
}

/**
 * Compute a structured diff between two configuration states.
 */
export function computeDiff(
  before: Record<string, unknown> | null,
  after: Record<string, unknown> | null
): ConfigDiff {
  const beforeKeys = new Set(before ? Object.keys(before) : []);
  const afterKeys = new Set(after ? Object.keys(after) : []);

  const added: string[] = [];
  const removed: string[] = [];
  const changed: Array<{ key: string; from: unknown; to: unknown }> = [];

  // Keys in after but not in before → added
  for (const key of afterKeys) {
    if (!beforeKeys.has(key)) {
      added.push(key);
    }
  }

  // Keys in before but not in after → removed
  for (const key of beforeKeys) {
    if (!afterKeys.has(key)) {
      removed.push(key);
    }
  }

  // Keys in both → check for changes
  for (const key of beforeKeys) {
    if (afterKeys.has(key)) {
      const beforeVal = before![key];
      const afterVal = after![key];
      if (JSON.stringify(beforeVal) !== JSON.stringify(afterVal)) {
        changed.push({ key, from: beforeVal, to: afterVal });
      }
    }
  }

  return {
    added,
    removed,
    changed,
    isEmpty: added.length === 0 && removed.length === 0 && changed.length === 0,
  };
}

export interface ConfigAuditLogOptions {
  target?: AuditTarget;
  targetId?: string;
  action?: AuditAction;
  source?: AuditSource;
  since?: string;
  limit?: number;
  offset?: number;
}
export interface ConfigAuditSummary {
  totalEntries: number;
  byTarget: Record<string, number>;
  byAction: Record<string, number>;
  bySource: Record<string, number>;
  oldestEntry: string | null;
  newestEntry: string | null;
}
export function createConfigAuditEntry(
  id: string,
  action: AuditAction,
  target: AuditTarget,
  targetId: string,
  targetName: string,
  before: Record<string, unknown> | null,
  after: Record<string, unknown> | null,
  source: AuditSource,
  note?: string | null
): ConfigAuditEntry {
  return {
    id,
    timestamp: new Date().toISOString(),
    action,
    target,
    targetId,
    targetName,
    before,
    after,
    source,
    diff: computeDiff(before, after),
    note: note ?? null,
  };
}
export function configAuditEntryParams(entry: ConfigAuditEntry) {
  return {
    id: entry.id,
    timestamp: entry.timestamp,
    action: entry.action,
    target: entry.target,
    targetId: entry.targetId,
    targetName: entry.targetName,
    beforeJson: entry.before === null ? null : JSON.stringify(entry.before),
    afterJson: entry.after === null ? null : JSON.stringify(entry.after),
    diffJson: JSON.stringify(entry.diff),
    source: entry.source,
    note: entry.note,
  };
}
export function summarizeConfigAuditRows(rows: readonly ConfigAuditRow[]): ConfigAuditSummary {
  const byTarget: Record<string, number> = {};
  const byAction: Record<string, number> = {};
  const bySource: Record<string, number> = {};

  for (const row of rows) {
    byTarget[row.target] = (byTarget[row.target] || 0) + 1;
    byAction[row.action] = (byAction[row.action] || 0) + 1;
    bySource[row.source] = (bySource[row.source] || 0) + 1;
  }

  return {
    totalEntries: rows.length,
    byTarget,
    byAction,
    bySource,
    oldestEntry: rows.length > 0 ? rows[rows.length - 1].timestamp : null,
    newestEntry: rows.length > 0 ? rows[0].timestamp : null,
  };
}
