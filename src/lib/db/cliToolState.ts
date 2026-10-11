import { getSelectedPostgresDatabase } from "./controlPlaneContext";
/**
 * CLI Tool State Persistence
 *
 * Stores last-configured timestamps and initial config snapshots
 * for CLI tools in the key_value table.
 *
 * Namespaces:
 *   - cliToolLastConfig: ISO timestamp of last configuration
 *   - cliToolInitialConfig: JSON snapshot of pre-OmniRoute configuration
 *
 * @module lib/db/cliToolState
 */

import { getDbInstance, isBuildPhase, isCloud } from "./core";

type JsonRecord = Record<string, unknown>;

interface StatementLike<TRow = unknown> {
  all: (...params: unknown[]) => TRow[];
  get: (...params: unknown[]) => TRow | undefined;
  run: (...params: unknown[]) => { changes?: number };
}

interface DbLike {
  prepare: <TRow = unknown>(sql: string) => StatementLike<TRow>;
}

interface KeyValueRow {
  key: string;
  value: string;
}

function parseJsonValue(raw: string): unknown {
  try {
    return JSON.parse(raw) as unknown;
  } catch {
    return null;
  }
}

function toRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : null;
}

// ──────────────── Last Configured Timestamp ────────────────

/**
 * Save last-configured timestamp for a CLI tool.
 */
export function saveCliToolLastConfigured(
  toolId: string,
  timestamp: string = new Date().toISOString()
): void {
  if (isBuildPhase || isCloud) return;
  const db = getDbInstance() as unknown as DbLike;
  db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
    "cliToolLastConfig",
    toolId,
    JSON.stringify(timestamp)
  );
}

/**
 * Get last-configured timestamp for a CLI tool.
 * @returns ISO timestamp string or null if never configured.
 */
export function getCliToolLastConfigured(toolId: string): string | null {
  if (isBuildPhase || isCloud) return null;
  const db = getDbInstance() as unknown as DbLike;
  const row = db
    .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
    .get("cliToolLastConfig", toolId);
  if (!row) return null;
  const parsed = parseJsonValue((row as KeyValueRow).value);
  return typeof parsed === "string" ? parsed : null;
}

/**
 * Get all CLI tool last-configured timestamps.
 * @returns Record<toolId, ISO timestamp>
 */
export function getAllCliToolLastConfigured(): Record<string, string> {
  if (isBuildPhase || isCloud) return {};
  const db = getDbInstance() as unknown as DbLike;
  const rows = db
    .prepare("SELECT key, value FROM key_value WHERE namespace = ?")
    .all("cliToolLastConfig") as KeyValueRow[];
  const result: Record<string, string> = {};
  for (const row of rows) {
    const parsed = parseJsonValue(row.value);
    if (typeof parsed === "string") {
      result[row.key] = parsed;
    }
  }
  return result;
}

/**
 * Delete last-configured timestamp for a CLI tool.
 */
export function deleteCliToolLastConfigured(toolId: string): void {
  if (isBuildPhase || isCloud) return;
  const db = getDbInstance() as unknown as DbLike;
  db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(
    "cliToolLastConfig",
    toolId
  );
}

// ──────────────── Initial Config Snapshot ────────────────

/**
 * Save the initial (pre-OmniRoute) config snapshot for a CLI tool.
 * Only saves if no snapshot exists yet (first-time only).
 * @returns true if saved, false if snapshot already exists.
 */
export function saveCliToolInitialConfig(toolId: string, config: JsonRecord): boolean {
  if (isBuildPhase || isCloud) return false;
  const db = getDbInstance() as unknown as DbLike;
  // Only save if not already stored
  const existing = db
    .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
    .get("cliToolInitialConfig", toolId);
  if (existing) return false;

  db.prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES (?, ?, ?)").run(
    "cliToolInitialConfig",
    toolId,
    JSON.stringify(config)
  );
  return true;
}

/**
 * Get the initial config snapshot for a CLI tool.
 * @returns Config object or null if no snapshot exists.
 */
export function getCliToolInitialConfig(toolId: string): JsonRecord | null {
  if (isBuildPhase || isCloud) return null;
  const db = getDbInstance() as unknown as DbLike;
  const row = db
    .prepare("SELECT value FROM key_value WHERE namespace = ? AND key = ?")
    .get("cliToolInitialConfig", toolId);
  if (!row) return null;
  const parsed = parseJsonValue((row as KeyValueRow).value);
  return toRecord(parsed);
}

/**
 * Delete the initial config snapshot for a CLI tool.
 */
export function deleteCliToolInitialConfig(toolId: string): void {
  if (isBuildPhase || isCloud) return;
  const db = getDbInstance() as unknown as DbLike;
  db.prepare("DELETE FROM key_value WHERE namespace = ? AND key = ?").run(
    "cliToolInitialConfig",
    toolId
  );
}

export async function saveCliToolLastConfiguredAsync(
  ...args: Parameters<typeof saveCliToolLastConfigured>
): Promise<ReturnType<typeof saveCliToolLastConfigured>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.saveCliToolLastConfigured(transaction, ...args)
    );
  }
  return saveCliToolLastConfigured(...args);
}

export async function getCliToolLastConfiguredAsync(
  ...args: Parameters<typeof getCliToolLastConfigured>
): Promise<ReturnType<typeof getCliToolLastConfigured>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.getCliToolLastConfigured(transaction, ...args)
    );
  }
  return getCliToolLastConfigured(...args);
}

export async function getAllCliToolLastConfiguredAsync(
  ...args: Parameters<typeof getAllCliToolLastConfigured>
): Promise<ReturnType<typeof getAllCliToolLastConfigured>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.getAllCliToolLastConfigured(transaction, ...args)
    );
  }
  return getAllCliToolLastConfigured(...args);
}

export async function deleteCliToolLastConfiguredAsync(
  ...args: Parameters<typeof deleteCliToolLastConfigured>
): Promise<ReturnType<typeof deleteCliToolLastConfigured>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.deleteCliToolLastConfigured(transaction, ...args)
    );
  }
  return deleteCliToolLastConfigured(...args);
}

export async function saveCliToolInitialConfigAsync(
  ...args: Parameters<typeof saveCliToolInitialConfig>
): Promise<ReturnType<typeof saveCliToolInitialConfig>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.saveCliToolInitialConfig(transaction, ...args)
    );
  }
  return saveCliToolInitialConfig(...args);
}

export async function getCliToolInitialConfigAsync(
  ...args: Parameters<typeof getCliToolInitialConfig>
): Promise<ReturnType<typeof getCliToolInitialConfig>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.getCliToolInitialConfig(transaction, ...args)
    );
  }
  return getCliToolInitialConfig(...args);
}

export async function deleteCliToolInitialConfigAsync(
  ...args: Parameters<typeof deleteCliToolInitialConfig>
): Promise<ReturnType<typeof deleteCliToolInitialConfig>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceOperations");
    return postgres.transaction((transaction) =>
      native.deleteCliToolInitialConfig(transaction, ...args)
    );
  }
  return deleteCliToolInitialConfig(...args);
}
