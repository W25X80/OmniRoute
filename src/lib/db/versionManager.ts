import {
  rowToVersionManager,
  stringifyConfigOverrides,
} from "./versionManager/persistenceSemantics";
import type { VersionManagerRow, VersionManagerTool } from "./versionManager/persistenceSemantics";
export {
  rowToVersionManager,
  stringifyConfigOverrides,
} from "./versionManager/persistenceSemantics";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
/** Version manager tool state persistence. */

import { getDbInstance } from "./core";

export async function getVersionManagerStatus(): Promise<VersionManagerTool[]> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceVersionManager");
    return postgres.transaction((transaction) => native.getVersionManagerStatus(transaction));
  }

  const db = getDbInstance();
  const rows = db.prepare("SELECT * FROM version_manager").all() as VersionManagerRow[];
  return rows.map(rowToVersionManager);
}

export async function getVersionManagerTool(tool: string): Promise<VersionManagerTool | null> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceVersionManager");
    return postgres.transaction((transaction) => native.getVersionManagerTool(transaction, tool));
  }

  const db = getDbInstance();
  const row = db.prepare("SELECT * FROM version_manager WHERE tool = ?").get(tool) as
    VersionManagerRow | undefined;
  if (!row) return null;
  return rowToVersionManager(row);
}

export async function upsertVersionManagerTool(data: {
  tool: string;
  currentVersion?: string | null;
  installedVersion?: string | null;
  pinnedVersion?: string | null;
  binaryPath?: string | null;
  status?: string;
  pid?: number | null;
  port?: number;
  apiKey?: string | null;
  managementKey?: string | null;
  autoUpdate?: boolean;
  autoStart?: boolean;
  healthStatus?: string;
  configOverrides?: Record<string, unknown> | null;
  errorMessage?: string | null;
}): Promise<VersionManagerTool> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceVersionManager");
    return postgres.transaction((transaction) =>
      native.upsertVersionManagerTool(transaction, data)
    );
  }

  const db = getDbInstance();
  db.prepare(
    `
    INSERT INTO version_manager (
      tool, current_version, installed_version, pinned_version, binary_path,
      status, pid, port, api_key, management_key, auto_update, auto_start,
      health_status, config_overrides, error_message, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
    ON CONFLICT(tool) DO UPDATE SET
      current_version = excluded.current_version,
      installed_version = excluded.installed_version,
      pinned_version = excluded.pinned_version,
      binary_path = excluded.binary_path,
      status = excluded.status,
      pid = excluded.pid,
      port = excluded.port,
      api_key = excluded.api_key,
      management_key = excluded.management_key,
      auto_update = excluded.auto_update,
      auto_start = excluded.auto_start,
      health_status = excluded.health_status,
      config_overrides = excluded.config_overrides,
      error_message = excluded.error_message,
      updated_at = datetime('now')
  `
  ).run(
    data.tool,
    data.currentVersion ?? null,
    data.installedVersion ?? null,
    data.pinnedVersion ?? null,
    data.binaryPath ?? null,
    data.status ?? "not_installed",
    data.pid ?? null,
    data.port ?? 8317,
    data.apiKey ?? null,
    data.managementKey ?? null,
    data.autoUpdate !== undefined ? (data.autoUpdate ? 1 : 0) : 1,
    data.autoStart !== undefined ? (data.autoStart ? 1 : 0) : 0,
    data.healthStatus ?? "unknown",
    stringifyConfigOverrides(data.configOverrides ?? null),
    data.errorMessage ?? null
  );
  const result = await getVersionManagerTool(data.tool);
  if (!result) throw new Error("Failed to retrieve inserted version manager tool");
  return result;
}

export async function updateVersionManagerTool(
  tool: string,
  updates: Record<string, unknown>
): Promise<VersionManagerTool | null> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceVersionManager");
    return postgres.transaction((transaction) =>
      native.updateVersionManagerTool(transaction, tool, updates)
    );
  }

  const db = getDbInstance();
  const existing = await getVersionManagerTool(tool);
  if (!existing) return null;

  const ALLOWED_COLUMNS = new Set([
    "currentVersion",
    "installedVersion",
    "pinnedVersion",
    "binaryPath",
    "status",
    "pid",
    "port",
    "apiKey",
    "managementKey",
    "autoUpdate",
    "autoStart",
    "autoRestartAdopted",
    "healthStatus",
    "configOverrides",
    "errorMessage",
    "logsBufferPath",
    "providerExpose",
    "lastSyncAt",
  ]);

  const sets: string[] = ["updated_at = datetime('now')"];
  const params: Record<string, unknown> = { tool };

  for (const [key, value] of Object.entries(updates)) {
    if (!ALLOWED_COLUMNS.has(key)) continue;
    const dbKey = key.replace(/([A-Z])/g, "_$1").toLowerCase();

    if (key === "configOverrides") {
      sets.push("config_overrides = @configOverrides");
      params.configOverrides = stringifyConfigOverrides(value as Record<string, unknown> | null);
    } else if (
      key === "autoUpdate" ||
      key === "autoStart" ||
      key === "autoRestartAdopted" ||
      key === "providerExpose"
    ) {
      sets.push(`${dbKey} = @${key}`);
      params[key] = value === true ? 1 : 0;
    } else if (value === null) {
      sets.push(`${dbKey} = null`);
    } else {
      sets.push(`${dbKey} = @${key}`);
      params[key] = value;
    }
  }

  db.prepare(`UPDATE version_manager SET ${sets.join(", ")} WHERE tool = @tool`).run(params);
  return getVersionManagerTool(tool);
}

export async function deleteVersionManagerTool(tool: string): Promise<boolean> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceVersionManager");
    return postgres.transaction((transaction) =>
      native.deleteVersionManagerTool(transaction, tool)
    );
  }

  const db = getDbInstance();
  const result = db.prepare("DELETE FROM version_manager WHERE tool = ?").run(tool);
  return result.changes > 0;
}

export async function updateToolHealth(tool: string, healthStatus: string): Promise<boolean> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceVersionManager");
    return postgres.transaction((transaction) =>
      native.updateToolHealth(transaction, tool, healthStatus)
    );
  }

  const db = getDbInstance();
  const result = db
    .prepare(
      "UPDATE version_manager SET health_status = ?, last_health_check = datetime('now') WHERE tool = ?"
    )
    .run(healthStatus, tool);
  return result.changes > 0;
}

export async function updateToolVersion(
  tool: string,
  field: "current_version" | "installed_version",
  version: string
): Promise<boolean> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceVersionManager");
    return postgres.transaction((transaction) =>
      native.updateToolVersion(transaction, tool, field, version)
    );
  }

  const db = getDbInstance();
  const result = db
    .prepare(`UPDATE version_manager SET ${field} = ?, updated_at = datetime('now') WHERE tool = ?`)
    .run(version, tool);
  return result.changes > 0;
}

export async function setToolStatus(
  tool: string,
  status: string,
  pid?: number,
  errorMessage?: string
): Promise<boolean> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceVersionManager");
    return postgres.transaction((transaction) =>
      native.setToolStatus(transaction, tool, status, pid, errorMessage)
    );
  }

  const db = getDbInstance();
  const result = db
    .prepare(
      pid !== undefined
        ? "UPDATE version_manager SET status = ?, pid = ?, error_message = ?, updated_at = datetime('now') WHERE tool = ?"
        : "UPDATE version_manager SET status = ?, error_message = ?, updated_at = datetime('now') WHERE tool = ?"
    )
    .run(
      ...(pid !== undefined
        ? [status, pid, errorMessage ?? null, tool]
        : [status, errorMessage ?? null, tool])
    );
  return result.changes > 0;
}

/** Typed alias for getVersionManagerTool — preferred entry point for embedded services. */
export async function getServiceRow(tool: string): Promise<VersionManagerTool | null> {
  return getVersionManagerTool(tool);
}

const SERVICE_FIELD_WHITELIST: Set<string> = new Set([
  "logsBufferPath",
  "providerExpose",
  "lastSyncAt",
  "status",
  "pid",
  "port",
  "apiKey",
  "autoStart",
  "autoRestartAdopted",
  "autoUpdate",
  "healthStatus",
  "errorMessage",
  "currentVersion",
  "installedVersion",
  "binaryPath",
]);

/** Single-field update for embedded services; whitelisted to prevent SQL injection. */
export async function updateServiceField(
  tool: string,
  field: string,
  value: unknown
): Promise<VersionManagerTool | null> {
  if (!SERVICE_FIELD_WHITELIST.has(field)) {
    throw new Error(`updateServiceField: field "${field}" is not in the allowed list`);
  }
  return updateVersionManagerTool(tool, { [field]: value });
}
