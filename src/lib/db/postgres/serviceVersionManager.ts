import {
  rowToVersionManager,
  stringifyConfigOverrides,
} from "../versionManager/persistenceSemantics";
import { decrypt } from "../encryption";
import type { PostgresTransaction } from "./database";
import { affected, first, rows, safeInteger, protectSecret, lockServices } from "./serviceQuery";

function map(row: Record<string, unknown>) {
  return rowToVersionManager({
    ...row,
    id: safeInteger(row.id),
    api_key: decrypt(row.api_key as string | null),
    management_key: decrypt(row.management_key as string | null),
  });
}
export async function getVersionManagerStatus(transaction: PostgresTransaction) {
  return (await rows(transaction, "SELECT * FROM omniroute.version_manager ORDER BY id")).map(map);
}
export async function getVersionManagerTool(transaction: PostgresTransaction, tool: string) {
  const row = await first(transaction, "SELECT * FROM omniroute.version_manager WHERE tool = $1", [
    tool,
  ]);
  return row ? map(row) : null;
}
export async function upsertVersionManagerTool(
  transaction: PostgresTransaction,
  data: Parameters<typeof import("../versionManager").upsertVersionManagerTool>[0]
) {
  await lockServices(transaction, "version-manager");
  const now = new Date().toISOString();
  await transaction.query(
    `INSERT INTO omniroute.version_manager
    (tool, current_version, installed_version, pinned_version, binary_path, status, pid, port, api_key, management_key,
    auto_update, auto_start, health_status, config_overrides, error_message, created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $16)
    ON CONFLICT (tool) DO UPDATE SET current_version = excluded.current_version, installed_version = excluded.installed_version,
    pinned_version = excluded.pinned_version, binary_path = excluded.binary_path, status = excluded.status, pid = excluded.pid,
    port = excluded.port, api_key = excluded.api_key, management_key = excluded.management_key, auto_update = excluded.auto_update,
    auto_start = excluded.auto_start, health_status = excluded.health_status, config_overrides = excluded.config_overrides,
    error_message = excluded.error_message, updated_at = excluded.updated_at`,
    [
      data.tool,
      data.currentVersion ?? null,
      data.installedVersion ?? null,
      data.pinnedVersion ?? null,
      data.binaryPath ?? null,
      data.status ?? "not_installed",
      data.pid ?? null,
      data.port ?? 8317,
      protectSecret(data.apiKey ?? null),
      protectSecret(data.managementKey ?? null),
      data.autoUpdate === undefined || data.autoUpdate ? 1 : 0,
      data.autoStart ? 1 : 0,
      data.healthStatus ?? "unknown",
      stringifyConfigOverrides(data.configOverrides ?? null),
      data.errorMessage ?? null,
      now,
    ]
  );
  const created = await getVersionManagerTool(transaction, data.tool);
  if (!created) throw new Error("Failed to retrieve inserted version manager tool");
  return created;
}
const ALLOWED = new Set([
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
export async function updateVersionManagerTool(
  transaction: PostgresTransaction,
  tool: string,
  updates: Record<string, unknown>
) {
  await lockServices(transaction, "version-manager");
  if (!(await getVersionManagerTool(transaction, tool))) return null;
  const columns: Record<string, unknown> = { updated_at: new Date().toISOString() };
  for (const [key, value] of Object.entries(updates)) {
    if (!ALLOWED.has(key)) continue;
    const column = key.replace(/([A-Z])/g, "_$1").toLowerCase();
    if (key === "configOverrides")
      columns[column] = stringifyConfigOverrides(value as Record<string, unknown> | null);
    else if (["autoUpdate", "autoStart", "autoRestartAdopted", "providerExpose"].includes(key))
      columns[column] = value === true ? 1 : 0;
    else if (key === "apiKey" || key === "managementKey")
      columns[column] = protectSecret(value as string | null);
    else columns[column] = value;
  }
  const keys = Object.keys(columns);
  await transaction.query(
    `UPDATE omniroute.version_manager SET ${keys.map((key, index) => `${key} = $${index + 1}`).join(", ")} WHERE tool = $${keys.length + 1}`,
    [...Object.values(columns), tool]
  );
  return getVersionManagerTool(transaction, tool);
}
export async function deleteVersionManagerTool(transaction: PostgresTransaction, tool: string) {
  return (
    (await affected(transaction, "DELETE FROM omniroute.version_manager WHERE tool = $1", [tool])) >
    0
  );
}
export async function updateToolHealth(
  transaction: PostgresTransaction,
  tool: string,
  healthStatus: string
) {
  return (
    (await affected(
      transaction,
      "UPDATE omniroute.version_manager SET health_status = $1, last_health_check = $2 WHERE tool = $3",
      [healthStatus, new Date().toISOString(), tool]
    )) > 0
  );
}
export async function updateToolVersion(
  transaction: PostgresTransaction,
  tool: string,
  field: "current_version" | "installed_version",
  version: string
) {
  if (field !== "current_version" && field !== "installed_version")
    throw new Error("Unsupported tool version field");
  return (
    (await affected(
      transaction,
      `UPDATE omniroute.version_manager SET ${field} = $1, updated_at = $2 WHERE tool = $3`,
      [version, new Date().toISOString(), tool]
    )) > 0
  );
}
export async function setToolStatus(
  transaction: PostgresTransaction,
  tool: string,
  status: string,
  pid?: number,
  errorMessage?: string
) {
  return (
    (await affected(
      transaction,
      `UPDATE omniroute.version_manager SET status = $1, error_message = $2, updated_at = $3 ${pid !== undefined ? ", pid = $5" : ""} WHERE tool = $4`,
      [
        status,
        errorMessage ?? null,
        new Date().toISOString(),
        tool,
        ...(pid !== undefined ? [pid] : []),
      ]
    )) > 0
  );
}
