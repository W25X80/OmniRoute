/** Shared version-manager config encoding and row projection; no database imports. */

export interface VersionManagerRow {
  id?: unknown;
  tool?: unknown;
  current_version?: unknown;
  installed_version?: unknown;
  pinned_version?: unknown;
  binary_path?: unknown;
  status?: unknown;
  pid?: unknown;
  port?: unknown;
  api_key?: unknown;
  management_key?: unknown;
  auto_update?: unknown;
  auto_start?: unknown;
  auto_restart_adopted?: unknown;
  last_health_check?: unknown;
  last_update_check?: unknown;
  health_status?: unknown;
  config_overrides?: unknown;
  error_message?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
  logs_buffer_path?: unknown;
  provider_expose?: unknown;
  last_sync_at?: unknown;
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

function parseConfigOverrides(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "string" || value.trim() === "") return null;
  try {
    const parsed = JSON.parse(value);
    return typeof parsed === "object" && parsed !== null ? parsed : null;
  } catch {
    return null;
  }
}

export function stringifyConfigOverrides(value: Record<string, unknown> | null): string | null {
  if (value === null) return null;
  try {
    return JSON.stringify(value);
  } catch {
    return null;
  }
}

export interface VersionManagerTool {
  id: number;
  tool: string;
  currentVersion: string | null;
  installedVersion: string | null;
  pinnedVersion: string | null;
  binaryPath: string | null;
  status: string;
  pid: number | null;
  port: number;
  apiKey: string | null;
  managementKey: string | null;
  autoUpdate: boolean;
  autoStart: boolean;
  /**
   * When true, an adopted (unsupervised, pre-existing) process is
   * immediately killed and replaced with a fresh spawn this supervisor
   * actually owns — trading a brief restart for working log capture.
   * Defaults off: adoption alone is already the safe, non-disruptive choice.
   */
  autoRestartAdopted: boolean;
  lastHealthCheck: string | null;
  lastUpdateCheck: string | null;
  healthStatus: string;
  configOverrides: Record<string, unknown> | null;
  errorMessage: string | null;
  createdAt: string;
  updatedAt: string;
  logsBufferPath: string | null;
  providerExpose: boolean;
  lastSyncAt: string | null;
}

export function rowToVersionManager(row: VersionManagerRow): VersionManagerTool {
  const record = toRecord(row);
  return {
    id: typeof record.id === "number" ? record.id : 0,
    tool: typeof record.tool === "string" ? record.tool : "",
    currentVersion:
      record.current_version === null
        ? null
        : typeof record.current_version === "string"
          ? record.current_version
          : null,
    installedVersion:
      record.installed_version === null
        ? null
        : typeof record.installed_version === "string"
          ? record.installed_version
          : null,
    pinnedVersion:
      record.pinned_version === null
        ? null
        : typeof record.pinned_version === "string"
          ? record.pinned_version
          : null,
    binaryPath:
      record.binary_path === null
        ? null
        : typeof record.binary_path === "string"
          ? record.binary_path
          : null,
    status: typeof record.status === "string" ? record.status : "not_installed",
    pid: record.pid === null ? null : typeof record.pid === "number" ? record.pid : null,
    port: typeof record.port === "number" ? record.port : 8317,
    apiKey:
      record.api_key === null ? null : typeof record.api_key === "string" ? record.api_key : null,
    managementKey:
      record.management_key === null
        ? null
        : typeof record.management_key === "string"
          ? record.management_key
          : null,
    autoUpdate:
      record.auto_update === 1 || record.auto_update === true || record.auto_update === "1",
    autoStart: record.auto_start === 1 || record.auto_start === true || record.auto_start === "1",
    autoRestartAdopted:
      record.auto_restart_adopted === 1 ||
      record.auto_restart_adopted === true ||
      record.auto_restart_adopted === "1",
    lastHealthCheck:
      record.last_health_check === null
        ? null
        : typeof record.last_health_check === "string"
          ? record.last_health_check
          : null,
    lastUpdateCheck:
      record.last_update_check === null
        ? null
        : typeof record.last_update_check === "string"
          ? record.last_update_check
          : null,
    healthStatus: typeof record.health_status === "string" ? record.health_status : "unknown",
    configOverrides: parseConfigOverrides(record.config_overrides),
    errorMessage:
      record.error_message === null
        ? null
        : typeof record.error_message === "string"
          ? record.error_message
          : null,
    createdAt: typeof record.created_at === "string" ? record.created_at : "",
    updatedAt: typeof record.updated_at === "string" ? record.updated_at : "",
    logsBufferPath:
      record.logs_buffer_path === null
        ? null
        : typeof record.logs_buffer_path === "string"
          ? record.logs_buffer_path
          : null,
    providerExpose:
      record.provider_expose === 1 ||
      record.provider_expose === true ||
      record.provider_expose === "1",
    lastSyncAt:
      record.last_sync_at === null
        ? null
        : typeof record.last_sync_at === "string"
          ? record.last_sync_at
          : null,
  };
}
