import { getSelectedPostgresDatabase } from "./controlPlaneContext";
/**
 * Database module: InspectorCustomHosts
 * CRUD operations for inspector_custom_hosts table.
 */

import { getDbInstance } from "./core.ts";
import type { InspectorCustomHostRow } from "./_rowTypes.ts";

// SQLite stores booleans as integers
interface InspectorCustomHostDbRow {
  host: string;
  enabled: number;
  label: string | null;
  kind: string;
  added_at: string;
  last_seen_at: string | null;
}

function mapRow(row: InspectorCustomHostDbRow): InspectorCustomHostRow {
  return {
    host: row.host,
    enabled: row.enabled === 1,
    label: row.label,
    kind: row.kind as "llm" | "app" | "custom",
    added_at: row.added_at,
    last_seen_at: row.last_seen_at,
  };
}

export function listCustomHosts(opts?: { enabledOnly?: boolean }): InspectorCustomHostRow[] {
  const db = getDbInstance();
  const enabledOnly = opts?.enabledOnly === true;

  const rows = enabledOnly
    ? (db
        .prepare("SELECT * FROM inspector_custom_hosts WHERE enabled = 1 ORDER BY host ASC")
        .all() as InspectorCustomHostDbRow[])
    : (db
        .prepare("SELECT * FROM inspector_custom_hosts ORDER BY host ASC")
        .all() as InspectorCustomHostDbRow[]);

  return rows.map(mapRow);
}

export function addCustomHost(
  host: string,
  kind: "llm" | "app" | "custom" = "custom",
  label?: string
): void {
  const db = getDbInstance();
  const now = new Date().toISOString();
  db.prepare(
    `INSERT OR IGNORE INTO inspector_custom_hosts (host, enabled, label, kind, added_at)
     VALUES (?, 1, ?, ?, ?)`
  ).run(host, label ?? null, kind, now);
}

export function removeCustomHost(host: string): void {
  const db = getDbInstance();
  db.prepare("DELETE FROM inspector_custom_hosts WHERE host = ?").run(host);
}

export function toggleCustomHost(host: string, enabled: boolean): void {
  const db = getDbInstance();
  db.prepare("UPDATE inspector_custom_hosts SET enabled = ? WHERE host = ?").run(
    enabled ? 1 : 0,
    host
  );
}

export function touchLastSeen(host: string): void {
  const db = getDbInstance();
  const now = new Date().toISOString();
  db.prepare("UPDATE inspector_custom_hosts SET last_seen_at = ? WHERE host = ?").run(now, host);
}

/**
 * Returns true when `host` is present in inspector_custom_hosts with enabled=1.
 * Used by agentBridgeHook to distinguish custom-host intercepts from agent-bridge
 * intercepts so that Mode 2 (Custom Hosts) entries appear in the "Custom" profile.
 */
export function isCustomHost(host: string): boolean {
  const db = getDbInstance();
  const row = db
    .prepare("SELECT 1 AS found FROM inspector_custom_hosts WHERE host = ? AND enabled = 1")
    .get(host) as { found: number } | undefined;
  return row !== undefined;
}

export async function listCustomHostsAsync(
  ...args: Parameters<typeof listCustomHosts>
): Promise<ReturnType<typeof listCustomHosts>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceInspector");
    return postgres.transaction((transaction) => native.listCustomHosts(transaction, ...args));
  }
  return listCustomHosts(...args);
}

export async function addCustomHostAsync(
  ...args: Parameters<typeof addCustomHost>
): Promise<ReturnType<typeof addCustomHost>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceInspector");
    return postgres.transaction((transaction) => native.addCustomHost(transaction, ...args));
  }
  return addCustomHost(...args);
}

export async function removeCustomHostAsync(
  ...args: Parameters<typeof removeCustomHost>
): Promise<ReturnType<typeof removeCustomHost>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceInspector");
    return postgres.transaction((transaction) => native.removeCustomHost(transaction, ...args));
  }
  return removeCustomHost(...args);
}

export async function toggleCustomHostAsync(
  ...args: Parameters<typeof toggleCustomHost>
): Promise<ReturnType<typeof toggleCustomHost>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceInspector");
    return postgres.transaction((transaction) => native.toggleCustomHost(transaction, ...args));
  }
  return toggleCustomHost(...args);
}

export async function touchLastSeenAsync(
  ...args: Parameters<typeof touchLastSeen>
): Promise<ReturnType<typeof touchLastSeen>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceInspector");
    return postgres.transaction((transaction) => native.touchLastSeen(transaction, ...args));
  }
  return touchLastSeen(...args);
}

export async function isCustomHostAsync(
  ...args: Parameters<typeof isCustomHost>
): Promise<ReturnType<typeof isCustomHost>> {
  const postgres = getSelectedPostgresDatabase();
  if (postgres) {
    const native = await import("./postgres/serviceInspector");
    return postgres.transaction((transaction) => native.isCustomHost(transaction, ...args));
  }
  return isCustomHost(...args);
}
