import { assertSqliteBackendAllowed } from "./controlPlaneContext";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import {
  catalogTableRows,
  requirePostgresCatalogSnapshot,
  isPostgresCatalogMutation,
  mutatePostgresCatalog,
  writePostgresCatalog,
} from "./postgres/catalogSnapshot";
import { getDbInstance } from "./core";
import type { TierConfig } from "../../../open-sse/services/tierTypes";
import { validateTierConfig, DEFAULT_TIER_CONFIG } from "../../../open-sse/services/tierConfig";
import { defaultLogger as log } from "@omniroute/open-sse/utils/logger";

const TABLE = "tier_config";
const CORRUPTED_VALUE_PREVIEW_LEN = 200;

export function initTierConfigTable(): void {
  if (getSelectedPostgresDatabase()) return;
  const db = getDbInstance();
  db.exec(`
    CREATE TABLE IF NOT EXISTS ${TABLE} (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT DEFAULT (datetime('now'))
    );
  `);
}

export async function saveTierConfigAsync(config: TierConfig): Promise<void> {
  const catalogDatabase = getSelectedPostgresDatabase();
  const execute = async () => {
    const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
    const serialized = JSON.stringify(config);
    getSelectedPostgresDatabase()
      ? {
          changes:
            (
              await writePostgresCatalog(
                "INSERT INTO omniroute.tier_config (key, value, updated_at) VALUES ('tier_config', $1, to_char(timezone('UTC',clock_timestamp()),'YYYY-MM-DD HH24:MI:SS')) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=EXCLUDED.updated_at",
                [serialized]
              )
            ).rowCount ?? 0,
        }
      : db!
          .prepare(
            `INSERT OR REPLACE INTO ${TABLE} (key, value, updated_at) VALUES ('tier_config', ?, datetime('now'))`
          )
          .run(serialized);
  };
  if (catalogDatabase && !isPostgresCatalogMutation(catalogDatabase))
    return mutatePostgresCatalog(catalogDatabase, execute);
  return execute();
}

/**
 * Truncate an unknown value (string) for safe inclusion in a log payload.
 * Returns the string verbatim when shorter than the cap, otherwise a
 * 200-char preview with an ellipsis. `String(value)` is used as a final
 * fallback so a non-string never throws here.
 */
function previewCorruptedValue(value: unknown): string {
  if (typeof value !== "string") return String(value);
  if (value.length <= CORRUPTED_VALUE_PREVIEW_LEN) return value;
  return `${value.slice(0, CORRUPTED_VALUE_PREVIEW_LEN)}…`;
}

/**
 * Load the persisted tier config from SQLite. Returns `null` when no row exists
 * OR when the stored value is unreadable (invalid JSON, fails Zod validation).
 *
 * The function NEVER throws on parse failure — instead it logs a structured
 * warning so operators can spot the corruption in logs and either:
 *   1. Manually delete the bad row:
 *        DELETE FROM tier_config WHERE key = 'tier_config';
 *   2. Re-save a clean config via the dashboard's Tier settings page.
 *
 * The caller (`loadTierConfig()`) then falls back to `DEFAULT_TIER_CONFIG`,
 * so a corrupted row never silently feeds invalid pricing into the router.
 */
export function loadTierConfigFromDb(): TierConfig | null {
  requirePostgresCatalogSnapshot();

  const db = getSelectedPostgresDatabase() ? undefined : getDbInstance();
  const row = (
    getSelectedPostgresDatabase()
      ? catalogTableRows<{ key: string; value: string }>("tier_config").filter(
          (row) => row.key === "tier_config"
        )[0]
      : db!.prepare(`SELECT value FROM ${TABLE} WHERE key = 'tier_config'`).get()
  ) as { value: string } | undefined;
  if (!row) return null;

  const raw = row.value;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    log.warn("TIER_CONFIG", "tier_config JSON.parse failed; falling back to DEFAULT_TIER_CONFIG", {
      err: err instanceof Error ? err.message : String(err),
      value: previewCorruptedValue(raw),
    });
    return null;
  }

  try {
    return validateTierConfig(parsed);
  } catch (err) {
    log.warn(
      "TIER_CONFIG",
      "tier_config Zod validation failed; falling back to DEFAULT_TIER_CONFIG",
      {
        err: err instanceof Error ? err.message : String(err),
        value: previewCorruptedValue(raw),
      }
    );
    return null;
  }
}

export function loadTierConfig(): TierConfig {
  return loadTierConfigFromDb() || DEFAULT_TIER_CONFIG;
}

export function saveTierConfig(config: TierConfig): void {
  assertSqliteBackendAllowed();
  const db = getDbInstance();
  const serialized = JSON.stringify(config);
  db.prepare(
    `INSERT OR REPLACE INTO ${TABLE} (key, value, updated_at) VALUES ('tier_config', ?, datetime('now'))`
  ).run(serialized);
}
