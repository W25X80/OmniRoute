import {
  getSelectedPostgresDatabase,
  UnsupportedPersistenceDomainError,
} from "./controlPlaneContext";
import {
  readPostgresCatalogNamespace,
  mutatePostgresCatalog,
  putCatalogValue,
} from "./postgres/catalogSnapshot";

export interface IPFilterSettings {
  enabled: boolean;
  mode: string;
  blacklist: string[];
  whitelist: string[];
}
export interface IPFilterUpdate {
  enabled?: boolean;
  mode?: string;
  blacklist?: string[];
  whitelist?: string[];
  addBlacklist?: string;
  removeBlacklist?: string;
  addWhitelist?: string;
  removeWhitelist?: string;
  tempBan?: { ip: string; durationMs?: number; reason?: string };
  removeBan?: string;
}
const NAMESPACE = "ipFilter";
const KEY = "config";

function decode(value: string | undefined): IPFilterSettings {
  if (value === undefined)
    return { enabled: false, mode: "blacklist", blacklist: [], whitelist: [] };
  const parsed: unknown = JSON.parse(value);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
    throw new Error("Invalid persisted IP filter configuration");
  const record = parsed as Record<string, unknown>;
  if (
    (record.enabled !== undefined && typeof record.enabled !== "boolean") ||
    (record.mode !== undefined && typeof record.mode !== "string") ||
    [record.blacklist, record.whitelist].some(
      (list) =>
        list !== undefined && (!Array.isArray(list) || list.some((ip) => typeof ip !== "string"))
    )
  )
    throw new Error("Invalid persisted IP filter configuration");
  return {
    enabled: record.enabled === true,
    mode: typeof record.mode === "string" ? record.mode : "blacklist",
    blacklist: Array.isArray(record.blacklist) ? ([...record.blacklist] as string[]) : [],
    whitelist: Array.isArray(record.whitelist) ? ([...record.whitelist] as string[]) : [],
  };
}

/** Prepared synchronous native read: no global fallback/cache and no SQL on the request leaf. */
export function getPreparedNativeIPFilterSettings(): IPFilterSettings | undefined {
  const rows = readPostgresCatalogNamespace(NAMESPACE);
  return rows ? decode(rows.find((row) => row.key === KEY)?.value) : undefined;
}

/** One fresh locked native read/modify/write; no effect is acknowledged before COMMIT. */
export async function updateNativeIPFilterSettings(update: IPFilterUpdate): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) throw new UnsupportedPersistenceDomainError();
  getPreparedNativeIPFilterSettings();
  await mutatePostgresCatalog(database, async () => {
    const config = getPreparedNativeIPFilterSettings()!;
    if (update.enabled !== undefined) config.enabled = update.enabled;
    if (update.mode) config.mode = update.mode;
    if (update.blacklist) config.blacklist = [...update.blacklist];
    if (update.whitelist) config.whitelist = [...update.whitelist];
    const blacklist = new Set(config.blacklist),
      whitelist = new Set(config.whitelist);
    if (update.addBlacklist !== undefined) blacklist.add(update.addBlacklist);
    if (update.removeBlacklist !== undefined) blacklist.delete(update.removeBlacklist);
    if (update.addWhitelist !== undefined) whitelist.add(update.addWhitelist);
    if (update.removeWhitelist !== undefined) whitelist.delete(update.removeWhitelist);
    await putCatalogValue(
      NAMESPACE,
      KEY,
      JSON.stringify({ ...config, blacklist: [...blacklist], whitelist: [...whitelist] })
    );
  });
}
