import fs from "node:fs";
import { readFile, realpath, stat, unlink } from "node:fs/promises";
import path from "node:path";
import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { resolveDataDir, resolveDataDirAsync } from "./dataPaths";
import {
  getWebdavConfigurationAsync,
  getObsidianVaultPathAsync as getObsidianVaultPath,
  saveObsidianSyncConfigurationAsync,
  clearObsidianSyncConfigurationAsync,
} from "./db/obsidian";

export type ObsidianSyncStatus = {
  vaultPath: string | null;
  webdavEnabled: boolean;
  webdavUsername: string | null;
  webdavPassword: string | null;
};

export type ObsidianSyncEnableResult =
  | { success: true; vaultPath: string; username: string; password: string }
  | { success: false; error: string };

export async function getObsidianSyncStatus(): Promise<ObsidianSyncStatus> {
  const configuration = await getWebdavConfigurationAsync();
  return {
    vaultPath: configuration.vaultPath,
    webdavEnabled: configuration.enabled,
    webdavUsername: configuration.username,
    webdavPassword: configuration.password,
  };
}

/** Canonical (symlink-resolved) form of a path; falls back to the lexical resolve. */
function canonicalPath(target: string): string {
  try {
    return fs.realpathSync.native(target);
  } catch {
    return path.resolve(target);
  }
}

/** True when `child` is `parent` itself or lives anywhere below it. */
function isSameOrInside(parent: string, child: string): boolean {
  const rel = path.relative(parent, child);
  if (rel === "") return true;
  if (path.isAbsolute(rel)) return false; // different drive (win32)
  return rel !== ".." && !rel.startsWith(`..${path.sep}`);
}

/**
 * GHSA-7pq4-8pvv-rx7r: the WebDAV file service (scripts/dev/webdav-handler.mjs)
 * serves the vault root to anyone holding the Basic credentials, before Next.js
 * and outside the authz pipeline. A vault that IS the data directory, sits
 * inside it, or CONTAINS it turns that service into a reader for server.env
 * (JWT_SECRET / STORAGE_ENCRYPTION_KEY / API_KEY_SECRET) and storage.sqlite.
 * Both sides are realpath-resolved so a symlink cannot dodge the comparison.
 */
export function vaultPathOverlapsDataDir(resolvedVaultPath: string): boolean {
  const vault = canonicalPath(resolvedVaultPath);
  const dataDir = canonicalPath(resolveDataDir());
  return isSameOrInside(dataDir, vault) || isSameOrInside(vault, dataDir);
}

async function canonicalPathAsync(target: string): Promise<string> {
  try {
    return await realpath(target);
  } catch {
    return path.resolve(target);
  }
}

export async function vaultPathOverlapsDataDirAsync(resolvedVaultPath: string): Promise<boolean> {
  const vault = await canonicalPathAsync(resolvedVaultPath);
  const dataDir = await canonicalPathAsync(await resolveDataDirAsync());
  return isSameOrInside(dataDir, vault) || isSameOrInside(vault, dataDir);
}

export const VAULT_OVERLAPS_DATA_DIR_ERROR =
  "Vault path must not be the OmniRoute data directory, a directory inside it, or a directory that contains it";

export async function enableObsidianVaultSync(
  vaultPath: string
): Promise<ObsidianSyncEnableResult> {
  const resolvedPath = path.resolve(vaultPath);

  const native = parseDatabaseBackend() === "postgres";
  const nativeStat = native ? await stat(resolvedPath).catch(() => null) : undefined;
  if (native ? !nativeStat : !fs.existsSync(resolvedPath)) {
    return {
      success: false,
      error: `Vault directory not found: ${resolvedPath}`,
    };
  }

  const directoryStat = native ? nativeStat! : fs.statSync(resolvedPath);
  if (!directoryStat.isDirectory()) {
    return {
      success: false,
      error: `Path is not a directory: ${resolvedPath}`,
    };
  }

  if (
    native
      ? await vaultPathOverlapsDataDirAsync(resolvedPath)
      : vaultPathOverlapsDataDir(resolvedPath)
  ) {
    return { success: false, error: VAULT_OVERLAPS_DATA_DIR_ERROR };
  }

  try {
    const username = generateRandomString(12);
    const password = generateRandomString(24);

    await saveObsidianSyncConfigurationAsync(resolvedPath, username, password);

    return { success: true, vaultPath: resolvedPath, username, password };
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    return { success: false, error: msg };
  }
}

export async function disableObsidianVaultSync(): Promise<{
  success: boolean;
  error?: string;
}> {
  try {
    const vaultPath = await getObsidianVaultPath();
    if (vaultPath) {
      if (parseDatabaseBackend() === "postgres") await removeStignoreAsync(vaultPath);
      else removeStignore(vaultPath);
    }
    await clearObsidianSyncConfigurationAsync();
    return { success: true };
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    return { success: false, error: msg };
  }
}

function removeStignore(vaultPath: string): void {
  try {
    const stignorePath = path.join(vaultPath, ".stignore");
    if (fs.existsSync(stignorePath)) {
      const content = fs.readFileSync(stignorePath, "utf-8");
      const marker = "# Managed by OmniRoute";
      if (content.includes(marker)) {
        fs.unlinkSync(stignorePath);
      }
    }
  } catch {
    // Non-critical
  }
}

async function removeStignoreAsync(vaultPath: string): Promise<void> {
  try {
    const file = path.join(vaultPath, ".stignore");
    if ((await readFile(file, "utf-8")).includes("# Managed by OmniRoute")) await unlink(file);
  } catch {
    // Non-critical, matching the synchronous cleanup policy.
  }
}

function generateRandomString(length: number): string {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  const buf = new Uint8Array(length);
  crypto.getRandomValues(buf);
  for (let i = 0; i < length; i++) {
    result += chars[buf[i] % chars.length];
  }
  return result;
}
