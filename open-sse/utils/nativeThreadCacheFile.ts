import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

const THREAD_CACHE_FILES = Symbol.for("omniroute.nativeThreadCacheFiles");
const cacheGlobals = globalThis as typeof globalThis & {
  [THREAD_CACHE_FILES]?: Map<string, Promise<void>>;
};

/** Serialize a provider file's complete read/modify/write operation across bundles. */
export function withNativeThreadCacheFile<T>(
  path: string | null,
  operation: () => Promise<T>
): Promise<T> {
  if (!path) return operation();
  const files = (cacheGlobals[THREAD_CACHE_FILES] ??= new Map());
  const pending = (files.get(path) ?? Promise.resolve()).then(operation);
  const settled = pending.then(
    () => {},
    () => {}
  );
  files.set(path, settled);
  void settled.then(() => {
    if (files.get(path) === settled) files.delete(path);
  });
  return pending;
}

/** Provider continuity files are best-effort local caches, not database authority. */
export async function readNativeThreadCacheFile<T>(
  path: string | null
): Promise<Record<string, T>> {
  if (!path) return {};
  try {
    const parsed = JSON.parse(await readFile(path, "utf8"));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

export async function writeNativeThreadCacheFile<T>(
  path: string | null,
  entries: Record<string, T>
): Promise<boolean> {
  if (!path) return false;
  try {
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, JSON.stringify(entries), "utf8");
    return true;
  } catch {
    return false;
  }
}
