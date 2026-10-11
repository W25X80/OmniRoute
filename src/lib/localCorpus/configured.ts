import path from "path";
import { getLocalCorpusRoot, getLocalCorpusRootAsync } from "../db/localCorpus";
import { getDefaultLocalCorpusStatus, LocalCorpusIndex } from "./index";

const indexCache = new Map<string, LocalCorpusIndex>();

export function resetLocalCorpusIndex(): void {
  indexCache.clear();
}

function getIndexForRoot(dbRoot: string | null, dynamicRoot?: string): LocalCorpusIndex {
  const finalRoot = dynamicRoot || dbRoot;
  if (!finalRoot) {
    throw new Error(
      "Local corpus is not configured. Pass absoluteRootPath or set a root in Settings > Context Sources"
    );
  }

  const boundingBox = dbRoot ? path.resolve(dbRoot) : process.cwd();
  if (dynamicRoot) {
    const resolvedDynamic = path.resolve(dynamicRoot);
    const relative = path.relative(boundingBox, resolvedDynamic);
    const isInside = relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
    if (!isInside) {
      throw new Error(
        `Path traversal forbidden: requested path ${resolvedDynamic} is outside allowed boundary ${boundingBox}`
      );
    }
  }

  const resolvedRoot = path.resolve(finalRoot);
  const existing = indexCache.get(resolvedRoot);
  if (existing) {
    indexCache.delete(resolvedRoot);
    indexCache.set(resolvedRoot, existing);
    return existing;
  }

  const maxCacheSize = Math.max(
    1,
    parseInt(process.env.OMNIROUTE_CORPUS_CACHE_SIZE || "5", 10) || 5
  );
  if (indexCache.size >= maxCacheSize) {
    const firstKey = indexCache.keys().next().value;
    if (firstKey) {
      indexCache.delete(firstKey);
    }
  }

  const newIndex = new LocalCorpusIndex(resolvedRoot);
  indexCache.set(resolvedRoot, newIndex);
  return newIndex;
}

function getConfiguredIndex(dynamicRoot?: string): LocalCorpusIndex {
  return getIndexForRoot(getLocalCorpusRoot(), dynamicRoot);
}
async function getConfiguredIndexAsync(dynamicRoot?: string): Promise<LocalCorpusIndex> {
  return getIndexForRoot(await getLocalCorpusRootAsync(), dynamicRoot);
}

export function getConfiguredLocalCorpusStatus(dynamicRoot?: string) {
  const root = dynamicRoot || getLocalCorpusRoot();
  return root ? getConfiguredIndex(dynamicRoot).getStatus() : getDefaultLocalCorpusStatus();
}

export async function searchConfiguredLocalCorpus(
  query: string,
  options: {
    limit?: number;
    refresh?: boolean;
    absoluteRootPath?: string;
    rootPath?: string;
  } = {}
) {
  const root = options.absoluteRootPath || options.rootPath;
  return (await getConfiguredIndexAsync(root)).search(query, options);
}

export async function readConfiguredLocalCorpus(
  relativePath: string,
  options: {
    startLine?: number;
    endLine?: number;
    absoluteRootPath?: string;
    rootPath?: string;
  } = {}
) {
  const root = options.absoluteRootPath || options.rootPath;
  return (await getConfiguredIndexAsync(root)).read(relativePath, options);
}

export async function getConfiguredLocalCorpusStatusAsync(dynamicRoot?: string) {
  const dbRoot = await getLocalCorpusRootAsync();
  return dynamicRoot || dbRoot
    ? getIndexForRoot(dbRoot, dynamicRoot).getStatus()
    : getDefaultLocalCorpusStatus();
}
