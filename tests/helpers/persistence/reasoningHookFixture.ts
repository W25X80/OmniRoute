import {
  fixture,
  withPostgresCatalogSnapshot as captureCatalog,
} from "./runtimeOwnedWorkFixture.ts";
import {
  getSelectedControlPlane,
  getSelectedPersistenceIdentity,
} from "../../../src/lib/db/controlPlaneContext.ts";
import type { PostgresDatabase } from "../../../src/lib/db/postgres/database.ts";

export {
  readPostgresEnvironment,
  readS3ArtifactConfiguration,
  initializePostgresControlPlane,
  createS3ArtifactStore,
  initializeCircuitBreakerPersistence,
  initializeQuotaSnapshotCache,
  flushCircuitBreakerPersistence,
} from "./runtimeOwnedWorkFixture.ts";

export const hookFixture = {
  enabled: true,
  delayNextCatalog: undefined as Promise<void> | undefined,
  finishWrite: undefined as Promise<void> | undefined,
  writeEntered: undefined as (() => void) | undefined,
  writeFailure: undefined as Error | undefined,
  writes: [] as Array<{
    key: string;
    reasoning: string;
    generation: number | undefined;
    authority: object | undefined;
  }>,
  sqliteWrites: [] as string[],
  reset() {
    hookFixture.enabled = true;
    hookFixture.delayNextCatalog = undefined;
    hookFixture.finishWrite = undefined;
    hookFixture.writeEntered = undefined;
    hookFixture.writeFailure = undefined;
    hookFixture.writes = [];
    hookFixture.sqliteWrites = [];
  },
};
export function isFeatureFlagEnabled() {
  return hookFixture.enabled;
}
export function getSelectedReasoningCacheRepository() {
  return getSelectedControlPlane() ? {} : undefined;
}
export function getSelectedReasoningCacheAuthority() {
  return getSelectedPersistenceIdentity();
}
export async function setReasoningCacheAsync(
  key: string,
  _provider: string,
  _model: string,
  reasoning: string
) {
  const authority = getSelectedPersistenceIdentity();
  hookFixture.writes.push({ key, reasoning, authority, generation: fixture.selectedGeneration() });
  hookFixture.writeEntered?.();
  if (hookFixture.finishWrite) await hookFixture.finishWrite;
  getSelectedControlPlane()!.assertReady();
  if (hookFixture.writeFailure) throw hookFixture.writeFailure;
}
export function setReasoningCache(key: string) {
  hookFixture.sqliteWrites.push(key);
}
export function getReasoningCache() {
  return null;
}
export function getReasoningCacheStats() {
  return { totalEntries: 0, totalChars: 0, byProvider: [], oldestEntry: null, newestEntry: null };
}
export function getReasoningCacheEntries() {
  return [];
}
export function clearAllReasoningCache() {
  return 0;
}
export function cleanupExpiredReasoning() {
  return 0;
}
export function deleteReasoningCache() {
  return 0;
}
export const getReasoningCacheStatsAsync = async () => getReasoningCacheStats();
export const getReasoningCacheEntriesAsync = async () => getReasoningCacheEntries();
export const clearAllReasoningCacheAsync = async () => clearAllReasoningCache();
export const cleanupExpiredReasoningAsync = async () => cleanupExpiredReasoning();
export const deleteReasoningCacheAsync = async () => deleteReasoningCache();
export async function withPostgresCatalogSnapshot<T>(
  database: PostgresDatabase,
  body: () => T | Promise<T>
): Promise<T> {
  const delayed = hookFixture.delayNextCatalog;
  hookFixture.delayNextCatalog = undefined;
  return captureCatalog(database, async () => {
    if (delayed) await delayed;
    return body();
  });
}
export function needsTranslation() {
  return false;
}
export function extractToolSchemaMap() {
  return new Map();
}
export function translateNonStreamingResponse(body: unknown) {
  return body;
}
