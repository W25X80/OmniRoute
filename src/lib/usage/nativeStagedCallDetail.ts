import {
  getSelectedPersistenceIdentity,
  getSelectedPostgresDatabase,
} from "../db/controlPlaneContext";
import { getPersistenceArtifactStore } from "../db/persistenceRuntime";
import { readStagedArtifact, projectStagedArtifact } from "../db/postgres/stagedArtifactRepository";
import { deserializeStagedCallArtifactAsync } from "./callLogArtifactWriter";

const MAX_READS = 2;
const DEADLINE_MS = 10_000;
const key = Symbol.for("omniroute.artifact.stagedDetail.admission.v1");
const root = globalThis as typeof globalThis & { [key]?: { active: number } };
const admission = (root[key] ??= { active: 0 });

/** Capture before the original summary read, never choose a new authority after suspension. */
export function captureStagedDetailAuthority() {
  const database = getSelectedPostgresDatabase();
  const identity = getSelectedPersistenceIdentity();
  const storageScope = getPersistenceArtifactStore().storageScope;
  return database && identity && storageScope ? { database, identity, storageScope } : null;
}

export async function readNativeStagedCallDetail(
  authority: NonNullable<ReturnType<typeof captureStagedDetailAuthority>>,
  callId: string,
  callOrder: number,
  signal?: AbortSignal,
  loadLegacyPipeline?: () => Promise<Record<string, unknown> | null>
) {
  if (signal?.aborted || admission.active >= MAX_READS) return null;
  admission.active++;
  const expiresAt = Date.now() + DEADLINE_MS;
  function current(): boolean {
    return (
      !signal?.aborted &&
      Date.now() < expiresAt &&
      getSelectedPersistenceIdentity() === authority.identity &&
      getSelectedPostgresDatabase() === authority.database &&
      getPersistenceArtifactStore().storageScope === authority.storageScope
    );
  }
  const work = (async () => {
    if (!current()) return null;
    const snapshot = await readStagedArtifact(authority.database, {
      callId,
      callOrder,
      storageScope: authority.storageScope,
    });
    if (!snapshot || !current()) return null;
    const result = await deserializeStagedCallArtifactAsync({
      ciphertext: snapshot.ciphertext,
      sizeBytes: snapshot.sizeBytes,
      sha256: snapshot.sha256,
      callId,
      expiresAt,
    });
    if (result.state !== "ready" || !result.artifact || !current()) return null;
    const artifact = result.artifact;
    // Preserve the ordinary detail reader's fallback before the final privacy fence.
    const pipelinePayloads = artifact.pipeline ?? (await loadLegacyPipeline?.()) ?? null;
    if (!current()) return null;
    const projected = await projectStagedArtifact(authority.database, snapshot, (uploadState) =>
      current()
        ? {
            artifact,
            pipelinePayloads,
            detailSource: "staging" as const,
            artifactPublicationState: uploadState,
          }
        : null
    );
    return current() ? projected : null;
  })()
    .catch(() => null)
    .finally(() => {
      // A response deadline/abort does not release ownership of still-running SQL/CPU work.
      admission.active--;
    });
  let timer: NodeJS.Timeout | undefined;
  let abort: (() => void) | undefined;
  const stopped = new Promise<null>((resolve) => {
    abort = () => resolve(null);
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) resolve(null);
    timer = setTimeout(() => resolve(null), Math.max(0, expiresAt - Date.now()));
    timer.unref?.();
  });
  try {
    return await Promise.race([work, stopped]);
  } finally {
    if (timer) clearTimeout(timer);
    if (abort) signal?.removeEventListener("abort", abort);
  }
}
