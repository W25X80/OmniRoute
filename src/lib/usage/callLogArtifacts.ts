import { isArtifactStorageErrorKind } from "@/domain/persistence/artifactStore";
import { getSelectedControlPlane } from "../db/controlPlaneContext";
import { getPersistenceArtifactStore } from "../db/persistenceRuntime";
import { readCallArtifact, deleteCallArtifact } from "./callLogArtifactFiles.ts";
import type { CallLogArtifactWriteResult } from "./callLogArtifactFiles.ts";

export {
  CALL_LOGS_DIR,
  MAX_CALL_LOG_ARTIFACT_BYTES,
  getArtifactMaxBytesForTest,
  isSizeLimitOmissionMarker,
  buildArtifactRelativePath,
  writeCallArtifact,
  readCallArtifact,
  deleteCallArtifact,
  cleanupEmptyCallLogDirs,
  listCallLogArtifactFiles,
  purgeCallLogArtifactDirectory,
} from "./callLogArtifactFiles.ts";
export type {
  CallLogDetailState,
  CallLogArtifact,
  CallLogArtifactWriteResult,
  PurgeCallLogArtifactDirectoryResult,
} from "./callLogArtifactFiles.ts";

/** The caller supplies worker-serialized bytes; serialization is never repeated on this thread. */
export async function writeCallArtifactToNativeStore(
  bytes: Uint8Array
): Promise<CallLogArtifactWriteResult> {
  if (!getSelectedControlPlane()) throw new Error("Native call-log persistence is not selected");
  const stored = await getPersistenceArtifactStore().put(bytes);
  return { relPath: stored.reference, sizeBytes: stored.sizeBytes, sha256: stored.checksum };
}

export async function readCallArtifactAsync(
  relativePath: string | null
): Promise<ReturnType<typeof readCallArtifact>> {
  if (!getSelectedControlPlane()) return readCallArtifact(relativePath);
  if (!relativePath) return { artifact: null, state: "missing" };
  let bytes: Uint8Array | null;
  try {
    bytes = await getPersistenceArtifactStore().get(relativePath);
  } catch (error) {
    if (isArtifactStorageErrorKind(error, "integrity")) {
      return { artifact: null, state: "corrupt" };
    }
    throw error;
  }
  if (bytes === null) return { artifact: null, state: "missing" };
  const { deserializeCallArtifactAsync } = await import("./callLogArtifactWriter.ts");
  return deserializeCallArtifactAsync(bytes);
}

export async function deleteCallArtifactAsync(relativePath: string | null): Promise<boolean> {
  if (!getSelectedControlPlane()) return deleteCallArtifact(relativePath);
  if (!relativePath) return false;
  return getPersistenceArtifactStore().delete(relativePath);
}
