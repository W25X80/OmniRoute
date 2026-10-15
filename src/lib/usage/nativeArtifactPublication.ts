import { randomUUID } from "node:crypto";
import type {
  PreparedArtifact,
  StoredArtifact,
  ArtifactPutOptions,
} from "@/domain/persistence/artifactStore";
import {
  ArtifactStorageError,
  isArtifactStorageErrorKind,
} from "@/domain/persistence/artifactStore";
import {
  artifactFailureDiagnostic,
  isSharedArtifactFailure,
  type ArtifactReconciliationOptions,
  type ArtifactStorageOperation,
} from "./artifactReconciliationStatus";
import {
  getSelectedPostgresDatabase,
  getSelectedPersistenceIdentity,
} from "../db/controlPlaneContext";
import { getPersistenceArtifactStore } from "../db/persistenceRuntime";
import {
  prepareArtifactPublication,
  abandonArtifactPublication,
  claimArtifactDeletion,
  confirmArtifactUpload,
  postponeArtifactReconciliation,
  isArtifactPublicationConflict,
} from "../db/postgres/artifactPublicationRepository";
import { createPostgresCleanupRepository } from "../db/postgres/cleanupRepository";

const PUBLICATIONS = Symbol.for("omniroute.artifact.publications");
const root = globalThis as typeof globalThis & {
  [key: symbol]: WeakMap<object, Map<string, Set<object>>> | undefined;
};
function activePublications(identity: object): Map<string, Set<object>> {
  const states = (root[PUBLICATIONS] ??= new WeakMap<object, Map<string, Set<object>>>());
  let active = states.get(identity);
  if (!active) {
    active = new Map();
    states.set(identity, active);
  }
  return active;
}
/** One owned lease spans serialization, preparation, PUT and terminal SQL reconciliation. */
export function createNativeArtifactPublication() {
  const database = getSelectedPostgresDatabase();
  const identity = getSelectedPersistenceIdentity();
  const store = getPersistenceArtifactStore();
  if (!database || !identity || !store.storageScope || !store.isPutActive)
    throw new Error("Native artifact publication authority is unavailable");
  const active = activePublications(identity),
    token = {},
    owner = randomUUID();
  let prepared: PreparedArtifact | undefined;
  let preparationFailed = false,
    preparationConflict = false,
    acknowledged = false;
  const options: ArtifactPutOptions = {
    async onPrepared(artifact) {
      if (prepared) throw new Error("Artifact publication cannot prepare a second identity");
      prepared = artifact;
      const owners = active.get(artifact.reference) ?? new Set<object>();
      owners.add(token);
      active.set(artifact.reference, owners);
      try {
        await prepareArtifactPublication(database, artifact, owner);
      } catch (error) {
        preparationFailed = true;
        preparationConflict = isArtifactPublicationConflict(error);
        throw error;
      }
    },
  };
  return {
    options,
    owner,
    acknowledge(artifact: StoredArtifact): void {
      if (
        !prepared ||
        artifact.reference !== prepared.reference ||
        artifact.sizeBytes !== prepared.sizeBytes ||
        artifact.checksum !== prepared.checksum
      )
        throw new Error("Artifact publication acknowledgement does not match preparation");
      acknowledged = true;
    },
    async fail(error: unknown): Promise<void> {
      if (!prepared || preparationConflict) return;
      const notSent =
        preparationFailed ||
        ((isArtifactStorageErrorKind(error, "deadline") ||
          isArtifactStorageErrorKind(error, "put_unknown")) &&
          error.putStarted === false);
      await abandonArtifactPublication(
        database,
        prepared,
        owner,
        acknowledged ? "acknowledged" : notSent ? "not_sent" : "uncertain"
      );
    },
    release(): void {
      if (!prepared) return;
      const owners = active.get(prepared.reference);
      owners?.delete(token);
      if (owners?.size === 0) active.delete(prepared.reference);
    },
  };
}
/** Rotation and scheduled cleanup share the same claimed native deletion contract. */
export async function deleteNativeCallArtifacts(
  paths: readonly string[],
  options: ArtifactReconciliationOptions = {}
): Promise<{
  deletedArtifacts: number;
  pendingArtifactDeletions: number;
  errors: number;
}> {
  const unique = [...new Set(paths)];
  const result = { deletedArtifacts: 0, pendingArtifactDeletions: unique.length, errors: 0 };
  if (unique.length === 0) return result;
  const database = getSelectedPostgresDatabase(),
    identity = getSelectedPersistenceIdentity();
  const store = getPersistenceArtifactStore();
  if (!database || !identity || !store.storageScope || !store.isPutActive)
    throw new Error("Native artifact deletion authority is unavailable");
  const isLive = (reference: string) =>
    (root[PUBLICATIONS]?.get(identity)?.get(reference)?.size ?? 0) > 0 ||
    store.isPutActive!(reference);
  const deadline = options.deadline ?? Date.now() + 30000;
  for (const reference of unique) {
    if (Date.now() >= deadline) break;
    let operation: ArtifactStorageOperation | undefined;
    try {
      const row = await claimArtifactDeletion(
        database,
        reference,
        () => isLive(reference),
        store.storageScope
      );
      if (row === null || row?.publication_state === "attached") {
        result.pendingArtifactDeletions--;
        continue;
      }
      if (!row) continue;
      if (row.store_scope !== store.storageScope) {
        // NULL is legacy authority, not evidence that this bucket owns the object.
        await postponeArtifactReconciliation(database, reference);
        continue;
      }
      if (row.upload_state === "legacy_unknown") {
        await postponeArtifactReconciliation(database, reference);
        continue;
      }
      if (row.upload_state === "uncertain") {
        operation = "get";
        const bytes = await store.get(reference);
        options.onStorageSuccess?.(operation);
        operation = undefined;
        if (bytes === null) {
          await postponeArtifactReconciliation(database, reference);
          continue;
        }
        if (
          row.expected_size_bytes === null ||
          BigInt(row.expected_size_bytes) !== BigInt(bytes.byteLength) ||
          row.expected_sha256 === null ||
          !reference.endsWith(`_${row.expected_sha256}.json`)
        )
          throw new ArtifactStorageError("integrity");
        if (isLive(reference)) {
          await postponeArtifactReconciliation(database, reference);
          continue;
        }
        await confirmArtifactUpload(database, reference);
      }
      if (isLive(reference)) {
        await postponeArtifactReconciliation(database, reference);
        continue;
      }
      // False is confirmed already-absent, not an availability failure. The upload
      // is completed (or never dispatched) before either result can retire identity.
      if (row.upload_state !== "not_sent") {
        operation = "delete";
        if (await store.delete(reference)) result.deletedArtifacts++;
        options.onStorageSuccess?.(operation);
        operation = undefined;
      }
      if (await createPostgresCleanupRepository(database).acknowledgeArtifactDeletion(reference))
        result.pendingArtifactDeletions--;
    } catch (error) {
      result.errors++;
      options.onFailure?.(error, operation);
      console.error("[Cleanup] Artifact reconciliation failed", artifactFailureDiagnostic(error));
      await postponeArtifactReconciliation(database, reference).catch(() => {});
      if (isSharedArtifactFailure(error)) break;
    }
  }
  return result;
}
