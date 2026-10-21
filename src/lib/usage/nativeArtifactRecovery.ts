import { createHash, randomUUID } from "node:crypto";
import type { NativeArtifactStore } from "@/domain/persistence/artifactStore";
import {
  ArtifactStorageError,
  isArtifactStorageErrorKind,
} from "@/domain/persistence/artifactStore";
import { getSelectedPostgresDatabase } from "../db/controlPlaneContext";
import {
  getPersistenceArtifactStore,
  runPersistenceBackgroundWork,
} from "../db/persistenceRuntime";
import { encrypt, decrypt, looksEncrypted } from "../db/encryption";
import type { PostgresDatabase } from "../db/postgres/database";
import {
  readArtifactRecovery,
  listArtifactRecovery,
  claimArtifactRecoveryDispatch,
  deferArtifactRecovery,
  attachArtifactRecovery,
  replaceUncertainArtifactRecovery,
  type ArtifactRecoveryIntent,
} from "../db/postgres/artifactRecoveryRepository";
import { confirmArtifactUpload } from "../db/postgres/artifactPublicationRepository";
import { deleteNativeCallArtifacts } from "./nativeArtifactPublication";
import { createPostgresCleanupRepository } from "../db/postgres/cleanupRepository";
import {
  artifactFailureDiagnostic,
  isSharedArtifactFailure,
  recordArtifactReconciliationPass,
  type ArtifactReconciliationOptions,
  type ArtifactStorageOperation,
} from "./artifactReconciliationStatus";

/** Immutable, privacy-filtered bytes only. Encryption and hashing precede SQL acquisition. */
export function prepareNativeArtifactRecovery(bytes: Uint8Array): ArtifactRecoveryIntent {
  const store = getPersistenceArtifactStore();
  if (!store.prepare) throw new Error("Artifact store does not support durable preparation");
  const artifact = store.prepare(bytes);
  const ciphertext = encrypt(Buffer.from(bytes).toString("base64"));
  if (!ciphertext || !looksEncrypted(ciphertext))
    throw new Error("Encrypted artifact staging is unavailable");
  return { artifact, ciphertext, owner: randomUUID() };
}
/** Injectable transport/database make crash/fault proofs independent of a live AWS account. */
export async function recoverNativeArtifact(
  reference: string,
  database: PostgresDatabase = getSelectedPostgresDatabase()!,
  store: NativeArtifactStore = getPersistenceArtifactStore(),
  options: ArtifactReconciliationOptions = {}
): Promise<boolean> {
  if (!database || !store.storageScope || !store.isPutActive)
    throw new Error("Artifact recovery authority is unavailable");
  const row = await readArtifactRecovery(database, reference);
  if (!row) return false;
  if (row.store_scope !== store.storageScope)
    throw new Error("Artifact recovery storage scope differs");
  if (row.publication_state !== "publishing") return row.publication_state === "attached";
  if (store.isPutActive(reference)) return false;
  let claimed = false;
  let claim: Promise<boolean> | undefined;
  const dispatch = randomUUID();
  let operation: ArtifactStorageOperation | undefined;
  try {
    if (row.upload_state === "not_sent") {
      if (!row.staged_payload || !looksEncrypted(row.staged_payload))
        throw new ArtifactStorageError("integrity");
      const plaintext = decrypt(row.staged_payload, { quiet: true });
      if (!plaintext) throw new ArtifactStorageError("integrity");
      const bytes = Buffer.from(plaintext, "base64");
      if (
        bytes.byteLength !== Number(row.expected_size_bytes) ||
        createHash("sha256").update(bytes).digest("hex") !== row.expected_sha256
      )
        throw new ArtifactStorageError("integrity");
      // Client preparation cannot consume or ambiguously claim a PUT attempt.
      operation = "put";
      await store.warmup?.();
      const stored = await store.put(bytes, {
        prepared: Object.freeze({
          reference,
          checksum: row.expected_sha256,
          sizeBytes: bytes.byteLength,
          storageScope: row.store_scope,
          singleAttempt: true,
        }),
        async onPrepared() {
          claim = claimArtifactRecoveryDispatch(
            database,
            reference,
            row.publication_owner,
            dispatch
          );
          claimed = await claim;
          if (!claimed) throw new Error("Artifact dispatch is already owned or revoked");
        },
      });
      options.onStorageSuccess?.(operation);
      operation = undefined;
      if (
        stored.reference !== reference ||
        stored.checksum !== row.expected_sha256 ||
        stored.sizeBytes !== bytes.byteLength
      )
        throw new Error("Artifact acknowledgement differs from intent");
    } else if (row.upload_state === "uncertain") {
      operation = "get";
      const bytes = await store.get(reference);
      options.onStorageSuccess?.(operation);
      operation = undefined;
      if (!bytes) {
        // Missing is not proof the old PUT cannot commit later. Transfer staging
        // to one fresh identity; the old key keeps its deletion-only liability.
        await replaceUncertainArtifactRecovery(database, row, () => store.isPutActive!(reference));
        return false;
      }
      if (
        bytes.byteLength !== Number(row.expected_size_bytes) ||
        createHash("sha256").update(bytes).digest("hex") !== row.expected_sha256
      )
        throw new ArtifactStorageError("integrity");
    } else return false;
    const attached = await attachArtifactRecovery(database, reference, row.publication_owner);
    if (!attached) await confirmArtifactUpload(database, reference);
    return attached;
  } catch (error) {
    // Only the acknowledged claim holder can undo its dispatch claim, and only
    // explicit pre-dispatch evidence permits that transition. COMMIT loss never does.
    // The public store deadline can precede the SQL claim acknowledgement.
    // Join that claim under this still-live recovery owner: a late positive ACK
    // plus proven non-dispatch may release its nonce; an unknown COMMIT may not.
    if (
      claim &&
      (isArtifactStorageErrorKind(error, "deadline") ||
        isArtifactStorageErrorKind(error, "unavailable") ||
        isArtifactStorageErrorKind(error, "put_unknown")) &&
      error.putStarted === false
    ) {
      try {
        claimed = await claim;
      } catch {
        // No acknowledgement means no authority to reset an uncertain claim.
      }
    }
    const notSent =
      claimed &&
      (isArtifactStorageErrorKind(error, "deadline") ||
        isArtifactStorageErrorKind(error, "unavailable") ||
        isArtifactStorageErrorKind(error, "put_unknown") ||
        isArtifactStorageErrorKind(error, "overloaded") ||
        isArtifactStorageErrorKind(error, "closed")) &&
      (error.putStarted === false || error.kind === "overloaded" || error.kind === "closed");
    await deferArtifactRecovery(database, reference, dispatch, notSent);
    options.onFailure?.(error, operation);
    console.warn(
      JSON.stringify({
        event: "artifact.publication.deferred",
        ...artifactFailureDiagnostic(error),
      })
    );
    return false;
  }
}

/** Request fast path and periodic recovery share the same durable CAS protocol. */
export async function publishNativeArtifactIntent(
  intent: ArtifactRecoveryIntent
): Promise<boolean> {
  const result = await recoverNativeArtifact(intent.artifact.reference);
  if (!result) await deleteNativeCallArtifacts([intent.artifact.reference]);
  return result;
}

const RECOVERY = Symbol.for("omniroute.artifact.recovery.lifecycle");
type RecoveryState = {
  timer?: ReturnType<typeof setInterval>;
  active?: Promise<void>;
  deletionFirst?: boolean;
  failures?: number;
  retryAt?: number;
};
const globals = globalThis as typeof globalThis & { [RECOVERY]?: RecoveryState };
function state(): RecoveryState {
  return (globals[RECOVERY] ??= {});
}

/** One admission budget and alternating first portion prevent either queue from starving. */
export async function runNativeArtifactReconciliationPass(deletionFirst = false): Promise<boolean> {
  const database = getSelectedPostgresDatabase();
  const store = getPersistenceArtifactStore();
  if (!database || !store.storageScope) return false;
  const repository = createPostgresCleanupRepository(database);
  const deadline = Date.now() + 10000;
  let sharedFailure: unknown;
  let failedOperation: ArtifactStorageOperation | undefined;
  let failure: unknown;
  const succeeded = new Set<ArtifactStorageOperation>();
  const onStorageSuccess = (operation: ArtifactStorageOperation) => {
    succeeded.add(operation);
  };
  const onFailure = (error: unknown, operation?: ArtifactStorageOperation) => {
    if (!failure) failedOperation = operation;
    failure ??= error;
    if (isSharedArtifactFailure(error) && !sharedFailure) {
      sharedFailure = error;
      failedOperation = operation;
    }
  };
  const publish = async (portionDeadline: number) => {
    for (const reference of await listArtifactRecovery(database, store.storageScope!)) {
      if (Date.now() >= portionDeadline || sharedFailure) break;
      await recoverNativeArtifact(reference, database, store, { onFailure, onStorageSuccess });
    }
  };
  const remove = async (portionDeadline: number) => {
    const references = await repository.dueArtifactDeletions(store.storageScope!);
    await deleteNativeCallArtifacts(references, {
      deadline: portionDeadline,
      onFailure,
      onStorageSuccess,
    });
  };
  try {
    const portions = deletionFirst ? [remove, publish] : [publish, remove];
    await portions[0](Math.min(deadline, Date.now() + 5000));
    if (!sharedFailure && Date.now() < deadline) await portions[1](deadline);
  } catch (error) {
    onFailure(error);
  }
  const recovered = recordArtifactReconciliationPass(
    await repository.artifactReconciliationCounts(store.storageScope),
    { failure: sharedFailure ?? failure, operation: failedOperation, succeeded }
  );
  if (sharedFailure) throw sharedFailure;
  return recovered;
}

export function startNativeArtifactRecovery(): void {
  const current = state();
  if (current.timer) return;
  const tick = () => {
    if (current.active || Date.now() < (current.retryAt ?? 0)) return;
    const deletionFirst = current.deletionFirst ?? false;
    current.deletionFirst = !deletionFirst;
    const work = runPersistenceBackgroundWork(
      async () => {
        if (await runNativeArtifactReconciliationPass(deletionFirst)) {
          current.failures = 0;
          current.retryAt = 0;
        }
      },
      { owner: "artifact.reconcile" }
    ).catch((error) => {
      current.failures = Math.min(5, (current.failures ?? 0) + 1);
      current.retryAt = Date.now() + Math.min(300000, 30000 * 2 ** (current.failures - 1));
      console.warn("[callLogs] Artifact recovery pass deferred", artifactFailureDiagnostic(error));
    });
    current.active = work;
    void work.finally(() => {
      if (current.active === work) current.active = undefined;
    });
  };
  current.timer = setInterval(tick, 30000);
  current.timer.unref?.();
}
export async function stopNativeArtifactRecovery(): Promise<void> {
  const current = state();
  if (current.timer) clearInterval(current.timer);
  current.timer = undefined;
  await current.active;
}
