import { createHash, randomUUID } from "node:crypto";
import type { NativeArtifactStore } from "@/domain/persistence/artifactStore";
import { isArtifactStorageErrorKind } from "@/domain/persistence/artifactStore";
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
  type ArtifactRecoveryIntent,
} from "../db/postgres/artifactRecoveryRepository";
import { confirmArtifactUpload } from "../db/postgres/artifactPublicationRepository";
import { deleteNativeCallArtifacts } from "./nativeArtifactPublication";

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
function safeDiagnostic(error: unknown) {
  if (
    isArtifactStorageErrorKind(error, "put_unknown") ||
    isArtifactStorageErrorKind(error, "unavailable")
  )
    return { kind: error.kind, ...error.diagnostic };
  return { kind: "publication_deferred" };
}

/** Injectable transport/database make crash/fault proofs independent of a live AWS account. */
export async function recoverNativeArtifact(
  reference: string,
  database: PostgresDatabase = getSelectedPostgresDatabase()!,
  store: NativeArtifactStore = getPersistenceArtifactStore()
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
  try {
    if (row.upload_state === "not_sent") {
      if (!row.staged_payload || !looksEncrypted(row.staged_payload))
        throw new Error("Artifact recovery bytes are unavailable");
      const plaintext = decrypt(row.staged_payload, { quiet: true });
      if (!plaintext) throw new Error("Artifact recovery bytes cannot be decrypted");
      const bytes = Buffer.from(plaintext, "base64");
      if (
        bytes.byteLength !== Number(row.expected_size_bytes) ||
        createHash("sha256").update(bytes).digest("hex") !== row.expected_sha256
      )
        throw new Error("Artifact recovery integrity check failed");
      // Client preparation cannot consume or ambiguously claim a PUT attempt.
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
      if (
        stored.reference !== reference ||
        stored.checksum !== row.expected_sha256 ||
        stored.sizeBytes !== bytes.byteLength
      )
        throw new Error("Artifact acknowledgement differs from intent");
    } else if (row.upload_state === "uncertain") {
      const bytes = await store.get(reference);
      if (!bytes) {
        await deferArtifactRecovery(database, reference);
        return false;
      }
      if (
        bytes.byteLength !== Number(row.expected_size_bytes) ||
        createHash("sha256").update(bytes).digest("hex") !== row.expected_sha256
      )
        throw new Error("Artifact recovery integrity check failed");
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
    console.warn("[callLogs] Artifact publication deferred", safeDiagnostic(error));
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
type RecoveryState = { timer?: ReturnType<typeof setInterval>; active?: Promise<void> };
const globals = globalThis as typeof globalThis & { [RECOVERY]?: RecoveryState };
function state(): RecoveryState {
  return (globals[RECOVERY] ??= {});
}
export function startNativeArtifactRecovery(): void {
  const current = state();
  if (current.timer) return;
  const tick = () => {
    if (current.active) return;
    const work = runPersistenceBackgroundWork(async () => {
      const database = getSelectedPostgresDatabase();
      const store = getPersistenceArtifactStore();
      if (!database || !store.storageScope) return;
      const started = Date.now();
      for (const reference of await listArtifactRecovery(database, store.storageScope)) {
        if (Date.now() - started >= 10000) break;
        await recoverNativeArtifact(reference, database, store);
      }
    }).catch(() => {
      console.warn("[callLogs] Artifact recovery pass deferred");
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
