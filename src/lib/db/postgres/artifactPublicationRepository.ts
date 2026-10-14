import { z } from "zod";
import { acquirePostgresLock, type PostgresLockMode } from "./locks";
import type { PreparedArtifact } from "@/domain/persistence/artifactStore";
import type { PostgresDatabase, PostgresTransaction } from "./database";

export type ArtifactUploadState = "uncertain" | "acknowledged" | "not_sent" | "legacy_unknown";
export interface ArtifactPublicationRow {
  relpath: string;
  publication_state: "publishing" | "attached" | "delete_pending";
  upload_state: ArtifactUploadState;
  publication_owner: string | null;
  store_scope: string | null;
  expected_size_bytes: string | null;
  expected_sha256: string | null;
  call_log_id?: string | null;
  call_log_order?: string | null;
}
const ownerSchema = z.string().uuid();
const preparedSchema = z
  .object({
    reference: z
      .string()
      .regex(
        /^s3\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_[0-9a-f]{64}\.json$/
      ),
    sizeBytes: z.number().int().min(0).max(8388608),
    checksum: z.string().regex(/^[0-9a-f]{64}$/),
    storageScope: z.string().regex(/^[0-9a-f]{64}$/),
    singleAttempt: z.literal(true),
  })
  .strict();
const CONFLICT = Symbol.for("omniroute.artifact.publication.conflict");
export class ArtifactPublicationConflictError extends Error {
  constructor() {
    super("Artifact publication identity is already owned");
    this.name = "ArtifactPublicationConflictError";
    Object.defineProperty(this, CONFLICT, { value: true });
  }
}
export function isArtifactPublicationConflict(error: unknown): boolean {
  return Boolean(
    error && typeof error === "object" && (error as Record<symbol, unknown>)[CONFLICT] === true
  );
}
export function lockArtifactPublication(
  transaction: PostgresTransaction,
  mode: PostgresLockMode = "exclusive"
) {
  return acquirePostgresLock(transaction, "accounting", mode);
}
export async function artifactHasSqlOwner(
  transaction: PostgresTransaction,
  reference: string
): Promise<boolean> {
  const { rows } = await transaction.query<{ present: boolean }>(
    `SELECT EXISTS(SELECT 1 FROM omniroute.call_logs WHERE artifact_relpath=$1
      UNION ALL SELECT 1 FROM omniroute.call_logs_v1_legacy WHERE artifact_relpath=$1) AS present`,
    [reference]
  );
  return rows[0]?.present === true;
}
export async function readArtifactPublication(
  transaction: PostgresTransaction,
  reference: string
): Promise<ArtifactPublicationRow | undefined> {
  const { rows } = await transaction.query<ArtifactPublicationRow>(
    "SELECT * FROM omniroute.artifact_deletion_queue WHERE relpath=$1 FOR UPDATE",
    [reference]
  );
  return rows[0];
}
/** Strict preparation rejects an allocated key replay; the nonce identifies a lost COMMIT. */
export async function prepareArtifactPublication(
  database: PostgresDatabase,
  artifact: PreparedArtifact,
  owner: string
): Promise<void> {
  const checked = preparedSchema.parse(artifact);
  ownerSchema.parse(owner);
  if (!checked.reference.endsWith(`_${checked.checksum}.json`))
    throw new Error("Artifact publication checksum does not match its identity");
  await database.transaction(async (transaction) => {
    await lockArtifactPublication(transaction);
    if (await artifactHasSqlOwner(transaction, checked.reference))
      throw new ArtifactPublicationConflictError();
    const result = await transaction.query(
      `INSERT INTO omniroute.artifact_deletion_queue
       (relpath,publication_state,upload_state,publication_owner,store_scope,expected_size_bytes,expected_sha256,next_attempt_at)
       VALUES($1,'publishing','uncertain',$2,$3,$4,$5,statement_timestamp()+interval '5 minutes')
       ON CONFLICT(relpath) DO NOTHING`,
      [checked.reference, owner, checked.storageScope, checked.sizeBytes, checked.checksum]
    );
    if (result.rowCount !== 1) throw new ArtifactPublicationConflictError();
  });
}
/** Called under the same history fence before any SQL row accepts an uploaded payload. */
export async function requireArtifactPublication(
  transaction: PostgresTransaction,
  reference: string,
  owner: unknown,
  size: unknown,
  checksum: unknown
): Promise<void> {
  const checkedOwner = ownerSchema.parse(owner);
  const row = await readArtifactPublication(transaction, reference);
  if (
    !row ||
    row.publication_state !== "publishing" ||
    row.publication_owner !== checkedOwner ||
    row.expected_sha256 !== checksum ||
    row.expected_size_bytes === null ||
    row.expected_size_bytes !== String(size) ||
    row.upload_state !== "uncertain"
  )
    throw new Error("Artifact publication ownership is unavailable");
}
/** Collision retries preserve the intent; only an actual inserted row transfers ownership. */
export async function finishArtifactPublication(
  transaction: PostgresTransaction,
  reference: string,
  owner: string,
  payloadAccepted: boolean
): Promise<void> {
  if (payloadAccepted) {
    await transaction.query(
      `UPDATE omniroute.artifact_deletion_queue SET publication_state='attached',
       upload_state='acknowledged' WHERE relpath=$1
       AND publication_owner=$2 AND publication_state='publishing'`,
      [reference, owner]
    );
  } else {
    await transaction.query(
      `UPDATE omniroute.artifact_deletion_queue SET publication_state='delete_pending',
       upload_state='acknowledged',next_attempt_at=statement_timestamp()
       WHERE relpath=$1 AND publication_owner=$2 AND publication_state='publishing'`,
      [reference, owner]
    );
  }
}
/** A fresh history fence resolves the old transaction before considering its own identity. */
export async function abandonArtifactPublication(
  database: PostgresDatabase,
  artifact: PreparedArtifact,
  owner: string,
  outcome: ArtifactUploadState
): Promise<void> {
  await database.transaction(async (transaction) => {
    await lockArtifactPublication(transaction);
    if (await artifactHasSqlOwner(transaction, artifact.reference)) return;
    await transaction.query(
      `INSERT INTO omniroute.artifact_deletion_queue
       (relpath,publication_state,upload_state,publication_owner,store_scope,expected_size_bytes,expected_sha256,next_attempt_at)
       VALUES($1,'delete_pending',$2,$3,$4,$5,$6,statement_timestamp())
       ON CONFLICT(relpath) DO UPDATE SET publication_state='delete_pending',
         upload_state=CASE WHEN artifact_deletion_queue.upload_state='acknowledged'
           THEN 'acknowledged' ELSE EXCLUDED.upload_state END,
         next_attempt_at=statement_timestamp()
       WHERE artifact_deletion_queue.publication_owner=EXCLUDED.publication_owner
         AND artifact_deletion_queue.store_scope=EXCLUDED.store_scope
         AND artifact_deletion_queue.expected_sha256=EXCLUDED.expected_sha256
         AND artifact_deletion_queue.expected_size_bytes=EXCLUDED.expected_size_bytes`,
      [
        artifact.reference,
        outcome,
        owner,
        artifact.storageScope,
        artifact.sizeBytes,
        artifact.checksum,
      ]
    );
  });
}
/** The durable claim forbids later attachment. Live work is postponed, never deleted. */
export async function claimArtifactDeletion(
  database: PostgresDatabase,
  reference: string,
  isLive: () => boolean
): Promise<ArtifactPublicationRow | null | undefined> {
  return database.transaction(async (transaction) => {
    await lockArtifactPublication(transaction);
    const row = await readArtifactPublication(transaction, reference);
    if (!row) return null;
    // Durable recovery owns this un-attached logical call until publication or
    // revocation. Cleanup cannot turn a pending metadata row into an orphan.
    if (row.publication_state === "publishing" && row.call_log_id) {
      const owner = await transaction.query(
        "SELECT 1 FROM omniroute.call_logs WHERE id=$1 AND insertion_order=$2",
        [row.call_log_id, row.call_log_order]
      );
      if (owner.rowCount === 1) return undefined;
    }
    if (isLive()) {
      await transaction.query(
        `UPDATE omniroute.artifact_deletion_queue SET
         next_attempt_at=statement_timestamp()+interval '5 minutes' WHERE relpath=$1`,
        [reference]
      );
      return row.publication_state === "attached" ? row : undefined;
    }
    if (await artifactHasSqlOwner(transaction, reference)) {
      await transaction.query(
        `UPDATE omniroute.artifact_deletion_queue SET publication_state='attached',
         next_attempt_at=statement_timestamp()+interval '5 minutes' WHERE relpath=$1`,
        [reference]
      );
      return { ...row, publication_state: "attached" };
    }
    await transaction.query(
      "UPDATE omniroute.artifact_deletion_queue SET publication_state='delete_pending',staged_payload=NULL,staged_bytes=0 WHERE relpath=$1",
      [reference]
    );
    return { ...row, publication_state: "delete_pending" };
  });
}
export async function postponeArtifactReconciliation(
  database: PostgresDatabase,
  reference: string
): Promise<void> {
  await database.transaction(async (transaction) => {
    await lockArtifactPublication(transaction);
    await transaction.query(
      "UPDATE omniroute.artifact_deletion_queue SET next_attempt_at=statement_timestamp()+interval '5 minutes' WHERE relpath=$1",
      [reference]
    );
  });
}
/** A verified positive GET proves the sole PUT's completed effect; 404 cannot do so. */
export async function confirmArtifactUpload(
  database: PostgresDatabase,
  reference: string
): Promise<void> {
  await database.transaction(async (transaction) => {
    await lockArtifactPublication(transaction);
    await transaction.query(
      `UPDATE omniroute.artifact_deletion_queue SET upload_state='acknowledged'
       WHERE relpath=$1 AND publication_state='delete_pending' AND upload_state='uncertain'`,
      [reference]
    );
  });
}
