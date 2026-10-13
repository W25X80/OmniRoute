import type { PreparedArtifact } from "@/domain/persistence/artifactStore";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";
import { lockArtifactPublication, readArtifactPublication } from "./artifactPublicationRepository";

export const ARTIFACT_STAGING_BYTE_LIMIT = 128 * 1024 * 1024;
export interface ArtifactRecoveryIntent {
  artifact: PreparedArtifact;
  owner: string;
  ciphertext: string;
}
export interface ArtifactRecoveryRow {
  relpath: string;
  publication_owner: string;
  store_scope: string;
  expected_size_bytes: string;
  expected_sha256: string;
  publication_state: "publishing" | "attached" | "delete_pending";
  upload_state: "not_sent" | "uncertain" | "acknowledged" | "legacy_unknown";
  call_log_id: string;
  call_log_order: string;
  staged_payload: string | null;
  dispatch_owner: string | null;
}

/** The caller holds keyPolicy(shared) then accounting, and has inserted metadata. */
export async function stageArtifactRecovery(
  tx: PostgresTransaction,
  intent: ArtifactRecoveryIntent,
  id: string,
  order: string
): Promise<boolean> {
  const bytes = Buffer.byteLength(intent.ciphertext);
  if (
    !intent.ciphertext.startsWith("enc:v1:") ||
    bytes > 22369720 ||
    !Number.isSafeInteger(intent.artifact.sizeBytes) ||
    intent.artifact.sizeBytes > 8388608
  )
    throw new Error("Invalid encrypted artifact recovery intent");
  const used = (
    await tx.query<{ used: string }>(
      "SELECT COALESCE(SUM(staged_bytes),0) AS used FROM omniroute.artifact_deletion_queue WHERE staged_payload IS NOT NULL"
    )
  ).rows[0].used;
  if (BigInt(used) + BigInt(bytes) > BigInt(ARTIFACT_STAGING_BYTE_LIMIT)) return false;
  await tx.query(
    `INSERT INTO omniroute.artifact_deletion_queue
      (relpath,publication_state,upload_state,publication_owner,store_scope,expected_size_bytes,
       expected_sha256,call_log_id,call_log_order,staged_payload,staged_bytes,next_attempt_at)
     VALUES($1,'publishing','not_sent',$2,$3,$4,$5,$6,$7,$8,$9,statement_timestamp())`,
    [
      intent.artifact.reference,
      intent.owner,
      intent.artifact.storageScope,
      intent.artifact.sizeBytes,
      intent.artifact.checksum,
      id,
      order,
      intent.ciphertext,
      bytes,
    ]
  );
  return true;
}

export async function readArtifactRecovery(database: PostgresDatabase, reference: string) {
  return database.transaction(
    async (tx) =>
      (
        await tx.query<ArtifactRecoveryRow>(
          "SELECT * FROM omniroute.artifact_deletion_queue WHERE relpath=$1 AND call_log_id IS NOT NULL",
          [reference]
        )
      ).rows[0]
  );
}
export async function listArtifactRecovery(database: PostgresDatabase, scope: string, limit = 8) {
  return database.transaction(async (tx) =>
    (
      await tx.query<{ relpath: string }>(
        `SELECT relpath FROM omniroute.artifact_deletion_queue
       WHERE publication_state='publishing' AND call_log_id IS NOT NULL AND store_scope=$1
       AND next_attempt_at<=statement_timestamp() ORDER BY next_attempt_at,created_at,relpath LIMIT $2`,
        [scope, Math.max(1, Math.min(8, limit))]
      )
    ).rows.map((row) => row.relpath)
  );
}
async function hasPrivateOwner(tx: PostgresTransaction, row: ArtifactRecoveryRow) {
  const result = await tx.query<{ allowed: boolean }>(
    `SELECT (NULLIF(c.api_key_id,'') IS NULL OR EXISTS
      (SELECT 1 FROM omniroute.api_keys k WHERE k.id=c.api_key_id AND k.no_log<>1)) AS allowed
     FROM omniroute.call_logs c WHERE c.id=$1 AND c.insertion_order=$2 AND c.artifact_relpath IS NULL
       AND c.detail_state='missing' AND c.detail_reason IN ('publication_pending','upload_unknown')`,
    [row.call_log_id, row.call_log_order]
  );
  return result.rows[0]?.allowed === true;
}
async function revoke(tx: PostgresTransaction, row: ArtifactRecoveryRow) {
  await tx.query(
    `UPDATE omniroute.artifact_deletion_queue SET publication_state='delete_pending',
     staged_payload=NULL,staged_bytes=0,next_attempt_at=statement_timestamp() WHERE relpath=$1`,
    [row.relpath]
  );
}
/** Acknowledged CAS authorizes exactly one attempt. An unknown COMMIT authorizes none. */
export async function claimArtifactRecoveryDispatch(
  database: PostgresDatabase,
  reference: string,
  owner: string,
  dispatch: string
): Promise<boolean> {
  return database.transaction(
    async (tx) => {
      await acquirePostgresLock(tx, "keyPolicy", "shared");
      await lockArtifactPublication(tx);
      const row = (await readArtifactPublication(tx, reference)) as ArtifactRecoveryRow | undefined;
      if (
        !row ||
        row.publication_owner !== owner ||
        row.publication_state !== "publishing" ||
        row.upload_state !== "not_sent"
      )
        return false;
      if (!(await hasPrivateOwner(tx, row))) {
        await revoke(tx, row);
        return false;
      }
      await tx.query(
        `UPDATE omniroute.artifact_deletion_queue SET upload_state='uncertain',dispatch_owner=$2,
       next_attempt_at=statement_timestamp()+interval '30 seconds' WHERE relpath=$1`,
        [reference, dispatch]
      );
      return true;
    },
    { operation: "artifact.claim" }
  );
}
export async function deferArtifactRecovery(
  database: PostgresDatabase,
  reference: string,
  dispatch?: string,
  notSent = false
): Promise<void> {
  await database.transaction(
    async (tx) => {
      await lockArtifactPublication(tx);
      await tx.query(
        `UPDATE omniroute.artifact_deletion_queue SET next_attempt_at=statement_timestamp()+interval '30 seconds',
       upload_state=CASE WHEN $3 AND dispatch_owner=$2::uuid THEN 'not_sent' ELSE upload_state END,
       dispatch_owner=CASE WHEN $3 AND dispatch_owner=$2::uuid THEN NULL ELSE dispatch_owner END
       WHERE relpath=$1 AND publication_state='publishing'`,
        [reference, dispatch ?? null, notSent]
      );
      await tx.query(
        `UPDATE omniroute.call_logs c SET detail_reason=CASE WHEN q.upload_state='uncertain' THEN 'upload_unknown' ELSE 'publication_pending' END
       FROM omniroute.artifact_deletion_queue q WHERE q.relpath=$1 AND q.call_log_id=c.id
        AND q.call_log_order=c.insertion_order AND q.publication_state='publishing' AND c.artifact_relpath IS NULL`,
        [reference]
      );
    },
    { operation: "artifact.defer" }
  );
}
/** Positive checksum/size evidence is supplied by the object owner, never inferred from 404. */
export async function attachArtifactRecovery(
  database: PostgresDatabase,
  reference: string,
  owner: string
): Promise<boolean> {
  return database.transaction(
    async (tx) => {
      await acquirePostgresLock(tx, "keyPolicy", "shared");
      await lockArtifactPublication(tx);
      const row = (await readArtifactPublication(tx, reference)) as ArtifactRecoveryRow | undefined;
      if (!row || row.publication_owner !== owner) return false;
      if (row.publication_state === "attached") return true;
      if (row.publication_state !== "publishing" || row.upload_state !== "uncertain") return false;
      if (!(await hasPrivateOwner(tx, row))) {
        await revoke(tx, row);
        return false;
      }
      const updated = await tx.query(
        `UPDATE omniroute.call_logs SET detail_state='ready',detail_reason=NULL,artifact_relpath=$3,
       artifact_size_bytes=$4,artifact_sha256=$5 WHERE id=$1 AND insertion_order=$2 AND artifact_relpath IS NULL`,
        [
          row.call_log_id,
          row.call_log_order,
          row.relpath,
          row.expected_size_bytes,
          row.expected_sha256,
        ]
      );
      if (updated.rowCount !== 1) {
        await revoke(tx, row);
        return false;
      }
      await tx.query(
        `UPDATE omniroute.artifact_deletion_queue SET publication_state='attached',upload_state='acknowledged',
       staged_payload=NULL,staged_bytes=0,dispatch_owner=NULL WHERE relpath=$1`,
        [reference]
      );
      return true;
    },
    { operation: "artifact.attach" }
  );
}
