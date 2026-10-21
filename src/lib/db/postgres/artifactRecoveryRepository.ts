import { randomUUID } from "node:crypto";
import { ArtifactStorageError, type PreparedArtifact } from "@/domain/persistence/artifactStore";
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
type ArtifactRecoveryOwner = Pick<
  ArtifactRecoveryRow,
  | "relpath"
  | "publication_owner"
  | "dispatch_owner"
  | "call_log_id"
  | "call_log_order"
  | "store_scope"
  | "expected_size_bytes"
  | "expected_sha256"
>;

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
/** Transfer staging, never replay an uncertain PUT or discard its cleanup liability. */
export async function replaceUncertainArtifactRecovery(
  database: PostgresDatabase,
  expected: ArtifactRecoveryOwner,
  isLive: () => boolean
): Promise<boolean> {
  return database.transaction(
    async (tx) => {
      await acquirePostgresLock(tx, "keyPolicy", "shared");
      // Attachment locks queue before call; exclude it before taking the trigger's
      // call-before-queue order. No storage I/O runs in this short transfer.
      await lockArtifactPublication(tx);
      await tx.query(
        "SELECT 1 FROM omniroute.call_logs WHERE id=$1 AND insertion_order=$2 FOR UPDATE",
        [expected.call_log_id, expected.call_log_order]
      );
      const row = (await readArtifactPublication(tx, expected.relpath)) as
        ArtifactRecoveryRow | undefined;
      if (
        !row ||
        row.publication_state !== "publishing" ||
        row.upload_state !== "uncertain" ||
        row.publication_owner !== expected.publication_owner ||
        row.dispatch_owner !== expected.dispatch_owner ||
        row.call_log_id !== expected.call_log_id ||
        row.call_log_order !== expected.call_log_order ||
        row.store_scope !== expected.store_scope ||
        row.expected_size_bytes !== expected.expected_size_bytes ||
        row.expected_sha256 !== expected.expected_sha256
      )
        return false;
      if (
        !row.staged_payload ||
        !/^[0-9a-f]{64}$/.test(row.expected_sha256) ||
        !/^[0-9a-f]{64}$/.test(row.store_scope) ||
        !row.relpath.endsWith(`_${row.expected_sha256}.json`)
      )
        throw new ArtifactStorageError("integrity");
      if (!(await hasPrivateOwner(tx, row))) {
        await revoke(tx, row);
        return false;
      }
      if (isLive()) return false;
      const retained = await tx.query<{ relpath: string }>(
        `SELECT relpath FROM omniroute.artifact_deletion_queue
       WHERE call_log_order=$1 AND call_log_id COLLATE "C"=$2 COLLATE "C" LIMIT 2`,
        [row.call_log_order, row.call_log_id]
      );
      // An acknowledged GET does not free credit: DELETE may still be failing.
      // Only terminal cleanup removing its row permits a later replacement.
      if (retained.rows.length !== 1) {
        await tx.query(
          `UPDATE omniroute.artifact_deletion_queue SET
         next_attempt_at=GREATEST(next_attempt_at,statement_timestamp()+interval '5 minutes')
         WHERE relpath=$1 AND publication_state='publishing' AND upload_state='uncertain'`,
          [row.relpath]
        );
        return false;
      }
      if (isLive()) return false;
      const replacement = `s3/${randomUUID()}_${row.expected_sha256}.json`;
      const inserted = await tx.query<{ relpath: string }>(
        `WITH source AS MATERIALIZED (
         SELECT * FROM omniroute.artifact_deletion_queue
         WHERE relpath=$1 AND publication_owner=$4::uuid
           AND dispatch_owner IS NOT DISTINCT FROM $5::uuid
           AND call_log_id=$6 AND call_log_order=$7 AND store_scope=$8
           AND expected_size_bytes=$9 AND expected_sha256=$10
           AND publication_state='publishing' AND upload_state='uncertain'
           AND staged_payload IS NOT NULL AND next_attempt_at<=statement_timestamp()
       ), retired AS (
         UPDATE omniroute.artifact_deletion_queue q
         SET publication_state='delete_pending',staged_payload=NULL,staged_bytes=0,
             next_attempt_at=statement_timestamp()+interval '5 minutes'
         FROM source s WHERE q.relpath=s.relpath RETURNING q.relpath
       )
       INSERT INTO omniroute.artifact_deletion_queue
         (relpath,publication_state,upload_state,publication_owner,store_scope,
          expected_size_bytes,expected_sha256,call_log_id,call_log_order,
          staged_payload,staged_bytes,dispatch_owner,next_attempt_at)
       SELECT $2,'publishing','not_sent',$3::uuid,s.store_scope,
         s.expected_size_bytes,s.expected_sha256,s.call_log_id,s.call_log_order,
         s.staged_payload,s.staged_bytes,NULL,statement_timestamp()+interval '30 seconds'
       FROM source s JOIN retired r ON r.relpath=s.relpath RETURNING relpath`,
        [
          row.relpath,
          replacement,
          randomUUID(),
          row.publication_owner,
          row.dispatch_owner,
          row.call_log_id,
          row.call_log_order,
          row.store_scope,
          row.expected_size_bytes,
          row.expected_sha256,
        ]
      );
      if (inserted.rowCount !== 1) return false;
      const updated = await tx.query(
        `UPDATE omniroute.call_logs SET detail_reason='publication_pending'
       WHERE id=$1 AND insertion_order=$2 AND artifact_relpath IS NULL`,
        [row.call_log_id, row.call_log_order]
      );
      if (updated.rowCount !== 1) throw new Error("Artifact replacement owner changed");
      return true;
    },
    { operation: "artifact.replace" }
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
      await lockArtifactPublication(tx, "shared");
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
      await lockArtifactPublication(tx, "shared");
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
      await lockArtifactPublication(tx, "shared");
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
