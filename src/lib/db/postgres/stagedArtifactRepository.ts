import type { PostgresDatabase, PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";

export interface StagedArtifactOwner {
  callId: string;
  callOrder: number;
  storageScope: string;
}

export interface StagedArtifactSnapshot extends StagedArtifactOwner {
  reference: string;
  owner: string;
  apiKeyId: string | null;
  sizeBytes: number;
  sha256: string;
  ciphertext: string;
}

type StagedRow = {
  relpath: string;
  publication_owner: string;
  api_key_id: string | null;
  expected_size_bytes: string;
  expected_sha256: string;
  staged_payload: string;
  upload_state: "not_sent" | "uncertain";
};

async function lockReaders(tx: PostgresTransaction): Promise<void> {
  await acquirePostgresLock(tx, "keyPolicy", "shared");
  await acquirePostgresLock(tx, "accounting", "shared");
}

// These predicates are shared by both reads. A missing principal is never a keyless call.
const ELIGIBLE = `q.call_log_id=c.id AND q.call_log_order=c.insertion_order
  AND q.publication_state='publishing' AND q.upload_state IN ('not_sent','uncertain')
  AND q.publication_owner IS NOT NULL AND q.staged_payload IS NOT NULL
  AND q.expected_size_bytes BETWEEN 1 AND 8388608
  AND q.staged_bytes BETWEEN 1 AND 22369720
  AND c.artifact_relpath IS NULL AND c.detail_state='missing'
  AND c.detail_reason IN ('publication_pending','upload_unknown')
  AND (NULLIF(c.api_key_id,'') IS NULL OR EXISTS
    (SELECT 1 FROM omniroute.api_keys k WHERE k.id=c.api_key_id AND k.no_log<>1))`;

/** Copy at most one bounded encrypted payload; no CPU work or external I/O under SQL locks. */
export async function readStagedArtifact(
  database: PostgresDatabase,
  selected: StagedArtifactOwner
): Promise<StagedArtifactSnapshot | null> {
  if (!Number.isSafeInteger(selected.callOrder) || selected.callOrder <= 0) return null;
  return database.transaction(
    async (tx) => {
      await lockReaders(tx);
      const { rows } = await tx.query<StagedRow>(
        `SELECT q.relpath,q.publication_owner,q.expected_size_bytes,q.expected_sha256,
        q.staged_payload,NULLIF(c.api_key_id,'') AS api_key_id
       FROM omniroute.call_logs c JOIN omniroute.artifact_deletion_queue q ON ${ELIGIBLE}
       WHERE c.id=$1 AND c.insertion_order=$2 AND q.store_scope=$3
       AND char_length(q.staged_payload)<=22369720 LIMIT 2 FOR SHARE OF c,q`,
        [selected.callId, selected.callOrder, selected.storageScope]
      );
      if (rows.length !== 1) return null;
      const row = rows[0];
      if (!/^[0-9a-f]{64}$/.test(row.expected_sha256)) return null;
      if (!row.relpath.endsWith(`_${row.expected_sha256}.json`)) return null;
      return {
        ...selected,
        reference: row.relpath,
        owner: row.publication_owner,
        apiKeyId: row.api_key_id,
        sizeBytes: Number(row.expected_size_bytes),
        sha256: row.expected_sha256,
        ciphertext: row.staged_payload,
      };
    },
    { operation: "artifact.staged.read" }
  );
}

/** The callback is a synchronous projection under fresh privacy and incarnation fences. */
export async function projectStagedArtifact<T>(
  database: PostgresDatabase,
  snapshot: StagedArtifactSnapshot,
  project: (uploadState: "not_sent" | "uncertain") => T
): Promise<T | null> {
  return database.transaction(
    async (tx) => {
      await lockReaders(tx);
      const { rows } = await tx.query<Pick<StagedRow, "upload_state">>(
        `SELECT q.upload_state FROM omniroute.call_logs c
       JOIN omniroute.artifact_deletion_queue q ON ${ELIGIBLE}
       WHERE c.id=$1 AND c.insertion_order=$2 AND q.store_scope=$3
       AND q.relpath=$4 AND q.publication_owner=$5
       AND q.expected_size_bytes=$6 AND q.expected_sha256=$7
       AND NULLIF(c.api_key_id,'') IS NOT DISTINCT FROM $8::text FOR SHARE OF c,q`,
        [
          snapshot.callId,
          snapshot.callOrder,
          snapshot.storageScope,
          snapshot.reference,
          snapshot.owner,
          snapshot.sizeBytes,
          snapshot.sha256,
          snapshot.apiKeyId,
        ]
      );
      return rows.length === 1 ? project(rows[0].upload_state) : null;
    },
    { operation: "artifact.staged.project" }
  );
}
