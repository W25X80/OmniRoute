/** Migration 5 only: existing deletion ownership also journals pre-send publication. */
export const POSTGRES_ARTIFACT_PUBLICATION_SCHEMA_STATEMENTS = [
  `ALTER TABLE omniroute.artifact_deletion_queue
     ADD COLUMN publication_state text NOT NULL DEFAULT 'delete_pending'
       CHECK(publication_state IN ('publishing','attached','delete_pending')),
     ADD COLUMN upload_state text NOT NULL DEFAULT 'acknowledged'
       CHECK(upload_state IN ('uncertain','acknowledged','not_sent','legacy_unknown')),
     ADD COLUMN publication_owner uuid,
     ADD COLUMN store_scope text CHECK(store_scope IS NULL OR store_scope ~ '^[0-9a-f]{64}$'),
     ADD COLUMN expected_size_bytes bigint CHECK(expected_size_bytes BETWEEN 0 AND 8388608),
     ADD COLUMN expected_sha256 text CHECK(expected_sha256 IS NULL OR expected_sha256 ~ '^[0-9a-f]{64}$'),
     ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP`,
  `CREATE INDEX artifact_deletion_queue_due_idx
     ON omniroute.artifact_deletion_queue(next_attempt_at,created_at,relpath)`,
  `CREATE INDEX call_logs_artifact_relpath_idx ON omniroute.call_logs(artifact_relpath) WHERE artifact_relpath IS NOT NULL`,
  `CREATE INDEX call_logs_v1_legacy_artifact_relpath_idx ON omniroute.call_logs_v1_legacy(artifact_relpath) WHERE artifact_relpath IS NOT NULL`,
] as const;
