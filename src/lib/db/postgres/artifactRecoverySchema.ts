/** Additive recovery intent; historical publication migrations remain immutable. */
export const POSTGRES_ARTIFACT_RECOVERY_SCHEMA_STATEMENTS = [
  `ALTER TABLE omniroute.call_logs ADD COLUMN detail_reason text`,
  `ALTER TABLE omniroute.artifact_deletion_queue
     ADD COLUMN call_log_id text,
     ADD COLUMN call_log_order bigint,
     ADD COLUMN staged_payload text,
     ADD COLUMN staged_bytes integer NOT NULL DEFAULT 0 CHECK(staged_bytes >= 0),
     ADD COLUMN dispatch_owner uuid,
     ADD CONSTRAINT artifact_staged_payload_shape CHECK(
       (staged_payload IS NULL AND staged_bytes=0) OR
       (staged_payload LIKE 'enc:v1:%' AND staged_bytes=octet_length(staged_payload)
        AND staged_bytes<=22369720 AND publication_state='publishing'
        AND call_log_id IS NOT NULL AND call_log_order IS NOT NULL))`,
  `CREATE UNIQUE INDEX artifact_recovery_call_owner_idx
     ON omniroute.artifact_deletion_queue(call_log_id,call_log_order)
     WHERE call_log_id IS NOT NULL AND publication_state IN ('publishing','attached')`,
  `CREATE INDEX artifact_recovery_staging_idx
     ON omniroute.artifact_deletion_queue(staged_bytes) WHERE staged_payload IS NOT NULL`,
  `CREATE FUNCTION omniroute.revoke_call_artifact_intent() RETURNS trigger LANGUAGE plpgsql AS $$
   BEGIN
     UPDATE omniroute.artifact_deletion_queue
       SET publication_state='delete_pending',staged_payload=NULL,staged_bytes=0,
           next_attempt_at=statement_timestamp()
       WHERE call_log_id=OLD.id AND call_log_order=OLD.insertion_order;
     RETURN OLD;
   END $$`,
  `CREATE TRIGGER revoke_call_artifact_intent AFTER DELETE ON omniroute.call_logs
     FOR EACH ROW EXECUTE FUNCTION omniroute.revoke_call_artifact_intent()`,
  `CREATE FUNCTION omniroute.revoke_private_artifact_intents() RETURNS trigger LANGUAGE plpgsql AS $$
   BEGIN
     IF TG_OP='DELETE' OR NEW.no_log=1 THEN
       UPDATE omniroute.artifact_deletion_queue q
         SET publication_state='delete_pending',staged_payload=NULL,staged_bytes=0,
             next_attempt_at=statement_timestamp()
         FROM omniroute.call_logs c
         WHERE c.api_key_id=OLD.id AND q.call_log_id=c.id
           AND q.call_log_order=c.insertion_order AND q.publication_state='publishing';
       UPDATE omniroute.call_logs c SET detail_state='none',detail_reason='privacy',
         request_summary=NULL,error_summary=NULL,has_content=NULL,
         has_request_body=0,has_response_body=0,has_pipeline_details=0
         WHERE c.api_key_id=OLD.id AND c.artifact_relpath IS NULL;
     END IF;
     RETURN OLD;
   END $$`,
  `CREATE TRIGGER revoke_private_artifact_intents AFTER UPDATE OF no_log OR DELETE
     ON omniroute.api_keys FOR EACH ROW EXECUTE FUNCTION omniroute.revoke_private_artifact_intents()`,
] as const;
