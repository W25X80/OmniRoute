/**
 * Keep imported/public timestamps verbatim; only the retention access path is typed.
 * Apply during the offline migration window: preflight, backfill and index creation
 * share the migration transaction, so invalid old data cannot leave a partial schema.
 */
export const POSTGRES_RETENTION_SCHEMA_STATEMENTS = [
  "SET LOCAL TIME ZONE 'UTC'",
  `DO $$ BEGIN
     IF EXISTS (SELECT 1 FROM omniroute.conversation_turn_nodes
       WHERE NOT pg_input_is_valid(last_seen_at, 'timestamp with time zone')
         OR lower(last_seen_at) ~ '(^|[[:space:]])(now|today|tomorrow|yesterday)([[:space:]]|$)') THEN
       RAISE EXCEPTION 'Conversation retention requires absolute timestamps; repair last_seen_at before migration'
         USING ERRCODE='22007';
     END IF;
   END $$`,
  `ALTER TABLE omniroute.conversation_turn_nodes
   ADD COLUMN last_seen_at_retention timestamptz`,
  // Rewrite once instead of maintaining every existing index for millions of UPDATEs.
  `ALTER TABLE omniroute.conversation_turn_nodes
   ALTER COLUMN last_seen_at_retention TYPE timestamptz USING last_seen_at::timestamptz`,
  // The default admits existing import column lists. The trigger always replaces it.
  `ALTER TABLE omniroute.conversation_turn_nodes
   ALTER COLUMN last_seen_at_retention SET NOT NULL,
   ALTER COLUMN last_seen_at_retention SET DEFAULT '-infinity'::timestamptz`,
  `CREATE FUNCTION omniroute.set_turn_node_retention_timestamp() RETURNS trigger
   LANGUAGE plpgsql SET timezone='UTC' AS $$ BEGIN
     IF lower(NEW.last_seen_at) ~ '(^|[[:space:]])(now|today|tomorrow|yesterday)([[:space:]]|$)' THEN
       RAISE EXCEPTION 'Conversation retention requires an absolute last_seen_at timestamp'
         USING ERRCODE='22007';
     END IF;
     NEW.last_seen_at_retention := NEW.last_seen_at::timestamptz;
     RETURN NEW;
   END $$`,
  `CREATE TRIGGER turn_node_retention_timestamp
   BEFORE INSERT OR UPDATE OF last_seen_at,last_seen_at_retention
   ON omniroute.conversation_turn_nodes FOR EACH ROW
   EXECUTE FUNCTION omniroute.set_turn_node_retention_timestamp()`,
  `CREATE INDEX conversation_turn_nodes_retention
   ON omniroute.conversation_turn_nodes(last_seen_at_retention,id)`,
] as const;
