/** Additive history protocol; migrations already deployed remain unchanged. */
export const POSTGRES_CONVERSATION_CONCURRENCY_STATEMENTS = [
  "ALTER TABLE omniroute.agentic_conversations ADD COLUMN history_revision bigint NOT NULL DEFAULT 0 CHECK(history_revision>=0)",
  "CREATE INDEX call_logs_session_timestamp_idx ON omniroute.call_logs(session_tag,timestamp DESC)",
  "CREATE INDEX conversations_fingerprint_recent_idx ON omniroute.agentic_conversations(fingerprint_hash,last_seen_at DESC,insertion_order)",
] as const;
