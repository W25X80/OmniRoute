/** Matches the request observation and conditional-cleanup high-water predicate. */
export const POSTGRES_SESSION_HISTORY_FENCE_SCHEMA_STATEMENTS = [
  `CREATE INDEX session_model_history_fence
   ON omniroute.session_model_history(session_id,combo_name,id DESC)`,
] as const;
