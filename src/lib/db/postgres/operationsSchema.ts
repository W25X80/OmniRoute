export const POSTGRES_OPERATIONS_SCHEMA_STATEMENTS = [
  `CREATE TABLE omniroute.middleware_hooks (
    name text PRIMARY KEY, description text NOT NULL DEFAULT '', priority integer NOT NULL DEFAULT 200,
    scope_type text NOT NULL DEFAULT 'global' CHECK (scope_type IN ('global', 'combo')),
    combo_id text, enabled integer NOT NULL DEFAULT 1, code text NOT NULL DEFAULT '',
    created_at text NOT NULL, updated_at text NOT NULL, run_count bigint NOT NULL DEFAULT 0,
    last_error text
  )`,
  `CREATE INDEX middleware_hooks_scope ON omniroute.middleware_hooks(scope_type, combo_id)`,
  `CREATE INDEX middleware_hooks_enabled ON omniroute.middleware_hooks(enabled, priority)`,
  `CREATE TABLE omniroute.middleware_logs (
    id text PRIMARY KEY, hook_name text NOT NULL REFERENCES omniroute.middleware_hooks(name) ON DELETE CASCADE,
    request_id text NOT NULL, duration_ms integer NOT NULL DEFAULT 0,
    mutated integer NOT NULL DEFAULT 0, skipped integer NOT NULL DEFAULT 0, error text,
    timestamp text NOT NULL
  )`,
  `CREATE INDEX middleware_logs_hook ON omniroute.middleware_logs(hook_name, timestamp)`,
  `CREATE INDEX middleware_logs_request ON omniroute.middleware_logs(request_id, timestamp)`,
  `CREATE TABLE omniroute.artifact_deletion_queue (
    relpath text PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
  )`,
  `CREATE INDEX artifact_deletion_queue_created ON omniroute.artifact_deletion_queue(created_at, relpath)`,
] as const;
